import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format,monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']="AKIAXLLNOI7YB6ECEDKY"
os.environ['AWS_SECRET_ACCESS_KEY']="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"

print(os.environ['AWS_ACCESS_KEY_ID'])
print(os.environ['AWS_SECRET_ACCESS_KEY'])


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Processing song_data from S3 
        Extracting them to create data to dimensional tables
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    #Creating a temporary SQL table
    df.createOrReplaceTempView("songdata_table")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
                                SELECT DISTINCT song_id, title, artist_id, year, duration
                                FROM songdata_table
                                WHERE song_id IS NOT NULL
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs_table/")

    # extract columns to create artists table
    artists_table = spark.sql("""
                                  SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude,
                                  artist_longitude
                                  FROM songdata_table
                                  WHERE artist_id IS NOT NULL
                              """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists_table/")


def process_log_data(spark, input_data, output_data):
    """
        Processing log_data from S3 
        Extracting them to create data to dimensional tables
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
     #Create a temporary SQL file
    df.createOrReplaceTempView('logdata_table')
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table    
    users_table = spark.sql("""
                        SELECT DISTINCT userId, firstName, lastName, gender, level
                        FROM logdata_table
                        WHERE userId IS NOT NULL
                   """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users_table/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df = df.withColumn('datetime', from_unixtime('start_time'))
    
    #create a temporary SQL table for time table
    df.createOrReplaceTempView('time_table')
 
    # extract columns to create time table
    time_table = spark.sql("""
                                SELECT DISTINCT datetime AS start_time,
                                                hour(datetime) AS hour, day(datetime) AS day, weekofyear(datetime) AS week,
                                                month(datetime) AS month, year(datetime) AS year, dayofweek(datetime) AS weekday
                                FROM time_table
                            """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time_table/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                    SELECT DISTINCT monotonically_increasing_id() as songplay_id,
                                                    to_timestamp(log.ts/1000) AS start_time,
                                                    month(to_timestamp(log.ts/1000)) As month,
                                                    year(to_timestamp(log.ts/1000)) AS year,
                                                    log.userId AS user_id,
                                                    log.level,
                                                    song.song_id,
                                                    song.artist_id,
                                                    log.sessionId AS session_id,
                                                    log.location,
                                                    log.userAgent AS user_agent
                                    FROM logdata_table AS log
                                    JOIN songdata_table AS song on log.artist = song.artist_name
                                                                AND log.song = song.title
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays_table/")


def main():
    """
    Spark: Spark session
    input_data: location of data that will be processed
    output_data: location of the processed data
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
