import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR, 
        author VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        iteminsession INT,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        session_id INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INT)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INT, 
        song_id VARCHAR, 
        title VARCHAR, 
        duration FLOAT, 
        year INT,
        artist_id VARCHAR,
        artist_name VARCHAR, 
        artist_latitude FLOAT, 
        artist_longtitude FLOAT,
        artist_location VARCHAR)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INT IDENTITY(0, 1) PRIMARY KEY, 
        start_time TIMESTAMP NOT NULL, 
        user_id int NOT NULL, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int,
        location varchar, 
        user_agent varchar)

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar,
        year int,
        duration float)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude float,
        longtitude float)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int)
""")

# STAGING TABLES
staging_events_copy = ("""copy staging_events 
                            from {}
                            credentials 'aws_iam_role={}'
                            json {}
                            compupdate off
                            region 'us-west-2';
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))
staging_songs_copy = ("""copy staging_songs 
                            from {} 
                            credentials 'aws_iam_role={}' 
                            JSON 'auto'
                            compupdate off 
                            region 'us-west-2';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, 
                    song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
        se.user_id, se.level, s.song_id, s.artist_id, 
        se.session_id, se.location, se.user_agent
FROM staging_events as se
JOIN staging_songs as s ON se.song = s.song_id
                        AND se.artist = s.artist_name
WHERE set.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longtitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longtitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time, 
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(dayofweek from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
