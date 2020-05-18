# INTRODUCTION

A music streaming startup, **Sparkify**, has grown their user database even more and want to move their data warehouse to a datalake. Their data resides in S3, in a directory of *JSON* logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data into S3 as a set of dimensionals tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# PROJECT DESCRIPTION

I will build an ETL pippline for a datalake hosted on S3. To complete this project, I will load data from S3, process the data into analytics tables using Spark, and load them back into S3.

# DATABASE SCHEMA AND ETL PIPELINE

Star schema will be built in order to help the analytic team get the most information and do their analysis in the most efficient way.

**Fact table**: songplays \
**Dimension tables**: users, artists, songs, time

# PYTHON SCRIPT

In order to successfuly create the tables and run the ETL pipeline, this script is run:

`python etl.py`

# FILES

This project includes 3 files:

* `etl.py` reads data from S3, processes that data using Spark, and writes them back to S3
* `dl.cfg` contains my AWS credentials
* `README.md` provides discussion on my process and decisions
