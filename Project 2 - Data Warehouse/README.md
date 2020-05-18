# INTRODUCTION

A music streaming startup **Sparkify** has grown their user base and song database. They want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

# PROJECT DESCRIPTION

In this project, I will build an ETL pipeline for a database hosted on Redshift. I will load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these tables.

# DATABASE SCHEMA DESIGN AND ETL PIPELINE

I decide to use star schema for this project because it's simple and straightforward. This will help the analytic team get the most information and do their analysis in the most efficient way.

In my star schema, there will be fact table and dimension table:

**Fact table**: songplays \
**Dimension tables**: users, artists, songs, time

In order to boost the efficiency and the performance of my ETL operations, I create 2 staging tables: *Staging_events, and Staging_songs*. These are 2 temporary tables that are used to stage the data just before loading them into my target tables.

# PYTHON SCRIPT

In order to successfully create the tables and run the ETL pipeline, the following steps have to be performed:

1. Run create_tables.py to connect to the databases and creates the fact and dimension tables.

`python create_tables.py`

2. Insert data into tables by running etl.py after running create_tables.py.

`python etl.py`

# FILES

This project includes four files:

- `create_tables.py`: where I create my fact and dimension tables for the star schema in Redshift.
- `etl.py`: where I'll load data from S3 into staging tables on Redshift and then process that data into my analytics tables on Redshift.
- `sql_queries.py`: where I'll define my SQL statement, which will be imported into the two other files above.
- `README.md`: where I'll provide discussion on my process and decisions for this ETL pipeline.
