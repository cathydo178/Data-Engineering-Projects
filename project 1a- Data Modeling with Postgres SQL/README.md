#Introduction

A start up called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening. 

They would like to create a Postgres database with tables designed to optimize queries on song play analysis. 

#Project description

As a data engineer, I will design a Postgres database and build an ETL pipeline using Python. I will need to define fact and dimension for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

#Database schema design

I choose star schema design for this project because it's simple and straightforward. This will help the analytic team get the most information and do their analysis in the most efficient way. 

In my star schema, there will be fact table and dimensional table:

**Fact table**: songplays
**Dimension tables**: users, artists, songs, time

#ETL pipeline

My ETL pipline is structured to read, process files from song_data and log_data, and load them accordingly to talbles. 




