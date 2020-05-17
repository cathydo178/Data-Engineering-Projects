# INTRODUCTION

A start up called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening.

They would like to create a Postgres database with tables designed to optimize queries on song play analysis.

# PROJECT DESCRIPTION

As a data engineer, I will design a Postgres database and build an ETL pipeline using Python. I will need to define fact and dimension for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

# DATABASE SCHEMA DESIGN

I choose star schema design for this project because it's simple and straightforward. This will help the analytic team get the most information and do their analysis in the most efficient way.

In my star schema, there will be fact table and dimensional table:

**Fact table**: songplays. \
**Dimension tables**: users, artists, songs, time.

# ETL PIPELINE

My ETL pipline is structured to read, process files from song_data and log_data, and load them accordingly to talbles.

# PYTHON SCRIPT

In order to successfuly create the tables and run the ETL pipeline, the following steps have to be performed:

1. Run create_tables.py to create the database and tables. \
`create_tables.py`

2. Run test.ipynb to confirm the creation of the tables with the correct columns. \
`test.ipynb`


# FILES

This project includes six files:

* `test.ipynb` displays the first few rows of each table to let I check my database.
* `create_tables.py` drops and creates my tables. I will need to make sure to run this file to reset my table before each time I run my ETL scripts.
* `etl.ipynb` reads and processes a single file from `song_data` and `log_data` and loads the data into my tables.
* `sql_queries.py` contains all my sql queries, and is imported into the last three files above.
* `README.md` provides discussion on my project.
