# INTRODUCTION

A start up called **Sparkify**, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conlusion that the best tool to achieve this is Apache Airflow.

# PROJECT DESCRIPTION

As a data engineer, I am tasked to create a high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.

# DATASETS

The source of data resides in S3 and needs to be processed in **Sparkify**'s data warehouse in AWS Redshift.

The source of datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

# BUILDING THE OPERATORS

To complete this project, I will build four different operators that will stage the data, transform the data, and run checks on data quality.

* *Stage operator*: is expected to be able to load any JSON formatted files from S3 to AWS Redshift.

* *Fact and dimension operators*: with dimension and fact operators, I can utilize the SQL helper class to run data transformation.

* *Data quality operator*: is used to run checks on the data itself.

# FOLDERS

* `Dags folder` has all the imports, task templates, and task dependencies in place
* `Plugins folder` contains two sub-folders:
    * `Operator folder` has operator templates
    * `Helper folder` has helper classes for the SQL transformation
