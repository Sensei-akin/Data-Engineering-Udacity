## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Schema

The schema used was designed to comply with the 3NF 

each table 
**users** - contains details of the users (user_id, first_name, last_name, gender, level)
**artist** - contains details of the artists (artist_id, name, location, latitude, longitude)
**songs** - contains all the songs in the db (song_id, title, artist_id, year, duration)
**time** - contains all timestamps of the records in songplays ()
**songsplays** - contains the records associated with song plays and serves as a fact table

## How to Run
Python3 is the recommended environment required to run this.

There are 2 python scripts:

- create_tables.py : This script drops existing tables and creates new ones.
- etl.py : This script uses data in ./data/song_data and ./data/log_data, processes it, and inserts the processed data into database.

To start you first run the create_tables.py : **python create_tables.py**
Secondly you run **python etl.py**

The project also contains the following files:

etl.ipynb: Jupyter Notebook for interactively develop and run python code to be used in etl.py.
test.ipynb: Jupyter Notebook for interactively run test SQL queries against used DB.