import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: This function initialises spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Transform raw song data from S3 into analytics tables on S3
    
    This function reads in song data in JSON format from S3; defines the schema
    of songs and artists analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    S3.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read song data in from
        output_data: an S3 bucket to write analytics tables to
    """
        
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView('songs')

    # extract columns to create songs table
    songs_table = spark.sql("""
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM songs
                        WHERE song_id IS NOT NULL
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+"songs/")

    # extract columns to create artists table
    artists_table = spark.sql("""
                    SELECT  artist_id, 
                            artist_name as name,
                            artist_location as location,
                            artist_latitude as latitude,
                            artist_longitude as longitude
                        FROM songs
                        WHERE artist_id IS NOT NULL
    """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"artists/")


def process_log_data(spark, input_data, output_data):
    """
    Transform raw log data from S3 into analytics tables on S3
    
    This function reads in log data in JSON format from S3; defines the schema
    of songplays, users, and time analytics tables; processes the raw data into
    those tables; and then writes the tables into partitioned parquet files on
    S3.
    
    Args:
        spark: a Spark session
        input_data: an S3 bucket to read log data in from
        output_data: an S3 bucket to write analytics tables to
    """
    # get filepath to log data file
    log_data = input_data+"log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    df.createOrReplaceTempView('log_data')

    # extract columns for users table    
    users_table = spark.sql("""
                        SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level
                        FROM log_data
                        WHERE userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).replace(microsecond=0),
        TimestampType())
    df = df.withColumn('start_time',get_timestamp('ts'))
    df.createOrReplaceTempView('time')
    
    # extract columns to create time table
    time_table = spark.sql("""
                        SELECT start_time,
                                hour(start_time) as hour,
                                dayofmonth(start_time) as day,
                                weekofyear(start_time) as week,
                                month(start_time) as month,
                                year(start_time) as year,
                                dayofweek(start_time) as weekday
                        FROM time
                        WHERE start_time IS NOT NULL
                            """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+'timetable/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs/*/*/*')
    df_artists = spark.read.parquet(output_data + 'artists/*')
    
    song_df.createOrReplaceTempView("songs_table")
    df_artists.createOrReplaceTempView("artists_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                             SELECT monotonically_increasing_id() as songplay_id,
                                    start_time, 
                                    month(start_time) as month,
                                    year(start_time) as year,
                                    se.userId as user_id, 
                                    se.level,
                                    ss.song_id, 
                                    at.artist_id,
                                    se.sessionId as session_id, 
                                    se.location, 
                                    se.userAgent as user_agent
                            FROM time se
                            JOIN songs_table ss on (ss.title = se.song)   
                            JOIN artists_table as at on (se.artist == at.name)
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data+"songsplay/")


def main():
    """
    Run ETL pipeline
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-deng-wande/"
    output_data = "s3a://udacity-deng-wande/outputs/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
