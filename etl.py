import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    # read the song data file into a dataframe
    songs_df = spark.read.json(path='./data/interim/song_data', multiLine=True)
    print(songs_df.printSchema())

    # extract columns from the songs dataframe
    songs_df = songs_df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs dataframe to parquet files partitioned by year and artist
    songs_df.write.parquet('./data/processed/star_schema')

    # # extract columns to create artists table
    # artists_table = 
    
    # # write artists table to parquet files
    # artists_table


def process_log_data(spark, input_data, output_data):
    pass
    # # get filepath to log data file
    # log_data =

    # # read log data file
    # df = 
    
    # # filter by actions for song plays
    # df = 

    # # extract columns for users table    
    # artists_table = 
    
    # # write users table to parquet files
    # artists_table

    # # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    # # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # # extract columns to create time table
    # time_table = 
    
    # # write time table to parquet files partitioned by year and month
    # time_table

    # # read in song data to use for songplays table
    # song_df = 

    # # extract columns from joined song and log datasets to create songplays table 
    # songplays_table = 

    # # write songplays table to parquet files partitioned by year and month
    # songplays_table


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    input_data = "./data/logs"
    output_data = "./data/star-tables"
    # input_data = "s3a://udacity-dend/"
    # output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
