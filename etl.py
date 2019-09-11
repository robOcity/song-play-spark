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

def from_disk(session, schema, path):
    return session.read.json(
        path=path, 
        schema=schema, 
        multiLine=True)

def to_disk(df, path, mode='overwrite'):
    df.write.mode(mode).parquet(path)

def process_song_data(spark, input_data, output_data):

    # specify schema for dataframe
    song_schema = T.StructType([
        T.StructField('song_id', T.StringType()),
        T.StructField('num_songs', T.IntegerType()),
        T.StructField('title', T.StringType()),
        T.StructField('artist_name', T.StringType()),
        T.StructField('artist_latitude', T.DoubleType()),
        T.StructField('year', T.IntegerType()),
        T.StructField('duration', T.DoubleType()),
        T.StructField('artist_id', T.StringType()),
        T.StructField('artist_longitude', T.DoubleType()),
        T.StructField('artist_location', T.StringType())
    ]) 

    # read the song data file into a dataframe
    songs_df = from_disk(spark, song_schema, './data/interim/song_data')

    # extract columns from the songs dataframe
    songs_table_df = songs_df.select([
        'song_id', 
        'title', 
        'year', 
        'duration', 
        'artist_id'
        ])
    
    # write songs dataframe to parquet files partitioned by year and artist
    #songs_table_df.write.mode('overwrite').parquet('./data/processed/star_schema/dim_song')
    to_disk(songs_table_df, './data/processed/star_schema/dim_song')

    # # extract columns to create artists table
    artists_table_df = songs_df.select([
        'artist_id', 
        'artist_name', 
        'artist_location', 
        'artist_latitude', 
        'artist_longitude'])
    
    # write artists table to parquet files
    #artists_table_df.write.mode('overwrite').parquet('./data/processed/star_schema/dim_artist')
    to_disk(artists_table_df, './data/processed/star_schema/dim_artist')

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
    config.read('.env/dl.cfg')
    print(type(config), config)

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
