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
        'artist_id',
        'year', 
        'duration', 
        ])
    
    # write songs dataframe to parquet files partitioned by year and artist
    to_disk(songs_table_df, './data/processed/star_schema/dim_song')

    # # extract columns to create artists table
    artists_table_df = songs_df.select([
        'artist_id', 
        'artist_name', 
        'artist_location', 
        'artist_latitude', 
        'artist_longitude'])
    
    # write artists table to parquet files
    to_disk(artists_table_df, './data/processed/star_schema/dim_artist')

def process_log_data(spark, input_data, output_data):
    
    # specify schema for dataframe
    event_schema = T.StructType([
        T.StructField('artist', T.StringType()),
        T.StructField('auth', T.StringType()),
        T.StructField('firstName', T.StringType()),
        T.StructField('gender', T.StringType()),
        T.StructField('itemInSession', T.IntegerType()),
        T.StructField('lastName', T.StringType()),
        T.StructField('length', T.DoubleType()),
        T.StructField('level', T.StringType()),
        T.StructField('location', T.StringType()),
        T.StructField('method', T.StringType()),
        T.StructField('page', T.StringType()),
        T.StructField('registration', T.StringType()),
        T.StructField('sessionId', T.IntegerType()),
        T.StructField('song', T.StringType()),
        T.StructField('status', T.IntegerType()),
        T.StructField('ts', T.StringType()),   # convert to timestamp after import
        T.StructField('userAgent', T.StringType()),
        T.StructField('userId', T.StringType())
    ])

    # read log data file
    events_df = from_disk(spark, event_schema, './data/interim/log_data')
    # TODO clean up print statements
    print('1', events_df.printSchema())
    print('Before Where:', events_df.toPandas().shape)
    
    # filter by actions for song plays
    events_df = events_df.where(events_df.page == 'NextSong')
    print('After Where:', events_df.toPandas().shape)

    # create a column containing a datetime value by converting 
    # epoch time in milliseconds stored as strings
    events_df = events_df.withColumn('start_time', F.to_timestamp('ts', 'S'))
    print('2', events_df.printSchema())

    # apply consistent naming scheme
    events_df = events_df.selectExpr([
        'firstName as first_name',
        'lastName as last_name',
        'userId as user_id', 
        'song as title',
        'gender as gender',
        'level as level',
        'start_time as start_time'])
    
    print('3', events_df.printSchema())

    # extract columns for users table    
    users_table_df = events_df.select([
        'user_id', 
        'first_name', 
        'last_name', 
        'gender', 
        'level'])

    # write users table to parquet files
    to_disk(users_table_df, './data/processed/star_schema/dim_user')

    # extract columns to create time table
    time_table_df = events_df.select(['start_time'])
    time_table_df = time_table_df.withColumn('hour', F.hour('start_time'))
    time_table_df = time_table_df.withColumn('day', F.dayofmonth('start_time'))
    time_table_df = time_table_df.withColumn('week', F.weekofyear('start_time'))
    time_table_df = time_table_df.withColumn('month', F.month('start_time'))
    time_table_df = time_table_df.withColumn('year', F.year('start_time'))
    time_table_df = time_table_df.withColumn('weekday_num', F.dayofweek('start_time'))
    time_table_df = time_table_df.withColumn('weekday_str', F.date_format('start_time', 'EEE'))
    
    # write time table to parquet files partitioned by year and month
    # TODO refactor into function
    STAR_SCHEMA_PATH = './data/processed/star_schema/'
    #curr_dir = os.path.dirname(__full__)
    time_table_df = (time_table_df.write.
        partitionBy('year', 'month').
        parquet(os.path.join(STAR_SCHEMA_PATH, 'dim_time')))

    # read in song data to use for songplays table
    # song_df = events_df.select([''])

    # extract columns from joined song and log datasets to create songplays table 
    # songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    # songplays_table


def main():
    config = configparser.ConfigParser()
    config.read('.env/dl.cfg')
    print(type(config), config)

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    #TODO fix
    input_data = "./data/logs"
    output_data = "./data/star-tables"
    # input_data = "s3a://udacity-dend/"
    # output_data = ""
    
    # TODO uncomments once log data processing is working
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
