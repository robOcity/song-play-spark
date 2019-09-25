import configparser
from datetime import datetime
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    #TODO clean-up
    print(f'\ncreate_spark_session() --> {type(spark)}\n')
    return spark

def rmdir(target):
    try:
        shutil.rmtree(target)
    except OSError as e:
        print(f'Error: {e.filename} - {e.strerror}')

def mkdir(target):
    try:
        os.mkdir(target)
    except OSError as e:
        print(f'Error: {e.filename} - {e.strerror}')

def inspect_df(title, df):
    message = (
        f'\n{80 * "-"}\n'\
        f'{title.upper()}: {df.toPandas().info()}\n{df.toPandas().head()}'\
        f'\n{80 * "-"}\n'
    )
    print(message)

def from_disk(session, schema, path, depth=0):
    
    # create path using wildcard characters to read directory trees to given depth
    wild_card_path = path + ''.join(['/*'for _ in range(depth)]) + '.json'
    
    return session.read.json(
        path=wild_card_path, 
        schema=schema, 
        multiLine=True,
        encoding='UTF-8',
        mode='DROPMALFORMED')

def to_disk(df, path, mode='overwrite'):
    df.write.mode(mode).parquet(path)

def inspect_df(title, df):
    print(f'{title.upper()}:\n{df.show(5)}')

def build_song_schema():
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
    return song_schema

def process_song_data(spark, input_data, output_data):

    song_schema = build_song_schema()

    # read the song data file into a dataframe
    songs_df = from_disk(spark, song_schema, input_data, depth=4)
    inspect_df('songs_df', songs_df)

    # extract columns from the songs dataframe
    songs_table_df = songs_df.select([
        'song_id', 
        'title', 
        'artist_id',
        'year', 
        'duration', 
        ])
    
    # write songs dataframe to parquet files partitioned by year and artist
    inspect_df('songs_table_df', songs_table_df)
    to_disk(songs_table_df, output_data + '/dim_song')

    # # extract columns to create artists table
    artists_table_df = songs_df.select([
        'artist_id', 
        'artist_name', 
        'artist_location', 
        'artist_latitude', 
        'artist_longitude'])
    
    # write artists table to parquet files
    inspect_df('artists_table_df', artists_table_df)
    to_disk(artists_table_df, output_data + '/dim_artist')

def build_event_schema():
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
    return event_schema

def process_log_data(spark, input_data, output_data):
    
    # specify schema for dataframe
    event_schema = build_event_schema()
    
    # read log data file
    events_df = from_disk(spark, event_schema, input_data, depth=3)
    
    # filter by actions for song plays
    events_df = events_df.filter(events_df.page == 'NextSong')
    
    # create a column containing a datetime value by converting 
    # epoch time in milliseconds stored as strings
    print(f'Events Columns: {events_df.columns}')
    print(events_df.printSchema())
    to_timestamp = F.udf(lambda s: datetime.fromtimestamp((int(s)/1000)), T.TimestampType())
    events_df = events_df.withColumn('start_time', to_timestamp(events_df.ts))
    inspect_df('events_df', events_df)
    
    # apply consistent naming scheme retaining only these columns
    events_df = events_df.selectExpr([
        'firstName as first_name',
        'lastName as last_name',
        'userId as user_id', 
        'song as title',
        'gender as gender',
        'level as level',
        'start_time as start_time'])

    # extract columns for users table 
    users_table_df = events_df.select([
        'user_id', 
        'first_name', 
        'last_name', 
        'gender', 
        'level'])
    
    # filter out rows with empty user_ids
    users_table_df = users_table_df.filter(users_table_df.user_id != '')

    # write users table to parquet files
    inspect_df('users_table_df', users_table_df)
    to_disk(users_table_df, output_data + '/dim_user')

    # extract columns to create time table
    print(f'events_df.columns={events_df.columns}')
    time_table_df = events_df.select(['start_time'])
    time_table_df = time_table_df.withColumn('hour', F.hour('start_time'))
    time_table_df = time_table_df.withColumn('day', F.dayofmonth('start_time'))
    time_table_df = time_table_df.withColumn('week', F.weekofyear('start_time'))
    time_table_df = time_table_df.withColumn('month', F.month('start_time'))
    time_table_df = time_table_df.withColumn('year', F.year('start_time'))
    time_table_df = time_table_df.withColumn('weekday_num', F.dayofweek('start_time'))
    time_table_df = time_table_df.withColumn('weekday_str', F.date_format('start_time', 'EEE'))
    
    # TODO clean-up
    inspect_df('time_table_df 1', time_table_df)
    print(f'time_table_df.count()={time_table_df.count()}')
    
    # write time table to parquet files partitioned by year and month
    time_table_df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + '/dim_time')
    
    # TODO clean-up
    inspect_df('time_table_df 2', time_table_df)
    print(f'time_table_df.count()={time_table_df.count()}')

    # read in song data to use for songplays table
    # song_df = events_df.select([''])

    # extract columns from joined song and log datasets to create songplays table 

    # write songplays table to parquet files partitioned by year and month
    # songplays_table

def main(config_data, song_data, log_data, output_data):
    config = configparser.ConfigParser()
    config.read(config_data)
    print(type(config), config)

    # get
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    # processing 
    spark = create_spark_session()
    process_song_data(spark, song_data, output_data)    
    process_log_data(spark, log_data, output_data)
    
if __name__ == "__main__":

    # data paths
    config_data = "https://dend-util.s3-us-west-2.amazonaws.com/config/dl.cfg"
    song_data = "s3a://udacity-dend/song_data"
    log_data = "s3a://udacity-dend/log_data"
    output_data = "s3://song-play-spark"

    main(config_data, song_data, log_data, output_data)

