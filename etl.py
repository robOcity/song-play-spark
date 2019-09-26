import configparser
from datetime import datetime
import os
import shutil
import sys
import argparse
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

def from_disk(session, schema, path, depth=0, extension=None):
    
    depth = depth if depth > 0 else 1
    wild_card_path = path + '/'.join(['*'for _ in range(depth)]) + '.' + extension
    
    df = None
    if extension == 'json':
        df = session.read.json(
            path=wild_card_path, 
            schema=schema, 
            multiLine=True,
            encoding='UTF-8',
            mode='DROPMALFORMED')
    elif extension == 'parquet':
        df = session.read.parquet(wild_card_path)
    else:
        print(f'ERROR: {extension} files are not supported')
        
    return df

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
    songs_df = from_disk(spark, song_schema, input_data, depth=4, extension='json')
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

def add_time_columns(df, timestamp_column):
    time_df = df.select(['start_time'])
    time_df = time_df.withColumn('hour', F.hour('start_time'))
    time_df = time_df.withColumn('day', F.dayofmonth('start_time'))
    time_df = time_df.withColumn('week', F.weekofyear('start_time'))
    time_df = time_df.withColumn('month', F.month('start_time'))
    time_df = time_df.withColumn('year', F.year('start_time'))
    time_df = time_df.withColumn('weekday_num', F.dayofweek('start_time'))
    time_df = time_df.withColumn('weekday_str', F.date_format('start_time', 'EEE'))
    return time_df

def process_log_data(spark, input_data, output_data):
    #TODO break out processing into one function / table
    
    # specify schema for dataframe
    event_schema = build_event_schema()
    
    # read log data file
    events_df = from_disk(spark, event_schema, input_data, depth=3, extension='json')
    
    # filter by actions for song plays
    events_df = events_df.filter(events_df.page == 'NextSong')
    
    # create a column containing a datetime value by converting 
    # epoch time in milliseconds stored as strings
    # TODO clean-up
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
        'length as length',
        'gender as gender',
        'level as level',
        'sessionId as session_id',
        'location as location',
        'page as page',
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

    # TODO create function to add these fields to the provided df
    # extract columns to create time table
    print(f'events_df.columns={events_df.columns}')
    time_table_df = add_time_columns(events_df, 'start_time')
    
    # TODO clean-up
    inspect_df('time_table_df 1', time_table_df)
    print(f'time_table_df.count()={time_table_df.count()}')
    
    # write time table to parquet files partitioned by year and month
    time_table_df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + '/dim_time')
    
    # TODO clean-up
    inspect_df('time_table_df 2', time_table_df)
    print(f'time_table_df.count()={time_table_df.count()}')

    # read in song data to use for songplays table
    # s3://song-play-spark/dim_song/*.parquet
    song_df = events_df.select(['user_id', 'session_id', 'start_time', 'level', 'location'])
    inspect_df('song_df', song_df)

    # read in the song table
    song_table_df = from_disk(spark, None, output_data + '/dim_song/', extension='parquet')
    inspect_df('song_table_df', song_table_df)
    
    # read in the artist table
    artist_table_df = from_disk(spark, None, output_data + '/dim_artist/', extension='parquet')
    inspect_df('artist_table_df', artist_table_df)
    
    # inner join of dataframes on artist_id and selecting columns of interest
    song_artist_table_df = (song_table_df.
        join(artist_table_df, 'artist_id').
        select(['song_id', 'title', 'duration', 'artist_id', 'artist_name']))

    # TODO clean-up
    print('events_df', events_df.columns)
    print('song_artist_table_df', song_artist_table_df.columns)

    
    # extract columns from joined song and log datasets to create songplays table 
    songplay_table_df = events_df.join(
        song_artist_table_df, 
        (events_df.title == song_artist_table_df.title) & (events_df.length == song_artist_table_df.duration)
    )
    
    # # only keep song play activity
    songplay_table_df = songplay_table_df.where(events_df.page == 'NextSong')
    
    # TODO clean-up
    print('songplay_table_df', songplay_table_df.columns)

    # # write songplays table to parquet files partitioned by year and month
    songplay_table_df = add_time_columns(songplay_table_df, 'start_time')
    songplay_table_df.write.mode('overwrite').parquet(output_data + '/fact_songplay/', partitionBy=['year', 'month'])


def get_config(config, group):
    group = group.upper()
    return config[group]['SONG_DATA'], config[group]['LOG_DATA'], config[group]['OUTPUT_DATA']


def set_aws_keys_in_env(config):
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--aws', help='run spark job on an aws emr cluster', action='store_true')
    parser.add_argument('-l', '--local', help='run spark job locally', action='store_true')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read('.env/dl.cfg')

    song_data, log_data, output_data = None, None, None

    # TODO clean-up
    print(args.aws, args.local, config)
    if args.aws:
        set_aws_keys_in_env(config)
        song_data, log_data, output_data = get_config(config, 'aws')
    elif args.local:
        song_data, log_data, output_data = get_config(config, 'local')
    else:
        parser.print_help()
        sys.exit(-1)

    # TODO clean-up
    print('\n', 80*'*')
    print(song_data, log_data, output_data)
    print(80*'*', '\n')

    # processing 
    spark = create_spark_session()
    process_song_data(spark, song_data, output_data)    
    process_log_data(spark, log_data, output_data)
    
if __name__ == "__main__":
    main()

