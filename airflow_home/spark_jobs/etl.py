import configparser
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import hour, date_format, monotonically_increasing_id, \
    from_unixtime, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType)

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


# create timestamp column from original timestamp column
def get_seconds_since_unix(milliseconds):
    return milliseconds / 1000


def process_song_data(spark, input_data, output_data):
    song_data = input_data + "song_data/*/*/*/*.json"

    song_data_schema = StructType([
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", StringType(), True),
        StructField("artist_longitude", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), False),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", DoubleType(), False),
        StructField("year", IntegerType(), False)
    ])

    df = spark.read.json(song_data, schema=song_data_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()

    # write songs table to parquet files partitioned by year and artist
    # songs_table
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_latitude', 'artist_longitude',
                              'artist_location').distinct()

    # write artists table to parquet files
    # artists_table
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])

    # read log data file
    df = spark.read.json(log_data, schema=log_data_schema)

    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'), 'gender', 'level').distinct()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    def get_seconds_since_unix(milliseconds):
        return milliseconds / 1000

    get_seconds_since_unix = udf(get_seconds_since_unix, DoubleType())

    # Original dataset ts column has the milliseconds from unix information. We need to convert this value to seconds in order to pass it as parameter to a
    # https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.functions.from_unixtime
    df = df.withColumn('seconds_since_unix', get_seconds_since_unix(col('ts')))
    df = df.withColumn('timestamp', from_unixtime(col('seconds_since_unix'), 'yyyy-MM-dd HH:mm:ss'))

    # extract columns to create time table
    time_table = df.withColumn('start_time', col('timestamp')) \
        .withColumn('hour', hour('start_time')) \
        .withColumn('day', date_format('start_time', 'd')) \
        .withColumn('week', date_format('start_time', 'W')) \
        .withColumn('month', date_format('start_time', 'M')) \
        .withColumn('year', date_format(to_timestamp(col('start_time')), 'y')) \
        .withColumn('weekday', date_format('start_time', 'E')) \
        .select('timestamp', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'time')

    songs_table = spark.read.parquet(output_data + 'songs')

    songs_table.show()

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(songs_table, df.song == songs_table.title, how='inner') \
        .join(
        time_table.select('timestamp', 'start_time', col('year').alias('time_year'), col('month').alias('time_month')), \
        df.timestamp == time_table.timestamp) \
        .select('start_time', 'level', 'song_id', 'artist_id', 'location',
                col('userId').alias('user_id'),
                col('sessionId').alias('session_id'),
                col('userAgent').alias('user_agent'),
                col('time_year').alias('year'),
                col('time_month').alias('month'))

    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'songplays')


def main():
    if len(sys.argv) == 3:
        # aws cluster mode
        input_data = sys.argv[1]
        output_data = sys.argv[2]
    else:
        # local mode
        config = configparser.ConfigParser()
        config.read('./dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        input_data = config['DATALAKE']['INPUT_DATA']
        output_data = config['DATALAKE']['OUTPUT_DATA']

    spark = create_spark_session()

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
