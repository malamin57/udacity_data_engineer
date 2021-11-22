import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import from_utc_timestamp
import calendar

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

#This function will extract data from udacity s3 bucket transform the data
# and load into a seperate s3 bucket

def process_song_data(spark, input_data, output_data):

    song_data = input_data  + "song_data/A/B/C/*.json"

    
  
    df = spark.read.json(song_data)

    
    songs_table = df[['song_id','title','artist_id','year','duration']].drop_duplicates()
    
  
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data+"song")
    

  
    artists_table = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].drop_duplicates()
    
    
    artists_table.write.mode('overwrite').parquet(output_data+"artist")

#This function willl extract data from s3 bucket transform the data 
# Load the data into the s3 bucket. 
#This function will create the songplay table
    
def process_log_data(spark, input_data, output_data):
  
    log_data = input_data + "log_data/2018/11/2018-11-12-events.json"
    

    df = spark.read.json(log_data)
    
   
    df = df.filter(df.page == 'NextSong')
    
      
    users_table = df[['userId','firstName','lastName','gender','level']].drop_duplicates()
    
  
    users_table.write.mode('overwrite').parquet(output_data+"users")


    
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
   
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    get_week = udf(lambda x: calendar.day_name[x.weekday()])
    get_weekday = udf(lambda x: x.isocalendar()[1])
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x : x.day)
    get_year = udf(lambda x: x.year)
    get_month = udf(lambda x: x.month)
    
   
    df = df.withColumn('datetime', from_utc_timestamp(df.start_time,'tz'))
    df = df.withColumn('start_time', get_datetime(df.ts))
    df = df.withColumn('day', get_day(df.start_time))
    df = df.withColumn('week', get_week(df.start_time))
    df = df.withColumn('month', get_month(df.start_time))
    df = df.withColumn('year', get_year(df.start_time))
    df = df.withColumn('weekday', get_weekday(df.start_time))
    df = df.withColumn('hour', get_hour(df.start_time))
    
    time_table = df[['start_time','hour','day','week','month','year','weekday']].drop_duplicates()
    
  
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data+"time")


    song_df = spark.read.parquet(output_data+"song")

    
    songplays_table = df.join(song_df, song_df.title == df['song']).select(
                    df['datetime'],
                    df['userId'],
                    df['level'],
                    song_df['song_id'],
                    song_df['artist_id'],
                    df['sessionId'],
                    df['location'],
                    df['userAgent'],
                    df['year'],
                    df['month']
                    ).drop_duplicates()
    
    
    
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data+"songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data  = "s3a://datalackproject/project/"
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
