# Do all imports and installs here
import pandas as pd
from datetime import datetime
import pandas as pd
import kaggle
import boto3
import os
import re
import numpy as np
import calendar
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, explode, split, regexp_extract, col, isnan, isnull, desc, when, sum, to_date, desc, regexp_replace, count, to_timestamp, lower 
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import from_utc_timestamp
from pyspark.sql.types import IntegerType, TimestampType


from pyspark.sql import SparkSession

spark = SparkSession.builder.\
config("spark.jars.repositories", "https://repos.spark-packages.org/").\
config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
enableHiveSupport().getOrCreate()

mon = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']

#df_spark_immigration = spark.read.format('com.github.saurfang.sas.spark').option("headers","true").load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')

for x in mon:
    df_spark_immigration = spark.read.format('com.github.saurfang.sas.spark').option("headers","true").load('../../data/18-83510-I94-Data-2016/i94_'+x+'16_sub.sas7bdat')
    df_spark_immigration = df_spark_immigration.union(df_spark_immigration)

df_spark_immigration.write.format("parquet").mode("overwrite").save("sas_data")
    
df_spark_airport = spark.read.csv("airport-codes_csv.csv")
df_spark_us_department = spark.read.option("delimiter", ";").option("header","true").csv("us-cities-demographics.csv" )
df_spark_temp = spark.read.option("delimiter", ",").option("header", "true").csv("GlobalLandTemperaturesByCity.csv")
df_spark_reference = spark.read.option("delimiter", ",").option("header", "true").csv("i94values - Sheet1.csv")

# Removing Duplications
df_spark_immigration = df_spark_immigration.dropDuplicates()

# Changing the data type from double to integers 
toInt = udf(lambda x: int(x) if x!=None else x, IntegerType())
for colname, coltype in df_spark_immigration.dtypes:
    if coltype == 'double':
        df_spark_immigration = df_spark_immigration.withColumn(colname, toInt(colname))

# Converting the arrdate and departe date from date number value to timestamp
@udf(TimestampType())
def get_date(x):
    try:
        return pd.to_timedelta(x, unit='D') + pd.Timestamp('1960-1-1')
    except:
        return pd.Timestamp('1900-1-1')

df_spark_immigration = df_spark_immigration.fillna({'depdate':'0'})
df_spark_immigration = df_spark_immigration.withColumn('arrdate', get_date(df_spark_immigration['arrdate']))
df_spark_immigration = df_spark_immigration.withColumn('depdate', get_date(df_spark_immigration['depdate']))

# Changing the tables to have lower case 
df_spark_temp = df_spark_temp.withColumn('City', lower(df_spark_temp.City))
df_spark_temp = df_spark_temp.withColumn('Country', lower(df_spark_temp.Country))
df_spark_temp = df_spark_temp.withColumn("AverageTemperature", df_spark_temp["AverageTemperature"].cast(IntegerType()))
df_spark_reference = df_spark_reference.withColumn('i94city', lower(df_spark_reference.i94city))
df_spark_reference = df_spark_reference.withColumn('I94CITValue', lower(df_spark_reference.I94CITValue))
df_spark_us_department = df_spark_us_department.withColumn('City', lower(df_spark_us_department.City))

#Data Model
df_from_country = df_spark_immigration.join(
        df_spark_reference,
        df_spark_reference['I94CIT']==df_spark_immigration['i94cit']
).select(
df_spark_reference['I94CITValue'],
df_spark_immigration['arrdate'],
df_spark_immigration['i94visa'],
df_spark_immigration['visatype'],  
).groupBy('I94CITValue','arrdate','i94visa','visatype').count()
    
df_to_country = df_spark_immigration.join(
df_spark_reference,
df_spark_reference['i94prtl']==df_spark_immigration['I94PORT']
).select(
df_spark_reference['i94city'],
df_spark_immigration['arrdate'],
df_spark_immigration['depdate']
)
    
df_to_country = df_spark_immigration.join(df_spark_reference,df_spark_reference['i94prtl']==df_spark_immigration['I94PORT']
).select(
df_spark_reference['i94city'],
df_spark_immigration['arrdate'],
df_spark_immigration['depdate']
)
    
df_to_country = df_spark_immigration.join(
df_spark_reference,
df_spark_reference['i94prtl']==df_spark_immigration['I94PORT']
).select(
df_spark_reference['i94city'],
df_spark_immigration['arrdate'],
df_spark_immigration['depdate']
)

df_to_country_info = df_to_country.join(df_spark_us_department,df_spark_us_department['City']==df_to_country['i94city']
).select(
df_to_country['i94city'],
df_to_country['arrdate'],
df_to_country['depdate'],
df_spark_us_department['Median Age'],
df_spark_us_department['Male Population'],
df_spark_us_department['Female Population'],
df_spark_us_department['Total Population'],
df_spark_us_department['Foreign-born'],
df_spark_us_department['Race'],
df_spark_us_department['Count'] 
).groupBy('i94city','arrdate','depdate','Median Age','Male Population','Female Population','Total Population','Foreign-born','Race','Count').count()
    

df_city_orgin = df_spark_immigration.join(df_spark_reference, df_spark_reference['I94CIT']==df_spark_immigration['i94cit']).select(
df_spark_reference['I94CITValue'],
df_spark_immigration['arrdate'])
df_avg_temp_orgin = df_city_orgin.join(df_spark_temp,df_spark_temp['City']==df_city_orgin['I94CITValue']).select(
df_city_orgin['I94CITValue'],
df_city_orgin['arrdate'],
df_spark_temp['AverageTemperature']
).groupBy('I94CITValue','arrdate').avg('AverageTemperature')
    
df_city_dest = df_spark_immigration.join(df_spark_reference,df_spark_reference['I94CIT']==df_spark_immigration['i94cit']).select(
df_spark_reference['i94city'],
df_spark_immigration['arrdate'])

df_avg_temp_dest = df_city_dest.join(df_spark_temp,df_spark_temp['City']==df_city_dest['i94city']).select(
df_city_dest['i94city'],
df_city_dest['arrdate'],
df_spark_temp['AverageTemperature']
).groupBy('i94city','arrdate').avg('AverageTemperature')
    


def df_from_country_table(x,y):
    df_from_country = x.join(
        y,
        y['I94CIT']==x['i94cit']
    ).select(
    y['I94CITValue'],
    x['arrdate'],
    x['i94visa'],
    x['visatype'],  
    ).groupBy('I94CITValue','arrdate','i94visa','visatype').count()
    
    df_to_country = df_spark_immigration.join(
    df_spark_reference,
    df_spark_reference['i94prtl']==df_spark_immigration['I94PORT']
    ).select(
    df_spark_reference['i94city'],
    df_spark_immigration['arrdate'],
    df_spark_immigration['depdate']
    )
    
    df_to_country = df_spark_immigration.join(
    df_spark_reference,
    df_spark_reference['i94prtl']==df_spark_immigration['I94PORT']
    ).select(
    df_spark_reference['i94city'],
    df_spark_immigration['arrdate'],
    df_spark_immigration['depdate']
    )
    
    return df_to_country

def df_to_country_info_table(x,y):
    df_to_country = x.join(
    y,
    y['i94prtl']==x['I94PORT']
    ).select(
    y['i94city'],
    x['arrdate'],
    x['depdate']
    )

    df_to_country_info = df_to_country.join(
                        df_spark_us_department,
                        df_spark_us_department['City']==df_to_country['i94city']
    ).select(
    df_to_country['i94city'],
    df_to_country['arrdate'],
    df_to_country['depdate'],
    df_spark_us_department['Median Age'],
    df_spark_us_department['Male Population'],
    df_spark_us_department['Female Population'],
    df_spark_us_department['Total Population'],
    df_spark_us_department['Foreign-born'],
    df_spark_us_department['Race'],
    df_spark_us_department['Count'] 
    ).groupBy('i94city','arrdate','depdate','Median Age','Male Population','Female Population','Total Population','Foreign-born','Race','Count').count()
    
    return df_to_country_info

def df_avg_temp_orgin_table(x,y):
    df_city_orgin = x.join(y, 
                    y['I94CIT']==x['i94cit']).select(
    y['I94CITValue'],
    x['arrdate']
    )
    df_avg_temp_orgin = df_city_orgin.join(df_spark_temp,df_spark_temp['City']==df_city_orgin['I94CITValue']).select(
    df_city_orgin['I94CITValue'],
    df_city_orgin['arrdate'],
    df_spark_temp['AverageTemperature']
    ).groupBy('I94CITValue','arrdate').avg('AverageTemperature')
    
    return df_avg_temp_orgin
    
def avg_temp_dest_table(x, y):
    df_city_dest = x.join(y,y['I94CIT']==x['i94cit']).select(
    y['i94city'],
    x['arrdate'])

    df_avg_temp_dest = df_city_dest.join(df_spark_temp,df_spark_temp['City']==df_city_dest['i94city']).select(
    df_city_dest['i94city'],
    df_city_dest['arrdate'],
    df_spark_temp['AverageTemperature']
    ).groupBy('i94city','arrdate').avg('AverageTemperature')
    
    return df_avg_temp_dest
    
