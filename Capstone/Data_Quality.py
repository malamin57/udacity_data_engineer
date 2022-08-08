# Perform quality checks here
# Quality to check for any tables that are empty 

if df_spark_immigration.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_from_country.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_spark_airport.count() > 0: 
    print('valid table')
else:
    print('empty table')

if df_spark_us_department.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_spark_temp.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_spark_reference.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_from_country.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_to_country.count() > 0:
    print('valid table')
else:
    print('empty table')
    
if df_to_country_info.count() > 0:
     print('valid table')
else:
    print('empty table')
    
if df_city_orgin.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_avg_temp_orgin.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_city_dest.count() > 0:
    print('valid table')
else:
    print('empty table')

if df_avg_temp_dest.count() > 0:
         print('valid table')
else:
    print('empty table')
    
duplication = df_spark_immigration.groupBy("cicid").count().where("count > 1")
duplication.show()


if df_spark_immigration.count() > 1000000:
    print('Has more than 1 million rows of data')
else:
    print('Need a larger dataset')
    
    
 ETL_Check = df_to_country_info.orderBy(col('count').desc(),col('Race').desc()).show()

try: 
    avg_temp_dest_table(df_spark_immigration,df_spark_reference)
    print('table is valid')
    
except:
    print('error')
    
try:
    df_avg_temp_orgin_table(df_spark_immigration,df_spark_reference)
    print('table is valid')
except:
    print('error')
    
try: 
    df_from_country_table(df_spark_immigration,df_spark_reference)
    print('table is valid')
except:
    print('error')

try:
    df_to_country_info_table(df_spark_immigration,df_spark_reference)
    print('table is valid')
except:
    print('error')
    
