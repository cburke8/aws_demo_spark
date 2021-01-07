#pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as psf

import os
######

spark = SparkSession.builder.appName("DataOps").config("hive.metastore.connect.retries",5).config("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()
sqlContext = SQLContext(spark)
spark.sparkContext.setLogLevel("ERROR")


bronze_path = 's3://citi-bike-batch-data-con/bronze/'
silver_path = 's3://citi-bike-batch-data-con/silver/'
gold_path   = 's3://citi-bike-batch-data-con/gold/'

trip_schema = StructType([
  StructField('tripduration', IntegerType()),
  StructField('start_time', StringType()),
  StructField('stop_time',  StringType()),
  StructField('start_station_id', IntegerType()),
  StructField('start_station_name', StringType()),
  StructField('start_station_latitude', StringType()),
  StructField('start_station_longitude', StringType()),
  StructField('end_station_id', IntegerType()),
  StructField('end_station_name', StringType()),
  StructField('end_station_latitude', StringType()),
  StructField('end_station_longitude', StringType()),
  StructField('bike_id', IntegerType()),
  StructField('user_type', StringType()),
  StructField('birth_year', StringType()),
  StructField('user_gender', StringType()),
  ])


# read the raw trip history data to dataframe, without triggering job, by passing csv schema
bronze_all_csv = bronze_path + '*'

bronzeDF = spark.read.csv(
  bronze_all_csv, 
  header=True,
  schema=trip_schema
  )


#drop duplicate rows
bronzeDF = bronzeDF.distinct()
#order by trip start time
bronzeDF = bronzeDF.orderBy(bronzeDF.start_time.desc())
#view top of DF
bronzeDF.head()

bronzeDF.write.format('parquet').mode('overwrite').save(silver_path)
