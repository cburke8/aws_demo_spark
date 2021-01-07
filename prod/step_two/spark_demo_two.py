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

#isolating circular trips
slvrDF = spark.read.format('parquet').load(silver_path)

slvrDF.createOrReplaceTempView("silver")

# Making DIM_STATION starts:
# 2 start cols:
startDF = slvrDF.select(["start_station_id","start_station_name"]).distinct().withColumnRenamed("start_station_id","station_id").withColumnRenamed("start_station_name","station_name")
# 2 end cols:
endDF = slvrDF.select(["end_station_id","end_station_name"]).distinct().withColumnRenamed("end_station_id","station_id").withColumnRenamed("end_station_name","station_name")

# Merge them into dim station DF:
unionDF = startDF.unionAll(endDF).distinct()

#display(unionDF)
unionDF.write.format('parquet').mode('overwrite').save(f"{gold_path}dim_station")

