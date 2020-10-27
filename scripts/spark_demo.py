import sys
import json
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

###invoke SparkSession

spark = SparkSession.builder.appName("Load jsontest Data").config("hive.metastore.connect.retries",5).config("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()
sc=spark.sparkContext
sqlContext = SQLContext(sc)

df=spark.read.json("s3://cognizant-demo-2533-inbound/demo_input3.json",multiLine='true')
df.coalesce(1).write.mode("Append").parquet("s3://cognizant-demo-2533-datalake/demoout/")
