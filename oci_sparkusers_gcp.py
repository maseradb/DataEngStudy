from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
import pyspark.sql.functions as f
import shutil
from delta import *
from credentials import *


#variables
key_credential = '/home/maseradb/Projects/svc_spark.json'
table_name='SPARKUSERS'

spark = SparkSession \
        .builder \
        .appName('PoC - Lakehouse - GCP') \
        .master('local[*]')\
        .config("spark.jars", "/home/maseradb/Projects/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")\
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_KEY)\
        .config('spark.driver.extraClassPath', "/home/maseradb/Projects/*")\
        .config('spark.delta.logStore.gs.impl','io.delta.storage.GCSLogStore')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('temporaryGcsBucket', 'gs://maseradb-stage/')\
        .getOrCreate()

#gcsDF = spark.read.parquet('gs://maseradb-bronze/sparusers.parquet')


jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", URL_OCI) \
    .option('query','SELECT COUNT(1) FROM BIGTABLE') \
    .option("user", USERNAME_OCI) \
    .option("password", PASSWORD_OCI) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

jdbcDF.show()
#jdbcDF.write.format("delta").save('gs://maseradb-bronze/sparkusers')
