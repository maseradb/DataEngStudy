# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from credentials import *
import pyspark.sql.functions as f
from datetime import datetime

# main spark program
# init application
if __name__ == '__main__':

    bucketprefix='maseradb-delta'
    jarsHome='/home/maseradb/DataEngStudy'

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName('PoC - Lakehouse - GCP') \
        .master('local[*]')\
        .config("spark.jars", f"{jarsHome}/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")\
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_KEY)\
        .config('spark.driver.extraClassPath', f"{jarsHome}/*")\
        .config('spark.delta.logStore.gs.impl','io.delta.storage.GCSLogStore')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('temporaryGcsBucket', f'gs://{bucketprefix}-stage/')\
        .getOrCreate()
    
    # show configured parameters
    #print(SparkConf().getAll())

    # set log level
    #spark.sparkContext.setLogLevel("INFO")

        # adjust data
    actual_year= datetime.now().year
    actual_month= datetime.now().month

    # read table from bronze
    deltaTable = DeltaTable.forPath(spark, f"gs://{bucketprefix}-silver/bigtable_last_month-{actual_year}-{actual_month}")
    df = deltaTable.toDF()

    print(df.count())

    # stop session
    spark.stop()   