# import libraries
from delta.tables import *
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from credentials import *

# main spark program
# init application
if __name__ == '__main__':

    bucketprefix='maseradb-delta'
    jarsHome='/home/maseradb/DataEngStudy'
    table_name = 'STREAMTABLE_DDL'


    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName('PoC - Lakehouse - GCP') \
        .master('local[*]')\
        .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:1.2.1,com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.1.3')\
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .config('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')\
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config('spark.delta.logStore.gs.impl','io.delta.storage.GCSLogStore')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_KEY)\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config('temporaryGcsBucket', f'gs://{bucketprefix}-stage/')\
        .config('spark.driver.extraClassPath', f"{jarsHome}/*")\
        .getOrCreate()
    
    # show configured parameters
    #print(SparkConf().getAll())
    
    # set log level
    spark.sparkContext.setLogLevel("WARN")

    # create delta table
    DeltaTable.createIfNotExists(spark) \
        .tableName('STREAMTABLE_DDL') \
        .addColumn("ID", "INT") \
        .addColumn("COL1", "STRING") \
        .addColumn("COL2", "STRING") \
        .addColumn("COL3", "STRING") \
        .addColumn("COL4", "STRING") \
        .addColumn("DATA_REF", "TIMESTAMP") \
        .addColumn("INTEGRATED_AT","TIMESTAMP") \
        .location(f"gs://{bucketprefix}-bronze/{table_name}") \
        .execute()
