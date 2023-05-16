# import libraries
from delta.tables import *
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from credentials import *
import json, os, re


# main spark program
# init application
if __name__ == '__main__':

    bucketprefix='maseradb-delta'
    jarsHome='/home/maseradb/DataEngStudy'
    delta_location = bucketprefix + "/delta-table"
    checkpoint_location = bucketprefix + "/checkpoints"
    schema_location = bucketprefix + "/kafka_schema.json"
    KAFKA_TOPIC = "oracle-streaming-1.MASERA.STREAMTABLE"
    topic = KAFKA_TOPIC
    KAFKA_SERVER = "kafka01:9092"

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName('PoC - Lakehouse - GCP') \
        .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:1.2.1,com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.1.3')\
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .config('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')\
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config('spark.delta.logStore.gs.impl','io.delta.storage.GCSLogStore')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_KEY)\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()
    
    # show configured parameters
    #print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")


    def infer_topic_schema_json(topic):

        df_json = (spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            # filter out empty values
            .withColumn("value", expr("string(value)"))
            .filter(col("value").isNotNull())
            # get latest version of each record
            .select("key", expr("struct(offset, value) r"))
            .groupBy("key").agg(expr("max(r) r")) 
            .select("r.value"))

        # decode the json values
        df_read = spark.read.json(
            df_json.rdd.map(lambda x: x.value), multiLine=True)

        # drop corrupt records
        if "_corrupt_record" in df_read.columns:
            df_read = (df_read
                        .filter(col("_corrupt_record").isNotNull())
                        .drop("_corrupt_record"))
        return df_read.schema.json()
    

    infer_schema = update_kafka_schema

    if not infer_schema:
        try:
            topic_schema_txt = dbutils.fs.head(schema_location)
        except:
            infer_schema = True
            pass

    if infer_schema:
        topic_schema_txt = infer_topic_schema_json(topic)
    
    dbutils.fs.rm(schema_location)
    dbutils.fs.put(schema_location, topic_schema_txt)

