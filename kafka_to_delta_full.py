# import libraries
from delta.tables import *
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from credentials import *
import pyspark.sql.functions as f
from datetime import datetime
import pytz



# main spark program
# init application
if __name__ == '__main__':

    bucketprefix='maseradb-delta'
    jarsHome='/home/maseradb/DataEngStudy'
    KAFKA_TOPIC = "oracle-log-stream-04.MASERA.STREAMTABLE"
    KAFKA_SERVER = "kafka01:9092"
    jsonOptions ={"timestampFormat": "dd-MM-yyyy HH:mm:ss"}
    #timezoneBR = "UTC-3"

    schema = StructType([
        StructField("before", StructType([
            StructField("ID", IntegerType(),True),
            StructField("COL1", StringType(),True),
            StructField("COL2", StringType(),True),
            StructField("DATA_REF", StringType(),True)
        ])),
        StructField("after", StructType([
            StructField("ID", IntegerType(),True),
            StructField("COL1", StringType(),True),
            StructField("COL2", StringType(),True),
            StructField("DATA_REF", StringType(),True)
        ])),
        StructField("source", StructType([
            StructField("version", StringType(),True),
            StructField("connector", StringType(),True),
            StructField("name", StringType(),True),
            StructField("ts_ms", TimestampType(),True),
            StructField("snapshot", StringType(),True),
            StructField("db", StringType(),True),
            StructField("sequence", StringType(),True),
            StructField("schema", StringType(),True),
            StructField("table", StringType(),True),
            StructField("txId", StringType(),True),
            StructField("scn", StringType(),True),
            StructField("commit_scn", StringType(),True),
            StructField("lcr_position", StringType(),True),
            StructField("rs_id", StringType(),True),
            StructField("ssn", DoubleType(),True),
            StructField("redo_thread", DoubleType(),True),
            StructField("user_name", StringType(),True)
        ])),
        StructField("op", StringType(),True),
        StructField("ts_ms", DoubleType(),True),
        StructField("transaction", StringType(),True)	
    ])

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
    spark.sparkContext.setLogLevel("WARN")


    df = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', KAFKA_SERVER)\
        .option('subscribe', KAFKA_TOPIC)\
        .option("startingOffsets", "earliest") \
        .load()\
        .select(from_json(col('value').cast('string') , schema ,jsonOptions).alias('data'))
        
    newdf = df.select(
        col('data.after.ID').alias('ID'),
        col('data.after.COL1').alias('COL1'),
        col('data.after.COL2').alias('COL2'),
        col('data.after.DATA_REF').alias('DATA_REF'))

    newdf = newdf\
        .withColumn("INTEGRATED_AT",from_utc_timestamp(current_timestamp(),'UTC-3'))\
        .withColumn("ID",newdf.ID.cast('int'))\
        .withColumn("DATA_REF", from_unixtime(col("DATA_REF") / 1000).cast("timestamp"))


    newdf.select('ID','COL1','COL2','DATA_REF','INTEGRATED_AT')\
        .writeStream\
        .format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", f"gs://{bucketprefix}-bronze/_STREAMTABLE_CTL")\
        .start(f"gs://{bucketprefix}-bronze/STREAMTABLE")\
        .awaitTermination()
    