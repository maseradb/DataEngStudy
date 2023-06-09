# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from credentials import *

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
 
    # read table from oracleDB
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", URL_ONP2)\
        .option('query', "SELECT * FROM BATCHTABLE WHERE DATA_REF > CURRENT_DATE - INTERVAL '1' HOUR") \
        .option("user", USERNAME_ONP) \
        .option("password", PASSWORD_ONP) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    
    # adjust data
    jdbcDF = jdbcDF\
    .withColumn("INTEGRATED_AT",to_timestamp(current_timestamp(),"dd-MM-yyyy HH:mm:ss"))\
    .withColumn("ID",jdbcDF.ID.cast('int'))

    # print data
    jdbcDF.show()

    # read data from the cloud
    deltaTable = DeltaTable.forPath(spark, f"gs://{bucketprefix}-bronze/BATCHTABLE")

    # Upsert (merge) new data
    deltaTable.alias("oldData") \
        .merge(
            jdbcDF.alias("newData"),
            "oldData.id = newData.id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
        .execute()
    
    # stop session
    spark.stop()   