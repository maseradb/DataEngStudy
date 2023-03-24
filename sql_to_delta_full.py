# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import credentials

#import gcp credential
key_credential = 'svc_spark.json'

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName('PoC - Lakehouse - GCP') \
        .master('local[*]')\
        .config("spark.jars", "/home/maseradb/Projects/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")\
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_credential)\
        .config('spark.driver.extraClassPath', "/home/maseradb/Projects/*")\
        .config('spark.delta.logStore.gs.impl','io.delta.storage.GCSLogStore')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('temporaryGcsBucket', 'gs://maseradb-stage/')\
        .getOrCreate()
    
    # show configured parameters
    #print(SparkConf().getAll())

    # set log level
    #spark.sparkContext.setLogLevel("INFO")

    # read table from oracleDB
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", credentials.URL_ONP)\
        .option('dbtable', 'BIGTABLE') \
        .option("user", credentials.USERNAME_ONP) \
        .option("password", credentials.PASSWORD_ONP) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()
    
    # adjust data
    jdbcDF = jdbcDF\
    .withColumn("INTEGRATED_AT",to_timestamp(current_timestamp(),"dd-MM-yyyy HH:mm:ss"))\
    .withColumn("ID",jdbcDF.ID.cast('int'))

    # print data
    #jdbcDF.show()    

    # create delta table
    DeltaTable.createIfNotExists(spark) \
        .tableName("BIGTABLE") \
        .addColumn("ID", "INT") \
        .addColumn("COL1", "STRING") \
        .addColumn("COL2", "STRING") \
        .addColumn("DATA_REF", "TIMESTAMP") \
        .addColumn("INTEGRATED_AT","TIMESTAMP") \
        .location("gs://maseradb-bronze/bigtable") \
        .execute()

    # write data to cloud
    jdbcDF.write.format("delta").mode("overwrite").save("gs://maseradb-bronze/bigtable")
    
    # stop session
    spark.stop()   