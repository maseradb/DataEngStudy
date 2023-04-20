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

    # read table from bronze
    deltaTable = DeltaTable.forPath(spark, f"gs://{bucketprefix}-bronze/bigtable")
    df = deltaTable.toDF()
    
    # adjust data
    actual_year= datetime.now().year
    actual_month= datetime.now().month
    
    df = df.select('ID','COL1','COL2','DATA_REF')\
        .where(f.year(f.col('DATA_REF')) == actual_year)\
        .where(f.month(f.col('DATA_REF')) == actual_month)

    # create delta table on silver
    DeltaTable.createIfNotExists(spark) \
        .tableName("BIGTABLE_LAST_MONTH") \
        .addColumn("ID", "INT") \
        .addColumn("COL1", "STRING") \
        .addColumn("COL2", "STRING") \
        .addColumn("DATA_REF", "TIMESTAMP") \
        .location(f"gs://{bucketprefix}-silver/bigtable_{actual_year}-{actual_month}") \
        .execute()

    # write data to silver
    df.write.format("delta").mode("overwrite").save(f"gs://{bucketprefix}-silver/bigtable_{actual_year}-{actual_month}")

    # stop session
    spark.stop()   