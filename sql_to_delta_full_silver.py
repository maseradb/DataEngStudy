# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import credentials

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
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials.GCS_KEY)\
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

    # read table from bronze
    deltaTable = DeltaTable.forPath(spark, "gs://maseradb-bronze/bigtable")
    df = deltaTable.toDF()
    
    # adjust data
    df.createOrReplaceTempView('vw_bigtable')
    df = spark.sql("""
    SELECT ID,
           COL1,
           COL2,
           DATA_REF 
    FROM vw_bigtable
    WHERE TRUNC(DATA_REF, 'MONTH') = TRUNC(CURRENT_TIMESTAMP -INTERVAL '0-1' YEAR TO MONTH, 'MONTH')
    """)

    # print data
    df.show()    

    # create delta table on silver
    DeltaTable.createIfNotExists(spark) \
        .tableName("BIGTABLE_LAST_MONTH") \
        .addColumn("ID", "INT") \
        .addColumn("COL1", "STRING") \
        .addColumn("COL2", "STRING") \
        .addColumn("DATA_REF", "TIMESTAMP") \
        .location("gs://maseradb-silver/bigtable_last_month") \
        .execute()

    # write data to silver
    df.write.format("delta").mode("overwrite").save("gs://maseradb-silver/bigtable_last_month")
    
    # stop session
    spark.stop()   