from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from credentials import *

#variables
table_name = 'SPARKUSERS'


conf = SparkConf() \
	.setMaster('local[*]') \
	.setAppName(f'OCI -> GCS - {table_name} Spark Integration') \
    .set('spark.driver.extraClassPath', "gs://maseradb-jars/ojdbc11.jar")\
    .set('temporaryGcsBucket', 'gs://maseradb-stage/')
    
sc = SparkContext(conf=conf) 

spark = SparkSession.builder\
    .config(conf=sc.getConf()) \
    .getOrCreate()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", URL_OCI) \
    .option("dbtable", table_name) \
    .option("user", USERNAME_OCI) \
    .option("password", PASSWORD_OCI) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

jdbcDF.write.parquet('sparkusers.parquet')\
    .mode('overwrite')\
    .save(f'gs://maseradb-bronze/')