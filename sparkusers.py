from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

#variables
username = 'SPARK'
password = 'MaseraDB1234'
conn_string = 'jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=gda48883422ef71_oradb_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'
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
    .option("url", conn_string) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

jdbcDF.write.parquet('sparkusers.parquet')\
    .mode('overwrite')\
    .save(f'gs://maseradb-bronze/')