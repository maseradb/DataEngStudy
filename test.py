from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

key_credential = 'svc_spark.json'

table_name='SPARKUSERS'

conf = SparkConf() \
	.setMaster('local[*]') \
	.setAppName(f'OCI -> GCS - {table_name} Spark Integration') \
	.set("spark.jars", "gcs-connector-hadoop2-latest.jar") \
	.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
	.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
	.set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
	.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_credential)\
    .set('spark.driver.extraClassPath', "/home/maseradb/Projects/*")\
    .set('temporaryGcsBucket', 'gs://maseradb-stage/')
    
sc = SparkContext(conf=conf) 

spark = SparkSession.builder\
    .config(conf=sc.getConf()) \
    .getOrCreate()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=gda48883422ef71_oradb_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))") \
    .option("dbtable", 'SPARKUSERS') \
    .option("user", "SPARK") \
    .option("password", "MaseraDB1234") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

jdbcDF.write.format('parquet')\
    .mode('overwrite')\
    .save(f'gs://maseradb-bronze/{table_name}/')