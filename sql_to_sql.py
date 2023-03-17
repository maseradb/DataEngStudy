from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]')\
	.appName("Iniciando com Spark")\
    .config('spark.driver.extraClassPath', "/home/maseradb/Projects/ojdbc8.jar")\
    .config('spark.executor.extraClassPath', "/home/maseradb/Projects/ojdbc8.jar")\
	.getOrCreate()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=gda48883422ef71_oradb_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))") \
    .option('dbtable', 'SPARKUSERS') \
    .option("user", "SPARK") \
    .option("password", "MaseraDB1234") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

jdbcDF = jdbcDF\
    .withColumn("INTEGRATED_AT",to_timestamp(current_timestamp(),"dd-MM-yyyy HH:mm:ss"))

jdbcDF.write.format('jdbc')\
    .option("url", "jdbc:oracle:thin:@(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=gda48883422ef71_oradb_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))") \
    .option('dbtable', 'SPARKINTEGRATION') \
    .option("user", "ALESSANDRO") \
    .option("password", "MaseraDB1234") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .mode('overwrite').save()