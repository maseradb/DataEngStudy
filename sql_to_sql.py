from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from credentials import *

spark = SparkSession.builder.master('local[*]')\
	.appName("Iniciando com Spark")\
    .config('spark.driver.extraClassPath', "/home/maseradb/Projects/ojdbc8.jar")\
    .config('spark.executor.extraClassPath', "/home/maseradb/Projects/ojdbc8.jar")\
	.getOrCreate()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", PYTHON_CONNECTSTRING) \
    .option('dbtable', 'SPARKUSERS') \
    .option("user", PYTHON_USERNAME) \
    .option("password", PYTHON_PASSWORD) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

jdbcDF = jdbcDF\
    .withColumn("INTEGRATED_AT",to_timestamp(current_timestamp(),"dd-MM-yyyy HH:mm:ss"))

jdbcDF.write.format('jdbc')\
    .option("url", URL_OCI) \
    .option('dbtable', 'SPARKINTEGRATION') \
    .option("user", USERNAME_OCI) \
    .option("password", PASSWORD_OCI) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .mode('overwrite').save()