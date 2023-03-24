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
jdbcDF.createOrReplaceTempView("UPDATES")

jdbcDF.write \
	.format('jdbc') \
	.option('url', URL_OCI) \
	.option('dbtable', 'SPARKINTEGRATION') \
	.option('user', USERNAME_OCI) \
	.option('password', PASSWORD_OCI) \
	.option('driver', 'oracle.jdbc.driver.OracleDriver') \
	.option('insertStatement', 'MERGE INTO SPARKINTEGRATION T USING UPDATES S ON S.ID = T.ID WHEN MATCHED THEN UPDATE SET T.NAME = S.NAME,T.SURNAME = S.SURNAME,T.PHONE = S.PHONE,T.UPDATED_AT = S.UPDATED_AT,T.INTEGRATED_AT = S.INTEGRATED_AT WHEN NOT MATCHED THEN INSERT (ID, NAME,SURNAME,PHONE,UPDATED_AT,INTEGRATED_AT) VALUES (S.ID, S.NAME,S.SURNAME,S.PHONE,S.UPDATED_AT,S.INTEGRATED_AT)') \
	.mode('overwrite') \
	.save()