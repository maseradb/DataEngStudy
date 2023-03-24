from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import bigquery
import credentials
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/maseradb/Projects/GBQ.json"

spark = SparkSession.builder.master('local[*]')\
	.appName("Iniciando com Spark")\
    .config('spark.driver.extraClassPath', "/home/maseradb/Projects/*")\
    .config("spark.jars", "/home/maseradb/Projects/gcs-connector-hadoop2-latest.jar") \
	.getOrCreate()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", credentials.URL_ONP)\
    .option('query', 'SELECT * FROM USUARIOS WHERE UPDATED_AT > SYSDATE -1') \
    .option("user", credentials.USERNAME_ONP) \
    .option("password", credentials.PASSWORD_ONP) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

jdbcDF = jdbcDF\
    .withColumn("INTEGRATED_AT",to_timestamp(current_timestamp(),"dd-MM-yyyy HH:mm:ss"))\
    .withColumn("ID",jdbcDF.ID.cast('int'))

#jdbcDF.show()
#jdbcDF.printSchema()

jdbcDF.write \
  .format("bigquery") \
  .option("table","ETL_BQ.MASERA_USUARIOS_UPDATES") \
  .option("parentProject", "dataengineeringstudy-377414")\
  .option('temporaryGcsBucket', 'maseradb-stage/')\
  .mode('overwrite') \
  .save()

client = bigquery.Client()

query = """
    MERGE ETL_BQ.MASERA_USUARIOS T
	USING ETL_BQ.MASERA_USUARIOS_UPDATES S 
	ON S.ID = T.ID 
	WHEN MATCHED THEN 
	UPDATE SET T.NAME = S.NAME,T.SURNAME = S.SURNAME,T.PHONE = S.PHONE,T.UPDATED_AT = S.UPDATED_AT,T.INTEGRATED_AT = S.INTEGRATED_AT 
	WHEN NOT MATCHED THEN 
	INSERT (ID, NAME,SURNAME,PHONE,UPDATED_AT,INTEGRATED_AT) VALUES (S.ID, S.NAME,S.SURNAME,S.PHONE,S.UPDATED_AT,S.INTEGRATED_AT)
"""
query_job = client.query(query)