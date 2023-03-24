import pyspark
from delta import *
from credentials import *

key_credential = 'svc_spark.json'

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.jars", "/home/maseradb/Projects/gcs-connector-hadoop2-latest.jar") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config('spark.driver.extraClassPath', "/home/maseradb/Projects/ojdbc8.jar")\
	.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_credential)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", URL_OCI) \
    .option('dbtable', 'SPARKUSERS') \
    .option("user", USERNAME_OCI) \
    .option("password", PASSWORD_OCI) \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

jdbcDF.show()
jdbcDF.write.format("delta").mode('overwrite').save("gs://maseradb-bronze/sparkusers-delta-table")