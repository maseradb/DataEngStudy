from pyspark.sql import SparkSession
import credentials

spark = SparkSession.builder\
    .appName("Iniciando com Spark")\
    .config('driver', 'spark.oracle.datasource.enabled')\
    .getOrCreate()

oracle_df = spark.read \
    .format("oracle") \
    .option("adbId",credentials.PYTHON_CONNECTSTRING) \
    .option("dbtable", 'SPARKUSERS') \
    .option("user", credentials.PYTHON_USERNAME) \
    .option("password", credentials.PYTHON_PASSWORD) \
    .load()

oracle_df.count()
