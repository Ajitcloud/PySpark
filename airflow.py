import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
spark = SparkSession.builder.appName("testing windows").master("yarn").config("spark.streaming.stopGracefullyOnShutdown","true").getOrCreate()
print("Hello Airflow")
