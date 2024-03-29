import findspark
findspark.init('/home/ajith/spark-3.4.1-bin-hadoop3')
import pyspark
import os
pyspark.sql.streaming.DataStreamReader
os.environ['SPARK_LOCAL_IP'] = '192.168.0.101'
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
spark = SparkSession.builder.appName("testing windows").master("local").config("spark.streaming.stopGracefullyOnShutdown","true").getOrCreate()
spark.sparkContext.setLogLevel("INFO")
df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers",'localhost:9092')
        .option("subscribe", 'kafka-engine')
        .option("startingOffsets", "latest")
        .load()
    )
    
df.printSchema()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType,TimestampType,LongType,DoubleType
schema= StructType([
    StructField("timestamp", TimestampType()),
    StructField("device_id", StringType()),
    StructField("temperature", DoubleType(),True),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType())
])
value_df=df.select(from_json(col("value").cast("string"),schema).alias("value"))
explode_df=value_df.selectExpr("value.timestamp","value.device_id","value.temperature","value.humidity","value.pressure")
from pyspark.sql.functions import *
from pyspark.sql.functions import window
value_df.printSchema()

win=explode_df.withWatermark("timestamp", "30 minutes").groupBy(col("device_id"),window(col("timestamp"),"10 minute")).agg(avg("temperature").alias("avg_temp"))
win.printSchema()
win=win.select("device_id","window.start","window.end","avg_temp")
# path="/home/ajith/Desktop"
write_query=win.writeStream.format("console").queryName("kafka_query").outputMode("update").trigger(processingTime="1 minute").start()
write_query.awaitTermination() 
spark.stop()   
