from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("SmartCityTrafficStreamProcessor") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("junction_name", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", DoubleType(), True),
    StructField("traffic_status", StringType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-data") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data")

traffic_df = json_df.select(
    from_json(col("json_data"), schema).alias("data")
).select("data.*")

traffic_df = traffic_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

critical_alerts_df = traffic_df.filter(col("avg_speed") < 10)

congestion_window_df = traffic_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("sensor_id"),
        col("junction_name")
    ) \
    .agg(
        spark_sum("vehicle_count").alias("total_vehicle_count"),
        avg("avg_speed").alias("window_avg_speed")
    ) \
    .withColumn(
        "congestion_index",
        col("total_vehicle_count") / col("window_avg_speed")
    )

# Query 1: Critical alerts → console
critical_query_console = critical_alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .queryName("CriticalTrafficAlerts_Console") \
    .start()

# Query 2: Critical alerts → folder
critical_query_file = critical_alerts_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "output/critical_alerts") \
    .option("checkpointLocation", "output/checkpoints/critical_alerts") \
    .queryName("CriticalTrafficAlerts_File") \
    .start()

# Query 3: Window aggregation → console
window_query = congestion_window_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .queryName("CongestionWindowAnalysis") \
    .start()

spark.streams.awaitAnyTermination()
