import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum as _sum
)
from pyspark.sql.types import (
    StructType, StringType, DoubleType, LongType, ArrayType
)

# 1. Kết nối qua mạng nội bộ Docker tới Redpanda
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC_NAME = "food-orders"

DELTA_BASE_PATH = "/opt/spark-apps/delta_lake"
DELTA_BRONZE_PATH = f"{DELTA_BASE_PATH}/bronze_orders"
CHECKPOINT_BRONZE = f"{DELTA_BASE_PATH}/_checkpoints/bronze"

spark = SparkSession.builder \
    .appName("FoodDeliveryRealTimeStreaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema cho message event
order_schema = StructType() \
    .add("order_id", StringType()) \
    .add("status", StringType()) \
    .add("restaurant_id", StringType()) \
    .add("restaurant_name", StringType()) \
    .add("restaurant_category", StringType()) \
    .add("restaurant_lat", DoubleType()) \
    .add("restaurant_lon", DoubleType()) \
    .add("customer_id", StringType()) \
    .add("customer_lat", DoubleType()) \
    .add("customer_lon", DoubleType()) \
    .add("driver_id", StringType()) \
    .add("items", ArrayType(StringType())) \
    .add("total_amount", DoubleType()) \
    .add("event_timestamp", LongType())

# 3. Đọc dữ liệu Real-time từ Kafka/Redpanda
raw_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_stream_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", (col("event_timestamp") / 1000).cast("timestamp"))

# ==========================================
# STREAM 1: Ghi vào BRONZE (Delta Lake)
# ==========================================
bronze_query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_BRONZE) \
    .start(DELTA_BRONZE_PATH)

# ==========================================
# STREAM 2: Sliding Window Aggregation ra Console
# ==========================================
silver_metrics_df = parsed_df \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute", "10 seconds"),
        col("restaurant_category")
    ) \
    .agg(
        count("order_id").alias("order_count"),
        _sum("total_amount").alias("gross_merchandise_value")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("restaurant_category"),
        col("order_count"),
        col("gross_merchandise_value")
    )

console_query = silver_metrics_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

print("Spark Streaming container đang chạy! Chờ nhận dữ liệu từ Kafka...")
spark.streams.awaitAnyTermination()