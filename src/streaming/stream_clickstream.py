# Databricks notebook source

# MAGIC %md
# MAGIC # Real-Time Clickstream Processing (Structured Streaming)
# MAGIC
# MAGIC **Pattern**: Streaming aggregation with tumbling windows
# MAGIC **Target**: Real-time engagement metrics per 5-minute window
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - **Window-based aggregation** (tumbling and sliding windows)
# MAGIC - **Watermarks** for bounded state in streaming aggregations
# MAGIC - **Output modes**: append, update, complete
# MAGIC - **Stateful streaming** with window functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# COMMAND ----------

SOURCE_TABLE = "customer_360_catalog.bronze.raw_clickstream"
TARGET_TABLE = "customer_360_catalog.silver.realtime_engagement"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/realtime_clickstream"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Windowed Streaming Aggregation
# MAGIC
# MAGIC **Tumbling Window**: Fixed-size, non-overlapping windows
# MAGIC ```
# MAGIC |--- 5min ---|--- 5min ---|--- 5min ---|
# MAGIC |  window 1  |  window 2  |  window 3  |
# MAGIC ```
# MAGIC
# MAGIC **Sliding Window**: Fixed-size, overlapping windows
# MAGIC ```
# MAGIC |--- 10min window ---|
# MAGIC     |--- 10min window ---|
# MAGIC         |--- 10min window ---|
# MAGIC (slides every 5 minutes)
# MAGIC ```
# MAGIC
# MAGIC **Watermark + Window = bounded state**: Spark drops window state once
# MAGIC the watermark passes the window end + threshold.

# COMMAND ----------

def process_realtime_clickstream():
    """
    Streaming aggregation of clickstream events into 5-minute engagement windows.

    This demonstrates:
    1. Watermark (15 min) for late data tolerance
    2. Tumbling window (5 min) for time-based aggregation
    3. Append output mode (emit finalized windows only)
    """

    stream = (
        spark.readStream
        .format("delta")
        .table(SOURCE_TABLE)
        .withWatermark("event_timestamp", "15 minutes")  # 15 min late tolerance
    )

    # Aggregate into 5-minute tumbling windows
    windowed_metrics = (
        stream
        .groupBy(
            F.window("event_timestamp", "5 minutes"),  # Tumbling window
            "customer_id",
            "device_type",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("page_url").alias("unique_pages"),
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.min("event_timestamp").alias("first_event"),
            F.max("event_timestamp").alias("last_event"),
        )
        # Extract window boundaries
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn("_processed_at", F.current_timestamp())
    )

    # Write with append mode (only emit fully-closed windows)
    query = (
        windowed_metrics.writeStream
        .format("delta")
        .outputMode("append")               # Append: only finalized windows
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .toTable(TARGET_TABLE)
    )

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Mode Guide (Interview Critical)
# MAGIC
# MAGIC | Mode | Behavior | Use With |
# MAGIC |------|----------|----------|
# MAGIC | **Append** | Only new rows added to result | Windowed agg (after watermark closes window) |
# MAGIC | **Update** | Only changed rows emitted | Aggregations without windows |
# MAGIC | **Complete** | Entire result table emitted | Small result sets, full recompute |
# MAGIC
# MAGIC **Rules:**
# MAGIC - Aggregations with watermark → Append mode ✓
# MAGIC - Aggregations without watermark → Update or Complete mode
# MAGIC - No aggregations (map-only) → Append mode only
# MAGIC - foreachBatch → Any mode (handled in your function)

# COMMAND ----------

query = process_realtime_clickstream()
