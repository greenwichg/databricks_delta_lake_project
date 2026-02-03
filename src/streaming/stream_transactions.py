# Databricks notebook source

# MAGIC %md
# MAGIC # Real-Time Transaction Processing (Structured Streaming)
# MAGIC
# MAGIC **Pattern**: Continuous streaming pipeline for low-latency transaction processing
# MAGIC **Trigger**: processingTime = "10 seconds" (near real-time)
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - **Structured Streaming** as a unified batch + streaming engine
# MAGIC - **Trigger modes**: processingTime vs availableNow vs once
# MAGIC - **Watermarks** for handling late-arriving data
# MAGIC - **Stateful processing** with deduplication
# MAGIC - **foreachBatch** for custom write logic (MERGE)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Trigger Modes (Interview Must-Know)
# MAGIC
# MAGIC | Mode | Syntax | Behavior | Cost | Use Case |
# MAGIC |------|--------|----------|------|----------|
# MAGIC | **Fixed interval** | `processingTime="10 seconds"` | Micro-batch every N seconds | $$$ (always running) | Real-time dashboards |
# MAGIC | **Available Now** | `availableNow=True` | Process all pending, then stop | $ (efficient) | Scheduled batch jobs |
# MAGIC | **Continuous** | `continuous="1 second"` | True continuous (~ms latency) | $$$$ | Ultra-low-latency (rare) |
# MAGIC | **Once** (deprecated) | `once=True` | One micro-batch, then stop | $ | Legacy, use availableNow |

# COMMAND ----------

SOURCE_TABLE = "customer_360_catalog.bronze.raw_transactions"
TARGET_TABLE = "customer_360_catalog.silver.transactions_realtime"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/realtime_transactions"

# COMMAND ----------

def process_realtime_transactions():
    """
    Continuous streaming pipeline for transaction processing.

    Key design decisions:
    1. Watermark of 10 minutes: tolerate late data up to 10 min
    2. Dedup on transaction_id within the watermark window
    3. foreachBatch for MERGE (upsert) into the target table
    4. processingTime trigger for near real-time latency
    """

    # Read stream with watermark
    stream = (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", 100)  # Control micro-batch size
        .table(SOURCE_TABLE)
        # Watermark: essential for stateful operations in streaming
        # Without it, state grows unbounded -> OOM
        .withWatermark("_ingested_at", "10 minutes")
    )

    # Deduplication within the watermark window
    deduped = stream.dropDuplicatesWithinWatermark(["transaction_id"])

    def merge_batch(batch_df, batch_id):
        """MERGE each micro-batch into the target table."""
        if batch_df.isEmpty():
            return

        cleaned = (
            batch_df
            .withColumn("amount", F.col("amount").cast("double"))
            .withColumn("net_amount",
                F.col("amount") * (1 - F.coalesce(F.col("discount_pct").cast("double"), F.lit(0.0))))
            .withColumn("transaction_date", F.to_date("transaction_date"))
            .withColumn("_processed_at", F.current_timestamp())
        )

        if not spark.catalog.tableExists(TARGET_TABLE):
            cleaned.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
            return

        target = DeltaTable.forName(spark, TARGET_TABLE)
        (
            target.alias("t")
            .merge(cleaned.alias("s"), "t.transaction_id = s.transaction_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Start the streaming query
    query = (
        deduped.writeStream
        .foreachBatch(merge_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        # For real-time: use processingTime
        .trigger(processingTime="10 seconds")
        # For cost-efficient batch: use availableNow=True instead
        # .trigger(availableNow=True)
        .start()
    )

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Streaming (Run this cell to start continuous processing)

# COMMAND ----------

# Start the real-time pipeline
query = process_realtime_transactions()

# COMMAND ----------

# Monitor the streaming query
# query.status        # Current status
# query.lastProgress  # Last micro-batch metrics
# query.stop()        # Stop the stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Streaming Queries
# MAGIC
# MAGIC ```python
# MAGIC # Check if query is active
# MAGIC query.isActive
# MAGIC
# MAGIC # Get processing metrics
# MAGIC query.lastProgress
# MAGIC # Returns: {
# MAGIC #   "inputRowsPerSecond": 1500.0,
# MAGIC #   "processedRowsPerSecond": 2000.0,
# MAGIC #   "batchId": 42,
# MAGIC #   "numInputRows": 15000,
# MAGIC #   "sources": [...],
# MAGIC #   "sink": {...}
# MAGIC # }
# MAGIC
# MAGIC # Get all active streaming queries
# MAGIC spark.streams.active
# MAGIC ```
