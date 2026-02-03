# Databricks notebook source

# MAGIC %md
# MAGIC # Silver: Transform Transactions (Streaming + Dedup + Enrichment)
# MAGIC
# MAGIC **Source**: Bronze raw_transactions
# MAGIC **Pattern**: Structured Streaming with watermark-based dedup
# MAGIC **Target**: Silver transactions table
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - **Structured Streaming** for unified batch + stream processing
# MAGIC - **Watermark-based deduplication** (handling late/duplicate events)
# MAGIC - **Stream-static join** (enrich transactions with customer data)
# MAGIC - **MERGE** for idempotent writes via `foreachBatch`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable

# COMMAND ----------

SOURCE_TABLE = "customer_360_catalog.bronze.raw_transactions"
TARGET_TABLE = "customer_360_catalog.silver.transactions"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/silver_transactions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach: Streaming with foreachBatch + MERGE
# MAGIC
# MAGIC **Why foreachBatch + MERGE instead of simple append?**
# MAGIC 1. Transactions can arrive more than once (at-least-once delivery)
# MAGIC 2. We need idempotent writes (processing the same record twice shouldn't create duplicates)
# MAGIC 3. foreachBatch gives us a micro-batch DataFrame we can MERGE into the target
# MAGIC 4. This pattern works for BOTH streaming and batch (unified engine)
# MAGIC
# MAGIC **Watermark explained:**
# MAGIC - Tells Spark how long to wait for late-arriving data
# MAGIC - After the watermark threshold, old state is dropped (memory management)
# MAGIC - Example: 10 minute watermark = we tolerate data up to 10 minutes late

# COMMAND ----------

def transform_and_clean(batch_df):
    """
    Apply Silver-layer cleaning and enrichment to a micro-batch of transactions.
    """

    cleaned = (
        batch_df
        # Remove records with null transaction_id or customer_id
        .filter(
            F.col("transaction_id").isNotNull() &
            F.col("customer_id").isNotNull()
        )

        # Standardize and validate amounts
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("discount_pct",
            F.when(F.col("discount_pct").between(0, 1), F.col("discount_pct"))
            .otherwise(0.0)
        )

        # Calculate derived columns
        .withColumn("gross_amount", F.col("unit_price") * F.col("quantity"))
        .withColumn("discount_amount", F.col("gross_amount") * F.col("discount_pct"))
        .withColumn("net_amount", F.col("gross_amount") - F.col("discount_amount"))

        # Standardize payment method
        .withColumn("payment_method",
            F.when(F.col("payment_method").isin("credit_card", "debit_card", "paypal",
                                                  "apple_pay", "bank_transfer"),
                   F.col("payment_method"))
            .otherwise("other")
        )

        # Standardize currency to uppercase
        .withColumn("currency", F.upper(F.coalesce(F.col("currency"), F.lit("USD"))))

        # Extract date components for downstream analytics
        .withColumn("transaction_date", F.to_date("transaction_date"))
        .withColumn("transaction_year", F.year("transaction_date"))
        .withColumn("transaction_month", F.month("transaction_date"))
        .withColumn("transaction_day_of_week", F.dayofweek("transaction_date"))

        # Add Silver metadata
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    return cleaned

# COMMAND ----------

def merge_batch_to_silver(batch_df, batch_id):
    """
    foreachBatch function: MERGE each micro-batch into the Silver Delta table.

    This makes the streaming pipeline idempotent:
    - If a transaction_id already exists, we update it
    - If it's new, we insert it
    - Re-processing the same batch produces the same result (idempotent)

    Parameters:
        batch_df: The micro-batch DataFrame from Structured Streaming
        batch_id: The unique ID of this micro-batch
    """

    if batch_df.isEmpty():
        return

    # Apply transformations
    cleaned_df = transform_and_clean(batch_df)

    # Deduplicate within the micro-batch (keep latest per transaction_id)
    deduped_df = (
        cleaned_df
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())
        ))
        .filter("_rn = 1")
        .drop("_rn")
    )

    # MERGE into Silver table
    if not spark.catalog.tableExists(TARGET_TABLE):
        deduped_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
        return

    target = DeltaTable.forName(spark, TARGET_TABLE)

    (
        target.alias("target")
        .merge(
            deduped_df.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedUpdateAll()        # Update if exists
        .whenNotMatchedInsertAll()     # Insert if new
        .execute()
    )

    print(f"Batch {batch_id}: Merged {deduped_df.count()} transactions")

# Need Window for dedup
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Streaming Pipeline

# COMMAND ----------

def run_transaction_pipeline(trigger_mode="availableNow"):
    """
    Run the transaction processing pipeline.

    trigger options:
    - availableNow=True: Process all available data, then stop (cost-efficient batch)
    - processingTime="30 seconds": Continuous micro-batch every 30 seconds
    - once=True: Process one micro-batch, then stop (deprecated, use availableNow)
    """

    stream = (
        spark.readStream
        .format("delta")
        .table(SOURCE_TABLE)
    )

    query = (
        stream.writeStream
        .foreachBatch(merge_batch_to_silver)           # MERGE each micro-batch
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)                    # Process all available, then stop
        .start()
    )

    query.awaitTermination()
    print(f"Transaction pipeline complete. Target: {TARGET_TABLE}")

# COMMAND ----------

run_transaction_pipeline()

# COMMAND ----------

# Verify
df = spark.table(TARGET_TABLE)
print(f"Total Silver transactions: {df.count()}")
print(f"Date range: {df.selectExpr('min(transaction_date)', 'max(transaction_date)').collect()}")
display(df.groupBy("payment_method").agg(
    F.count("*").alias("count"),
    F.sum("net_amount").alias("total_revenue")
).orderBy("total_revenue", ascending=False))
