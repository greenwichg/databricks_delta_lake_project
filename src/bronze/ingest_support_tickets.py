# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze: Ingest Support Tickets (Auto Loader - Delta CDC)
# MAGIC
# MAGIC **Source**: Delta table with Change Data Feed (CDF) enabled from the support system
# MAGIC **Pattern**: Reading CDC changes from an upstream Delta table
# MAGIC **Target**: Bronze Delta table preserving full change history
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - **Delta Change Data Feed (CDF)** - reading insert/update/delete changes
# MAGIC - **CDC propagation** through the Medallion architecture
# MAGIC - Preserving **operation type** (_change_type) for downstream MERGE

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# When source is a Delta table with Change Data Feed enabled
SOURCE_TABLE = "support_system.tickets.support_tickets"
TARGET_TABLE = "customer_360_catalog.bronze.raw_support_tickets"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/support_tickets"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Data Feed (CDF) Explained
# MAGIC
# MAGIC **What is CDF?** When enabled on a Delta table, it records row-level changes:
# MAGIC - `_change_type`: "insert", "update_preimage", "update_postimage", or "delete"
# MAGIC - `_commit_version`: The Delta version where the change happened
# MAGIC - `_commit_timestamp`: When the change was committed
# MAGIC
# MAGIC **Enable CDF on a source table:**
# MAGIC ```sql
# MAGIC ALTER TABLE support_system.tickets.support_tickets
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```
# MAGIC
# MAGIC **Why use CDF?**
# MAGIC - Efficiently propagate only changes (not full table scans)
# MAGIC - Essential for CDC pipelines in the Lakehouse
# MAGIC - Works with both batch and streaming reads

# COMMAND ----------

def ingest_support_tickets_cdc():
    """
    Ingest support ticket changes using Delta Change Data Feed.

    readChangeData=true tells Spark to read the change feed instead of
    the full table. This gives us only the rows that changed since our
    last checkpoint.

    The _change_type column tells us what happened:
    - "insert": New row added
    - "update_preimage": Row before update (old values)
    - "update_postimage": Row after update (new values)
    - "delete": Row was deleted

    In Bronze, we preserve ALL change types for full audit history.
    In Silver, we'll apply these changes using MERGE.
    """

    cdc_stream = (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")           # Enable CDF reading
        .option("startingVersion", "latest")         # Start from latest (or specific version)
        # .option("startingTimestamp", "2024-01-01")  # Alternative: start from timestamp
        .table(SOURCE_TABLE)
    )

    # Preserve the CDC metadata columns and add our audit columns
    enriched = (
        cdc_stream
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_datasource", F.lit("support_system"))
        # Rename CDF columns to be explicit about their origin
        .withColumnRenamed("_change_type", "cdc_operation")
        .withColumnRenamed("_commit_version", "cdc_version")
        .withColumnRenamed("_commit_timestamp", "cdc_timestamp")
    )

    query = (
        enriched.writeStream
        .format("delta")
        .outputMode("append")                         # Append all changes (including deletes)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()
    print(f"Support ticket CDC ingestion complete. Target: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Ingest Support Tickets from JSON files (non-CDC)
# MAGIC
# MAGIC If your source doesn't support CDF, you can still implement CDC in Silver
# MAGIC by comparing incoming records with existing ones using MERGE.

# COMMAND ----------

def ingest_support_tickets_files():
    """
    Alternative: Ingest support tickets from JSON files (when CDF is not available).
    The CDC logic would then be handled in the Silver layer using MERGE.
    """

    SUPPORT_FILES_PATH = "/Volumes/customer_360_catalog/landing/raw_data/support_tickets"

    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
        .option("rescuedDataColumn", "_rescued_data")
        .load(SUPPORT_FILES_PATH)
    )

    enriched = (
        raw_stream
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_datasource", F.lit("support_system"))
    )

    query = (
        enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}_files")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()

# COMMAND ----------

# Run the CDC version (primary) or file version (fallback)
ingest_support_tickets_cdc()

# COMMAND ----------

df = spark.table(TARGET_TABLE)
print(f"Total CDC records: {df.count()}")
print(f"Operations breakdown:")
display(df.groupBy("cdc_operation").count())
