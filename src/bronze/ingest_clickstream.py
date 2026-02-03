# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze: Ingest Clickstream Data (Auto Loader - Parquet)
# MAGIC
# MAGIC **Source**: Parquet files from web analytics / clickstream pipeline
# MAGIC **Pattern**: Auto Loader with Parquet (schema embedded in files)
# MAGIC **Target**: Bronze Delta table
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - Auto Loader with **Parquet** (self-describing format - schema is embedded)
# MAGIC - **High-volume ingestion** patterns for event data
# MAGIC - **Partitioned writes** for downstream query performance
# MAGIC - Handling **file notification mode** for cloud-scale ingestion

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SOURCE_PATH = "/Volumes/customer_360_catalog/landing/raw_data/clickstream"
TARGET_TABLE = "customer_360_catalog.bronze.raw_clickstream"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/clickstream"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet Auto Loader
# MAGIC
# MAGIC Parquet is the easiest format for Auto Loader because:
# MAGIC 1. Schema is embedded in the file (no inference needed)
# MAGIC 2. Data types are already correct (no string parsing)
# MAGIC 3. Columnar format = efficient reads
# MAGIC
# MAGIC For **high-volume** sources (millions of files), use **File Notification** mode:
# MAGIC ```python
# MAGIC .option("cloudFiles.useNotifications", "true")
# MAGIC .option("cloudFiles.resourceGroup", "<resource-group>")  # Azure
# MAGIC .option("cloudFiles.region", "<region>")                 # AWS
# MAGIC ```

# COMMAND ----------

def ingest_clickstream():
    """
    Ingest clickstream Parquet files using Auto Loader.

    For Parquet files:
    - Schema is already embedded, no inference needed
    - No header/delimiter/escape config needed
    - Still benefits from incremental processing and checkpointing

    File Notification vs Directory Listing:
    ┌─────────────────────┬──────────────────┬─────────────────────┐
    │ Feature             │ Directory Listing│ File Notification   │
    ├─────────────────────┼──────────────────┼─────────────────────┤
    │ Setup               │ None             │ Cloud events config │
    │ Latency             │ Higher (polling) │ Lower (push-based)  │
    │ Scale               │ ~millions files  │ Billions of files   │
    │ Cost                │ List API calls   │ Event subscription  │
    │ Best for            │ Dev / small data │ Production / large  │
    └─────────────────────┴──────────────────┴─────────────────────┘
    """

    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        # For production high-volume, uncomment below for File Notification mode:
        # .option("cloudFiles.useNotifications", "true")
        .load(SOURCE_PATH)
    )

    enriched = (
        raw_stream
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_datasource", F.lit("web_analytics"))
        # Derive event_date for partition pruning in downstream queries
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    )

    query = (
        enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()
    print(f"Clickstream ingestion complete. Target: {TARGET_TABLE}")

# COMMAND ----------

ingest_clickstream()

# COMMAND ----------

df = spark.table(TARGET_TABLE)
print(f"Total records: {df.count()}")
print(f"Date range: {df.selectExpr('min(event_date)', 'max(event_date)').collect()}")
display(df.limit(10))
