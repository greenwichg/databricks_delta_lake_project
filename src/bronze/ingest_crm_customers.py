# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze: Ingest CRM Customer Data (Auto Loader - JSON)
# MAGIC
# MAGIC **Source**: JSON files landing in cloud storage from the CRM system
# MAGIC **Pattern**: Auto Loader with schema inference + evolution
# MAGIC **Target**: Bronze Delta table (append-only, raw)
# MAGIC
# MAGIC ## Key Concepts Demonstrated
# MAGIC - **Auto Loader** (`cloudFiles` format) for incremental file ingestion
# MAGIC - **Schema inference** and **schema evolution** (new columns auto-added)
# MAGIC - **Rescue data column** for malformed records
# MAGIC - **Checkpoint-based** exactly-once processing guarantees
# MAGIC - **Metadata columns** for auditing (_datasource, _ingest_timestamp, _file_path)

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Source and target configuration
SOURCE_PATH = "/Volumes/customer_360_catalog/landing/raw_data/crm_customers"
TARGET_TABLE = "customer_360_catalog.bronze.raw_customers"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/crm_customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition (Optional - Auto Loader Can Infer)
# MAGIC
# MAGIC Providing schema hints helps Auto Loader make better type decisions while
# MAGIC still allowing schema evolution for new columns.

# COMMAND ----------

# Schema hints guide Auto Loader's inference without being rigid
# This is preferred over a full schema when sources evolve frequently
SCHEMA_HINTS = """
    customer_id STRING,
    email STRING,
    first_name STRING,
    last_name STRING,
    phone STRING,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    loyalty_tier STRING,
    lifetime_value DOUBLE,
    address STRUCT<
        street: STRING,
        city: STRING,
        state: STRING,
        zip_code: STRING,
        country: STRING
    >
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Ingestion
# MAGIC
# MAGIC **How Auto Loader Works (Interview Talking Points):**
# MAGIC 1. Uses `cloudFiles` format to discover new files incrementally
# MAGIC 2. Two modes: **Directory Listing** (simple) vs **File Notification** (scalable via cloud events)
# MAGIC 3. Maintains a checkpoint to track which files have been processed
# MAGIC 4. Automatically infers and evolves schema as source data changes
# MAGIC 5. Rescue column (`_rescued_data`) captures data that doesn't match the schema
# MAGIC 6. Supports JSON, CSV, Parquet, Avro, ORC, Text, and Binary formats

# COMMAND ----------

def ingest_crm_customers(trigger_mode="availableNow"):
    """
    Ingest CRM customer JSON files using Auto Loader.

    Parameters:
        trigger_mode: "availableNow" for batch-style (cost efficient)
                      "processingTime" for continuous streaming

    Auto Loader Key Options Explained:
        - cloudFiles.format: Source file format (json, csv, parquet, etc.)
        - cloudFiles.inferColumnTypes: Enable type inference (default is all STRING)
        - cloudFiles.schemaLocation: Where to store the inferred schema
        - cloudFiles.schemaEvolutionMode: How to handle new columns
            * "addNewColumns" - automatically add new columns to the table
            * "rescue" - put new columns in _rescued_data
            * "failOnNewColumns" - fail the stream on schema change
            * "none" - ignore new columns
        - cloudFiles.schemaHints: Override inferred types for specific columns
        - rescuedDataColumn: Column name to store data that doesn't match schema
    """

    raw_stream = (
        spark.readStream
        .format("cloudFiles")                              # <-- Auto Loader format
        .option("cloudFiles.format", "json")               # Source file format
        .option("cloudFiles.inferColumnTypes", "true")     # Infer actual types, not just STRING
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")  # Persist inferred schema
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")         # Auto-evolve schema
        .option("cloudFiles.schemaHints", SCHEMA_HINTS)    # Guide type inference
        .option("rescuedDataColumn", "_rescued_data")      # Capture malformed data
        .load(SOURCE_PATH)
    )

    # Add audit/metadata columns for lineage tracking
    enriched_stream = (
        raw_stream
        .withColumn("_ingested_at", F.current_timestamp())           # When we ingested it
        .withColumn("_source_file", F.input_file_name())             # Which file it came from
        .withColumn("_datasource", F.lit("crm"))                     # Source system identifier
        .withColumn("_file_modification_time",                       # When file was modified
                    F.col("_metadata.file_modification_time"))
    )

    # Write to Bronze Delta table
    query = (
        enriched_stream.writeStream
        .format("delta")
        .outputMode("append")                             # Bronze is always append-only
        .option("checkpointLocation", CHECKPOINT_PATH)    # Exactly-once guarantee
        .option("mergeSchema", "true")                    # Allow schema evolution on write
        .trigger(availableNow=True)                       # Process all available files, then stop
        .toTable(TARGET_TABLE)                            # Write to Unity Catalog table
    )

    query.awaitTermination()
    print(f"CRM customer ingestion complete. Target: {TARGET_TABLE}")
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion

# COMMAND ----------

# Run the ingestion
ingest_crm_customers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Ingestion Results

# COMMAND ----------

# Check record counts and sample data
df = spark.table(TARGET_TABLE)
print(f"Total records: {df.count()}")
print(f"Columns: {df.columns}")
display(df.limit(10))

# COMMAND ----------

# Check for rescued (malformed) data
rescued = df.filter(F.col("_rescued_data").isNotNull())
print(f"Records with rescued data: {rescued.count()}")
if rescued.count() > 0:
    display(rescued.select("_rescued_data", "_source_file", "_ingested_at").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader vs Manual File Listing (Interview Comparison)
# MAGIC
# MAGIC | Feature | Auto Loader | Manual `spark.read` |
# MAGIC |---------|------------|---------------------|
# MAGIC | Incremental | Yes (checkpoint-based) | No (re-reads everything) |
# MAGIC | Schema Evolution | Automatic | Manual |
# MAGIC | Scalability | File notifications for billions of files | Degrades with file count |
# MAGIC | Exactly-Once | Built-in via checkpoints | Must implement yourself |
# MAGIC | Cost | Efficient (only new files) | Expensive (full scan) |
# MAGIC | Setup | One-line format change | Custom tracking logic |
