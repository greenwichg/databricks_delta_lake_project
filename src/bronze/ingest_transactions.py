# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze: Ingest E-Commerce Transactions (Auto Loader - CSV)
# MAGIC
# MAGIC **Source**: CSV files from the e-commerce transaction system
# MAGIC **Pattern**: Auto Loader with header inference and data type hints
# MAGIC **Target**: Bronze Delta table (append-only)
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - Auto Loader for **CSV** with header detection and multiLine support
# MAGIC - **Schema hints** to force correct types on CSV data
# MAGIC - **Corrupt record handling** with `_rescued_data`

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SOURCE_PATH = "/Volumes/customer_360_catalog/landing/raw_data/transactions"
TARGET_TABLE = "customer_360_catalog.bronze.raw_transactions"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/transactions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV-Specific Auto Loader Configuration
# MAGIC
# MAGIC CSV requires more configuration than self-describing formats like JSON/Parquet:
# MAGIC - Header detection
# MAGIC - Delimiter specification
# MAGIC - Quote/escape character handling
# MAGIC - Null value representation

# COMMAND ----------

def ingest_transactions():
    """
    Ingest transaction CSV files using Auto Loader.

    CSV-specific options explained:
        - cloudFiles.format: "csv" for CSV files
        - header: Whether first row is a header
        - delimiter: Field separator character
        - inferSchema: Let Auto Loader infer types (combined with schemaHints)
        - multiLine: Handle CSV fields that span multiple lines
        - escape: Escape character for special characters in fields
        - nullValue: String representation of null in the CSV
    """

    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaHints", """
            transaction_id STRING,
            customer_id STRING,
            transaction_date TIMESTAMP,
            amount DOUBLE,
            quantity INT,
            unit_price DOUBLE,
            discount_pct DOUBLE,
            product_id STRING,
            product_name STRING,
            product_category STRING,
            store_id STRING,
            payment_method STRING,
            currency STRING
        """)
        .option("header", "true")                   # First row is header
        .option("delimiter", ",")                    # Standard CSV delimiter
        .option("multiLine", "true")                 # Handle multi-line values
        .option("escape", '"')                       # Escape character
        .option("nullValue", "NULL")                 # How nulls are represented
        .option("rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
    )

    # Add audit columns
    enriched = (
        raw_stream
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_datasource", F.lit("ecommerce"))
        # Add a processing date partition column for efficient querying
        .withColumn("_ingest_date", F.current_date())
    )

    # Write to Bronze Delta table
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
    print(f"Transaction ingestion complete. Target: {TARGET_TABLE}")

# COMMAND ----------

ingest_transactions()

# COMMAND ----------

# Verify
df = spark.table(TARGET_TABLE)
print(f"Total records: {df.count()}")
display(df.limit(10))
