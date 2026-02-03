# Databricks notebook source

# MAGIC %md
# MAGIC # DLT Pipeline: Bronze to Silver
# MAGIC ## Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables)
# MAGIC
# MAGIC **What is DLT / Lakeflow Declarative Pipelines?**
# MAGIC A **table-first, declarative** approach to building data pipelines:
# MAGIC - You define WHAT the table should look like, not HOW to compute it
# MAGIC - DLT handles orchestration, error handling, retries, and monitoring
# MAGIC - Built-in data quality with **Expectations**
# MAGIC - Automatic dependency resolution between tables
# MAGIC - Supports both batch and streaming with the same code
# MAGIC
# MAGIC ## Key Concepts Demonstrated
# MAGIC - `@dlt.table` decorator for declaring pipeline tables
# MAGIC - `dlt.read()` and `dlt.read_stream()` for reading upstream tables
# MAGIC - **Expectations**: `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`
# MAGIC - Streaming vs materialized views
# MAGIC - Auto Loader integration in DLT

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Auto Loader Ingestion via DLT
# MAGIC
# MAGIC In DLT, Auto Loader integration is seamless - just use `spark.readStream`
# MAGIC with `cloudFiles` inside a `@dlt.table` decorated function.

# COMMAND ----------

@dlt.table(
    name="bronze_customers",
    comment="Raw CRM customer data ingested via Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"  # DLT auto-manages OPTIMIZE
    }
)
def bronze_customers():
    """
    Ingest raw CRM customer JSON files.

    DLT handles:
    - Checkpoint management (no manual checkpoint path needed!)
    - Schema evolution
    - Retry on failures
    - Monitoring and metrics
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/customer_360_catalog/landing/raw_data/crm_customers")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

@dlt.table(
    name="bronze_transactions",
    comment="Raw e-commerce transaction data ingested via Auto Loader"
)
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("/Volumes/customer_360_catalog/landing/raw_data/transactions")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

@dlt.table(
    name="bronze_clickstream",
    comment="Raw clickstream events ingested via Auto Loader"
)
def bronze_clickstream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/customer_360_catalog/landing/raw_data/clickstream")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleaned Data with Expectations
# MAGIC
# MAGIC ### Expectations Explained (Interview Critical!)
# MAGIC
# MAGIC Expectations are DLT's built-in data quality enforcement:
# MAGIC
# MAGIC | Decorator | Behavior | Use When |
# MAGIC |-----------|----------|----------|
# MAGIC | `@dlt.expect("name", "condition")` | **Warn**: Record passes, metric logged | Monitoring quality trends |
# MAGIC | `@dlt.expect_or_drop("name", "condition")` | **Drop**: Bad records silently removed | Filtering clearly invalid data |
# MAGIC | `@dlt.expect_or_fail("name", "condition")` | **Fail**: Pipeline stops immediately | Critical data integrity violations |
# MAGIC | `@dlt.expect_all(dict)` | Multiple warn expectations at once | Batch quality rules |
# MAGIC | `@dlt.expect_all_or_drop(dict)` | Drop if ANY rule fails | Multiple filter conditions |
# MAGIC | `@dlt.expect_all_or_fail(dict)` | Fail if ANY rule fails | Multiple critical checks |

# COMMAND ----------

@dlt.table(
    name="silver_customers",
    comment="Cleaned and validated customer profiles",
    table_properties={"quality": "silver"}
)
# ---- DATA QUALITY EXPECTATIONS ----

# WARN: Log metrics but let records through (for monitoring)
@dlt.expect("valid_email", "email IS NOT NULL AND email LIKE '%@%'")

# DROP: Silently remove records that fail (clearly invalid)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("not_test_account", "email NOT LIKE '%test%'")

# FAIL: Stop the pipeline if this is violated (critical integrity)
@dlt.expect_or_fail("unique_customer_id",
    "customer_id IS NOT NULL")  # In practice, use row-level dedup

def silver_customers():
    """
    Clean and validate customer data from Bronze.

    The expectations above create a quality gate:
    - Records without customer_id are DROPPED
    - Test accounts are DROPPED
    - Email quality is WARNED (logged but passed through)
    - All quality metrics are visible in the DLT pipeline UI
    """

    return (
        dlt.read_stream("bronze_customers")  # Read from upstream DLT table
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("phone", F.regexp_replace("phone", "[^0-9+]", ""))
        .withColumn("loyalty_tier",
            F.when(F.col("loyalty_tier").isin("bronze", "silver", "gold", "platinum"),
                   F.col("loyalty_tier"))
            .otherwise("unknown")
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="silver_transactions",
    comment="Cleaned and enriched transaction data",
    table_properties={"quality": "silver"}
)
# Multiple expectations using expect_all
@dlt.expect_all_or_drop({
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "positive_amount": "amount > 0",
    "valid_date": "transaction_date IS NOT NULL",
})
@dlt.expect("reasonable_amount", "amount < 100000")  # Warn on very large transactions
def silver_transactions():
    """
    Clean transactions with multiple quality expectations.

    expect_all_or_drop applies ALL rules - record must pass every one.
    This is cleaner than writing multiple individual expect_or_drop decorators.
    """

    return (
        dlt.read_stream("bronze_transactions")
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("gross_amount", F.col("unit_price") * F.col("quantity"))
        .withColumn("discount_pct",
            F.when(F.col("discount_pct").between(0, 1), F.col("discount_pct"))
            .otherwise(0.0)
        )
        .withColumn("net_amount",
            F.col("gross_amount") * (1 - F.col("discount_pct"))
        )
        .withColumn("transaction_date", F.to_date("transaction_date"))
        .withColumn("currency", F.upper(F.coalesce("currency", F.lit("USD"))))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="silver_clickstream_events",
    comment="Cleaned clickstream events",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_event", "customer_id IS NOT NULL AND event_timestamp IS NOT NULL")
@dlt.expect("valid_event_type",
    "event_type IN ('page_view', 'click', 'add_to_cart', 'purchase', 'search')")
def silver_clickstream_events():
    return (
        dlt.read_stream("bronze_clickstream")
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics
# MAGIC
# MAGIC After running the pipeline, check the DLT UI for:
# MAGIC - **Expectation pass rates** (% of records passing each rule)
# MAGIC - **Drop counts** (how many records were filtered out)
# MAGIC - **Data quality trends** over time
# MAGIC - Pipeline DAG showing table dependencies
# MAGIC
# MAGIC These metrics are stored in the **event log** and queryable:
# MAGIC ```sql
# MAGIC SELECT * FROM event_log(TABLE(customer_360_catalog.silver.silver_customers))
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC ```
