# Databricks notebook source

# MAGIC %md
# MAGIC # Complete DLT Pipeline: End-to-End Customer 360
# MAGIC ## Single Pipeline Definition: Bronze -> Silver -> Gold
# MAGIC
# MAGIC This notebook defines the **complete pipeline** in a single file.
# MAGIC DLT automatically resolves the dependency graph between tables.
# MAGIC
# MAGIC **Pipeline Configuration** (set in Databricks UI or API):
# MAGIC ```json
# MAGIC {
# MAGIC     "name": "customer_360_pipeline",
# MAGIC     "target": "customer_360_catalog.default",
# MAGIC     "catalog": "customer_360_catalog",
# MAGIC     "configuration": {
# MAGIC         "landing_path": "/Volumes/customer_360_catalog/landing/raw_data"
# MAGIC     },
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {"min_workers": 1, "max_workers": 5},
# MAGIC             "node_type_id": "i3.xlarge",
# MAGIC             "runtime_engine": "PHOTON"
# MAGIC         }
# MAGIC     ],
# MAGIC     "development": true,
# MAGIC     "continuous": false,
# MAGIC     "channel": "CURRENT",
# MAGIC     "photon": true
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Run modes:**
# MAGIC - `development = true`: Run on-demand, no auto-retry, good for dev/test
# MAGIC - `development = false`: Production mode with auto-retry and monitoring
# MAGIC - `continuous = true`: Continuously running streaming pipeline
# MAGIC - `continuous = false`: Triggered (batch) pipeline

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Pipeline-level configuration (read from DLT pipeline settings)
LANDING_PATH = spark.conf.get("landing_path",
    "/Volumes/customer_360_catalog/landing/raw_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ===== BRONZE LAYER =====

# COMMAND ----------

@dlt.table(
    name="raw_customers",
    comment="Raw CRM customer data - Auto Loader ingestion",
    table_properties={"quality": "bronze"}
)
def raw_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING_PATH}/crm_customers")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

@dlt.table(
    name="raw_transactions",
    comment="Raw e-commerce transactions - Auto Loader ingestion",
    table_properties={"quality": "bronze"}
)
def raw_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING_PATH}/transactions")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

@dlt.table(
    name="raw_clickstream",
    comment="Raw clickstream events - Auto Loader ingestion",
    table_properties={"quality": "bronze"}
)
def raw_clickstream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING_PATH}/clickstream")
        .withColumn("_ingested_at", F.current_timestamp())
    )

@dlt.table(
    name="raw_support_tickets",
    comment="Raw support tickets - Auto Loader ingestion",
    table_properties={"quality": "bronze"}
)
def raw_support_tickets():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{LANDING_PATH}/support_tickets")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ===== SILVER LAYER =====

# COMMAND ----------

@dlt.table(
    name="clean_customers",
    comment="Cleaned and validated customer profiles"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email_format", "email LIKE '%@%.%'")
@dlt.expect("has_name", "first_name IS NOT NULL OR last_name IS NOT NULL")
def clean_customers():
    return (
        dlt.read_stream("raw_customers")
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("first_name", F.initcap(F.trim("first_name")))
        .withColumn("last_name", F.initcap(F.trim("last_name")))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("phone", F.regexp_replace("phone", "[^0-9+]", ""))
        .withColumn("loyalty_tier",
            F.when(F.col("loyalty_tier").isin("bronze", "silver", "gold", "platinum"),
                   F.col("loyalty_tier")).otherwise("unknown"))
        .withColumn("_cleaned_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="clean_transactions",
    comment="Validated and enriched transactions"
)
@dlt.expect_all_or_drop({
    "valid_txn_id": "transaction_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "positive_amount": "CAST(amount AS DOUBLE) > 0",
})
@dlt.expect("reasonable_amount", "CAST(amount AS DOUBLE) < 50000")
def clean_transactions():
    return (
        dlt.read_stream("raw_transactions")
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("discount_pct",
            F.when(F.col("discount_pct").cast("double").between(0, 1),
                   F.col("discount_pct").cast("double")).otherwise(0.0))
        .withColumn("net_amount",
            F.col("amount") * (1 - F.col("discount_pct")))
        .withColumn("transaction_date", F.to_date("transaction_date"))
        .withColumn("_cleaned_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="clean_clickstream",
    comment="Validated clickstream events"
)
@dlt.expect_or_drop("valid_event", "customer_id IS NOT NULL AND event_timestamp IS NOT NULL")
def clean_clickstream():
    return (
        dlt.read_stream("raw_clickstream")
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("_cleaned_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="clean_support_tickets",
    comment="Validated support tickets"
)
@dlt.expect_or_drop("valid_ticket", "ticket_id IS NOT NULL AND customer_id IS NOT NULL")
@dlt.expect("valid_priority",
    "priority IN ('low', 'medium', 'high', 'critical')")
def clean_support_tickets():
    return (
        dlt.read_stream("raw_support_tickets")
        .withColumn("priority",
            F.when(F.col("priority").isin("low", "medium", "high", "critical"),
                   F.col("priority")).otherwise("medium"))
        .withColumn("status",
            F.when(F.col("status").isin("open", "in_progress", "resolved", "closed"),
                   F.col("status")).otherwise("open"))
        .withColumn("_cleaned_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ===== GOLD LAYER =====

# COMMAND ----------

@dlt.table(
    name="customer_360",
    comment="Unified Customer 360 profile - the crown jewel of the platform",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "customer_id"
    }
)
@dlt.expect("has_customer_id", "customer_id IS NOT NULL")
def customer_360():
    """
    Complete Customer 360 view.

    DLT automatically determines that this depends on:
    - clean_customers (direct read)
    - clean_transactions (through txn_agg)
    - clean_clickstream (through click_agg)
    - clean_support_tickets (through support_agg)

    And schedules execution accordingly. No manual orchestration needed!
    """

    customers = dlt.read("clean_customers")

    txn_agg = (
        dlt.read("clean_transactions")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("net_amount").alias("lifetime_revenue"),
            F.avg("net_amount").alias("avg_order_value"),
            F.max("transaction_date").alias("last_order_date"),
            F.datediff(F.current_date(), F.max("transaction_date")).alias("recency_days"),
        )
    )

    click_agg = (
        dlt.read("clean_clickstream")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("event_date").alias("active_days"),
            F.max("event_date").alias("last_active_date"),
        )
    )

    support_agg = (
        dlt.read("clean_support_tickets")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_tickets"),
            F.sum(F.when(F.col("status") == "open", 1).otherwise(0)).alias("open_tickets"),
        )
    )

    return (
        customers
        .join(txn_agg, "customer_id", "left")
        .join(click_agg, "customer_id", "left")
        .join(support_agg, "customer_id", "left")
        .fillna(0, subset=["total_orders", "lifetime_revenue", "total_events", "total_tickets"])
        .withColumn("customer_health",
            F.when(
                (F.col("lifetime_revenue") > 1000) & (F.col("open_tickets") == 0) & (F.col("recency_days") < 30),
                "healthy"
            ).when(
                (F.col("recency_days") > 90) | (F.col("open_tickets") > 2),
                "at_risk"
            ).otherwise("neutral")
        )
        .withColumn("_computed_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Dependency Graph (Auto-resolved by DLT)
# MAGIC
# MAGIC ```
# MAGIC raw_customers ──> clean_customers ──┐
# MAGIC raw_transactions -> clean_transactions ──> customer_360
# MAGIC raw_clickstream -> clean_clickstream ──┘       │
# MAGIC raw_support -> clean_support ─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC DLT visualizes this graph in the pipeline UI, showing:
# MAGIC - Data flow between tables
# MAGIC - Record counts at each stage
# MAGIC - Quality metrics (expectation pass rates)
# MAGIC - Processing time per table
