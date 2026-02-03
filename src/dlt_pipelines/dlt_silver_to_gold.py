# Databricks notebook source

# MAGIC %md
# MAGIC # DLT Pipeline: Silver to Gold
# MAGIC ## Materialized Views and Aggregation Tables
# MAGIC
# MAGIC **DLT Table Types:**
# MAGIC - **Streaming Table** (`dlt.read_stream`): Append-only, processes new data incrementally
# MAGIC - **Materialized View** (`dlt.read`): Fully recomputed on each update (like a batch view)
# MAGIC
# MAGIC Gold layer typically uses **Materialized Views** because aggregations need
# MAGIC to be recomputed when underlying data changes.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer 360 View (Materialized View)

# COMMAND ----------

@dlt.table(
    name="gold_customer_360",
    comment="Unified customer profile combining all data sources",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "customer_id"  # Auto Z-ORDER
    }
)
@dlt.expect("has_customer_id", "customer_id IS NOT NULL")
def gold_customer_360():
    """
    Build Customer 360 as a DLT materialized view.

    Using dlt.read() (not dlt.read_stream()) makes this a materialized view:
    - Fully recomputed on each pipeline run
    - Appropriate for aggregations and multi-source joins
    - DLT handles incremental refresh optimization internally
    """

    customers = dlt.read("silver_customers")

    # Transaction aggregates
    txn_metrics = (
        dlt.read("silver_transactions")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("net_amount").alias("total_spend"),
            F.avg("net_amount").alias("avg_order_value"),
            F.max("transaction_date").alias("last_purchase_date"),
            F.datediff(F.current_date(), F.max("transaction_date")).alias("days_since_last_purchase"),
        )
    )

    # Clickstream aggregates
    click_metrics = (
        dlt.read("silver_clickstream_events")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("event_date").alias("active_days"),
            F.max("event_date").alias("last_active_date"),
        )
    )

    # Build 360
    return (
        customers
        .join(txn_metrics, "customer_id", "left")
        .join(click_metrics, "customer_id", "left")
        .fillna(0, subset=["total_transactions", "total_spend", "total_events"])
        .withColumn("customer_value_tier",
            F.when(F.col("total_spend") > 5000, "high")
            .when(F.col("total_spend") > 1000, "medium")
            .otherwise("low")
        )
        .withColumn("_gold_computed_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue Analytics (Materialized View)

# COMMAND ----------

@dlt.table(
    name="gold_revenue_daily",
    comment="Daily revenue aggregations by category"
)
def gold_revenue_daily():
    """Daily revenue metrics - materialized view pattern."""

    return (
        dlt.read("silver_transactions")
        .groupBy(
            "transaction_date",
            "product_category",
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("net_amount").alias("net_revenue"),
            F.avg("net_amount").alias("avg_order_value"),
        )
        .withColumn("_gold_computed_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Segments (Materialized View)

# COMMAND ----------

@dlt.table(
    name="gold_customer_segments",
    comment="RFM-based customer segmentation"
)
@dlt.expect_or_drop("has_purchases", "total_transactions > 0")
def gold_customer_segments():
    """
    RFM segmentation computed on each pipeline run.
    Expectations ensure we only segment customers with actual purchase history.
    """

    c360 = dlt.read("gold_customer_360")

    return (
        c360
        .filter("total_transactions > 0")
        .withColumn("recency_score",
            6 - F.ntile(5).over(Window.orderBy("days_since_last_purchase")))
        .withColumn("frequency_score",
            F.ntile(5).over(Window.orderBy("total_transactions")))
        .withColumn("monetary_score",
            F.ntile(5).over(Window.orderBy("total_spend")))
        .withColumn("segment",
            F.when(
                (F.col("recency_score") >= 4) & (F.col("frequency_score") >= 4) & (F.col("monetary_score") >= 4),
                "Champions"
            )
            .when(
                (F.col("recency_score") <= 2) & (F.col("frequency_score") >= 3),
                "At Risk"
            )
            .when(
                (F.col("recency_score") <= 2) & (F.col("frequency_score") <= 2),
                "Lost"
            )
            .otherwise("Active")
        )
        .select(
            "customer_id", "full_name", "loyalty_tier",
            "recency_score", "frequency_score", "monetary_score", "segment",
            "total_spend", "total_transactions",
        )
        .withColumn("_segmented_at", F.current_timestamp())
    )
