# Databricks notebook source

# MAGIC %md
# MAGIC # Gold: Revenue Analytics
# MAGIC
# MAGIC **Source**: Silver transactions + customers
# MAGIC **Target**: Revenue rollups by time period, category, and segment
# MAGIC
# MAGIC Demonstrates: Gold-layer aggregation patterns, Delta MERGE for incremental updates

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

SILVER_TRANSACTIONS = "customer_360_catalog.silver.transactions"
SILVER_CUSTOMERS = "customer_360_catalog.silver.customers"
TARGET_TABLE = "customer_360_catalog.gold.revenue_analytics"

# COMMAND ----------

def build_revenue_analytics():
    """
    Build daily revenue analytics table with multi-dimensional rollups.
    This table powers executive dashboards and financial reporting.
    """

    txn = spark.table(SILVER_TRANSACTIONS)
    customers = spark.table(SILVER_CUSTOMERS).select(
        "customer_id", "loyalty_tier", "state", "country"
    )

    # Join transactions with customer dimensions
    enriched = txn.join(customers, "customer_id", "left")

    # Daily revenue by category + segment
    revenue = (
        enriched
        .groupBy(
            "transaction_date",
            "product_category",
            "payment_method",
            "loyalty_tier",
            "country",
        )
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("gross_amount").alias("gross_revenue"),
            F.sum("discount_amount").alias("total_discounts"),
            F.sum("net_amount").alias("net_revenue"),
            F.avg("net_amount").alias("avg_order_value"),
            F.sum("quantity").alias("total_units_sold"),
        )
        # Add running totals
        .withColumn("cumulative_revenue",
            F.sum("net_revenue").over(
                Window.partitionBy("product_category")
                .orderBy("transaction_date")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
        )
        # MoM and YoY comparisons can be computed downstream
        .withColumn("_gold_updated_at", F.current_timestamp())
    )

    # Write with MERGE for incremental updates
    if not spark.catalog.tableExists(TARGET_TABLE):
        revenue.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    else:
        target = DeltaTable.forName(spark, TARGET_TABLE)
        (
            target.alias("t")
            .merge(
                revenue.alias("s"),
                """t.transaction_date = s.transaction_date
                   AND t.product_category = s.product_category
                   AND t.payment_method = s.payment_method
                   AND t.loyalty_tier = s.loyalty_tier
                   AND t.country = s.country"""
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"Revenue analytics updated: {TARGET_TABLE}")

# COMMAND ----------

build_revenue_analytics()

# COMMAND ----------

# Top categories by revenue
display(
    spark.table(TARGET_TABLE)
    .groupBy("product_category")
    .agg(F.sum("net_revenue").alias("total_revenue"))
    .orderBy("total_revenue", ascending=False)
)
