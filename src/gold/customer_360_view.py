# Databricks notebook source

# MAGIC %md
# MAGIC # Gold: Customer 360 Unified View
# MAGIC
# MAGIC **Source**: All Silver tables (customers, transactions, clickstream, support)
# MAGIC **Pattern**: Multi-source JOIN + aggregation into a single customer profile
# MAGIC **Target**: Gold customer_360 table - the single source of truth per customer
# MAGIC
# MAGIC ## What is Customer 360?
# MAGIC A **complete, unified view** of each customer combining data from all touchpoints:
# MAGIC - CRM profile (demographics, loyalty tier)
# MAGIC - Transaction history (spend, frequency, recency)
# MAGIC - Digital behavior (sessions, page views, engagement)
# MAGIC - Support interactions (tickets, resolution, satisfaction)
# MAGIC
# MAGIC This is the most valuable table in the platform - it powers dashboards,
# MAGIC segmentation, personalization, and ML models.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# Source tables
SILVER_CUSTOMERS = "customer_360_catalog.silver.customers"
SILVER_TRANSACTIONS = "customer_360_catalog.silver.transactions"
SILVER_CLICKSTREAM = "customer_360_catalog.silver.clickstream_sessions"
SILVER_SUPPORT = "customer_360_catalog.silver.support_tickets"

# Target
GOLD_TABLE = "customer_360_catalog.gold.customer_360"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Aggregate Transaction Metrics per Customer

# COMMAND ----------

def get_transaction_metrics():
    """Aggregate transaction data to customer level."""

    txn = spark.table(SILVER_TRANSACTIONS)

    return (
        txn
        .groupBy("customer_id")
        .agg(
            # Recency
            F.max("transaction_date").alias("last_transaction_date"),
            F.datediff(F.current_date(), F.max("transaction_date")).alias("days_since_last_purchase"),

            # Frequency
            F.count("transaction_id").alias("total_transactions"),
            F.countDistinct(F.date_format("transaction_date", "yyyy-MM")).alias("active_months"),

            # Monetary
            F.sum("net_amount").alias("total_spend"),
            F.avg("net_amount").alias("avg_transaction_value"),
            F.max("net_amount").alias("max_transaction_value"),
            F.min("net_amount").alias("min_transaction_value"),
            F.stddev("net_amount").alias("stddev_transaction_value"),

            # Product preferences
            F.first("product_category", ignorenulls=True).alias("top_category"),
            F.countDistinct("product_category").alias("unique_categories_purchased"),

            # Payment preferences
            F.first("payment_method", ignorenulls=True).alias("preferred_payment_method"),

            # Time patterns
            F.avg(F.dayofweek("transaction_date")).alias("avg_purchase_day_of_week"),

            # Recent activity (last 30 days)
            F.sum(
                F.when(F.col("transaction_date") >= F.date_sub(F.current_date(), 30),
                       F.col("net_amount")).otherwise(0)
            ).alias("spend_last_30_days"),
            F.sum(
                F.when(F.col("transaction_date") >= F.date_sub(F.current_date(), 30), 1).otherwise(0)
            ).alias("transactions_last_30_days"),

            # 90-day activity
            F.sum(
                F.when(F.col("transaction_date") >= F.date_sub(F.current_date(), 90),
                       F.col("net_amount")).otherwise(0)
            ).alias("spend_last_90_days"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Aggregate Clickstream Metrics per Customer

# COMMAND ----------

def get_clickstream_metrics():
    """Aggregate clickstream session data to customer level."""

    clicks = spark.table(SILVER_CLICKSTREAM)

    return (
        clicks
        .groupBy("customer_id")
        .agg(
            # Session metrics
            F.count("session_id").alias("total_sessions"),
            F.avg("session_duration_seconds").alias("avg_session_duration_seconds"),
            F.avg("total_events").alias("avg_events_per_session"),
            F.avg("unique_pages").alias("avg_pages_per_session"),

            # Engagement metrics
            F.sum("page_views").alias("total_page_views"),
            F.sum("clicks").alias("total_clicks"),
            F.sum("add_to_carts").alias("total_add_to_carts"),
            F.sum("purchases").alias("total_web_purchases"),

            # Behavior patterns
            F.avg(F.col("is_bounce").cast("int")).alias("bounce_rate"),
            F.avg(F.col("has_conversion").cast("int")).alias("conversion_rate"),

            # Device and browser
            F.first("device_type").alias("primary_device"),
            F.first("browser").alias("primary_browser"),

            # Recency
            F.max("session_date").alias("last_session_date"),
            F.datediff(F.current_date(), F.max("session_date")).alias("days_since_last_session"),

            # Recent activity
            F.sum(
                F.when(F.col("session_date") >= F.date_sub(F.current_date(), 30), 1).otherwise(0)
            ).alias("sessions_last_30_days"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Aggregate Support Metrics per Customer

# COMMAND ----------

def get_support_metrics():
    """Aggregate support ticket data to customer level."""

    support = spark.table(SILVER_SUPPORT)

    return (
        support
        .filter("status != 'deleted'")
        .groupBy("customer_id")
        .agg(
            F.count("ticket_id").alias("total_tickets"),

            # Ticket status breakdown
            F.sum(F.when(F.col("status") == "open", 1).otherwise(0)).alias("open_tickets"),
            F.sum(F.when(F.col("status") == "resolved", 1).otherwise(0)).alias("resolved_tickets"),
            F.sum(F.when(F.col("status") == "closed", 1).otherwise(0)).alias("closed_tickets"),

            # Priority breakdown
            F.sum(F.when(F.col("priority") == "critical", 1).otherwise(0)).alias("critical_tickets"),
            F.sum(F.when(F.col("priority") == "high", 1).otherwise(0)).alias("high_priority_tickets"),

            # Resolution metrics
            F.avg("resolution_hours").alias("avg_resolution_hours"),
            F.max("resolution_hours").alias("max_resolution_hours"),

            # Recency
            F.max("created_at").alias("last_ticket_date"),
            F.datediff(F.current_date(), F.max("created_at")).alias("days_since_last_ticket"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build the Customer 360 View

# COMMAND ----------

def build_customer_360():
    """
    Join all customer metrics into a single unified profile.

    This is a LEFT JOIN from the customer dimension (Silver customers)
    to all fact aggregations, ensuring every customer appears even if
    they have no transactions, sessions, or tickets.
    """

    customers = spark.table(SILVER_CUSTOMERS)
    txn_metrics = get_transaction_metrics()
    click_metrics = get_clickstream_metrics()
    support_metrics = get_support_metrics()

    # Build the 360 view with LEFT JOINs
    customer_360 = (
        customers
        .join(txn_metrics, "customer_id", "left")
        .join(click_metrics, "customer_id", "left")
        .join(support_metrics, "customer_id", "left")

        # Fill nulls for customers with no activity
        .fillna(0, subset=[
            "total_transactions", "total_spend", "total_sessions",
            "total_page_views", "total_tickets", "open_tickets",
        ])

        # Compute derived scores
        .withColumn("engagement_score",
            # Simple engagement score: weighted combo of activity metrics
            (F.coalesce(F.col("total_transactions"), F.lit(0)) * 10 +
             F.coalesce(F.col("total_sessions"), F.lit(0)) * 2 +
             F.coalesce(F.col("total_page_views"), F.lit(0)) * 0.5) /
            F.greatest(F.col("account_age_days"), F.lit(1))  # Normalize by account age
        )

        .withColumn("health_score",
            # Customer health: high spend + engagement - support issues
            F.when(
                (F.col("total_spend") > 1000) & (F.col("open_tickets") == 0) &
                (F.col("days_since_last_purchase") < 30),
                F.lit("healthy")
            )
            .when(
                (F.col("days_since_last_purchase") > 90) | (F.col("critical_tickets") > 0),
                F.lit("at_risk")
            )
            .otherwise(F.lit("neutral"))
        )

        # Add Gold layer metadata
        .withColumn("_gold_updated_at", F.current_timestamp())
        .withColumn("_gold_version", F.lit(1))
    )

    # Write using MERGE for idempotent updates
    if not spark.catalog.tableExists(GOLD_TABLE):
        customer_360.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
    else:
        target = DeltaTable.forName(spark, GOLD_TABLE)
        (
            target.alias("target")
            .merge(customer_360.alias("source"), "target.customer_id = source.customer_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"Customer 360 built: {customer_360.count()} profiles in {GOLD_TABLE}")
    return customer_360

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

customer_360 = build_customer_360()

# COMMAND ----------

# Summary stats
display(
    customer_360.select(
        F.count("*").alias("total_customers"),
        F.avg("total_spend").alias("avg_lifetime_spend"),
        F.avg("total_transactions").alias("avg_transactions"),
        F.avg("total_sessions").alias("avg_sessions"),
        F.avg("engagement_score").alias("avg_engagement"),
    )
)

# COMMAND ----------

# Health score distribution
display(customer_360.groupBy("health_score").count().orderBy("count", ascending=False))
