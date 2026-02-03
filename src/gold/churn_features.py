# Databricks notebook source

# MAGIC %md
# MAGIC # Gold: Churn Prediction Feature Table
# MAGIC
# MAGIC **Source**: Gold customer_360 + segments
# MAGIC **Target**: ML-ready feature table for churn prediction
# MAGIC
# MAGIC This demonstrates how Gold tables serve as **Feature Tables** for ML models,
# MAGIC a common pattern in Databricks with Feature Store / Unity Catalog.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

SOURCE_360 = "customer_360_catalog.gold.customer_360"
SOURCE_SEGMENTS = "customer_360_catalog.gold.customer_segments"
TARGET_TABLE = "customer_360_catalog.gold.churn_features"

# COMMAND ----------

def build_churn_features():
    """
    Build ML feature table for churn prediction.

    Features are derived from Customer 360 data and designed for
    a binary classification model (churned vs active).

    Churn definition: No purchase in the last 90 days.
    """

    c360 = spark.table(SOURCE_360)

    features = (
        c360
        # Target variable
        .withColumn("is_churned",
            F.when(F.col("days_since_last_purchase") > 90, 1).otherwise(0)
        )

        # Feature engineering
        .withColumn("purchase_frequency_monthly",
            F.col("total_transactions") / F.greatest(F.col("account_age_days") / 30, F.lit(1))
        )
        .withColumn("avg_monthly_spend",
            F.col("total_spend") / F.greatest(F.col("account_age_days") / 30, F.lit(1))
        )
        .withColumn("spend_trend",
            # Ratio of recent spend to historical average
            F.col("spend_last_90_days") / F.greatest(F.col("total_spend") / 4, F.lit(1))
        )
        .withColumn("support_intensity",
            # Tickets per transaction (high = problematic)
            F.col("total_tickets") / F.greatest(F.col("total_transactions"), F.lit(1))
        )
        .withColumn("has_open_tickets",
            F.when(F.col("open_tickets") > 0, 1).otherwise(0)
        )
        .withColumn("digital_engagement_ratio",
            # Sessions per transaction
            F.col("total_sessions") / F.greatest(F.col("total_transactions"), F.lit(1))
        )

        .select(
            "customer_id",
            # Target
            "is_churned",
            # Recency features
            "days_since_last_purchase",
            "days_since_last_session",
            "days_since_last_ticket",
            # Frequency features
            "total_transactions", "purchase_frequency_monthly",
            "total_sessions", "sessions_last_30_days",
            # Monetary features
            "total_spend", "avg_transaction_value", "avg_monthly_spend",
            "spend_last_30_days", "spend_last_90_days", "spend_trend",
            # Engagement features
            "bounce_rate", "conversion_rate", "engagement_score",
            "digital_engagement_ratio",
            # Support features
            "total_tickets", "open_tickets", "critical_tickets",
            "avg_resolution_hours", "support_intensity", "has_open_tickets",
            # Profile features
            "loyalty_tier", "account_age_days",
        )
        .withColumn("_feature_updated_at", F.current_timestamp())
    )

    # Write as a feature table
    if not spark.catalog.tableExists(TARGET_TABLE):
        features.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    else:
        target = DeltaTable.forName(spark, TARGET_TABLE)
        (
            target.alias("t")
            .merge(features.alias("s"), "t.customer_id = s.customer_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"Churn features built: {features.count()} records")

    # Print feature summary
    churn_rate = features.agg(F.avg("is_churned")).collect()[0][0]
    print(f"Overall churn rate: {churn_rate:.2%}")

# COMMAND ----------

build_churn_features()
