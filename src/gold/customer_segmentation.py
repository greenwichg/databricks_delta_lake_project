# Databricks notebook source

# MAGIC %md
# MAGIC # Gold: Customer Segmentation (RFM Analysis)
# MAGIC
# MAGIC **Source**: Gold customer_360
# MAGIC **Pattern**: RFM scoring for customer segmentation
# MAGIC **Target**: Gold customer_segments table
# MAGIC
# MAGIC ## RFM Model
# MAGIC - **Recency**: How recently did the customer purchase? (lower = better)
# MAGIC - **Frequency**: How often do they purchase? (higher = better)
# MAGIC - **Monetary**: How much do they spend? (higher = better)
# MAGIC
# MAGIC Each dimension gets a 1-5 score. Combined, they create segments like
# MAGIC "Champions" (5-5-5), "At Risk" (1-1-5), "Lost" (1-1-1), etc.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

SOURCE_TABLE = "customer_360_catalog.gold.customer_360"
TARGET_TABLE = "customer_360_catalog.gold.customer_segments"

# COMMAND ----------

def build_rfm_segments():
    """
    Build RFM-based customer segments from the Customer 360 view.

    Scoring uses percentile-based binning:
    - Score 5 = top 20% (best)
    - Score 1 = bottom 20% (worst)
    """

    df = spark.table(SOURCE_TABLE)

    # Calculate RFM percentiles using ntile
    rfm = (
        df
        .filter(F.col("total_transactions") > 0)  # Only customers with purchases
        .withColumn("recency_score",
            # Lower days_since_last_purchase = higher score
            6 - F.ntile(5).over(
                Window.orderBy("days_since_last_purchase")
            )
        )
        .withColumn("frequency_score",
            F.ntile(5).over(
                Window.orderBy("total_transactions")
            )
        )
        .withColumn("monetary_score",
            F.ntile(5).over(
                Window.orderBy("total_spend")
            )
        )
        # Combined RFM score
        .withColumn("rfm_score",
            F.concat(
                F.col("recency_score").cast("string"),
                F.col("frequency_score").cast("string"),
                F.col("monetary_score").cast("string"),
            )
        )
        # Map to business segments
        .withColumn("segment",
            F.when(
                (F.col("recency_score") >= 4) & (F.col("frequency_score") >= 4) & (F.col("monetary_score") >= 4),
                "Champions"
            )
            .when(
                (F.col("recency_score") >= 3) & (F.col("frequency_score") >= 3) & (F.col("monetary_score") >= 4),
                "Loyal Customers"
            )
            .when(
                (F.col("recency_score") >= 4) & (F.col("frequency_score") <= 2),
                "New Customers"
            )
            .when(
                (F.col("recency_score") >= 3) & (F.col("frequency_score") >= 3) & (F.col("monetary_score") <= 2),
                "Promising"
            )
            .when(
                (F.col("recency_score") <= 2) & (F.col("frequency_score") >= 3) & (F.col("monetary_score") >= 3),
                "At Risk"
            )
            .when(
                (F.col("recency_score") <= 2) & (F.col("frequency_score") >= 4) & (F.col("monetary_score") >= 4),
                "Can't Lose Them"
            )
            .when(
                (F.col("recency_score") <= 2) & (F.col("frequency_score") <= 2),
                "Lost"
            )
            .otherwise("Needs Attention")
        )
        .select(
            "customer_id", "full_name", "email", "loyalty_tier",
            "days_since_last_purchase", "total_transactions", "total_spend",
            "recency_score", "frequency_score", "monetary_score",
            "rfm_score", "segment", "engagement_score", "health_score",
        )
        .withColumn("_segmented_at", F.current_timestamp())
    )

    # Write segments
    if not spark.catalog.tableExists(TARGET_TABLE):
        rfm.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    else:
        target = DeltaTable.forName(spark, TARGET_TABLE)
        (
            target.alias("t")
            .merge(rfm.alias("s"), "t.customer_id = s.customer_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"Segmentation complete: {rfm.count()} customers segmented")
    return rfm

# Need Window import
from pyspark.sql.window import Window

# COMMAND ----------

segments = build_rfm_segments()

# COMMAND ----------

# Segment distribution
display(
    segments.groupBy("segment").agg(
        F.count("*").alias("customer_count"),
        F.avg("total_spend").alias("avg_spend"),
        F.avg("total_transactions").alias("avg_transactions"),
    ).orderBy("customer_count", ascending=False)
)
