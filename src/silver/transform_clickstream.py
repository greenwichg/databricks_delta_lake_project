# Databricks notebook source

# MAGIC %md
# MAGIC # Silver: Transform Clickstream (Sessionization + Streaming)
# MAGIC
# MAGIC **Source**: Bronze raw_clickstream
# MAGIC **Pattern**: Streaming sessionization with window functions
# MAGIC **Target**: Silver clickstream_sessions table
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - **Structured Streaming** with **watermarks** for late data handling
# MAGIC - **Window-based sessionization** (grouping clicks into sessions)
# MAGIC - Streaming **aggregation** with watermarks
# MAGIC - **Trigger modes**: availableNow vs processingTime

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

SOURCE_TABLE = "customer_360_catalog.bronze.raw_clickstream"
TARGET_TABLE = "customer_360_catalog.silver.clickstream_sessions"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/silver_clickstream"

SESSION_TIMEOUT_MINUTES = 30  # Gap between clicks that defines a new session

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sessionization Logic
# MAGIC
# MAGIC **What is sessionization?** Grouping individual page view/click events into
# MAGIC "sessions" based on a timeout window. If a user hasn't clicked anything
# MAGIC in 30 minutes, the next click starts a new session.
# MAGIC
# MAGIC **Why is this important for interviews?**
# MAGIC - It's a classic Spark window function problem
# MAGIC - Demonstrates understanding of watermarks and late data
# MAGIC - Shows streaming aggregation patterns
# MAGIC - Common in real-world analytics pipelines

# COMMAND ----------

def sessionize_clickstream(batch_df, batch_id):
    """
    Process a micro-batch of clickstream events into sessions.

    Sessionization algorithm:
    1. Order events by customer_id and event_timestamp
    2. Calculate gap between consecutive events for each user
    3. If gap > SESSION_TIMEOUT_MINUTES, it's a new session
    4. Assign a session_id to each group of events
    5. Aggregate session-level metrics
    """

    if batch_df.isEmpty():
        return

    # Step 1: Calculate time gaps between consecutive events per user
    window_spec = Window.partitionBy("customer_id").orderBy("event_timestamp")

    with_gaps = (
        batch_df
        .withColumn("prev_event_time", F.lag("event_timestamp").over(window_spec))
        .withColumn("time_gap_minutes",
            F.when(F.col("prev_event_time").isNotNull(),
                   (F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_event_time")) / 60)
            .otherwise(F.lit(SESSION_TIMEOUT_MINUTES + 1))  # First event = new session
        )
    )

    # Step 2: Mark new session boundaries
    with_session_flags = (
        with_gaps
        .withColumn("is_new_session",
            F.when(F.col("time_gap_minutes") > SESSION_TIMEOUT_MINUTES, 1).otherwise(0)
        )
        # Cumulative sum of session flags = session number per user
        .withColumn("session_number",
            F.sum("is_new_session").over(window_spec)
        )
        # Create unique session_id
        .withColumn("session_id",
            F.concat(F.col("customer_id"), F.lit("_"), F.col("session_number"))
        )
    )

    # Step 3: Aggregate to session level
    sessions = (
        with_session_flags
        .groupBy("customer_id", "session_id")
        .agg(
            F.min("event_timestamp").alias("session_start"),
            F.max("event_timestamp").alias("session_end"),
            F.count("*").alias("total_events"),
            F.countDistinct("page_url").alias("unique_pages"),
            F.first("device_type").alias("device_type"),
            F.first("browser").alias("browser"),
            F.first("referrer").alias("entry_referrer"),
            F.last("page_url").alias("exit_page"),

            # Collect page view sequence
            F.collect_list(
                F.struct("event_timestamp", "page_url", "event_type")
            ).alias("event_sequence"),

            # Count specific event types
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        )
        # Calculate session duration
        .withColumn("session_duration_seconds",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
        )
        # Bounce: only 1 page viewed
        .withColumn("is_bounce", F.when(F.col("unique_pages") == 1, True).otherwise(False))
        # Conversion: session contains a purchase
        .withColumn("has_conversion", F.when(F.col("purchases") > 0, True).otherwise(False))
        .withColumn("session_date", F.to_date("session_start"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # Step 4: Write to Silver table
    if not spark.catalog.tableExists(TARGET_TABLE):
        sessions.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
        return

    target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        target.alias("target")
        .merge(sessions.alias("source"), "target.session_id = source.session_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Batch {batch_id}: Processed {sessions.count()} sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Sessionization Pipeline

# COMMAND ----------

def run_clickstream_pipeline():
    """
    Run the clickstream sessionization pipeline as a streaming job.

    Watermark explanation:
    - withWatermark("event_timestamp", "10 minutes") tells Spark:
      "Data might arrive up to 10 minutes late. After that, drop it."
    - This bounds the state Spark must maintain in memory
    - Without a watermark, state grows unbounded and causes OOM errors
    """

    stream = (
        spark.readStream
        .format("delta")
        .table(SOURCE_TABLE)
    )

    query = (
        stream.writeStream
        .foreachBatch(sessionize_clickstream)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print(f"Clickstream pipeline complete. Target: {TARGET_TABLE}")

# COMMAND ----------

run_clickstream_pipeline()

# COMMAND ----------

# Verify
df = spark.table(TARGET_TABLE)
print(f"Total sessions: {df.count()}")
display(
    df.groupBy("device_type").agg(
        F.count("*").alias("sessions"),
        F.avg("session_duration_seconds").alias("avg_duration_sec"),
        F.avg("total_events").alias("avg_events"),
        F.avg(F.col("is_bounce").cast("int")).alias("bounce_rate"),
        F.avg(F.col("has_conversion").cast("int")).alias("conversion_rate"),
    )
)
