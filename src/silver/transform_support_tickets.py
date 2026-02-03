# Databricks notebook source

# MAGIC %md
# MAGIC # Silver: Transform Support Tickets (CDC Apply + Enrichment)
# MAGIC
# MAGIC **Source**: Bronze raw_support_tickets (with CDC operations)
# MAGIC **Pattern**: Apply CDC operations from Change Data Feed
# MAGIC **Target**: Silver support_tickets table (current state)
# MAGIC
# MAGIC ## Key Concepts
# MAGIC - **Applying CDC changes** using MERGE (insert/update/delete)
# MAGIC - **Handling delete operations** in the lakehouse
# MAGIC - **Stream-static join** for enrichment
# MAGIC - **Schema enforcement** during the Silver write

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

SOURCE_TABLE = "customer_360_catalog.bronze.raw_support_tickets"
TARGET_TABLE = "customer_360_catalog.silver.support_tickets"
CHECKPOINT_PATH = "/Volumes/customer_360_catalog/checkpoints/silver_support"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying CDC Operations
# MAGIC
# MAGIC The Bronze table contains rows with `cdc_operation`:
# MAGIC - `"insert"`: New ticket created
# MAGIC - `"update_postimage"`: Ticket was updated (new values)
# MAGIC - `"delete"`: Ticket was removed
# MAGIC
# MAGIC We apply these operations using MERGE to maintain the current state in Silver.

# COMMAND ----------

def apply_cdc_changes(batch_df, batch_id):
    """
    Apply CDC operations from Bronze to Silver using MERGE.

    CDC application logic:
    1. Filter to only keep actionable operations (insert, update_postimage, delete)
    2. For each ticket_id, take the latest operation (by cdc_timestamp)
    3. Apply the operation:
       - insert/update_postimage → UPSERT into Silver
       - delete → DELETE from Silver (or soft delete)
    """

    if batch_df.isEmpty():
        return

    # Step 1: Keep only actionable operations (skip update_preimage)
    actionable = batch_df.filter(
        F.col("cdc_operation").isin("insert", "update_postimage", "delete")
    )

    # Step 2: Deduplicate - keep latest operation per ticket_id
    dedup_window = Window.partitionBy("ticket_id").orderBy(
        F.col("cdc_timestamp").desc(),
        F.col("_ingested_at").desc()
    )

    latest_changes = (
        actionable
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter("_rn = 1")
        .drop("_rn")
    )

    # Split into upserts and deletes
    upserts = latest_changes.filter(
        F.col("cdc_operation").isin("insert", "update_postimage")
    )

    deletes = latest_changes.filter(F.col("cdc_operation") == "delete")

    # Step 3: Clean and transform upserts
    cleaned_upserts = (
        upserts
        .withColumn("priority",
            F.when(F.col("priority").isin("low", "medium", "high", "critical"),
                   F.col("priority"))
            .otherwise("medium")
        )
        .withColumn("status",
            F.when(F.col("status").isin("open", "in_progress", "resolved", "closed"),
                   F.col("status"))
            .otherwise("open")
        )
        # Calculate resolution time if resolved/closed
        .withColumn("resolution_hours",
            F.when(
                F.col("status").isin("resolved", "closed") & F.col("resolved_at").isNotNull(),
                (F.unix_timestamp("resolved_at") - F.unix_timestamp("created_at")) / 3600
            )
        )
        # Categorize resolution speed
        .withColumn("resolution_speed",
            F.when(F.col("resolution_hours") < 1, "fast")
            .when(F.col("resolution_hours") < 24, "normal")
            .when(F.col("resolution_hours") < 72, "slow")
            .when(F.col("resolution_hours").isNotNull(), "very_slow")
            .otherwise("unresolved")
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("cdc_operation", "cdc_version", "cdc_timestamp", "_rescued_data")
    )

    # Step 4: Apply MERGE
    if not spark.catalog.tableExists(TARGET_TABLE):
        cleaned_upserts.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
        return

    target = DeltaTable.forName(spark, TARGET_TABLE)

    # Merge upserts
    if cleaned_upserts.count() > 0:
        (
            target.alias("target")
            .merge(cleaned_upserts.alias("source"),
                   "target.ticket_id = source.ticket_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # Apply deletes (soft delete by marking as deleted, preserving audit trail)
    if deletes.count() > 0:
        delete_ids = deletes.select("ticket_id")
        (
            target.alias("target")
            .merge(delete_ids.alias("source"),
                   "target.ticket_id = source.ticket_id")
            .whenMatchedUpdate(set={
                "status": F.lit("deleted"),
                "_silver_processed_at": F.current_timestamp()
            })
            .execute()
        )

    print(f"Batch {batch_id}: Applied {cleaned_upserts.count()} upserts, "
          f"{deletes.count()} deletes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run CDC Pipeline

# COMMAND ----------

def run_support_pipeline():
    stream = (
        spark.readStream
        .format("delta")
        .table(SOURCE_TABLE)
    )

    query = (
        stream.writeStream
        .foreachBatch(apply_cdc_changes)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print(f"Support ticket pipeline complete. Target: {TARGET_TABLE}")

# COMMAND ----------

run_support_pipeline()

# COMMAND ----------

# Verify
df = spark.table(TARGET_TABLE)
print(f"Total Silver tickets: {df.count()}")
display(
    df.groupBy("status", "priority").agg(
        F.count("*").alias("count"),
        F.avg("resolution_hours").alias("avg_resolution_hours")
    ).orderBy("priority", "status")
)
