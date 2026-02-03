# Databricks notebook source

# MAGIC %md
# MAGIC # Delta Lake Deep Dive: ACID, Time Travel, Schema Evolution, MERGE
# MAGIC
# MAGIC This notebook is a **hands-on reference** for all major Delta Lake features.
# MAGIC Run each section independently to explore the concepts.
# MAGIC
# MAGIC ## Topics Covered
# MAGIC 1. ACID Transactions
# MAGIC 2. Time Travel (versioning + restore)
# MAGIC 3. Schema Enforcement vs Schema Evolution
# MAGIC 4. MERGE / Upsert patterns
# MAGIC 5. Change Data Feed (CDF)
# MAGIC 6. Delta Table Utilities (DESCRIBE, HISTORY, VACUUM)

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. ACID Transactions
# MAGIC
# MAGIC **What makes Delta Lake ACID-compliant?**
# MAGIC
# MAGIC | Property | How Delta Achieves It |
# MAGIC |----------|----------------------|
# MAGIC | **Atomicity** | Transaction log (_delta_log) - writes are all-or-nothing |
# MAGIC | **Consistency** | Schema enforcement rejects mismatched data |
# MAGIC | **Isolation** | Optimistic concurrency control (snapshot isolation) |
# MAGIC | **Durability** | Data stored in Parquet files on cloud object storage |
# MAGIC
# MAGIC **The Transaction Log (_delta_log/)**
# MAGIC - JSON files recording every operation (add file, remove file, metadata change)
# MAGIC - Each commit creates a new JSON log entry (000000.json, 000001.json, ...)
# MAGIC - Every 10 commits, a checkpoint Parquet file is created for fast reads
# MAGIC - This log is the source of truth - not the data files themselves

# COMMAND ----------

# Examine the transaction log of a table
# MAGIC %sql
# MAGIC -- View the transaction log
# MAGIC DESCRIBE HISTORY customer_360_catalog.silver.customers;

# COMMAND ----------

# Programmatic access to history
history = (
    DeltaTable.forName(spark, "customer_360_catalog.silver.customers")
    .history()
)
display(history.select(
    "version", "timestamp", "operation", "operationParameters",
    "operationMetrics", "userName"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Time Travel
# MAGIC
# MAGIC **Query any previous version of a Delta table.** This is one of Delta's
# MAGIC most powerful features for debugging, auditing, and rollback.
# MAGIC
# MAGIC **Two ways to time travel:**
# MAGIC 1. By **version number**: `VERSION AS OF 5`
# MAGIC 2. By **timestamp**: `TIMESTAMP AS OF '2024-01-15'`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Historical Versions

# COMMAND ----------

# Time travel by VERSION NUMBER
df_v0 = (
    spark.read.format("delta")
    .option("versionAsOf", 0)  # Read the very first version
    .table("customer_360_catalog.silver.customers")
)
print(f"Version 0 had {df_v0.count()} records")

# COMMAND ----------

# Time travel by TIMESTAMP
df_yesterday = (
    spark.read.format("delta")
    .option("timestampAsOf", "2024-12-01")  # Read as of a specific date
    .table("customer_360_catalog.silver.customers")
)
print(f"As of 2024-12-01: {df_yesterday.count()} records")

# COMMAND ----------

# SQL syntax for time travel
# MAGIC %sql
# MAGIC -- Version-based time travel
# MAGIC SELECT COUNT(*) FROM customer_360_catalog.silver.customers VERSION AS OF 0;
# MAGIC
# MAGIC -- Timestamp-based time travel
# MAGIC SELECT COUNT(*) FROM customer_360_catalog.silver.customers TIMESTAMP AS OF '2024-12-01';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Versions (Debugging Changes)

# COMMAND ----------

def compare_versions(table_name, old_version, new_version):
    """
    Compare two versions of a Delta table to understand what changed.
    Extremely useful for debugging data quality issues.
    """

    old_df = (
        spark.read.format("delta")
        .option("versionAsOf", old_version)
        .table(table_name)
    )

    new_df = (
        spark.read.format("delta")
        .option("versionAsOf", new_version)
        .table(table_name)
    )

    print(f"Version {old_version}: {old_df.count()} rows")
    print(f"Version {new_version}: {new_df.count()} rows")
    print(f"Row diff: {new_df.count() - old_df.count()}")

    # Find added records
    added = new_df.subtract(old_df)
    print(f"Records added: {added.count()}")

    # Find removed records
    removed = old_df.subtract(new_df)
    print(f"Records removed: {removed.count()}")

    return added, removed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore to a Previous Version

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RESTORE a table to a previous version (undo changes!)
# MAGIC -- RESTORE TABLE customer_360_catalog.silver.customers TO VERSION AS OF 5;
# MAGIC
# MAGIC -- Or restore to a timestamp
# MAGIC -- RESTORE TABLE customer_360_catalog.silver.customers TO TIMESTAMP AS OF '2024-12-01';

# COMMAND ----------

# Programmatic restore
def restore_table(table_name, version):
    """
    Restore a Delta table to a specific version.
    This creates a NEW version that matches the old version's data.
    The history is preserved - nothing is deleted.
    """
    spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
    print(f"Restored {table_name} to version {version}")
    # Verify
    current_count = spark.table(table_name).count()
    print(f"Current row count after restore: {current_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Schema Enforcement & Evolution
# MAGIC
# MAGIC **Schema Enforcement** (default): Delta REJECTS writes that don't match the table schema.
# MAGIC **Schema Evolution**: Optionally ALLOW the schema to change over time.
# MAGIC
# MAGIC | Scenario | Enforcement (default) | Evolution (opt-in) |
# MAGIC |----------|----------------------|-------------------|
# MAGIC | New column in data | REJECTED ❌ | Added to table ✓ |
# MAGIC | Missing column | Filled with NULL ✓ | Filled with NULL ✓ |
# MAGIC | Type mismatch | REJECTED ❌ | REJECTED ❌ |
# MAGIC | Column order change | OK (matched by name) ✓ | OK ✓ |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Enforcement (default behavior)

# COMMAND ----------

def demonstrate_schema_enforcement():
    """
    Show how Delta Lake rejects schema mismatches by default.
    """

    # Create a sample table
    spark.sql("""
        CREATE OR REPLACE TABLE customer_360_catalog.silver.schema_demo (
            id INT,
            name STRING,
            value DOUBLE
        ) USING DELTA
    """)

    # This WORKS - columns match
    matching_data = spark.createDataFrame([(1, "Alice", 100.0)], ["id", "name", "value"])
    matching_data.write.format("delta").mode("append").saveAsTable(
        "customer_360_catalog.silver.schema_demo"
    )
    print("Matching schema: Write succeeded ✓")

    # This FAILS - extra column 'new_col' not in the table schema
    try:
        extra_col_data = spark.createDataFrame(
            [(2, "Bob", 200.0, "extra")], ["id", "name", "value", "new_col"]
        )
        extra_col_data.write.format("delta").mode("append").saveAsTable(
            "customer_360_catalog.silver.schema_demo"
        )
    except Exception as e:
        print(f"Schema enforcement rejected write: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution (opt-in)

# COMMAND ----------

def demonstrate_schema_evolution():
    """
    Show how to enable schema evolution to allow adding new columns.
    """

    # Option 1: mergeSchema on write
    extra_col_data = spark.createDataFrame(
        [(3, "Charlie", 300.0, "new_value")], ["id", "name", "value", "category"]
    )

    extra_col_data.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("customer_360_catalog.silver.schema_demo")
    print("Schema evolution: New column 'category' added ✓")

    # Option 2: Set at session level
    # spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Option 3: ALTER TABLE for manual schema changes
    # spark.sql("ALTER TABLE schema_demo ADD COLUMNS (new_col STRING)")

    # Verify the evolved schema
    display(spark.table("customer_360_catalog.silver.schema_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. MERGE Patterns (Complete Reference)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Simple Upsert (Insert + Update)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_360_catalog.silver.customers AS target
# MAGIC USING (SELECT * FROM customer_360_catalog.bronze.raw_customers) AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Conditional Update

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Only update if source is newer
# MAGIC MERGE INTO customer_360_catalog.silver.customers AS target
# MAGIC USING source_data AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED AND source.updated_date > target.updated_date
# MAGIC   THEN UPDATE SET
# MAGIC     target.email = source.email,
# MAGIC     target.loyalty_tier = source.loyalty_tier,
# MAGIC     target.updated_date = source.updated_date
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3: Full CDC (Insert + Update + Delete)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Handle all CDC operations in a single MERGE
# MAGIC MERGE INTO silver_table AS target
# MAGIC USING (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY cdc_timestamp DESC) as rn
# MAGIC     FROM bronze_cdc_table
# MAGIC ) AS source
# MAGIC ON target.id = source.id AND source.rn = 1
# MAGIC WHEN MATCHED AND source.cdc_operation = 'delete'
# MAGIC   THEN DELETE
# MAGIC WHEN MATCHED AND source.cdc_operation IN ('insert', 'update')
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED AND source.cdc_operation != 'delete'
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 4: MERGE with Schema Evolution

# COMMAND ----------

# Enable schema evolution during MERGE
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Or per-operation (PySpark):
# target.alias("t").merge(source.alias("s"), "t.id = s.id") \
#     .whenMatchedUpdateAll() \
#     .whenNotMatchedInsertAll() \
#     .execute()
# With: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Change Data Feed (CDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable CDF on a table
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# Read the change feed
changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    # .option("startingTimestamp", "2024-01-01")
    .table("customer_360_catalog.silver.customers")
)

display(changes.select(
    "customer_id", "_change_type", "_commit_version", "_commit_timestamp"
).orderBy("_commit_version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Table Maintenance Utilities

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE DETAIL: Full metadata about a Delta table
# MAGIC DESCRIBE DETAIL customer_360_catalog.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM: Remove old data files no longer referenced by the transaction log
# MAGIC -- Default retention: 7 days (168 hours)
# MAGIC -- WARNING: After VACUUM, time travel to versions before the retention period is impossible
# MAGIC
# MAGIC -- Dry run (show what would be deleted):
# MAGIC VACUUM customer_360_catalog.silver.customers DRY RUN;
# MAGIC
# MAGIC -- Actually vacuum (retain 168 hours of history):
# MAGIC -- VACUUM customer_360_catalog.silver.customers RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM Explained (Interview Question)
# MAGIC
# MAGIC **What does VACUUM do?**
# MAGIC - Removes data files that are no longer referenced by the Delta log
# MAGIC - These files are from old versions (after updates/deletes/merges)
# MAGIC - Reduces storage costs
# MAGIC
# MAGIC **What's the retention period?**
# MAGIC - Default: 7 days (168 hours)
# MAGIC - Can be customized: `VACUUM table RETAIN 720 HOURS` (30 days)
# MAGIC - **WARNING**: Setting < 7 days requires `delta.retentionDurationCheck.enabled = false`
# MAGIC
# MAGIC **Impact on Time Travel?**
# MAGIC - After VACUUM, you CANNOT time travel to versions whose files were deleted
# MAGIC - Time travel only works within the retention window
# MAGIC
# MAGIC **When to run?**
# MAGIC - Schedule weekly or after large MERGE/UPDATE operations
# MAGIC - Run during off-peak hours
