# Databricks notebook source

# MAGIC %md
# MAGIC # Silver: Transform CRM Customers (CDC MERGE + SCD Type 2)
# MAGIC
# MAGIC **Source**: Bronze raw_customers
# MAGIC **Pattern**: MERGE INTO for upserts + SCD Type 2 for history tracking
# MAGIC **Target**: Silver customers table (current + historical records)
# MAGIC
# MAGIC ## Key Concepts Demonstrated
# MAGIC - **MERGE INTO** (upsert) - the core Delta Lake CDC pattern
# MAGIC - **SCD Type 2** - Slowly Changing Dimensions with effective dates
# MAGIC - **Schema enforcement** - Delta rejects mismatched schemas by default
# MAGIC - **ACID transactions** - MERGE is atomic (all-or-nothing)
# MAGIC - **Data quality checks** before and after the merge

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

SOURCE_TABLE = "customer_360_catalog.bronze.raw_customers"
TARGET_TABLE = "customer_360_catalog.silver.customers"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read and Deduplicate Bronze Data
# MAGIC
# MAGIC Bronze is append-only, so we may have duplicate records from re-deliveries.
# MAGIC We deduplicate by taking the latest record per customer_id.

# COMMAND ----------

def get_latest_bronze_customers():
    """
    Read Bronze customer data and deduplicate.

    Dedup strategy: For each customer_id, keep only the most recent record
    based on updated_date (source timestamp) and _ingested_at (processing time).
    """

    bronze_df = spark.table(SOURCE_TABLE)

    # Deduplication window: partition by customer_id, order by freshness
    dedup_window = Window.partitionBy("customer_id").orderBy(
        F.col("updated_date").desc(),
        F.col("_ingested_at").desc()
    )

    deduped = (
        bronze_df
        .withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    return deduped

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Clean and Validate Data

# COMMAND ----------

def clean_customers(df):
    """
    Apply data cleaning and standardization transformations.

    This is where Silver layer adds value over Bronze:
    - Standardize formats (emails, phone numbers, names)
    - Derive computed columns
    - Apply business rules
    - Drop raw/audit columns not needed downstream
    """

    cleaned = (
        df
        # Standardize email to lowercase
        .withColumn("email", F.lower(F.trim(F.col("email"))))

        # Standardize names
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))

        # Create full name
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))

        # Standardize phone (remove non-digits)
        .withColumn("phone", F.regexp_replace(F.col("phone"), "[^0-9+]", ""))

        # Validate and standardize loyalty tier
        .withColumn("loyalty_tier",
            F.when(F.col("loyalty_tier").isin("bronze", "silver", "gold", "platinum"),
                   F.col("loyalty_tier"))
            .otherwise("unknown")
        )

        # Extract address components
        .withColumn("city", F.col("address.city"))
        .withColumn("state", F.col("address.state"))
        .withColumn("country", F.coalesce(F.col("address.country"), F.lit("US")))

        # Compute customer age (account age, not person age)
        .withColumn("account_age_days",
            F.datediff(F.current_date(), F.col("created_date"))
        )

        # Add Silver layer metadata
        .withColumn("_silver_updated_at", F.current_timestamp())

        # Select final columns (schema enforcement - only defined columns pass through)
        .select(
            "customer_id", "email", "first_name", "last_name", "full_name",
            "phone", "loyalty_tier", "lifetime_value",
            "city", "state", "country",
            "created_date", "updated_date", "account_age_days",
            "_silver_updated_at", "_datasource"
        )
    )

    return cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: MERGE INTO (Upsert Pattern)
# MAGIC
# MAGIC **The MERGE statement is the heart of CDC in Delta Lake.**
# MAGIC
# MAGIC ```
# MAGIC MERGE INTO target USING source ON match_condition
# MAGIC   WHEN MATCHED AND condition THEN UPDATE SET ...
# MAGIC   WHEN NOT MATCHED THEN INSERT ...
# MAGIC   WHEN NOT MATCHED BY SOURCE THEN DELETE/UPDATE ...
# MAGIC ```
# MAGIC
# MAGIC **Interview Talking Points:**
# MAGIC 1. MERGE is **atomic** (ACID) - either all changes apply or none do
# MAGIC 2. MERGE handles **insert + update + delete** in a single operation
# MAGIC 3. Much more efficient than DELETE + INSERT (only touches affected rows)
# MAGIC 4. Can include **multiple WHEN clauses** with different conditions
# MAGIC 5. Supports **schema evolution** with `spark.databricks.delta.schema.autoMerge`

# COMMAND ----------

def merge_customers(source_df):
    """
    MERGE (upsert) cleaned customer data into the Silver table.

    This implements a simple upsert:
    - If customer_id exists → UPDATE the record
    - If customer_id is new → INSERT the record

    The merge is ACID-compliant: concurrent reads see a consistent snapshot,
    and the operation either fully succeeds or fully rolls back.
    """

    # Check if target table exists (first run)
    if not spark.catalog.tableExists(TARGET_TABLE):
        print(f"Target table {TARGET_TABLE} doesn't exist. Creating with initial load...")
        source_df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
        print(f"Initial load complete: {source_df.count()} records")
        return

    # Get reference to the target Delta table
    target = DeltaTable.forName(spark, TARGET_TABLE)

    # Execute the MERGE
    merge_result = (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            condition="target.customer_id = source.customer_id"       # Match on business key
        )
        # WHEN MATCHED: Update only if the source is newer
        .whenMatchedUpdate(
            condition="source.updated_date > target.updated_date",    # Only update if source is fresher
            set={
                "email": "source.email",
                "first_name": "source.first_name",
                "last_name": "source.last_name",
                "full_name": "source.full_name",
                "phone": "source.phone",
                "loyalty_tier": "source.loyalty_tier",
                "lifetime_value": "source.lifetime_value",
                "city": "source.city",
                "state": "source.state",
                "country": "source.country",
                "updated_date": "source.updated_date",
                "account_age_days": "source.account_age_days",
                "_silver_updated_at": "source._silver_updated_at",
            }
        )
        # WHEN NOT MATCHED: Insert new customers
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"MERGE complete for {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: SCD Type 2 (Full History Tracking)
# MAGIC
# MAGIC SCD Type 2 maintains **complete history** of changes by:
# MAGIC 1. Closing the current record (setting `effective_end_date` and `is_current = false`)
# MAGIC 2. Inserting a new record with updated values (with `is_current = true`)
# MAGIC
# MAGIC This is critical for analytics that need to know what a customer looked like
# MAGIC at any point in time (e.g., "What tier was this customer in Q3 2024?").

# COMMAND ----------

SCD2_TABLE = "customer_360_catalog.silver.customers_history"

def merge_customers_scd2(source_df):
    """
    Implement SCD Type 2 using MERGE with multiple WHEN clauses.

    The trick: when a row is "matched" (existing customer with changes),
    we UPDATE the old row to close it AND INSERT a new row for the new version.
    Delta Lake MERGE supports this with whenMatchedUpdate + whenNotMatchedInsert.
    """

    source_with_scd = (
        source_df
        .withColumn("effective_start_date", F.current_timestamp())
        .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if not spark.catalog.tableExists(SCD2_TABLE):
        source_with_scd.write.format("delta").mode("overwrite").saveAsTable(SCD2_TABLE)
        return

    target = DeltaTable.forName(spark, SCD2_TABLE)

    # Step A: Create a staging view that identifies which records have changes
    # Compare key business columns to detect actual changes
    changes_df = (
        source_with_scd.alias("source")
        .join(
            spark.table(SCD2_TABLE).filter("is_current = true").alias("target"),
            "customer_id",
            "left"
        )
        .filter(
            # Only include records where something actually changed
            (F.col("target.customer_id").isNull()) |  # New customer
            (F.col("source.email") != F.col("target.email")) |
            (F.col("source.loyalty_tier") != F.col("target.loyalty_tier")) |
            (F.col("source.lifetime_value") != F.col("target.lifetime_value")) |
            (F.col("source.city") != F.col("target.city"))
        )
        .select("source.*")
    )

    # Step B: Close old records and insert new ones
    # Use a union approach: old records to close + new records to insert
    target.alias("target").merge(
        changes_df.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true"
    ).whenMatchedUpdate(
        set={
            "effective_end_date": F.current_timestamp(),
            "is_current": F.lit(False)
        }
    ).whenNotMatchedInsertAll(
    ).execute()

    # Insert the new current records for updated customers
    updated_customers = changes_df.join(
        spark.table(SCD2_TABLE).filter("is_current = false"),
        "customer_id",
        "inner"
    ).select(changes_df["*"])

    if updated_customers.count() > 0:
        updated_customers.write.format("delta").mode("append").saveAsTable(SCD2_TABLE)

    print(f"SCD Type 2 merge complete for {SCD2_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute the Pipeline

# COMMAND ----------

# Step 1: Get latest Bronze data
bronze_customers = get_latest_bronze_customers()
print(f"Bronze records (deduped): {bronze_customers.count()}")

# Step 2: Clean and validate
silver_customers = clean_customers(bronze_customers)
print(f"Silver records (cleaned): {silver_customers.count()}")

# Step 3: MERGE into Silver (current state)
merge_customers(silver_customers)

# Step 4: SCD Type 2 (full history) -- optional, for advanced use cases
merge_customers_scd2(silver_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

# Current state
display(spark.table(TARGET_TABLE).limit(10))

# SCD2 history - show a customer with multiple versions
display(
    spark.table(SCD2_TABLE)
    .filter("customer_id = 'C-1001'")
    .orderBy("effective_start_date")
)
