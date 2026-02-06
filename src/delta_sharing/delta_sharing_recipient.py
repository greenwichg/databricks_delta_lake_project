# Databricks notebook source

# MAGIC %md
# MAGIC # Delta Sharing: Recipient (Data Consumer)
# MAGIC
# MAGIC ## Consuming Shared Data
# MAGIC As a **recipient**, you can access shared data from a provider through:
# MAGIC 1. **Databricks-to-Databricks**: Create a catalog from the share (native integration)
# MAGIC 2. **Open Sharing**: Use delta-sharing-python or Spark connector with a credential file
# MAGIC 3. **BI Tools**: Power BI, Tableau connectors with the activation token
# MAGIC
# MAGIC ## Access Pattern
# MAGIC ```
# MAGIC Provider (remote)                Recipient (your workspace)
# MAGIC ┌──────────────────┐            ┌──────────────────────────┐
# MAGIC │ gold.customer_360│   Share     │ partner_catalog          │
# MAGIC │ gold.revenue     │ ─────────> │   .shared_data           │
# MAGIC │ gold.segments    │            │     .customer_profiles   │
# MAGIC └──────────────────┘            │     .revenue_analytics   │
# MAGIC                                 │     .customer_segments   │
# MAGIC  Data stays here               └──────────────────────────┘
# MAGIC  (no copy)                       Query as normal tables
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Databricks-to-Databricks Sharing (Unity Catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog from a provider's share
# MAGIC -- This gives you access to shared tables as if they were local
# MAGIC CREATE CATALOG IF NOT EXISTS partner_data
# MAGIC USING SHARE `provider_account`.customer_360_analytics_share
# MAGIC COMMENT 'Shared data from the Customer 360 provider';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Browse the shared catalog
# MAGIC SHOW SCHEMAS IN partner_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query shared data exactly like a local table
# MAGIC SELECT *
# MAGIC FROM partner_data.customer_analytics.customer_profiles
# MAGIC WHERE loyalty_tier = 'Platinum'
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join shared data with your own tables
# MAGIC SELECT
# MAGIC     p.customer_id,
# MAGIC     p.loyalty_tier,
# MAGIC     p.lifetime_value,
# MAGIC     m.campaign_name,
# MAGIC     m.conversion_rate
# MAGIC FROM partner_data.customer_analytics.customer_profiles p
# MAGIC INNER JOIN our_catalog.marketing.campaign_results m
# MAGIC     ON p.customer_id = m.customer_id
# MAGIC WHERE p.lifetime_value > 5000
# MAGIC ORDER BY p.lifetime_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Open Sharing (Non-Databricks Consumers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading with delta-sharing-python (pandas)
# MAGIC ```python
# MAGIC # Install: pip install delta-sharing
# MAGIC import delta_sharing
# MAGIC
# MAGIC # Path to the credential file (downloaded via activation link)
# MAGIC profile = "config.share"
# MAGIC
# MAGIC # List available shares and tables
# MAGIC client = delta_sharing.SharingClient(profile)
# MAGIC shares = client.list_shares()
# MAGIC for share in shares:
# MAGIC     print(f"Share: {share.name}")
# MAGIC     schemas = client.list_schemas(share)
# MAGIC     for schema in schemas:
# MAGIC         tables = client.list_tables(schema)
# MAGIC         for table in tables:
# MAGIC             print(f"  {schema.name}.{table.name}")
# MAGIC
# MAGIC # Read a shared table as a pandas DataFrame
# MAGIC table_url = f"{profile}#customer_360_analytics_share.customer_analytics.customer_profiles"
# MAGIC df = delta_sharing.load_as_pandas(table_url)
# MAGIC print(f"Rows: {len(df)}")
# MAGIC print(df.head())
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading with Apache Spark

# COMMAND ----------

def read_shared_data_with_spark(spark, profile_path):
    """
    Read Delta Sharing data using Apache Spark.
    Works in any Spark environment (Databricks, EMR, local).

    Args:
        spark: SparkSession
        profile_path: Path to the .share credential file
    """

    # Read a shared table as a Spark DataFrame
    shared_df = (
        spark.read
        .format("deltaSharing")
        .load(f"{profile_path}#customer_360_analytics_share.customer_analytics.customer_profiles")
    )

    shared_df.printSchema()
    print(f"Total rows: {shared_df.count()}")

    # Apply filters (pushed down to the provider)
    platinum_customers = shared_df.filter("loyalty_tier = 'Platinum'")
    print(f"Platinum customers: {platinum_customers.count()}")

    return shared_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reading Change Data Feed (Incremental Sync)

# COMMAND ----------

def read_shared_cdf(spark, profile_path):
    """
    Read Change Data Feed from a shared table.
    Enables incremental sync without re-reading the full table.
    """
    from pyspark.sql import functions as F

    # Read changes since a specific version
    changes_df = (
        spark.read
        .format("deltaSharing")
        .option("readChangeFeed", "true")
        .option("startingVersion", 5)
        .load(f"{profile_path}#customer_360_analytics_share.customer_analytics.customer_profiles")
    )

    # CDF adds _change_type, _commit_version, _commit_timestamp columns
    print("Change types:")
    changes_df.groupBy("_change_type").count().show()

    # Apply changes incrementally
    inserts = changes_df.filter(F.col("_change_type") == "insert")
    updates = changes_df.filter(F.col("_change_type").isin("update_postimage"))
    deletes = changes_df.filter(F.col("_change_type") == "delete")

    print(f"Inserts: {inserts.count()}, Updates: {updates.count()}, Deletes: {deletes.count()}")

    return changes_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Streaming from Shared Tables

# COMMAND ----------

def stream_shared_table(spark, profile_path):
    """
    Set up a streaming reader on a shared table.
    Continuously processes new data as the provider updates it.
    """

    shared_stream = (
        spark.readStream
        .format("deltaSharing")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load(f"{profile_path}#customer_360_analytics_share.customer_analytics.customer_profiles")
    )

    # Write the stream to a local Delta table for further processing
    query = (
        shared_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/customer_360_catalog/checkpoints/shared_data_sync")
        .trigger(processingTime="5 minutes")
        .toTable("customer_360_catalog.staging.synced_customer_profiles")
    )

    print(f"Streaming query started: {query.id}")
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. BI Tool Integration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Power BI
# MAGIC ```
# MAGIC 1. Open Power BI Desktop
# MAGIC 2. Get Data > Delta Sharing
# MAGIC 3. Paste the activation link from the provider
# MAGIC 4. Select tables to import
# MAGIC 5. Build dashboards on shared data (auto-refreshes)
# MAGIC ```
# MAGIC
# MAGIC ### Tableau
# MAGIC ```
# MAGIC 1. Open Tableau Desktop
# MAGIC 2. Connect > Delta Sharing
# MAGIC 3. Upload the .share credential file
# MAGIC 4. Select shared tables
# MAGIC 5. Create visualizations (live connection - no extract needed)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Delta Sharing (Recipient)
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **How do you consume shared data?** | D2D: CREATE CATALOG USING SHARE. Open: delta-sharing-python with credential file |
# MAGIC | **Is data copied?** | No - reads go directly to provider's storage. Zero-copy sharing |
# MAGIC | **Can you join shared + local data?** | Yes - shared catalogs appear as normal Unity Catalog objects |
# MAGIC | **Incremental sync?** | Use CDF (readChangeFeed) to read only changes since last sync |
# MAGIC | **Streaming support?** | Yes - readStream with deltaSharing format for continuous sync |
# MAGIC | **BI tool access?** | Native connectors for Power BI, Tableau via activation link |
