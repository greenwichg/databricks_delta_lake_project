# Databricks notebook source

# MAGIC %md
# MAGIC # Unity Catalog: Setup & Governance
# MAGIC
# MAGIC ## What is Unity Catalog?
# MAGIC Unity Catalog is Databricks' **unified governance solution** for the lakehouse:
# MAGIC - **Centralized access control** (who can access what)
# MAGIC - **Data lineage** (where data comes from and where it goes)
# MAGIC - **Auditing** (who accessed what and when)
# MAGIC - **Data discovery** (search and find datasets)
# MAGIC - **Cross-workspace sharing** (share data securely)
# MAGIC
# MAGIC ## Three-Level Namespace
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC   │       │      └── Tables, Views, Functions
# MAGIC   │       └── Logical grouping (like a database)
# MAGIC   └── Top-level container (like a folder of databases)
# MAGIC ```
# MAGIC
# MAGIC Example: `customer_360_catalog.silver.customers`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the Catalog and Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the top-level catalog
# MAGIC CREATE CATALOG IF NOT EXISTS customer_360_catalog
# MAGIC COMMENT 'Customer 360 Analytics Platform - All data assets';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schemas for each layer of the Medallion architecture
# MAGIC CREATE SCHEMA IF NOT EXISTS customer_360_catalog.bronze
# MAGIC COMMENT 'Raw data layer - append-only, no transformations';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS customer_360_catalog.silver
# MAGIC COMMENT 'Cleaned and conformed data - business rules applied';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS customer_360_catalog.gold
# MAGIC COMMENT 'Business-ready aggregations and analytics tables';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS customer_360_catalog.quality
# MAGIC COMMENT 'Data quality metrics, quarantine tables, and monitoring';
# MAGIC
# MAGIC -- Managed storage location (where data files live)
# MAGIC -- CREATE SCHEMA IF NOT EXISTS customer_360_catalog.silver
# MAGIC -- MANAGED LOCATION 's3://my-bucket/customer-360/silver';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Managed Volumes (for file landing zones)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Volumes are Unity Catalog's way to manage files
# MAGIC -- Two types: MANAGED (UC controls storage) and EXTERNAL (you control storage)
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS customer_360_catalog.landing.raw_data
# MAGIC COMMENT 'Landing zone for raw data files from source systems';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS customer_360_catalog.checkpoints.streaming
# MAGIC COMMENT 'Checkpoint storage for streaming pipelines';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Table Ownership and Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set table properties for governance
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC SET TBLPROPERTIES (
# MAGIC     'quality' = 'silver',
# MAGIC     'owner_team' = 'data-engineering',
# MAGIC     'pii_columns' = 'email,phone,first_name,last_name',
# MAGIC     'data_classification' = 'confidential',
# MAGIC     'retention_days' = '365',
# MAGIC     'sla_freshness_hours' = '24',
# MAGIC     delta.enableChangeDataFeed = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add column comments for documentation
# MAGIC ALTER TABLE customer_360_catalog.gold.customer_360
# MAGIC ALTER COLUMN customer_id COMMENT 'Unique customer identifier from CRM system';
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.gold.customer_360
# MAGIC ALTER COLUMN total_spend COMMENT 'Total lifetime spend across all channels (USD)';
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.gold.customer_360
# MAGIC ALTER COLUMN health_score COMMENT 'Customer health: healthy, neutral, or at_risk';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tags for Data Classification and Discovery

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply tags for data classification (requires UC with tags enabled)
# MAGIC -- Tags make tables discoverable and enable automated governance policies
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC SET TAGS ('pii' = 'true', 'team' = 'data-engineering', 'domain' = 'customer');
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.gold.customer_360
# MAGIC SET TAGS ('domain' = 'customer', 'tier' = 'gold', 'dashboard' = 'customer-360');
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.gold.revenue_analytics
# MAGIC SET TAGS ('domain' = 'finance', 'tier' = 'gold', 'dashboard' = 'revenue');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Unity Catalog Three-Level Namespace Examples

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Browse the catalog hierarchy
# MAGIC SHOW CATALOGS;
# MAGIC -- SHOW SCHEMAS IN customer_360_catalog;
# MAGIC -- SHOW TABLES IN customer_360_catalog.silver;

# COMMAND ----------

# Programmatic catalog exploration
catalogs = spark.sql("SHOW CATALOGS").collect()
print("Available catalogs:")
for c in catalogs:
    print(f"  - {c.catalog}")

schemas = spark.sql("SHOW SCHEMAS IN customer_360_catalog").collect()
print(f"\nSchemas in customer_360_catalog:")
for s in schemas:
    print(f"  - {s.databaseName}")
