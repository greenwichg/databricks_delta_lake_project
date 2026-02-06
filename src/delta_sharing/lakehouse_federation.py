# Databricks notebook source

# MAGIC %md
# MAGIC # Lakehouse Federation: Query External Data Sources
# MAGIC
# MAGIC ## What is Lakehouse Federation?
# MAGIC Lakehouse Federation lets you **query external databases** directly from Databricks
# MAGIC without moving data into the Lakehouse:
# MAGIC - **Zero-copy queries** against PostgreSQL, MySQL, SQL Server, Snowflake, BigQuery, Redshift
# MAGIC - **Unity Catalog governed** - same access control, lineage, and audit
# MAGIC - **Predicate pushdown** - filters are pushed to the source for performance
# MAGIC - **Join federated + local data** in a single query
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────┐
# MAGIC │                     Unity Catalog                                │
# MAGIC │                 (Unified Governance Layer)                       │
# MAGIC ├──────────────┬──────────────┬──────────────┬────────────────────┤
# MAGIC │  Delta Lake   │  Delta Share  │  Federated   │  External         │
# MAGIC │  Tables       │  Tables       │  Catalogs    │  Locations        │
# MAGIC │              │              │              │                    │
# MAGIC │ customer_360 │ partner_data │ postgresql_  │ s3://bucket/       │
# MAGIC │ _catalog     │              │ catalog      │                    │
# MAGIC └──────────────┴──────────────┴──────┬───────┴────────────────────┘
# MAGIC                                      │
# MAGIC                          ┌───────────┴───────────┐
# MAGIC                          │   External Databases   │
# MAGIC                          ├───────────────────────┤
# MAGIC                          │ PostgreSQL │ MySQL     │
# MAGIC                          │ SQL Server │ Snowflake │
# MAGIC                          │ BigQuery   │ Redshift  │
# MAGIC                          └───────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a Connection to an External Database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a connection to PostgreSQL (CRM system)
# MAGIC CREATE CONNECTION IF NOT EXISTS crm_postgresql
# MAGIC TYPE POSTGRESQL
# MAGIC OPTIONS (
# MAGIC     host 'crm-db.company.internal',
# MAGIC     port '5432',
# MAGIC     user secret('federation_secrets', 'pg_user'),
# MAGIC     password secret('federation_secrets', 'pg_password')
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a connection to MySQL (e-commerce system)
# MAGIC CREATE CONNECTION IF NOT EXISTS ecommerce_mysql
# MAGIC TYPE MYSQL
# MAGIC OPTIONS (
# MAGIC     host 'ecommerce-db.company.internal',
# MAGIC     port '3306',
# MAGIC     user secret('federation_secrets', 'mysql_user'),
# MAGIC     password secret('federation_secrets', 'mysql_password')
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a connection to Snowflake (data warehouse)
# MAGIC CREATE CONNECTION IF NOT EXISTS analytics_snowflake
# MAGIC TYPE SNOWFLAKE
# MAGIC OPTIONS (
# MAGIC     host 'company.snowflakecomputing.com',
# MAGIC     warehouse 'COMPUTE_WH',
# MAGIC     user secret('federation_secrets', 'sf_user'),
# MAGIC     password secret('federation_secrets', 'sf_password')
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Federated Catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a foreign catalog from the PostgreSQL connection
# MAGIC -- This exposes all PostgreSQL schemas and tables in Unity Catalog
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS crm_postgres
# MAGIC USING CONNECTION crm_postgresql
# MAGIC OPTIONS (database 'crm_production');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a foreign catalog from MySQL
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS ecommerce_mysql_catalog
# MAGIC USING CONNECTION ecommerce_mysql
# MAGIC OPTIONS (database 'ecommerce_prod');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a foreign catalog from Snowflake
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS snowflake_analytics
# MAGIC USING CONNECTION analytics_snowflake
# MAGIC OPTIONS (database 'ANALYTICS_DB');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query Federated Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Browse federated catalog (just like a local catalog)
# MAGIC SHOW SCHEMAS IN crm_postgres;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN crm_postgres.public;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query PostgreSQL data directly (predicate pushdown happens automatically)
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     created_at
# MAGIC FROM crm_postgres.public.customers
# MAGIC WHERE created_at >= '2024-01-01'
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query MySQL e-commerce data
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     total_amount,
# MAGIC     order_status,
# MAGIC     created_at
# MAGIC FROM ecommerce_mysql_catalog.orders.order_headers
# MAGIC WHERE order_status = 'completed'
# MAGIC   AND total_amount > 100
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Join Federated Data with Delta Lake Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join federated PostgreSQL data with local Delta Lake Gold table
# MAGIC -- This is the key power of Lakehouse Federation!
# MAGIC SELECT
# MAGIC     c360.customer_id,
# MAGIC     crm.first_name,
# MAGIC     crm.last_name,
# MAGIC     crm.email AS crm_email,
# MAGIC     c360.lifetime_value,
# MAGIC     c360.loyalty_tier,
# MAGIC     c360.total_transactions,
# MAGIC     c360.churn_risk_score
# MAGIC FROM customer_360_catalog.gold.customer_360 c360
# MAGIC INNER JOIN crm_postgres.public.customers crm
# MAGIC     ON c360.customer_id = crm.customer_id
# MAGIC WHERE c360.loyalty_tier IN ('Gold', 'Platinum')
# MAGIC ORDER BY c360.lifetime_value DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cross-source analytics: Delta Lake + PostgreSQL + MySQL
# MAGIC SELECT
# MAGIC     crm.customer_id,
# MAGIC     crm.first_name || ' ' || crm.last_name AS full_name,
# MAGIC     COUNT(DISTINCT orders.order_id) AS live_order_count,
# MAGIC     SUM(orders.total_amount) AS live_revenue,
# MAGIC     c360.total_transactions AS lakehouse_txn_count,
# MAGIC     c360.lifetime_value AS lakehouse_ltv,
# MAGIC     c360.loyalty_tier
# MAGIC FROM crm_postgres.public.customers crm
# MAGIC LEFT JOIN ecommerce_mysql_catalog.orders.order_headers orders
# MAGIC     ON crm.customer_id = orders.customer_id
# MAGIC LEFT JOIN customer_360_catalog.gold.customer_360 c360
# MAGIC     ON crm.customer_id = c360.customer_id
# MAGIC GROUP BY ALL
# MAGIC ORDER BY live_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Federated Governance & Access Control

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant access to federated catalogs (same as local tables)
# MAGIC GRANT USAGE ON CATALOG crm_postgres TO `analytics_team`;
# MAGIC GRANT SELECT ON SCHEMA crm_postgres.public TO `analytics_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restrict specific tables
# MAGIC REVOKE SELECT ON TABLE crm_postgres.public.sensitive_data FROM `analytics_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Audit federated access via system tables
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     action_name,
# MAGIC     request_params.full_name_arg AS table_accessed,
# MAGIC     user_identity.email AS user_email,
# MAGIC     source_ip_address
# MAGIC FROM system.access.audit
# MAGIC WHERE request_params.full_name_arg LIKE 'crm_postgres%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Programmatic Federation Management

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def manage_connections():
    """
    Manage federated connections using the Databricks SDK.
    """
    w = WorkspaceClient()

    # List all connections
    connections = w.connections.list()
    print("=== Federated Connections ===")
    for conn in connections:
        print(f"  {conn.name} ({conn.connection_type}) -> {conn.comment}")

    # Test a connection
    print("\nTesting PostgreSQL connection...")
    try:
        spark = DatabricksSession.builder.getOrCreate()
        test_df = spark.sql("SELECT 1 AS test FROM crm_postgres.public.customers LIMIT 1")
        test_df.show()
        print("Connection test PASSED")
    except Exception as e:
        print(f"Connection test FAILED: {e}")


def create_mirror_table(spark, source_federated_table, target_delta_table):
    """
    Create a Delta Lake mirror of a federated table for performance.
    Use this when you need to repeatedly query external data.
    """
    from pyspark.sql import functions as F

    print(f"Mirroring {source_federated_table} -> {target_delta_table}")

    # Read from federated source
    source_df = spark.table(source_federated_table)

    # Write to Delta Lake
    (
        source_df
        .withColumn("_mirror_timestamp", F.current_timestamp())
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_delta_table)
    )

    count = spark.table(target_delta_table).count()
    print(f"Mirrored {count} rows to {target_delta_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance Considerations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Predicate Pushdown
# MAGIC Lakehouse Federation automatically pushes filters to the external source:
# MAGIC ```sql
# MAGIC -- This WHERE clause is pushed down to PostgreSQL
# MAGIC -- Only matching rows are transferred over the network
# MAGIC SELECT * FROM crm_postgres.public.customers
# MAGIC WHERE created_at >= '2024-01-01' AND region = 'US';
# MAGIC ```
# MAGIC
# MAGIC ### When to Mirror vs Federate
# MAGIC | Scenario | Approach | Why |
# MAGIC |----------|----------|-----|
# MAGIC | Ad-hoc exploration | Federate (live query) | No setup needed, always fresh |
# MAGIC | Frequent joins with local data | Mirror to Delta | Better join performance |
# MAGIC | Dashboard queries | Mirror to Delta | Consistent low latency |
# MAGIC | One-time analysis | Federate | No storage cost |
# MAGIC | Real-time data needed | Federate | Always current |
# MAGIC | Large table scans | Mirror to Delta | Avoid network transfer |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Lakehouse Federation
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What is Lakehouse Federation?** | Query external DBs (Postgres, MySQL, Snowflake) directly from Databricks without ETL |
# MAGIC | **How does it work?** | Foreign catalogs expose external tables in Unity Catalog with full governance |
# MAGIC | **Performance?** | Predicate pushdown to source; mirror to Delta for repeated queries |
# MAGIC | **Governance?** | Same GRANT/REVOKE, lineage tracking, and audit as local tables |
# MAGIC | **When to use vs ETL?** | Federation for ad-hoc/exploration; ETL (Bronze/Silver) for production pipelines |
# MAGIC | **Supported sources?** | PostgreSQL, MySQL, SQL Server, Snowflake, BigQuery, Redshift, and more |
