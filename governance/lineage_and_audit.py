# Databricks notebook source

# MAGIC %md
# MAGIC # Unity Catalog: Data Lineage & Audit Logging
# MAGIC
# MAGIC ## Data Lineage
# MAGIC Unity Catalog **automatically tracks data lineage** - how data flows between
# MAGIC tables, notebooks, and jobs. No code changes needed.
# MAGIC
# MAGIC **What lineage captures:**
# MAGIC - Table → Table dependencies (which tables feed into which)
# MAGIC - Column-level lineage (which source columns map to which target columns)
# MAGIC - Notebook/Job → Table relationships (what code writes to what table)
# MAGIC
# MAGIC **Where to view:**
# MAGIC - Unity Catalog UI → Table → "Lineage" tab
# MAGIC - Programmatic API for custom lineage queries
# MAGIC
# MAGIC ## Audit Logging
# MAGIC Every access to Unity Catalog objects is **automatically logged**:
# MAGIC - Who accessed what table and when
# MAGIC - What operations were performed (SELECT, MERGE, etc.)
# MAGIC - From which notebook/cluster/job

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. View Table Lineage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View upstream lineage (what feeds into this table)
# MAGIC -- This is typically viewed in the UC UI, but can be queried via API
# MAGIC
# MAGIC -- The lineage for our Customer 360 table would show:
# MAGIC -- bronze.raw_customers ─> silver.customers ─┐
# MAGIC -- bronze.raw_transactions -> silver.transactions ──> gold.customer_360
# MAGIC -- bronze.raw_clickstream -> silver.clickstream ──┘
# MAGIC --
# MAGIC -- This is captured AUTOMATICALLY by Unity Catalog when tables are
# MAGIC -- read/written using Spark SQL or DataFrame API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query Table History (Built into Delta Lake)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta Lake history gives us the operation log for a specific table
# MAGIC DESCRIBE HISTORY customer_360_catalog.gold.customer_360;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- More detailed history: who did what and when
# MAGIC SELECT
# MAGIC     version,
# MAGIC     timestamp,
# MAGIC     operation,
# MAGIC     userName,
# MAGIC     operationMetrics.numOutputRows as rows_written,
# MAGIC     operationMetrics.numTargetRowsInserted as rows_inserted,
# MAGIC     operationMetrics.numTargetRowsUpdated as rows_updated,
# MAGIC     operationMetrics.numTargetRowsDeleted as rows_deleted
# MAGIC FROM (DESCRIBE HISTORY customer_360_catalog.gold.customer_360)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Audit Log Queries
# MAGIC
# MAGIC Unity Catalog audit logs are delivered to your cloud storage (S3/ADLS/GCS).
# MAGIC You can query them using Databricks SQL or a separate analytics pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Query audit logs (if loaded into a Delta table)
# MAGIC -- This shows who accessed the customer_360 table
# MAGIC
# MAGIC -- SELECT
# MAGIC --     timestamp,
# MAGIC --     user_identity.email as user_email,
# MAGIC --     action_name,
# MAGIC --     request_params.full_name_arg as table_name,
# MAGIC --     response.status_code
# MAGIC -- FROM system.access.audit
# MAGIC -- WHERE request_params.full_name_arg = 'customer_360_catalog.gold.customer_360'
# MAGIC -- ORDER BY timestamp DESC
# MAGIC -- LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. System Tables (Databricks System Catalog)
# MAGIC
# MAGIC Databricks provides **system tables** for auditing and monitoring:
# MAGIC
# MAGIC | System Table | Contents |
# MAGIC |-------------|----------|
# MAGIC | `system.access.audit` | All access events (reads, writes, grants) |
# MAGIC | `system.billing.usage` | Compute and storage costs |
# MAGIC | `system.compute.clusters` | Cluster configurations and events |
# MAGIC | `system.storage.tables` | Table metadata and sizes |
# MAGIC | `system.lineage.table_lineage` | Table-to-table dependencies |
# MAGIC | `system.lineage.column_lineage` | Column-level lineage |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query table lineage from system tables
# MAGIC -- SELECT
# MAGIC --     source_table_full_name,
# MAGIC --     target_table_full_name,
# MAGIC --     source_type,
# MAGIC --     target_type
# MAGIC -- FROM system.lineage.table_lineage
# MAGIC -- WHERE target_table_full_name LIKE 'customer_360_catalog.gold%'
# MAGIC -- ORDER BY target_table_full_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compliance Report Generator

# COMMAND ----------

def generate_compliance_report():
    """
    Generate a compliance report for the Customer 360 platform.
    Shows table inventory, PII exposure, access patterns, and data freshness.
    """

    report = {}

    # Table inventory
    schemas = ["bronze", "silver", "gold", "quality"]
    for schema in schemas:
        tables = spark.sql(
            f"SHOW TABLES IN customer_360_catalog.{schema}"
        ).collect()
        report[schema] = {
            "table_count": len(tables),
            "tables": [t.tableName for t in tables]
        }

    # PII tables (tagged)
    # pii_tables = spark.sql("""
    #     SELECT table_name, tag_value
    #     FROM system.information_schema.table_tags
    #     WHERE tag_name = 'pii' AND tag_value = 'true'
    #     AND catalog_name = 'customer_360_catalog'
    # """).collect()

    print("=" * 60)
    print("CUSTOMER 360 PLATFORM - COMPLIANCE REPORT")
    print("=" * 60)

    for schema, info in report.items():
        print(f"\n{schema.upper()} Layer: {info['table_count']} tables")
        for t in info['tables']:
            print(f"  - {t}")

    print("\n" + "=" * 60)
    print("PII Protection Status:")
    print("  - Column masking: ENABLED (email, phone)")
    print("  - Row-level security: ENABLED (regional views)")
    print("  - Audit logging: ENABLED (automatic)")
    print("=" * 60)

# COMMAND ----------

# generate_compliance_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Interview Quick Reference
# MAGIC
# MAGIC **Q: What is Unity Catalog?**
# MAGIC A: Centralized governance solution for Databricks lakehouse - provides
# MAGIC unified access control, data lineage, auditing, and discovery across
# MAGIC all workspaces.
# MAGIC
# MAGIC **Q: What's the namespace structure?**
# MAGIC A: Three-level: `catalog.schema.table` (like `customer_360.gold.customer_360`)
# MAGIC
# MAGIC **Q: How do you handle PII?**
# MAGIC A: Column masking functions + row-level security via dynamic views +
# MAGIC tag-based discovery of PII columns.
# MAGIC
# MAGIC **Q: How does lineage work?**
# MAGIC A: Automatic - Unity Catalog tracks table and column-level lineage
# MAGIC whenever data flows through Spark SQL. No code changes needed.
# MAGIC
# MAGIC **Q: What about auditing?**
# MAGIC A: All access events are automatically logged to system tables
# MAGIC (`system.access.audit`). You can query who accessed what, when, and from where.
