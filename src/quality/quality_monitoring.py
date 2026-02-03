# Databricks notebook source

# MAGIC %md
# MAGIC # Data Quality Monitoring Dashboard
# MAGIC
# MAGIC Queries for monitoring data quality across the platform.
# MAGIC These can be used in Databricks SQL dashboards.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Quality Metrics Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality pass rates by rule over time
# MAGIC SELECT
# MAGIC     DATE(timestamp) as check_date,
# MAGIC     rule_name,
# MAGIC     source_table,
# MAGIC     AVG(pass_rate) as avg_pass_rate,
# MAGIC     SUM(failing_records) as total_failures,
# MAGIC     SUM(total_records) as total_checked
# MAGIC FROM customer_360_catalog.quality.quality_metrics
# MAGIC GROUP BY DATE(timestamp), rule_name, source_table
# MAGIC ORDER BY check_date DESC, rule_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quarantine Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Records in quarantine by reason
# MAGIC SELECT
# MAGIC     _quarantine_reason,
# MAGIC     _quarantine_source,
# MAGIC     COUNT(*) as quarantined_count,
# MAGIC     MIN(_quarantined_at) as first_quarantined,
# MAGIC     MAX(_quarantined_at) as last_quarantined
# MAGIC FROM customer_360_catalog.quality.quarantine_records
# MAGIC GROUP BY _quarantine_reason, _quarantine_source
# MAGIC ORDER BY quarantined_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Table Freshness Check

# COMMAND ----------

def check_table_freshness(table_name: str, timestamp_col: str,
                          max_age_hours: int = 24):
    """
    Check if a table's data is fresh (within expected age).
    Returns True if fresh, False if stale.
    """
    result = (
        spark.table(table_name)
        .agg(F.max(timestamp_col).alias("latest_record"))
        .collect()[0]
    )

    latest = result["latest_record"]
    if latest is None:
        print(f"WARNING: {table_name} has no records!")
        return False

    from datetime import datetime, timedelta
    age = datetime.now() - latest
    age_hours = age.total_seconds() / 3600

    is_fresh = age_hours <= max_age_hours
    status = "FRESH" if is_fresh else "STALE"

    print(f"{table_name}: {status} (age: {age_hours:.1f}h, max: {max_age_hours}h)")
    return is_fresh

# COMMAND ----------

# Check freshness of all key tables
# check_table_freshness("customer_360_catalog.silver.transactions",
#                       "_silver_processed_at", max_age_hours=24)
# check_table_freshness("customer_360_catalog.gold.customer_360",
#                       "_gold_updated_at", max_age_hours=48)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Row Count Monitoring

# COMMAND ----------

def monitor_row_counts():
    """
    Monitor row counts across all tables and alert on significant changes.
    """
    tables = [
        "customer_360_catalog.bronze.raw_customers",
        "customer_360_catalog.bronze.raw_transactions",
        "customer_360_catalog.silver.customers",
        "customer_360_catalog.silver.transactions",
        "customer_360_catalog.gold.customer_360",
        "customer_360_catalog.gold.customer_segments",
    ]

    results = []
    for table in tables:
        try:
            count = spark.table(table).count()
            results.append((table, count, "OK"))
        except Exception as e:
            results.append((table, 0, f"ERROR: {str(e)[:100]}"))

    monitor_df = spark.createDataFrame(results,
        ["table_name", "row_count", "status"])
    display(monitor_df)
    return monitor_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DLT Expectation Metrics Query
# MAGIC
# MAGIC DLT automatically stores expectation results in the event log:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query DLT expectation results from the event log
# MAGIC -- (Requires the DLT pipeline to have run)
# MAGIC -- SELECT
# MAGIC --     details:flow_progress.data_quality.expectations AS expectations,
# MAGIC --     timestamp
# MAGIC -- FROM event_log(TABLE(customer_360_catalog.silver.clean_customers))
# MAGIC -- WHERE event_type = 'flow_progress'
# MAGIC -- ORDER BY timestamp DESC;
