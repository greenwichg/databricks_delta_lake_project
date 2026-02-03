# Databricks notebook source

# MAGIC %md
# MAGIC # Common Utility Functions
# MAGIC Shared helper functions used across the pipeline.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

def deduplicate(df: DataFrame, key_columns: list, order_column: str,
                ascending: bool = False) -> DataFrame:
    """
    Deduplicate a DataFrame by keeping one record per key.

    Args:
        df: Input DataFrame
        key_columns: Columns to partition by (business key)
        order_column: Column to order by (latest wins)
        ascending: Sort order (False = latest first)
    """
    window = Window.partitionBy(*key_columns).orderBy(
        F.col(order_column).asc() if ascending else F.col(order_column).desc()
    )
    return (
        df
        .withColumn("_dedup_rn", F.row_number().over(window))
        .filter("_dedup_rn = 1")
        .drop("_dedup_rn")
    )

# COMMAND ----------

def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    """Add standard audit columns to a DataFrame."""
    return (
        df
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_datasource", F.lit(source_name))
    )

# COMMAND ----------

def get_table_stats(table_name: str) -> dict:
    """Get basic statistics for a Delta table."""
    df = spark.table(table_name)
    count = df.count()
    columns = len(df.columns)

    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]

    return {
        "table": table_name,
        "row_count": count,
        "column_count": columns,
        "size_bytes": detail["sizeInBytes"],
        "num_files": detail["numFiles"],
        "created_at": detail["createdAt"],
    }

# COMMAND ----------

def log_pipeline_metric(pipeline_name: str, stage: str, metric_name: str,
                        metric_value, table_name: str = None):
    """Log a pipeline metric to the quality metrics table."""
    from datetime import datetime

    metric_df = spark.createDataFrame(
        [(pipeline_name, stage, metric_name, float(metric_value),
          table_name, datetime.now())],
        ["pipeline_name", "stage", "metric_name", "metric_value",
         "table_name", "logged_at"]
    )

    metric_df.write.format("delta").mode("append").saveAsTable(
        "customer_360_catalog.quality.pipeline_metrics"
    )
