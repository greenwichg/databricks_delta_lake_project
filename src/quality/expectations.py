# Databricks notebook source

# MAGIC %md
# MAGIC # Data Quality Framework: Expectations & Validation
# MAGIC
# MAGIC A reusable data quality framework that works OUTSIDE of DLT pipelines.
# MAGIC For DLT expectations, see `dlt_pipelines/dlt_bronze_to_silver.py`.
# MAGIC
# MAGIC ## Approaches to Data Quality in Databricks
# MAGIC
# MAGIC | Approach | Where | How |
# MAGIC |----------|-------|-----|
# MAGIC | **DLT Expectations** | DLT pipelines | `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail` |
# MAGIC | **Custom Framework** | Any notebook | Python functions + quarantine tables (this file) |
# MAGIC | **Great Expectations** | Any notebook | Third-party library integration |
# MAGIC | **Unity Catalog Tags** | Governance | Metadata-based quality tagging |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from typing import Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Rule Engine

# COMMAND ----------

class QualityAction(Enum):
    """What to do when a quality rule fails."""
    WARN = "warn"       # Log metric, pass records through
    DROP = "drop"       # Remove failing records
    FAIL = "fail"       # Stop pipeline execution
    QUARANTINE = "quarantine"  # Move bad records to quarantine table

@dataclass
class QualityRule:
    """Definition of a data quality rule."""
    name: str
    description: str
    condition: str          # SQL expression that should be TRUE for valid records
    action: QualityAction
    column: str = None      # Optional: specific column being checked

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-defined Quality Rules (Reusable Across Tables)

# COMMAND ----------

# ---- Customer Quality Rules ----
CUSTOMER_RULES = [
    QualityRule(
        name="not_null_customer_id",
        description="Customer ID must not be null",
        condition="customer_id IS NOT NULL",
        action=QualityAction.DROP,
        column="customer_id"
    ),
    QualityRule(
        name="valid_email_format",
        description="Email must contain @ symbol",
        condition="email IS NOT NULL AND email LIKE '%@%.%'",
        action=QualityAction.QUARANTINE,
        column="email"
    ),
    QualityRule(
        name="valid_loyalty_tier",
        description="Loyalty tier must be a known value",
        condition="loyalty_tier IN ('bronze', 'silver', 'gold', 'platinum', 'unknown')",
        action=QualityAction.WARN,
        column="loyalty_tier"
    ),
    QualityRule(
        name="positive_lifetime_value",
        description="Lifetime value should be non-negative",
        condition="lifetime_value >= 0 OR lifetime_value IS NULL",
        action=QualityAction.WARN,
        column="lifetime_value"
    ),
]

# ---- Transaction Quality Rules ----
TRANSACTION_RULES = [
    QualityRule(
        name="not_null_transaction_id",
        description="Transaction ID must not be null",
        condition="transaction_id IS NOT NULL",
        action=QualityAction.DROP,
        column="transaction_id"
    ),
    QualityRule(
        name="positive_amount",
        description="Transaction amount must be positive",
        condition="amount > 0",
        action=QualityAction.DROP,
        column="amount"
    ),
    QualityRule(
        name="reasonable_amount",
        description="Transaction amount should be under $100K (potential fraud)",
        condition="amount < 100000",
        action=QualityAction.QUARANTINE,
        column="amount"
    ),
    QualityRule(
        name="valid_date",
        description="Transaction date must not be in the future",
        condition="transaction_date <= current_date()",
        action=QualityAction.WARN,
        column="transaction_date"
    ),
    QualityRule(
        name="valid_discount",
        description="Discount must be between 0% and 100%",
        condition="discount_pct >= 0 AND discount_pct <= 1",
        action=QualityAction.DROP,
        column="discount_pct"
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Engine: Apply Rules to DataFrames

# COMMAND ----------

class DataQualityEngine:
    """
    Engine to apply quality rules to DataFrames.

    Mirrors DLT Expectation behavior but works in any notebook/pipeline:
    - WARN: log metric, pass all records
    - DROP: remove failing records
    - FAIL: raise exception if any records fail
    - QUARANTINE: separate bad records into a quarantine table
    """

    def __init__(self, spark_session, quarantine_table: str = None):
        self.spark = spark_session
        self.quarantine_table = quarantine_table or \
            "customer_360_catalog.quality.quarantine_records"
        self.results = []

    def apply_rules(self, df: DataFrame, rules: List[QualityRule],
                    source_table: str) -> DataFrame:
        """
        Apply a list of quality rules to a DataFrame.

        Returns the DataFrame after applying DROP/QUARANTINE actions.
        WARN rules only log metrics.
        FAIL rules raise exceptions.
        """
        current_df = df
        total_records = df.count()

        for rule in rules:
            passing = current_df.filter(rule.condition)
            failing = current_df.filter(f"NOT ({rule.condition})")
            pass_count = passing.count()
            fail_count = failing.count()
            pass_rate = pass_count / total_records if total_records > 0 else 1.0

            # Log the result
            result = {
                "rule_name": rule.name,
                "source_table": source_table,
                "total_records": total_records,
                "passing_records": pass_count,
                "failing_records": fail_count,
                "pass_rate": pass_rate,
                "action": rule.action.value,
                "timestamp": datetime.now().isoformat(),
            }
            self.results.append(result)

            print(f"  Rule '{rule.name}': {pass_rate:.1%} pass rate "
                  f"({fail_count} failures) -> {rule.action.value}")

            if rule.action == QualityAction.WARN:
                # Just log, keep all records
                pass

            elif rule.action == QualityAction.DROP:
                # Remove failing records
                current_df = passing

            elif rule.action == QualityAction.FAIL:
                if fail_count > 0:
                    raise ValueError(
                        f"Quality rule '{rule.name}' FAILED: {fail_count} records "
                        f"violated constraint '{rule.condition}'"
                    )

            elif rule.action == QualityAction.QUARANTINE:
                # Move bad records to quarantine table
                if fail_count > 0:
                    self._quarantine_records(failing, rule, source_table)
                current_df = passing

        records_after = current_df.count()
        print(f"\n  Summary: {total_records} -> {records_after} records "
              f"({total_records - records_after} filtered/quarantined)")

        return current_df

    def _quarantine_records(self, bad_df: DataFrame, rule: QualityRule,
                            source_table: str):
        """Send bad records to the quarantine table."""
        quarantined = (
            bad_df
            .withColumn("_quarantine_reason", F.lit(rule.name))
            .withColumn("_quarantine_rule", F.lit(rule.condition))
            .withColumn("_quarantine_source", F.lit(source_table))
            .withColumn("_quarantined_at", F.current_timestamp())
        )

        quarantined.write.format("delta").mode("append").saveAsTable(
            self.quarantine_table
        )
        print(f"    -> {bad_df.count()} records quarantined to {self.quarantine_table}")

    def get_results_df(self) -> DataFrame:
        """Get quality check results as a DataFrame."""
        if not self.results:
            return None
        return self.spark.createDataFrame(self.results)

    def save_results(self, metrics_table: str = None):
        """Save quality metrics to a Delta table for dashboarding."""
        table = metrics_table or "customer_360_catalog.quality.quality_metrics"
        results_df = self.get_results_df()
        if results_df:
            results_df.write.format("delta").mode("append").saveAsTable(table)
            print(f"Quality metrics saved to {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Example

# COMMAND ----------

# Example: Apply quality rules to customer data
# engine = DataQualityEngine(spark)
#
# raw_customers = spark.table("customer_360_catalog.bronze.raw_customers")
#
# print("Applying customer quality rules:")
# clean_customers = engine.apply_rules(
#     df=raw_customers,
#     rules=CUSTOMER_RULES,
#     source_table="bronze.raw_customers"
# )
#
# # Save quality metrics for monitoring
# engine.save_results()
#
# # View results
# display(engine.get_results_df())

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Expectations Quick Reference
# MAGIC
# MAGIC For use inside DLT pipelines (see `dlt_pipelines/` for full examples):
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table(name="my_table")
# MAGIC # WARN: Log metric, pass records through (monitoring)
# MAGIC @dlt.expect("rule_name", "SQL condition")
# MAGIC
# MAGIC # DROP: Remove failing records silently
# MAGIC @dlt.expect_or_drop("rule_name", "SQL condition")
# MAGIC
# MAGIC # FAIL: Stop pipeline on violation
# MAGIC @dlt.expect_or_fail("rule_name", "SQL condition")
# MAGIC
# MAGIC # Multiple rules at once
# MAGIC @dlt.expect_all({"rule1": "cond1", "rule2": "cond2"})
# MAGIC @dlt.expect_all_or_drop({"rule1": "cond1", "rule2": "cond2"})
# MAGIC @dlt.expect_all_or_fail({"rule1": "cond1", "rule2": "cond2"})
# MAGIC
# MAGIC def my_table():
# MAGIC     return dlt.read_stream("upstream_table")
# MAGIC ```
