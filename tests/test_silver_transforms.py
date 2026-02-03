# Databricks notebook source

# MAGIC %md
# MAGIC # Unit Tests: Silver Layer Transformations
# MAGIC
# MAGIC Tests for the Silver layer cleaning and transformation functions.
# MAGIC These run on small in-memory DataFrames (no Delta tables needed).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Framework

# COMMAND ----------

class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def assert_equals(self, actual, expected, test_name):
        if actual == expected:
            self.passed += 1
            print(f"  PASS: {test_name}")
        else:
            self.failed += 1
            self.errors.append(f"{test_name}: expected {expected}, got {actual}")
            print(f"  FAIL: {test_name} (expected {expected}, got {actual})")

    def assert_true(self, condition, test_name):
        self.assert_equals(condition, True, test_name)

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*40}")
        print(f"Results: {self.passed}/{total} passed, {self.failed} failed")
        if self.errors:
            print("Failures:")
            for e in self.errors:
                print(f"  - {e}")
        print(f"{'='*40}")

results = TestResult()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Customer Email Standardization

# COMMAND ----------

print("Test Suite: Customer Email Standardization")

test_data = spark.createDataFrame([
    ("C-001", "  JOHN@EXAMPLE.COM  ", "John", "Doe"),
    ("C-002", "jane.smith@test.com", "  jane ", "SMITH  "),
    ("C-003", None, "Bob", "Wilson"),
], ["customer_id", "email", "first_name", "last_name"])

cleaned = (
    test_data
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
)

rows = cleaned.collect()

results.assert_equals(rows[0]["email"], "john@example.com", "Email lowercase + trimmed")
results.assert_equals(rows[1]["first_name"], "Jane", "First name InitCap + trimmed")
results.assert_equals(rows[1]["last_name"], "Smith", "Last name InitCap + trimmed")
results.assert_equals(rows[0]["full_name"], "John Doe", "Full name concatenation")
results.assert_true(rows[2]["email"] is None, "Null email preserved as null")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Transaction Amount Validation

# COMMAND ----------

print("\nTest Suite: Transaction Amount Validation")

txn_data = spark.createDataFrame([
    ("TXN-1", 100.0, 0.10, "C-001"),
    ("TXN-2", 50.0, 0.0, "C-002"),
    ("TXN-3", 200.0, 0.25, "C-003"),
    ("TXN-4", -10.0, 0.0, "C-004"),    # Invalid: negative
    ("TXN-5", 100.0, 1.5, "C-005"),    # Invalid: discount > 100%
], ["transaction_id", "amount", "discount_pct", "customer_id"])

cleaned_txn = (
    txn_data
    .withColumn("discount_pct",
        F.when(F.col("discount_pct").between(0, 1), F.col("discount_pct"))
        .otherwise(0.0)
    )
    .withColumn("net_amount",
        F.col("amount") * (1 - F.col("discount_pct"))
    )
)

txn_rows = cleaned_txn.collect()

results.assert_equals(txn_rows[0]["net_amount"], 90.0, "10% discount on $100 = $90")
results.assert_equals(txn_rows[1]["net_amount"], 50.0, "No discount = full amount")
results.assert_equals(txn_rows[2]["net_amount"], 150.0, "25% discount on $200 = $150")
results.assert_equals(txn_rows[4]["discount_pct"], 0.0, "Invalid discount reset to 0")
results.assert_equals(txn_rows[4]["net_amount"], 100.0, "Invalid discount = full amount")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Deduplication Logic

# COMMAND ----------

print("\nTest Suite: Deduplication Logic")

from pyspark.sql.window import Window

dup_data = spark.createDataFrame([
    ("C-001", "john@old.com", "2024-01-01 10:00:00"),
    ("C-001", "john@new.com", "2024-06-15 14:30:00"),   # Newer - should win
    ("C-001", "john@oldest.com", "2023-06-01 08:00:00"),
    ("C-002", "jane@example.com", "2024-03-20 12:00:00"),  # Only record
], ["customer_id", "email", "updated_date"])

dup_data = dup_data.withColumn("updated_date", F.to_timestamp("updated_date"))

# Dedup: keep latest per customer_id
window = Window.partitionBy("customer_id").orderBy(F.col("updated_date").desc())
deduped = (
    dup_data
    .withColumn("_rn", F.row_number().over(window))
    .filter("_rn = 1")
    .drop("_rn")
)

results.assert_equals(deduped.count(), 2, "Dedup reduces 4 rows to 2")
c001 = deduped.filter("customer_id = 'C-001'").collect()[0]
results.assert_equals(c001["email"], "john@new.com", "Latest record wins for C-001")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Loyalty Tier Validation

# COMMAND ----------

print("\nTest Suite: Loyalty Tier Validation")

tier_data = spark.createDataFrame([
    ("C-001", "gold"),
    ("C-002", "PREMIUM"),       # Invalid
    ("C-003", None),            # Null
    ("C-004", "platinum"),
], ["customer_id", "loyalty_tier"])

cleaned_tiers = (
    tier_data
    .withColumn("loyalty_tier",
        F.when(F.col("loyalty_tier").isin("bronze", "silver", "gold", "platinum"),
               F.col("loyalty_tier"))
        .otherwise("unknown")
    )
)

tier_rows = cleaned_tiers.collect()

results.assert_equals(tier_rows[0]["loyalty_tier"], "gold", "Valid tier preserved")
results.assert_equals(tier_rows[1]["loyalty_tier"], "unknown", "Invalid tier → unknown")
results.assert_equals(tier_rows[2]["loyalty_tier"], "unknown", "Null tier → unknown")
results.assert_equals(tier_rows[3]["loyalty_tier"], "platinum", "Valid tier preserved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results Summary

# COMMAND ----------

results.summary()
