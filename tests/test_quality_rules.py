# Databricks notebook source

# MAGIC %md
# MAGIC # Unit Tests: Data Quality Rules
# MAGIC
# MAGIC Tests for the custom data quality engine and rule definitions.

# COMMAND ----------

from pyspark.sql import functions as F

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
        print(f"\nResults: {self.passed}/{total} passed, {self.failed} failed")

results = TestResult()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Null Check Quality Rule

# COMMAND ----------

print("Test Suite: Null Check Quality Rules")

data = spark.createDataFrame([
    ("C-001", "john@example.com", 100.0),
    (None, "jane@example.com", 200.0),       # NULL customer_id
    ("C-003", None, 300.0),                    # NULL email
    ("C-004", "bob@example.com", None),        # NULL amount
], ["customer_id", "email", "amount"])

# Test: customer_id not null
not_null_check = data.filter("customer_id IS NOT NULL")
results.assert_equals(not_null_check.count(), 3, "Null customer_id filtered out")

# Test: email not null and valid format
email_check = data.filter("email IS NOT NULL AND email LIKE '%@%.%'")
results.assert_equals(email_check.count(), 3, "Valid emails pass check")

# Test: amount positive
amount_check = data.filter("amount IS NOT NULL AND amount > 0")
results.assert_equals(amount_check.count(), 3, "Positive amounts pass check")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Combined Quality Rules (expect_all_or_drop pattern)

# COMMAND ----------

print("\nTest Suite: Combined Quality Rules")

txn_data = spark.createDataFrame([
    ("TXN-1", "C-001", 100.0, "2024-01-15"),   # All valid
    ("TXN-2", "C-002", 50.0, "2024-02-20"),    # All valid
    (None, "C-003", 75.0, "2024-03-10"),        # Fails: null txn_id
    ("TXN-4", None, 25.0, "2024-04-05"),        # Fails: null customer_id
    ("TXN-5", "C-005", -10.0, "2024-05-01"),   # Fails: negative amount
    ("TXN-6", "C-006", 0.0, "2024-06-15"),     # Fails: zero amount
], ["transaction_id", "customer_id", "amount", "transaction_date"])

# Combined check (mimics expect_all_or_drop behavior)
combined_condition = """
    transaction_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND amount > 0
    AND transaction_date IS NOT NULL
"""
passed = txn_data.filter(combined_condition)
failed = txn_data.filter(f"NOT ({combined_condition})")

results.assert_equals(passed.count(), 2, "Only 2 records pass all rules")
results.assert_equals(failed.count(), 4, "4 records fail at least one rule")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Quarantine Logic

# COMMAND ----------

print("\nTest Suite: Quarantine Logic")

data = spark.createDataFrame([
    ("C-001", "john@example.com", 50000.0),    # Valid
    ("C-002", "jane@example.com", 150000.0),   # Quarantine: amount > 100K
    ("C-003", "bob@example.com", 75000.0),     # Valid
], ["customer_id", "email", "amount"])

# Quarantine rule: amount > 100K (potential fraud)
quarantine_condition = "amount >= 100000"
normal = data.filter(f"NOT ({quarantine_condition})")
quarantined = data.filter(quarantine_condition)

results.assert_equals(normal.count(), 2, "2 records pass fraud check")
results.assert_equals(quarantined.count(), 1, "1 record quarantined for fraud review")

quarantined_with_reason = (
    quarantined
    .withColumn("_quarantine_reason", F.lit("high_amount_fraud_check"))
    .withColumn("_quarantined_at", F.current_timestamp())
)

results.assert_true(
    "_quarantine_reason" in quarantined_with_reason.columns,
    "Quarantine reason column added"
)

# COMMAND ----------

results.summary()
