# Databricks notebook source

# MAGIC %md
# MAGIC # Databricks Connect: Local Testing & CI/CD Integration
# MAGIC
# MAGIC This notebook demonstrates how to use Databricks Connect for **local testing**,
# MAGIC **unit testing with pytest**, and **CI/CD pipeline integration**.
# MAGIC
# MAGIC ## Why Local Testing with Databricks Connect?
# MAGIC - **Fast feedback loop**: Run tests from your IDE without deploying to Databricks
# MAGIC - **Real Spark execution**: Tests run on actual Spark, not mocks
# MAGIC - **Unity Catalog access**: Validate against real schemas and data
# MAGIC - **CI/CD ready**: Integrate into GitHub Actions, Azure DevOps, Jenkins

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Testing Setup with pytest

# COMMAND ----------

import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


# ---- Shared Spark Session Fixture ----
# In a real project, this lives in conftest.py

@pytest.fixture(scope="session")
def spark():
    """
    Create a single DatabricksSession for the entire test session.
    Reuses the cluster connection across all tests for efficiency.
    """
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def test_schema(spark):
    """
    Create a temporary test schema for isolated test data.
    Cleaned up after all tests complete.
    """
    schema_name = "customer_360_catalog.test_temp"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    yield schema_name
    spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Unit Tests for Silver Transformations

# COMMAND ----------

class TestCustomerTransformations:
    """Test suite for customer silver layer transformations."""

    def test_email_standardization(self, spark):
        """Verify emails are lowercased and trimmed."""
        input_df = spark.createDataFrame([
            ("CUST-001", "  John.Doe@GMAIL.COM  "),
            ("CUST-002", "jane@email.com"),
            ("CUST-003", None),
        ], ["customer_id", "email"])

        result = input_df.withColumn(
            "email_clean",
            F.lower(F.trim(F.col("email")))
        )

        rows = result.collect()
        assert rows[0]["email_clean"] == "john.doe@gmail.com"
        assert rows[1]["email_clean"] == "jane@email.com"
        assert rows[2]["email_clean"] is None

    def test_deduplication_keeps_latest(self, spark):
        """Verify deduplication keeps the most recent record per customer."""
        from pyspark.sql import Window

        input_df = spark.createDataFrame([
            ("CUST-001", "2024-01-01", "Alice v1"),
            ("CUST-001", "2024-06-15", "Alice v2"),
            ("CUST-002", "2024-03-01", "Bob v1"),
        ], ["customer_id", "updated_at", "name"])

        window = Window.partitionBy("customer_id").orderBy(F.desc("updated_at"))
        result = (
            input_df
            .withColumn("row_num", F.row_number().over(window))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        assert result.count() == 2
        alice = result.filter(F.col("customer_id") == "CUST-001").collect()[0]
        assert alice["name"] == "Alice v2"

    def test_loyalty_tier_assignment(self, spark):
        """Verify loyalty tier is correctly assigned based on lifetime value."""
        input_df = spark.createDataFrame([
            ("CUST-001", 500.0),
            ("CUST-002", 2500.0),
            ("CUST-003", 7500.0),
            ("CUST-004", 15000.0),
        ], ["customer_id", "lifetime_value"])

        result = input_df.withColumn(
            "loyalty_tier",
            F.when(F.col("lifetime_value") >= 10000, "Platinum")
            .when(F.col("lifetime_value") >= 5000, "Gold")
            .when(F.col("lifetime_value") >= 1000, "Silver")
            .otherwise("Bronze")
        )

        rows = {r["customer_id"]: r["loyalty_tier"] for r in result.collect()}
        assert rows["CUST-001"] == "Bronze"
        assert rows["CUST-002"] == "Silver"
        assert rows["CUST-003"] == "Gold"
        assert rows["CUST-004"] == "Platinum"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Integration Tests Against Real Tables

# COMMAND ----------

class TestGoldLayerIntegration:
    """Integration tests that validate Gold layer tables against real data."""

    def test_customer_360_has_all_sources(self, spark):
        """Verify Customer 360 table includes data from all source systems."""
        c360 = spark.table("customer_360_catalog.gold.customer_360")

        schema_fields = [f.name for f in c360.schema.fields]

        # Verify columns from each source exist
        assert "customer_id" in schema_fields, "Missing customer_id"
        assert "total_transactions" in schema_fields or "transaction_count" in schema_fields
        assert "total_sessions" in schema_fields or "session_count" in schema_fields

    def test_customer_360_no_orphan_records(self, spark):
        """Verify all customers in Gold exist in Silver."""
        gold_customers = spark.table("customer_360_catalog.gold.customer_360").select("customer_id")
        silver_customers = spark.table("customer_360_catalog.silver.customers").select("customer_id")

        orphans = gold_customers.subtract(silver_customers)
        assert orphans.count() == 0, f"Found {orphans.count()} orphan records in Gold"

    def test_revenue_analytics_positive_amounts(self, spark):
        """Verify revenue analytics has no negative total amounts."""
        revenue = spark.table("customer_360_catalog.gold.revenue_analytics")

        if "total_revenue" in [f.name for f in revenue.schema.fields]:
            negatives = revenue.filter(F.col("total_revenue") < 0).count()
            assert negatives == 0, f"Found {negatives} negative revenue records"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality Validation Tests

# COMMAND ----------

class TestDataQuality:
    """Test suite for data quality rules across all layers."""

    def test_silver_customers_no_null_ids(self, spark):
        """Critical: customer_id must never be null in Silver."""
        customers = spark.table("customer_360_catalog.silver.customers")
        null_count = customers.filter(F.col("customer_id").isNull()).count()
        assert null_count == 0, f"Found {null_count} null customer_ids"

    def test_silver_transactions_valid_amounts(self, spark):
        """Verify transaction amounts are within expected bounds."""
        txns = spark.table("customer_360_catalog.silver.transactions")
        invalid = txns.filter(
            (F.col("amount") <= 0) | (F.col("amount") > 100000)
        ).count()
        total = txns.count()
        invalid_pct = (invalid / total * 100) if total > 0 else 0
        assert invalid_pct < 5.0, f"Invalid amount rate: {invalid_pct:.2f}% (threshold: 5%)"

    def test_table_freshness(self, spark):
        """Verify data was updated within the last 24 hours."""
        from datetime import datetime, timedelta

        history = spark.sql(
            "DESCRIBE HISTORY customer_360_catalog.silver.customers LIMIT 1"
        ).collect()

        if history:
            last_update = history[0]["timestamp"]
            hours_ago = (datetime.now() - last_update).total_seconds() / 3600
            assert hours_ago < 24, f"Data is {hours_ago:.1f} hours old (threshold: 24h)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CI/CD Pipeline Integration

# COMMAND ----------

def show_ci_cd_test_configuration():
    """
    Show how to integrate Databricks Connect tests into CI/CD pipelines.
    """
    print("""
    ══════════════════════════════════════════════════════════
    CI/CD TEST CONFIGURATION
    ══════════════════════════════════════════════════════════

    === conftest.py ===

    import pytest
    from databricks.connect import DatabricksSession

    @pytest.fixture(scope="session")
    def spark():
        return DatabricksSession.builder.getOrCreate()

    === pytest.ini ===

    [pytest]
    testpaths = tests
    python_files = test_*.py
    python_classes = Test*
    python_functions = test_*
    markers =
        integration: marks tests requiring Databricks cluster
        unit: marks pure unit tests

    === GitHub Actions (run tests) ===

    jobs:
      test:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v4
          - uses: actions/setup-python@v5
            with:
              python-version: '3.11'

          - name: Install dependencies
            run: |
              pip install databricks-connect==14.3.*
              pip install pytest

          - name: Run unit tests
            env:
              DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
              DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
              DATABRICKS_CLUSTER_ID: ${{ secrets.TEST_CLUSTER_ID }}
            run: pytest tests/ -v -m "unit"

          - name: Run integration tests
            env:
              DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
              DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
              DATABRICKS_CLUSTER_ID: ${{ secrets.TEST_CLUSTER_ID }}
            run: pytest tests/ -v -m "integration"

    ══════════════════════════════════════════════════════════
    RUNNING TESTS LOCALLY
    ══════════════════════════════════════════════════════════

    # Set environment variables
    export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
    export DATABRICKS_TOKEN=dapi_xxx
    export DATABRICKS_CLUSTER_ID=0123-456789-abcdefgh

    # Run all tests
    pytest tests/ -v

    # Run only unit tests
    pytest tests/ -v -m "unit"

    # Run with coverage
    pytest tests/ -v --cov=src --cov-report=html
    """)

show_ci_cd_test_configuration()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Local Testing
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **How do you test PySpark locally?** | Databricks Connect v2 with pytest - real Spark execution, not mocks |
# MAGIC | **Unit vs integration tests?** | Unit: test transformations with created DataFrames. Integration: validate real tables |
# MAGIC | **CI/CD testing strategy?** | GitHub Actions with Databricks Connect pointing to a test cluster |
# MAGIC | **How to isolate tests?** | Create temporary schemas, use test fixtures, clean up in teardown |
# MAGIC | **Performance of remote tests?** | Session-scoped fixtures reuse connections; small test data keeps it fast |
