# Databricks notebook source

# MAGIC %md
# MAGIC # Data Governance: Classification & Tagging
# MAGIC
# MAGIC ## What is Data Classification?
# MAGIC Data classification is the process of **categorizing data by sensitivity level** to ensure
# MAGIC appropriate security controls are applied:
# MAGIC - **PII (Personally Identifiable Information)**: Names, emails, phone numbers, SSNs
# MAGIC - **PHI (Protected Health Information)**: Medical records, insurance IDs
# MAGIC - **PCI (Payment Card Industry)**: Credit card numbers, CVVs
# MAGIC - **Confidential**: Internal business data, trade secrets
# MAGIC - **Public**: Non-sensitive, freely shareable data
# MAGIC
# MAGIC ## Classification Hierarchy
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    Unity Catalog Tags                           │
# MAGIC ├─────────────────────────────────────────────────────────────────┤
# MAGIC │  Catalog Level    → project, domain, cost_center               │
# MAGIC │  Schema Level     → layer (bronze/silver/gold), team           │
# MAGIC │  Table Level      → data_classification, retention_days        │
# MAGIC │  Column Level     → pii, sensitivity, data_type               │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define Classification Tags

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create tag definitions for data classification
# MAGIC -- Tags are Unity Catalog's mechanism for classification
# MAGIC
# MAGIC -- Sensitivity levels (applied at column or table level)
# MAGIC ALTER CATALOG customer_360_catalog
# MAGIC SET TAGS ('project' = 'customer_360', 'domain' = 'customer_analytics');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag schemas with their architectural layer
# MAGIC ALTER SCHEMA customer_360_catalog.bronze SET TAGS ('layer' = 'bronze', 'data_quality' = 'raw');
# MAGIC ALTER SCHEMA customer_360_catalog.silver SET TAGS ('layer' = 'silver', 'data_quality' = 'cleaned');
# MAGIC ALTER SCHEMA customer_360_catalog.gold SET TAGS ('layer' = 'gold', 'data_quality' = 'curated');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Classify Tables by Sensitivity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classify Silver tables with sensitivity and retention
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC SET TAGS (
# MAGIC     'data_classification' = 'confidential',
# MAGIC     'contains_pii' = 'true',
# MAGIC     'retention_days' = '2555',
# MAGIC     'data_owner' = 'customer_data_team',
# MAGIC     'gdpr_relevant' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customer_360_catalog.silver.transactions
# MAGIC SET TAGS (
# MAGIC     'data_classification' = 'confidential',
# MAGIC     'contains_pii' = 'false',
# MAGIC     'contains_pci' = 'true',
# MAGIC     'retention_days' = '2555',
# MAGIC     'data_owner' = 'payments_team'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customer_360_catalog.gold.customer_360
# MAGIC SET TAGS (
# MAGIC     'data_classification' = 'internal',
# MAGIC     'contains_pii' = 'true',
# MAGIC     'data_owner' = 'analytics_team',
# MAGIC     'purpose' = 'customer_analytics_and_reporting'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customer_360_catalog.gold.revenue_analytics
# MAGIC SET TAGS (
# MAGIC     'data_classification' = 'internal',
# MAGIC     'contains_pii' = 'false',
# MAGIC     'data_owner' = 'finance_team',
# MAGIC     'purpose' = 'revenue_reporting'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Classify Columns (PII Detection)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag individual columns with PII classification
# MAGIC -- This enables automated masking and access controls
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_type' = 'email', 'sensitivity' = 'high');
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC ALTER COLUMN full_name SET TAGS ('pii' = 'true', 'pii_type' = 'name', 'sensitivity' = 'high');
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC ALTER COLUMN phone SET TAGS ('pii' = 'true', 'pii_type' = 'phone', 'sensitivity' = 'medium');
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC ALTER COLUMN address SET TAGS ('pii' = 'true', 'pii_type' = 'address', 'sensitivity' = 'high');
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC ALTER COLUMN customer_id SET TAGS ('pii' = 'false', 'sensitivity' = 'low');
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC ALTER COLUMN loyalty_tier SET TAGS ('pii' = 'false', 'sensitivity' = 'low');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Automated PII Discovery

# COMMAND ----------

from pyspark.sql import functions as F
import re

def discover_pii_columns(spark, table_name):
    """
    Automatically scan a table to discover potential PII columns.
    Uses pattern matching on column names and data sampling.
    """
    df = spark.table(table_name)

    # Column name patterns that suggest PII
    pii_patterns = {
        "email": r"(?i)(email|e_mail|email_address|mail)",
        "name": r"(?i)(name|first_name|last_name|full_name|customer_name)",
        "phone": r"(?i)(phone|mobile|telephone|cell|contact_number)",
        "address": r"(?i)(address|street|city|state|zip|postal|country)",
        "ssn": r"(?i)(ssn|social_security|national_id|tax_id)",
        "credit_card": r"(?i)(card_number|credit_card|cc_num|pan)",
        "date_of_birth": r"(?i)(dob|date_of_birth|birth_date|birthday)",
        "ip_address": r"(?i)(ip_address|ip_addr|source_ip|client_ip)",
    }

    results = []
    for col_name in df.columns:
        for pii_type, pattern in pii_patterns.items():
            if re.match(pattern, col_name):
                results.append({
                    "table": table_name,
                    "column": col_name,
                    "detected_pii_type": pii_type,
                    "confidence": "HIGH (name match)",
                })
                break

    # Sample data for pattern detection
    sample = df.limit(100).toPandas()
    for col_name in sample.columns:
        if sample[col_name].dtype == "object":
            values = sample[col_name].dropna().astype(str)
            if len(values) > 0:
                # Email pattern
                email_matches = values.str.match(r"^[\w.-]+@[\w.-]+\.\w+$").sum()
                if email_matches > len(values) * 0.5:
                    if not any(r["column"] == col_name for r in results):
                        results.append({
                            "table": table_name,
                            "column": col_name,
                            "detected_pii_type": "email",
                            "confidence": "HIGH (data pattern)",
                        })

                # Phone pattern
                phone_matches = values.str.match(r"^[\d\s\-\+\(\)]{7,15}$").sum()
                if phone_matches > len(values) * 0.5:
                    if not any(r["column"] == col_name for r in results):
                        results.append({
                            "table": table_name,
                            "column": col_name,
                            "detected_pii_type": "phone",
                            "confidence": "MEDIUM (data pattern)",
                        })

    print(f"=== PII Discovery Results for {table_name} ===")
    for r in results:
        print(f"  [{r['confidence']}] {r['column']} -> {r['detected_pii_type']}")

    if not results:
        print("  No PII columns detected")

    return results


# Run PII discovery across all Silver tables
def scan_all_tables(spark):
    """Scan all Silver and Gold tables for PII."""
    tables_to_scan = [
        "customer_360_catalog.silver.customers",
        "customer_360_catalog.silver.transactions",
        "customer_360_catalog.silver.clickstream_sessions",
        "customer_360_catalog.silver.support_tickets",
        "customer_360_catalog.gold.customer_360",
    ]

    all_results = []
    for table in tables_to_scan:
        results = discover_pii_columns(spark, table)
        all_results.extend(results)

    print(f"\n=== SUMMARY: Found {len(all_results)} potential PII columns ===")
    return all_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tag-Based Access Control

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query all PII-tagged columns across the catalog
# MAGIC SELECT
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     tag_name,
# MAGIC     tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE tag_name = 'pii' AND tag_value = 'true'
# MAGIC ORDER BY table_schema, table_name, column_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all tables with specific classification
# MAGIC SELECT
# MAGIC     table_catalog,
# MAGIC     table_schema,
# MAGIC     table_name,
# MAGIC     tag_name,
# MAGIC     tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE tag_name = 'data_classification'
# MAGIC ORDER BY tag_value, table_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Classification Report

# COMMAND ----------

def generate_classification_report(spark):
    """
    Generate a comprehensive data classification report.
    """
    print("=" * 70)
    print("DATA CLASSIFICATION REPORT")
    print("=" * 70)

    # Table-level classification
    table_tags = spark.sql("""
        SELECT table_schema, table_name, tag_name, tag_value
        FROM system.information_schema.table_tags
        WHERE table_catalog = 'customer_360_catalog'
        ORDER BY table_schema, table_name
    """).collect()

    print("\n--- Table Classifications ---")
    current_table = ""
    for row in table_tags:
        table_key = f"{row.table_schema}.{row.table_name}"
        if table_key != current_table:
            current_table = table_key
            print(f"\n  {current_table}")
        print(f"    {row.tag_name}: {row.tag_value}")

    # PII column summary
    pii_columns = spark.sql("""
        SELECT table_schema, table_name, column_name, tag_value AS pii_type
        FROM system.information_schema.column_tags
        WHERE table_catalog = 'customer_360_catalog'
          AND tag_name = 'pii_type'
        ORDER BY table_schema, table_name
    """).collect()

    print(f"\n--- PII Columns ({len(pii_columns)} found) ---")
    for row in pii_columns:
        print(f"  {row.table_schema}.{row.table_name}.{row.column_name} -> {row.pii_type}")

    print("""
    ─────────────────────────────────────────
    RECOMMENDATIONS:
    • Apply column masking to all HIGH sensitivity PII columns
    • Enable row-level security for customer-facing tables
    • Schedule PII discovery scans weekly
    • Review and re-classify quarterly
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Data Classification
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What is data classification?** | Categorizing data by sensitivity (PII, PCI, public) to apply appropriate controls |
# MAGIC | **How does Unity Catalog support it?** | Tags at catalog/schema/table/column level + information_schema views |
# MAGIC | **Automated PII discovery?** | Pattern matching on column names + data sampling for email/phone/SSN patterns |
# MAGIC | **How do tags connect to security?** | Tags inform masking policies, access controls, and compliance reporting |
# MAGIC | **GDPR compliance?** | Tag PII columns, enable column masking, implement right-to-delete via MERGE |
# MAGIC | **Audit trail?** | System tables track who accessed tagged columns and when |
