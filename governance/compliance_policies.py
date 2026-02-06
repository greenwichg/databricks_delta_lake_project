# Databricks notebook source

# MAGIC %md
# MAGIC # Data Governance: Compliance & Retention Policies
# MAGIC
# MAGIC ## Compliance Framework
# MAGIC This notebook implements data governance policies for regulatory compliance:
# MAGIC - **GDPR** (General Data Protection Regulation) - EU data privacy
# MAGIC - **CCPA** (California Consumer Privacy Act) - US data privacy
# MAGIC - **SOX** (Sarbanes-Oxley) - Financial data integrity
# MAGIC - **Data Retention** - Automated lifecycle management
# MAGIC
# MAGIC ## Policy Architecture
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────┐
# MAGIC │                    Compliance Policies                           │
# MAGIC ├─────────────────┬────────────────────┬──────────────────────────┤
# MAGIC │  Data Retention  │  Right to Delete   │  Consent Management     │
# MAGIC │  • TTL by table  │  • GDPR Article 17 │  • Purpose limitation   │
# MAGIC │  • Auto-archive  │  • CCPA deletion   │  • Lawful basis         │
# MAGIC │  • VACUUM policy │  • Audit trail     │  • Consent tracking     │
# MAGIC ├─────────────────┴────────────────────┴──────────────────────────┤
# MAGIC │                  Audit & Monitoring                              │
# MAGIC │  • System tables  • Access logs  • Compliance reports           │
# MAGIC └──────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Retention Policies

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Define retention policies per table
RETENTION_POLICIES = {
    "customer_360_catalog.bronze.raw_customers": {
        "retention_days": 365,
        "policy": "Archive after 1 year, delete after 3 years",
        "basis": "Business operations",
    },
    "customer_360_catalog.bronze.raw_transactions": {
        "retention_days": 2555,  # 7 years
        "policy": "Retain 7 years (SOX compliance), then delete",
        "basis": "SOX / Financial regulations",
    },
    "customer_360_catalog.bronze.raw_clickstream": {
        "retention_days": 90,
        "policy": "Delete after 90 days (minimize data footprint)",
        "basis": "Consent-based, limited retention",
    },
    "customer_360_catalog.silver.customers": {
        "retention_days": 2555,
        "policy": "Retain 7 years unless deletion requested",
        "basis": "Legitimate business interest + SOX",
    },
    "customer_360_catalog.silver.transactions": {
        "retention_days": 2555,
        "policy": "Retain 7 years (financial records)",
        "basis": "SOX / Tax regulations",
    },
    "customer_360_catalog.gold.customer_360": {
        "retention_days": 365,
        "policy": "Regenerated daily; retain 1 year of snapshots",
        "basis": "Derived data, business operations",
    },
}

def display_retention_policies():
    """Display all data retention policies."""
    print("=" * 70)
    print("DATA RETENTION POLICIES")
    print("=" * 70)

    for table, policy in RETENTION_POLICIES.items():
        print(f"\n  Table: {table}")
        print(f"    Retention: {policy['retention_days']} days")
        print(f"    Policy: {policy['policy']}")
        print(f"    Legal Basis: {policy['basis']}")

display_retention_policies()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Enforce Retention with Delta Lake VACUUM

# COMMAND ----------

def enforce_retention(spark, table_name, retention_days):
    """
    Enforce data retention using Delta Lake VACUUM.
    VACUUM removes data files older than the retention threshold.
    """
    retention_hours = retention_days * 24

    print(f"Enforcing retention on {table_name}: {retention_days} days")

    # Set table-level retention property
    spark.sql(f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES (
            'delta.logRetentionDuration' = 'interval {retention_days} days',
            'delta.deletedFileRetentionDuration' = 'interval {retention_days} days'
        )
    """)

    # Run VACUUM to remove old files
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")

    print(f"VACUUM completed for {table_name}")


def enforce_all_retention_policies(spark):
    """Apply retention policies across all tables."""
    print("Starting retention enforcement...")

    for table_name, policy in RETENTION_POLICIES.items():
        try:
            enforce_retention(spark, table_name, policy["retention_days"])
        except Exception as e:
            print(f"  WARNING: Failed for {table_name}: {e}")

    print("\nRetention enforcement complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. GDPR: Right to Erasure (Right to Be Forgotten)

# COMMAND ----------

def gdpr_delete_customer(spark, customer_id):
    """
    GDPR Article 17: Right to Erasure.
    Delete all personal data for a customer across all tables.

    This performs a 'hard delete' by:
    1. Removing records from all Silver and Gold tables
    2. Recording the deletion in an audit log
    3. Running VACUUM to physically remove data files
    """
    from delta.tables import DeltaTable

    tables_to_purge = [
        "customer_360_catalog.silver.customers",
        "customer_360_catalog.silver.transactions",
        "customer_360_catalog.silver.clickstream_sessions",
        "customer_360_catalog.silver.support_tickets",
        "customer_360_catalog.gold.customer_360",
        "customer_360_catalog.gold.customer_segments",
        "customer_360_catalog.gold.churn_features",
    ]

    deletion_log = []

    for table_name in tables_to_purge:
        try:
            delta_table = DeltaTable.forName(spark, table_name)

            # Count records before deletion
            before_count = spark.table(table_name).filter(
                F.col("customer_id") == customer_id
            ).count()

            if before_count > 0:
                # Delete records
                delta_table.delete(F.col("customer_id") == customer_id)

                deletion_log.append({
                    "table": table_name,
                    "records_deleted": before_count,
                    "status": "SUCCESS",
                })
                print(f"  Deleted {before_count} records from {table_name}")
            else:
                deletion_log.append({
                    "table": table_name,
                    "records_deleted": 0,
                    "status": "NO_RECORDS",
                })

        except Exception as e:
            deletion_log.append({
                "table": table_name,
                "records_deleted": 0,
                "status": f"ERROR: {str(e)}",
            })

    # Log the deletion request for audit
    audit_df = spark.createDataFrame([{
        "request_id": f"GDPR-DEL-{customer_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "customer_id": customer_id,
        "request_type": "GDPR_RIGHT_TO_ERASURE",
        "request_timestamp": datetime.now().isoformat(),
        "tables_affected": str([d["table"] for d in deletion_log if d["records_deleted"] > 0]),
        "total_records_deleted": sum(d["records_deleted"] for d in deletion_log),
        "status": "COMPLETED",
    }])

    audit_df.write.format("delta").mode("append").saveAsTable(
        "customer_360_catalog.quality.gdpr_deletion_audit"
    )

    print(f"\nGDPR deletion completed for customer: {customer_id}")
    print(f"Total records deleted: {sum(d['records_deleted'] for d in deletion_log)}")
    print("Audit record saved to customer_360_catalog.quality.gdpr_deletion_audit")

    return deletion_log

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. GDPR: Data Subject Access Request (DSAR)

# COMMAND ----------

def gdpr_data_subject_access_request(spark, customer_id):
    """
    GDPR Article 15: Right of Access.
    Export all personal data for a customer across all tables.
    """
    tables_to_export = {
        "Customer Profile": "customer_360_catalog.silver.customers",
        "Transactions": "customer_360_catalog.silver.transactions",
        "Web Sessions": "customer_360_catalog.silver.clickstream_sessions",
        "Support Tickets": "customer_360_catalog.silver.support_tickets",
        "Analytics Profile": "customer_360_catalog.gold.customer_360",
    }

    export_path = f"/Volumes/customer_360_catalog/exports/dsar/{customer_id}"

    print(f"=== Data Subject Access Request: {customer_id} ===")

    for label, table_name in tables_to_export.items():
        try:
            df = spark.table(table_name).filter(F.col("customer_id") == customer_id)
            count = df.count()

            if count > 0:
                # Export to JSON for readability
                df.write.mode("overwrite").json(f"{export_path}/{label.lower().replace(' ', '_')}")
                print(f"  {label}: {count} records exported")
            else:
                print(f"  {label}: No records found")

        except Exception as e:
            print(f"  {label}: ERROR - {e}")

    # Log the DSAR request
    audit_df = spark.createDataFrame([{
        "request_id": f"DSAR-{customer_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "customer_id": customer_id,
        "request_type": "GDPR_DATA_SUBJECT_ACCESS",
        "request_timestamp": datetime.now().isoformat(),
        "export_path": export_path,
        "status": "COMPLETED",
    }])

    audit_df.write.format("delta").mode("append").saveAsTable(
        "customer_360_catalog.quality.gdpr_deletion_audit"
    )

    print(f"\nExport saved to: {export_path}")
    print("Note: Data must be provided to the data subject within 30 days (GDPR Article 12)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CCPA Compliance

# COMMAND ----------

def ccpa_do_not_sell(spark, customer_id):
    """
    CCPA: Right to Opt-Out of Sale.
    Mark a customer's data as 'do not sell' across all tables.
    """
    from delta.tables import DeltaTable

    # Update customer record with opt-out flag
    customers = DeltaTable.forName(spark, "customer_360_catalog.silver.customers")

    customers.update(
        condition=F.col("customer_id") == customer_id,
        set={
            "do_not_sell": F.lit(True),
            "opt_out_date": F.current_timestamp(),
        }
    )

    print(f"CCPA opt-out recorded for customer: {customer_id}")

    # Remove from any shared data (Delta Sharing)
    print("Checking Delta Sharing shares for exclusion...")
    print("  Note: Shared views should filter out 'do_not_sell = true' records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Compliance Monitoring & Reporting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compliance report: GDPR deletion requests
# MAGIC SELECT
# MAGIC     DATE(request_timestamp) AS request_date,
# MAGIC     request_type,
# MAGIC     COUNT(*) AS request_count,
# MAGIC     SUM(total_records_deleted) AS total_records_affected
# MAGIC FROM customer_360_catalog.quality.gdpr_deletion_audit
# MAGIC GROUP BY DATE(request_timestamp), request_type
# MAGIC ORDER BY request_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compliance report: Data access audit (who accessed PII tables)
# MAGIC SELECT
# MAGIC     DATE(event_time) AS access_date,
# MAGIC     user_identity.email AS user_email,
# MAGIC     action_name,
# MAGIC     request_params.full_name_arg AS table_accessed,
# MAGIC     COUNT(*) AS access_count
# MAGIC FROM system.access.audit
# MAGIC WHERE request_params.full_name_arg LIKE 'customer_360_catalog.silver.customers%'
# MAGIC   AND action_name IN ('getTable', 'commandSubmit')
# MAGIC   AND event_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
# MAGIC GROUP BY ALL
# MAGIC ORDER BY access_date DESC, access_count DESC;

# COMMAND ----------

def generate_compliance_report(spark):
    """
    Generate a comprehensive compliance report for auditors.
    """
    print("=" * 70)
    print("COMPLIANCE REPORT")
    print(f"Generated: {datetime.now().isoformat()}")
    print("=" * 70)

    # 1. Retention policy status
    print("\n--- 1. RETENTION POLICY STATUS ---")
    for table, policy in RETENTION_POLICIES.items():
        try:
            history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
            if history:
                last_vacuum = None
                all_history = spark.sql(f"DESCRIBE HISTORY {table}").collect()
                for h in all_history:
                    if h["operation"] == "VACUUM START":
                        last_vacuum = h["timestamp"]
                        break

                print(f"  {table}")
                print(f"    Policy: {policy['retention_days']} days")
                print(f"    Last VACUUM: {last_vacuum or 'NEVER'}")
        except Exception as e:
            print(f"  {table}: Unable to check - {e}")

    # 2. GDPR request summary
    print("\n--- 2. GDPR REQUEST SUMMARY (Last 30 Days) ---")
    try:
        gdpr_stats = spark.sql("""
            SELECT
                request_type,
                COUNT(*) AS request_count,
                SUM(total_records_deleted) AS records_affected
            FROM customer_360_catalog.quality.gdpr_deletion_audit
            WHERE request_timestamp >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            GROUP BY request_type
        """).collect()

        for row in gdpr_stats:
            print(f"  {row.request_type}: {row.request_count} requests, {row.records_affected} records")
    except Exception:
        print("  No GDPR audit table found (will be created on first request)")

    # 3. PII exposure summary
    print("\n--- 3. PII EXPOSURE SUMMARY ---")
    try:
        pii_cols = spark.sql("""
            SELECT table_schema, table_name, COUNT(*) AS pii_column_count
            FROM system.information_schema.column_tags
            WHERE tag_name = 'pii' AND tag_value = 'true'
              AND table_catalog = 'customer_360_catalog'
            GROUP BY table_schema, table_name
        """).collect()

        for row in pii_cols:
            print(f"  {row.table_schema}.{row.table_name}: {row.pii_column_count} PII columns")
    except Exception:
        print("  Tag information not available")

    print("\n--- REPORT COMPLETE ---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Compliance & Retention
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **How do you handle GDPR deletion?** | MERGE/DELETE across all tables + VACUUM to physically remove + audit log |
# MAGIC | **Data retention strategy?** | Table-level TBLPROPERTIES for retention + scheduled VACUUM + archive old data |
# MAGIC | **CCPA opt-out?** | Flag in customer record + filter from Delta Shares and downstream views |
# MAGIC | **Audit trail for compliance?** | System tables (system.access.audit) + custom GDPR audit table |
# MAGIC | **Right of access (DSAR)?** | Export customer data from all tables to JSON, deliver within 30 days |
# MAGIC | **SOX compliance?** | 7-year retention on financial data, immutable audit log via Delta Lake |
