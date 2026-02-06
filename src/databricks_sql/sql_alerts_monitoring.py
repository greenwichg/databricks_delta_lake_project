# Databricks notebook source

# MAGIC %md
# MAGIC # Databricks SQL: Alerts & Monitoring
# MAGIC
# MAGIC ## What are SQL Alerts?
# MAGIC SQL Alerts automatically monitor your data and notify you when conditions are met:
# MAGIC - **Threshold-based** alerts (e.g., revenue drops below $X)
# MAGIC - **Anomaly detection** (e.g., row count changes by more than 20%)
# MAGIC - **Data freshness** monitoring (e.g., table not updated in 24h)
# MAGIC - **Quality gates** (e.g., null rate exceeds 5%)
# MAGIC
# MAGIC ## Alert Flow
# MAGIC ```
# MAGIC SQL Query ──> Evaluate Schedule ──> Condition Check ──> Notification
# MAGIC (runs on       (every 5 min /       (>, <, ==, !=)     (Email / Slack
# MAGIC  warehouse)     hourly / daily)                          / PagerDuty)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Freshness Alert Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert: Table freshness check
# MAGIC -- Trigger when: result > 0 (any stale tables)
# MAGIC -- Schedule: Every 1 hour
# MAGIC SELECT COUNT(*) AS stale_table_count
# MAGIC FROM (
# MAGIC     SELECT 'silver.customers' AS table_name,
# MAGIC            MAX(timestamp) AS last_modified
# MAGIC     FROM (DESCRIBE HISTORY customer_360_catalog.silver.customers LIMIT 1)
# MAGIC     HAVING TIMESTAMPDIFF(HOUR, MAX(timestamp), CURRENT_TIMESTAMP()) > 24
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'silver.transactions',
# MAGIC            MAX(timestamp)
# MAGIC     FROM (DESCRIBE HISTORY customer_360_catalog.silver.transactions LIMIT 1)
# MAGIC     HAVING TIMESTAMPDIFF(HOUR, MAX(timestamp), CURRENT_TIMESTAMP()) > 24
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'gold.customer_360',
# MAGIC            MAX(timestamp)
# MAGIC     FROM (DESCRIBE HISTORY customer_360_catalog.gold.customer_360 LIMIT 1)
# MAGIC     HAVING TIMESTAMPDIFF(HOUR, MAX(timestamp), CURRENT_TIMESTAMP()) > 24
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Quality Alert Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert: Null rate in critical columns
# MAGIC -- Trigger when: max_null_pct > 5
# MAGIC -- Schedule: Every 6 hours
# MAGIC SELECT MAX(null_pct) AS max_null_pct
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         'customer_id' AS column_name,
# MAGIC         ROUND(COUNT(CASE WHEN customer_id IS NULL THEN 1 END) * 100.0 / COUNT(*), 2)
# MAGIC             AS null_pct
# MAGIC     FROM customer_360_catalog.silver.customers
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'email',
# MAGIC         ROUND(COUNT(CASE WHEN email IS NULL THEN 1 END) * 100.0 / COUNT(*), 2)
# MAGIC     FROM customer_360_catalog.silver.customers
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'transaction_id',
# MAGIC         ROUND(COUNT(CASE WHEN transaction_id IS NULL THEN 1 END) * 100.0 / COUNT(*), 2)
# MAGIC     FROM customer_360_catalog.silver.transactions
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert: Duplicate records detected
# MAGIC -- Trigger when: duplicate_count > 0
# MAGIC -- Schedule: Daily
# MAGIC SELECT COUNT(*) AS duplicate_count
# MAGIC FROM (
# MAGIC     SELECT customer_id, COUNT(*) AS cnt
# MAGIC     FROM customer_360_catalog.silver.customers
# MAGIC     GROUP BY customer_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Business Metric Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert: Revenue drop detection
# MAGIC -- Trigger when: revenue_change_pct < -20
# MAGIC -- Schedule: Daily at 8am
# MAGIC WITH daily_revenue AS (
# MAGIC     SELECT
# MAGIC         DATE(transaction_date) AS txn_date,
# MAGIC         SUM(amount) AS daily_revenue
# MAGIC     FROM customer_360_catalog.silver.transactions
# MAGIC     WHERE status = 'completed'
# MAGIC       AND transaction_date >= DATEADD(DAY, -7, CURRENT_DATE())
# MAGIC     GROUP BY DATE(transaction_date)
# MAGIC )
# MAGIC SELECT
# MAGIC     ROUND(
# MAGIC         (today.daily_revenue - yesterday.daily_revenue) /
# MAGIC         NULLIF(yesterday.daily_revenue, 0) * 100, 1
# MAGIC     ) AS revenue_change_pct
# MAGIC FROM daily_revenue today
# MAGIC CROSS JOIN daily_revenue yesterday
# MAGIC WHERE today.txn_date = CURRENT_DATE()
# MAGIC   AND yesterday.txn_date = DATEADD(DAY, -1, CURRENT_DATE());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert: High churn risk customers increasing
# MAGIC -- Trigger when: high_risk_count > previous day count by 10%
# MAGIC -- Schedule: Daily
# MAGIC SELECT
# MAGIC     COUNT(*) AS high_risk_count,
# MAGIC     ROUND(SUM(lifetime_value), 2) AS revenue_at_risk
# MAGIC FROM customer_360_catalog.gold.churn_features f
# MAGIC INNER JOIN customer_360_catalog.gold.customer_360 c
# MAGIC     ON f.customer_id = c.customer_id
# MAGIC WHERE f.churn_risk_score >= 0.7;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alert: Unusual transaction volume
# MAGIC -- Trigger when: abs(volume_zscore) > 3 (3 standard deviations from mean)
# MAGIC -- Schedule: Every 4 hours
# MAGIC WITH daily_volumes AS (
# MAGIC     SELECT
# MAGIC         DATE(transaction_date) AS txn_date,
# MAGIC         COUNT(*) AS txn_count
# MAGIC     FROM customer_360_catalog.silver.transactions
# MAGIC     WHERE transaction_date >= DATEADD(DAY, -30, CURRENT_DATE())
# MAGIC     GROUP BY DATE(transaction_date)
# MAGIC ),
# MAGIC stats AS (
# MAGIC     SELECT
# MAGIC         AVG(txn_count) AS mean_count,
# MAGIC         STDDEV(txn_count) AS stddev_count
# MAGIC     FROM daily_volumes
# MAGIC     WHERE txn_date < CURRENT_DATE()
# MAGIC )
# MAGIC SELECT
# MAGIC     ROUND(
# MAGIC         (d.txn_count - s.mean_count) / NULLIF(s.stddev_count, 0), 2
# MAGIC     ) AS volume_zscore
# MAGIC FROM daily_volumes d, stats s
# MAGIC WHERE d.txn_date = CURRENT_DATE();

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Programmatic Alert Management

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def create_alert(query_id, name, condition_column, condition_op, condition_threshold):
    """
    Create a SQL Alert programmatically.

    Args:
        query_id: ID of the saved SQL query to monitor
        name: Alert name
        condition_column: Column to evaluate
        condition_op: Operator (>, <, ==, !=)
        condition_threshold: Threshold value
    """
    w = WorkspaceClient()

    alert = w.alerts.create(
        name=name,
        query_id=query_id,
        condition={
            "op": condition_op,
            "operand": {"column": {"name": condition_column}},
            "threshold": {"value": {"double_value": condition_threshold}},
        },
        display_options={"column": condition_column, "title": name},
    )

    print(f"Created alert: {alert.name} (ID: {alert.id})")
    return alert.id


def setup_customer_360_alerts():
    """
    Set up all monitoring alerts for the Customer 360 platform.
    """
    alerts_config = [
        {
            "name": "Data Freshness - Stale Tables",
            "description": "Fires when any Silver/Gold table hasn't been updated in 24 hours",
            "schedule": "Every 1 hour",
            "condition": "stale_table_count > 0",
            "severity": "HIGH",
        },
        {
            "name": "Data Quality - Null Rate Exceeded",
            "description": "Fires when null rate in critical columns exceeds 5%",
            "schedule": "Every 6 hours",
            "condition": "max_null_pct > 5",
            "severity": "CRITICAL",
        },
        {
            "name": "Data Quality - Duplicates Detected",
            "description": "Fires when duplicate customer records are found",
            "schedule": "Daily",
            "condition": "duplicate_count > 0",
            "severity": "HIGH",
        },
        {
            "name": "Revenue Drop Alert",
            "description": "Fires when daily revenue drops more than 20%",
            "schedule": "Daily at 8am",
            "condition": "revenue_change_pct < -20",
            "severity": "CRITICAL",
        },
        {
            "name": "Churn Risk Spike",
            "description": "Fires when high-risk customer count increases significantly",
            "schedule": "Daily",
            "condition": "high_risk_count increases > 10%",
            "severity": "MEDIUM",
        },
        {
            "name": "Anomalous Transaction Volume",
            "description": "Fires when transaction volume deviates > 3 std devs from mean",
            "schedule": "Every 4 hours",
            "condition": "abs(volume_zscore) > 3",
            "severity": "HIGH",
        },
    ]

    print("=" * 70)
    print("CUSTOMER 360 ALERT CONFIGURATION")
    print("=" * 70)

    for alert in alerts_config:
        print(f"\n  [{alert['severity']}] {alert['name']}")
        print(f"    {alert['description']}")
        print(f"    Schedule: {alert['schedule']}")
        print(f"    Condition: {alert['condition']}")

    print("""
    Notification Destinations:
    ─────────────────────────
    • Email: data-engineering@company.com
    • Slack: #data-platform-alerts (via webhook)
    • PagerDuty: Critical alerts only
    """)

setup_customer_360_alerts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Notification Destinations

# COMMAND ----------

def configure_notification_destinations():
    """
    Set up notification destinations for SQL Alerts.
    """
    w = WorkspaceClient()

    print("""
    === Supported Notification Destinations ===

    1. Email
       - Individual emails or distribution lists
       - Built-in, no setup needed

    2. Slack
       - Requires Slack webhook URL
       - Configure in Workspace Settings > Notification Destinations

    3. PagerDuty
       - Integration key from PagerDuty service
       - Best for critical alerts requiring on-call response

    4. Microsoft Teams
       - Incoming webhook connector URL
       - Posts alert cards with query results

    5. Generic Webhook
       - Any HTTP endpoint
       - Custom integrations (ServiceNow, Jira, etc.)

    === Setup Steps ===
    1. Go to SQL > Alert Destinations
    2. Click "New Destination"
    3. Select type (Slack/PagerDuty/Email/Webhook)
    4. Configure credentials
    5. Test the destination
    6. Attach to alerts
    """)

configure_notification_destinations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: SQL Alerts & Monitoring
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What are SQL Alerts?** | Automated monitors that evaluate SQL queries on a schedule and notify on conditions |
# MAGIC | **What can you monitor?** | Data freshness, quality metrics, business KPIs, anomalies, pipeline health |
# MAGIC | **Notification options?** | Email, Slack, PagerDuty, Teams, generic webhooks |
# MAGIC | **How do alerts execute?** | Run on SQL Warehouses on a cron schedule; compare results to threshold |
# MAGIC | **Alert vs DLT Expectations?** | Alerts: business-level monitoring, post-hoc. DLT Expectations: inline data quality, preventive |
# MAGIC | **Cost optimization?** | Use serverless warehouse for alerts; schedule during off-peak hours |
