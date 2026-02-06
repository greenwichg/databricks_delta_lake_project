# Databricks notebook source

# MAGIC %md
# MAGIC # Databricks SQL: Queries & Dashboards
# MAGIC
# MAGIC This notebook demonstrates production SQL queries for the Customer 360 platform
# MAGIC and patterns for building Databricks SQL Dashboards.
# MAGIC
# MAGIC ## Dashboard Architecture
# MAGIC ```
# MAGIC Gold Tables ──> SQL Queries ──> Dashboard Widgets ──> Scheduled Refresh
# MAGIC                                    │
# MAGIC                              ┌─────┼─────┐
# MAGIC                              ▼     ▼     ▼
# MAGIC                           Counter Table  Chart
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Executive KPI Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI: Total customers, active rate, average LTV
# MAGIC -- Dashboard widget: Counter / Stat cards
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT customer_id) AS total_customers,
# MAGIC     COUNT(DISTINCT CASE WHEN last_activity_date >= DATEADD(DAY, -30, CURRENT_DATE())
# MAGIC                        THEN customer_id END) AS active_customers_30d,
# MAGIC     ROUND(
# MAGIC         COUNT(DISTINCT CASE WHEN last_activity_date >= DATEADD(DAY, -30, CURRENT_DATE())
# MAGIC                             THEN customer_id END) * 100.0 /
# MAGIC         NULLIF(COUNT(DISTINCT customer_id), 0), 1
# MAGIC     ) AS active_rate_pct,
# MAGIC     ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value,
# MAGIC     ROUND(SUM(lifetime_value), 2) AS total_lifetime_value
# MAGIC FROM customer_360_catalog.gold.customer_360;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI: Revenue metrics (daily, weekly, monthly)
# MAGIC -- Dashboard widget: Counter with comparison to previous period
# MAGIC WITH current_period AS (
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN transaction_date >= DATEADD(DAY, -1, CURRENT_DATE())
# MAGIC                  THEN amount END) AS revenue_today,
# MAGIC         SUM(CASE WHEN transaction_date >= DATEADD(DAY, -7, CURRENT_DATE())
# MAGIC                  THEN amount END) AS revenue_7d,
# MAGIC         SUM(CASE WHEN transaction_date >= DATEADD(DAY, -30, CURRENT_DATE())
# MAGIC                  THEN amount END) AS revenue_30d,
# MAGIC         COUNT(DISTINCT CASE WHEN transaction_date >= DATEADD(DAY, -1, CURRENT_DATE())
# MAGIC                             THEN transaction_id END) AS txn_count_today
# MAGIC     FROM customer_360_catalog.silver.transactions
# MAGIC     WHERE status = 'completed'
# MAGIC ),
# MAGIC previous_period AS (
# MAGIC     SELECT
# MAGIC         SUM(CASE WHEN transaction_date BETWEEN DATEADD(DAY, -60, CURRENT_DATE())
# MAGIC                  AND DATEADD(DAY, -30, CURRENT_DATE()) THEN amount END) AS revenue_prev_30d
# MAGIC     FROM customer_360_catalog.silver.transactions
# MAGIC     WHERE status = 'completed'
# MAGIC )
# MAGIC SELECT
# MAGIC     c.revenue_today,
# MAGIC     c.revenue_7d,
# MAGIC     c.revenue_30d,
# MAGIC     c.txn_count_today,
# MAGIC     ROUND((c.revenue_30d - p.revenue_prev_30d) / NULLIF(p.revenue_prev_30d, 0) * 100, 1)
# MAGIC         AS revenue_growth_pct
# MAGIC FROM current_period c, previous_period p;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer Segmentation Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer segments breakdown
# MAGIC -- Dashboard widget: Pie chart / Donut chart
# MAGIC SELECT
# MAGIC     segment,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(AVG(lifetime_value), 2) AS avg_ltv,
# MAGIC     ROUND(SUM(lifetime_value), 2) AS total_ltv,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total
# MAGIC FROM customer_360_catalog.gold.customer_segments
# MAGIC GROUP BY segment
# MAGIC ORDER BY total_ltv DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Loyalty tier distribution with revenue contribution
# MAGIC -- Dashboard widget: Stacked bar chart
# MAGIC SELECT
# MAGIC     loyalty_tier,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(SUM(lifetime_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(lifetime_value), 2) AS avg_revenue,
# MAGIC     ROUND(SUM(lifetime_value) * 100.0 / SUM(SUM(lifetime_value)) OVER(), 1) AS revenue_share_pct
# MAGIC FROM customer_360_catalog.gold.customer_360
# MAGIC GROUP BY loyalty_tier
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Revenue Trend Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily revenue trend (last 90 days)
# MAGIC -- Dashboard widget: Line chart with moving average
# MAGIC SELECT
# MAGIC     DATE(transaction_date) AS txn_date,
# MAGIC     COUNT(DISTINCT transaction_id) AS transaction_count,
# MAGIC     ROUND(SUM(amount), 2) AS daily_revenue,
# MAGIC     ROUND(AVG(SUM(amount)) OVER (
# MAGIC         ORDER BY DATE(transaction_date)
# MAGIC         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC     ), 2) AS revenue_7d_avg,
# MAGIC     ROUND(AVG(SUM(amount)) OVER (
# MAGIC         ORDER BY DATE(transaction_date)
# MAGIC         ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
# MAGIC     ), 2) AS revenue_30d_avg
# MAGIC FROM customer_360_catalog.silver.transactions
# MAGIC WHERE transaction_date >= DATEADD(DAY, -90, CURRENT_DATE())
# MAGIC   AND status = 'completed'
# MAGIC GROUP BY DATE(transaction_date)
# MAGIC ORDER BY txn_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue by product category (monthly)
# MAGIC -- Dashboard widget: Grouped bar chart
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', transaction_date) AS month,
# MAGIC     product_category,
# MAGIC     ROUND(SUM(amount), 2) AS monthly_revenue,
# MAGIC     COUNT(DISTINCT transaction_id) AS transaction_count,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM customer_360_catalog.silver.transactions
# MAGIC WHERE transaction_date >= DATEADD(MONTH, -6, CURRENT_DATE())
# MAGIC   AND status = 'completed'
# MAGIC GROUP BY DATE_TRUNC('month', transaction_date), product_category
# MAGIC ORDER BY month, monthly_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Customer Health & Churn Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Churn risk distribution
# MAGIC -- Dashboard widget: Histogram / Bar chart
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN churn_risk_score >= 0.8 THEN 'Critical (80-100%)'
# MAGIC         WHEN churn_risk_score >= 0.6 THEN 'High (60-80%)'
# MAGIC         WHEN churn_risk_score >= 0.4 THEN 'Medium (40-60%)'
# MAGIC         WHEN churn_risk_score >= 0.2 THEN 'Low (20-40%)'
# MAGIC         ELSE 'Minimal (0-20%)'
# MAGIC     END AS risk_category,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(SUM(lifetime_value), 2) AS revenue_at_risk,
# MAGIC     ROUND(AVG(lifetime_value), 2) AS avg_ltv
# MAGIC FROM customer_360_catalog.gold.churn_features
# MAGIC GROUP BY 1
# MAGIC ORDER BY MIN(churn_risk_score);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top customers at risk of churning (high value + high risk)
# MAGIC -- Dashboard widget: Table with conditional formatting
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.lifetime_value,
# MAGIC     c.loyalty_tier,
# MAGIC     c.total_transactions,
# MAGIC     f.churn_risk_score,
# MAGIC     f.days_since_last_activity,
# MAGIC     f.spend_trend
# MAGIC FROM customer_360_catalog.gold.customer_360 c
# MAGIC INNER JOIN customer_360_catalog.gold.churn_features f
# MAGIC     ON c.customer_id = f.customer_id
# MAGIC WHERE f.churn_risk_score >= 0.7
# MAGIC   AND c.lifetime_value >= 1000
# MAGIC ORDER BY c.lifetime_value DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Operational Data Quality Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table freshness monitoring
# MAGIC -- Dashboard widget: Status indicators (green/yellow/red)
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     last_modified,
# MAGIC     TIMESTAMPDIFF(HOUR, last_modified, CURRENT_TIMESTAMP()) AS hours_since_update,
# MAGIC     CASE
# MAGIC         WHEN TIMESTAMPDIFF(HOUR, last_modified, CURRENT_TIMESTAMP()) < 6 THEN 'FRESH'
# MAGIC         WHEN TIMESTAMPDIFF(HOUR, last_modified, CURRENT_TIMESTAMP()) < 24 THEN 'STALE'
# MAGIC         ELSE 'CRITICAL'
# MAGIC     END AS freshness_status
# MAGIC FROM (
# MAGIC     SELECT 'silver.customers' AS table_name,
# MAGIC            MAX(timestamp) AS last_modified
# MAGIC     FROM (DESCRIBE HISTORY customer_360_catalog.silver.customers LIMIT 1)
# MAGIC     UNION ALL
# MAGIC     SELECT 'silver.transactions' AS table_name,
# MAGIC            MAX(timestamp) AS last_modified
# MAGIC     FROM (DESCRIBE HISTORY customer_360_catalog.silver.transactions LIMIT 1)
# MAGIC     UNION ALL
# MAGIC     SELECT 'gold.customer_360' AS table_name,
# MAGIC            MAX(timestamp) AS last_modified
# MAGIC     FROM (DESCRIBE HISTORY customer_360_catalog.gold.customer_360 LIMIT 1)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row count trends (detect data anomalies)
# MAGIC -- Dashboard widget: Line chart with annotations
# MAGIC SELECT
# MAGIC     'silver.customers' AS table_name,
# MAGIC     (SELECT COUNT(*) FROM customer_360_catalog.silver.customers) AS current_count
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'silver.transactions',
# MAGIC     (SELECT COUNT(*) FROM customer_360_catalog.silver.transactions)
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'gold.customer_360',
# MAGIC     (SELECT COUNT(*) FROM customer_360_catalog.gold.customer_360)
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'gold.revenue_analytics',
# MAGIC     (SELECT COUNT(*) FROM customer_360_catalog.gold.revenue_analytics);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Programmatic Dashboard Management

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def create_dashboard_programmatically():
    """
    Create a Databricks SQL Dashboard using the SDK.
    Useful for templated dashboard deployment across environments.
    """
    w = WorkspaceClient()

    # Create a new Lakeview dashboard (AI/BI Dashboard)
    dashboard = w.lakeview.create(
        display_name="Customer 360 Executive Dashboard",
        warehouse_id="your-warehouse-id",
    )

    print(f"Created dashboard: {dashboard.display_name}")
    print(f"Dashboard ID: {dashboard.dashboard_id}")
    print(f"URL: {dashboard.path}")

    return dashboard.dashboard_id


def schedule_dashboard_refresh(dashboard_id, warehouse_id):
    """
    Schedule automatic dashboard refresh.
    """
    w = WorkspaceClient()

    schedule = w.lakeview.create_schedule(
        dashboard_id=dashboard_id,
        cron_schedule={
            "quartz_cron_expression": "0 0 6,12,18 * * ?",
            "timezone_id": "America/New_York",
        },
    )

    print(f"Dashboard {dashboard_id} scheduled for refresh at 6am, 12pm, 6pm ET")
    return schedule

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: SQL Queries & Dashboards
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What are DBSQL Dashboards?** | SQL-native visualization layer built on Lakeview; supports charts, counters, tables |
# MAGIC | **Lakeview vs Legacy Dashboards?** | Lakeview: AI-assisted, canvas layout, better sharing. Legacy: widget-based, being deprecated |
# MAGIC | **How do you optimize dashboard queries?** | Pre-aggregate in Gold layer, use Z-ORDER on filter columns, leverage caching |
# MAGIC | **Refresh strategy?** | Scheduled refresh (cron) + manual refresh; queries use warehouse caching |
# MAGIC | **Access control?** | Dashboard permissions + underlying table permissions via Unity Catalog |
