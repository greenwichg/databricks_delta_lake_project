# Databricks notebook source

# MAGIC %md
# MAGIC # Delta Sharing: Provider (Data Publisher)
# MAGIC
# MAGIC ## What is Delta Sharing?
# MAGIC Delta Sharing is an **open protocol** for secure, real-time data sharing:
# MAGIC - **Cross-organization** sharing without data copying
# MAGIC - **Cross-platform** - recipients can use Spark, pandas, Power BI, Tableau
# MAGIC - **Governed** via Unity Catalog (track who accesses what)
# MAGIC - **Live data** - recipients always see the latest version
# MAGIC
# MAGIC ## Provider vs Recipient
# MAGIC ```
# MAGIC ┌──────────────────────┐                ┌──────────────────────┐
# MAGIC │     PROVIDER         │   Delta Share   │     RECIPIENT        │
# MAGIC │  (Data Publisher)    │ ──────────────> │  (Data Consumer)     │
# MAGIC │                      │                 │                      │
# MAGIC │  • Creates shares    │                 │  • Reads shared data │
# MAGIC │  • Grants access     │                 │  • Creates catalog   │
# MAGIC │  • Manages recipients│                 │  • Queries tables    │
# MAGIC │  • Monitors usage    │                 │  • No data copy      │
# MAGIC └──────────────────────┘                 └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Sharing Models
# MAGIC | Model | Description | Use Case |
# MAGIC |-------|------------|----------|
# MAGIC | **Open sharing** | Share via activation link / token file | External partners without Databricks |
# MAGIC | **Databricks-to-Databricks** | Share via Unity Catalog | Teams in different Databricks workspaces |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a Share (Collection of Tables to Share)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a share for the Customer 360 analytics data
# MAGIC CREATE SHARE IF NOT EXISTS customer_360_analytics_share
# MAGIC COMMENT 'Customer 360 aggregated analytics for partner teams';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add Gold layer tables to the share
# MAGIC -- Only share business-ready aggregated tables, never raw data
# MAGIC ALTER SHARE customer_360_analytics_share
# MAGIC ADD TABLE customer_360_catalog.gold.customer_segments
# MAGIC COMMENT 'Customer segmentation (RFM-based)';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER SHARE customer_360_analytics_share
# MAGIC ADD TABLE customer_360_catalog.gold.revenue_analytics
# MAGIC COMMENT 'Revenue analytics with time-series rollups';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Share a table with a different name (alias)
# MAGIC ALTER SHARE customer_360_analytics_share
# MAGIC ADD TABLE customer_360_catalog.gold.customer_360
# MAGIC AS customer_analytics.customer_profiles
# MAGIC COMMENT 'Unified customer profiles (anonymized)';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Share a table with PARTITIONS for data filtering
# MAGIC -- Recipients only see data matching the partition spec
# MAGIC ALTER SHARE customer_360_analytics_share
# MAGIC ADD TABLE customer_360_catalog.gold.revenue_analytics
# MAGIC PARTITION (region = 'US')
# MAGIC AS customer_analytics.revenue_us_only
# MAGIC COMMENT 'Revenue analytics - US region only';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Manage Recipients

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a recipient for Databricks-to-Databricks sharing
# MAGIC CREATE RECIPIENT IF NOT EXISTS partner_analytics_team
# MAGIC USING ID 'aws:us-west-2:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
# MAGIC COMMENT 'Partner analytics team - Databricks workspace';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a recipient for open sharing (non-Databricks)
# MAGIC -- This generates an activation link
# MAGIC CREATE RECIPIENT IF NOT EXISTS external_bi_team
# MAGIC COMMENT 'External BI team using Tableau/Power BI';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant access to the share
# MAGIC GRANT SELECT ON SHARE customer_360_analytics_share TO RECIPIENT partner_analytics_team;
# MAGIC GRANT SELECT ON SHARE customer_360_analytics_share TO RECIPIENT external_bi_team;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Programmatic Share Management

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def manage_shares_programmatically():
    """
    Manage Delta Shares using the Databricks SDK.
    Useful for automating share creation in CI/CD pipelines.
    """
    w = WorkspaceClient()

    # --- List all shares ---
    shares = w.shares.list()
    print("=== Existing Shares ===")
    for share in shares:
        print(f"  Share: {share.name}")
        if share.objects:
            for obj in share.objects:
                print(f"    - {obj.name} ({obj.data_object_type})")

    # --- List all recipients ---
    recipients = w.recipients.list()
    print("\n=== Recipients ===")
    for recipient in recipients:
        print(f"  {recipient.name} (activated: {recipient.activated})")

    # --- Get share permissions ---
    perms = w.shares.share_permissions("customer_360_analytics_share")
    print("\n=== Share Permissions ===")
    for assignment in perms.privilege_assignments:
        print(f"  {assignment.principal}: {[p.privilege for p in assignment.privileges]}")


def get_activation_link(recipient_name):
    """
    Get the activation link for an open sharing recipient.
    Send this link to the recipient to grant them access.
    """
    w = WorkspaceClient()

    info = w.recipients.get(recipient_name)
    if info.tokens:
        for token in info.tokens:
            print(f"Activation link: {token.activation_url}")
            print(f"Expiration: {token.expiration_time}")
    else:
        print("No activation tokens found. Recipient may already be activated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Share with Change Data Feed (CDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable Change Data Feed on a table before sharing
# MAGIC ALTER TABLE customer_360_catalog.gold.customer_360
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Share table with history (CDF) - recipients can read change feed
# MAGIC ALTER SHARE customer_360_analytics_share
# MAGIC ADD TABLE customer_360_catalog.gold.customer_360
# MAGIC WITH HISTORY
# MAGIC COMMENT 'Customer profiles with change data feed for incremental sync';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Monitor Share Usage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View share details
# MAGIC DESCRIBE SHARE customer_360_analytics_share;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recipient details
# MAGIC DESCRIBE RECIPIENT partner_analytics_team;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Audit share access via system tables
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     service_name,
# MAGIC     action_name,
# MAGIC     request_params.share_name,
# MAGIC     request_params.recipient_name,
# MAGIC     source_ip_address,
# MAGIC     user_identity.email AS accessed_by
# MAGIC FROM system.access.audit
# MAGIC WHERE service_name = 'unityCatalog'
# MAGIC   AND action_name LIKE '%Share%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Delta Sharing (Provider)
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What is Delta Sharing?** | Open protocol for secure, real-time data sharing without copying |
# MAGIC | **Open vs D2D sharing?** | Open: any platform via token. D2D: native Unity Catalog integration |
# MAGIC | **How is data secured?** | Recipients only see tables explicitly added to the share; partition filtering for row-level control |
# MAGIC | **Does data get copied?** | No - recipients read directly from provider's storage (read-only) |
# MAGIC | **Change Data Feed sharing?** | WITH HISTORY lets recipients do incremental reads via CDF |
# MAGIC | **How do you audit?** | System tables track all share access events |
