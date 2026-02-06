# Databricks notebook source

# =============================================================================
# PIPELINE CONFIGURATION
# Centralized configuration for the Customer 360 Analytics Platform
# =============================================================================

# ---- Catalog & Schema (Unity Catalog) ----
CATALOG = "customer_360_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
QUALITY_SCHEMA = "quality"

# ---- Storage Paths ----
# Landing zone where raw files arrive (cloud storage)
LANDING_ZONE = "/Volumes/customer_360_catalog/landing/raw_data"
CHECKPOINT_BASE = "/Volumes/customer_360_catalog/checkpoints"

# Source-specific landing paths
CRM_LANDING_PATH = f"{LANDING_ZONE}/crm_customers"
TRANSACTIONS_LANDING_PATH = f"{LANDING_ZONE}/transactions"
CLICKSTREAM_LANDING_PATH = f"{LANDING_ZONE}/clickstream"
SUPPORT_LANDING_PATH = f"{LANDING_ZONE}/support_tickets"

# Checkpoint paths (one per streaming source)
CRM_CHECKPOINT = f"{CHECKPOINT_BASE}/crm_customers"
TRANSACTIONS_CHECKPOINT = f"{CHECKPOINT_BASE}/transactions"
CLICKSTREAM_CHECKPOINT = f"{CHECKPOINT_BASE}/clickstream"
SUPPORT_CHECKPOINT = f"{CHECKPOINT_BASE}/support_tickets"

# ---- Table Names ----
# Bronze tables (raw, append-only)
BRONZE_CUSTOMERS = f"{CATALOG}.{BRONZE_SCHEMA}.raw_customers"
BRONZE_TRANSACTIONS = f"{CATALOG}.{BRONZE_SCHEMA}.raw_transactions"
BRONZE_CLICKSTREAM = f"{CATALOG}.{BRONZE_SCHEMA}.raw_clickstream"
BRONZE_SUPPORT = f"{CATALOG}.{BRONZE_SCHEMA}.raw_support_tickets"

# Silver tables (cleaned, conformed)
SILVER_CUSTOMERS = f"{CATALOG}.{SILVER_SCHEMA}.customers"
SILVER_TRANSACTIONS = f"{CATALOG}.{SILVER_SCHEMA}.transactions"
SILVER_CLICKSTREAM = f"{CATALOG}.{SILVER_SCHEMA}.clickstream_sessions"
SILVER_SUPPORT = f"{CATALOG}.{SILVER_SCHEMA}.support_tickets"

# Gold tables (business-ready)
GOLD_CUSTOMER_360 = f"{CATALOG}.{GOLD_SCHEMA}.customer_360"
GOLD_REVENUE = f"{CATALOG}.{GOLD_SCHEMA}.revenue_analytics"
GOLD_SEGMENTATION = f"{CATALOG}.{GOLD_SCHEMA}.customer_segments"
GOLD_CHURN_FEATURES = f"{CATALOG}.{GOLD_SCHEMA}.churn_features"

# Quality tables
QUARANTINE_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.quarantine_records"
QUALITY_METRICS = f"{CATALOG}.{QUALITY_SCHEMA}.quality_metrics"
GDPR_AUDIT_TABLE = f"{CATALOG}.{QUALITY_SCHEMA}.gdpr_deletion_audit"

# ML / Feature Store tables
GOLD_CHURN_PREDICTIONS = f"{CATALOG}.{GOLD_SCHEMA}.churn_predictions"
GOLD_BEHAVIOR_FEATURES = f"{CATALOG}.{GOLD_SCHEMA}.customer_behavior_features"
CHURN_MODEL_NAME = f"{CATALOG}.{GOLD_SCHEMA}.churn_prediction_model"
CHURN_MODEL_FS_NAME = f"{CATALOG}.{GOLD_SCHEMA}.churn_model_fs"

# ---- Delta Sharing Settings ----
SHARE_NAME = "customer_360_analytics_share"
SHARE_TABLES = [
    f"{CATALOG}.{GOLD_SCHEMA}.customer_segments",
    f"{CATALOG}.{GOLD_SCHEMA}.revenue_analytics",
    f"{CATALOG}.{GOLD_SCHEMA}.customer_360",
]

# ---- Lakehouse Federation Connections ----
FEDERATION_CONNECTIONS = {
    "crm_postgresql": {
        "type": "POSTGRESQL",
        "host": "crm-db.company.internal",
        "port": "5432",
        "secret_scope": "federation_secrets",
    },
    "ecommerce_mysql": {
        "type": "MYSQL",
        "host": "ecommerce-db.company.internal",
        "port": "3306",
        "secret_scope": "federation_secrets",
    },
}

# ---- Databricks SQL Settings ----
SQL_WAREHOUSE_CONFIG = {
    "name": "Customer 360 Analytics Warehouse",
    "cluster_size": "Medium",
    "min_num_clusters": 1,
    "max_num_clusters": 4,
    "auto_stop_mins": 30,
    "enable_photon": True,
}

# ---- MLflow Settings ----
MLFLOW_EXPERIMENT_NAME = "/Workspace/customer_360/experiments/churn_prediction"
MODEL_SERVING_ENDPOINT = "customer-churn-prediction"

# ---- Auto Loader Settings ----
AUTOLOADER_SETTINGS = {
    "cloudFiles.useNotifications": "false",       # Use directory listing (simpler setup)
    "cloudFiles.inferColumnTypes": "true",         # Auto-detect types
    "cloudFiles.schemaEvolutionMode": "addNewColumns",  # Handle schema changes
    "cloudFiles.schemaHints": "",                  # Override per source
}

# ---- Streaming Settings ----
STREAMING_TRIGGER_INTERVAL = "30 seconds"    # For near-real-time
STREAMING_TRIGGER_ONCE = True                # For batch-style streaming (cost-efficient)
WATERMARK_DELAY = "10 minutes"               # Late data tolerance

# ---- Performance Settings ----
OPTIMIZE_ZORDER_COLUMNS = {
    SILVER_CUSTOMERS: ["customer_id"],
    SILVER_TRANSACTIONS: ["customer_id", "transaction_date"],
    SILVER_CLICKSTREAM: ["customer_id", "session_id"],
    GOLD_CUSTOMER_360: ["customer_id"],
    GOLD_REVENUE: ["transaction_date", "product_category"],
}

# ---- Data Quality Thresholds ----
QUALITY_THRESHOLDS = {
    "null_rate_max": 0.05,           # Max 5% nulls in critical columns
    "duplicate_rate_max": 0.01,      # Max 1% duplicates
    "freshness_max_hours": 24,       # Data must be < 24 hours old
    "row_count_min_pct_change": -20, # Alert if row count drops > 20%
}
