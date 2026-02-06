# Real-Time Customer 360 - Analytics Platform on Databricks

## Project Overview

A production-grade **Customer 360 Analytics Platform** built on the Databricks Lakehouse, demonstrating end-to-end data engineering with every major Databricks feature. The platform ingests data from multiple source systems (CRM, e-commerce transactions, clickstream, support tickets), processes it through a Medallion Architecture (Bronze -> Silver -> Gold), and produces a unified Customer 360 view powering real-time analytics dashboards.

```
                        ┌─────────────────────────────────────────────────────┐
                        │              Unity Catalog (Governance)              │
                        │   Security · Lineage · Auditing · Data Discovery    │
                        └─────────────────────────────────────────────────────┘
                                              │
  ┌──────────────┐    ┌───────────┐    ┌──────────────┐    ┌──────────────┐
  │ Source Systems│───>│  BRONZE   │───>│   SILVER     │───>│    GOLD      │
  │              │    │ (Raw)     │    │ (Cleaned)    │    │ (Business)   │
  │ • CRM (JSON) │    │           │    │              │    │              │
  │ • Txns (CSV) │    │ Auto      │    │ Streaming +  │    │ Customer 360 │
  │ • Clicks     │    │ Loader    │    │ CDC (MERGE)  │    │ Aggregations │
  │   (Parquet)  │    │ Ingestion │    │ Expectations │    │ Materialized │
  │ • Support    │    │           │    │ Schema Enf.  │    │ Views        │
  │   (Delta CDC)│    └───────────┘    └──────────────┘    └──────────────┘
  └──────────────┘           │                │                    │
                             └────────────────┴────────────────────┘
                                              │
                        ┌─────────────────────────────────────────────────────┐
                        │         DLT / Lakeflow Declarative Pipelines        │
                        │      Orchestrated via Databricks Workflows          │
                        └─────────────────────────────────────────────────────┘
                                              │
              ┌───────────────────────────────┼───────────────────────────────┐
              ▼                               ▼                               ▼
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│  Databricks SQL       │    │  MLflow / ML Ops      │    │  Delta Sharing       │
│  Warehouses ·         │    │  Experiments ·         │    │  Cross-org sharing · │
│  Dashboards · Alerts  │    │  Registry · Serving    │    │  Lakehouse Federation│
└──────────────────────┘    └──────────────────────┘    └──────────────────────┘
              │                               │                               │
              └───────────────────────────────┼───────────────────────────────┘
                                              │
                        ┌─────────────────────────────────────────────────────┐
                        │             Performance Optimization                │
                        │   OPTIMIZE · Z-ORDER · Caching · Photon Engine     │
                        └─────────────────────────────────────────────────────┘
                                              │
              ┌───────────────────────────────┼───────────────────────────────┐
              ▼                               ▼                               ▼
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│  Asset Bundles (DABs) │    │  Databricks Connect   │    │  Data Governance     │
│  CI/CD · IaC ·        │    │  Local IDE · Remote    │    │  Classification ·    │
│  Multi-environment    │    │  Debugging · Testing   │    │  GDPR · Compliance   │
└──────────────────────┘    └──────────────────────┘    └──────────────────────┘
```

---

## Databricks Concepts Covered

| # | Concept | Where in Project | Key Files |
|---|---------|-----------------|-----------|
| 1 | **Delta Lake** (ACID, schema enforcement, time travel, MERGE) | All layers | `src/silver/`, `src/utils/delta_lake_features.py` |
| 2 | **Auto Loader** (incremental file ingestion) | Bronze layer | `src/bronze/` |
| 3 | **Streaming + CDC** (unified batch + streaming) | Silver layer | `src/silver/`, `src/streaming/` |
| 4 | **DLT / Lakeflow Declarative Pipelines** | Pipeline definitions | `src/dlt_pipelines/` |
| 5 | **Expectations** (data quality: fail/drop/warn) | Silver + DLT layers | `src/quality/`, `src/dlt_pipelines/` |
| 6 | **Workflows & Jobs** (orchestration, retries, schedules) | Orchestration | `orchestration/` |
| 7 | **Performance** (OPTIMIZE, Z-ORDER, caching, Photon) | Utilities | `src/utils/performance_optimization.py` |
| 8 | **Unity Catalog** (security, lineage, auditing) | Governance | `governance/` |
| 9 | **Asset Bundles (DABs)** (CI/CD, IaC, multi-environment deployment) | Deployment | `asset_bundles/` |
| 10 | **Databricks Connect** (remote IDE development, local testing) | Development | `src/databricks_connect/` |
| 11 | **Delta Sharing** (cross-organization data sharing, open protocol) | Data Sharing | `src/delta_sharing/` |
| 12 | **Lakehouse Federation** (query external DBs: PostgreSQL, MySQL, Snowflake) | Data Sharing | `src/delta_sharing/lakehouse_federation.py` |
| 13 | **Databricks SQL** (SQL warehouses, dashboards, alerts) | Analytics | `src/databricks_sql/` |
| 14 | **Data Governance** (classification, PII tagging, GDPR/CCPA compliance) | Governance | `governance/data_classification.py`, `governance/compliance_policies.py` |
| 15 | **MLflow** (experiment tracking, model registry, model serving) | ML Operations | `src/ml_operations/` |
| 16 | **Feature Store** (feature engineering, online/offline serving) | ML Operations | `src/ml_operations/feature_store.py` |

---

## Project Structure

```
├── README.md
├── config/
│   ├── pipeline_config.py          # Centralized configuration
│   └── cluster_config.json         # Cluster + Photon settings
│
├── src/
│   ├── bronze/                     # Raw ingestion layer
│   │   ├── ingest_crm_customers.py       # Auto Loader: JSON
│   │   ├── ingest_transactions.py        # Auto Loader: CSV
│   │   ├── ingest_clickstream.py         # Auto Loader: Parquet
│   │   └── ingest_support_tickets.py     # Auto Loader: Delta CDC
│   │
│   ├── silver/                     # Cleaned + conformed layer
│   │   ├── transform_customers.py        # CDC MERGE / SCD Type 2
│   │   ├── transform_transactions.py     # Dedup + enrich
│   │   ├── transform_clickstream.py      # Sessionization
│   │   └── transform_support_tickets.py  # Sentiment join
│   │
│   ├── gold/                       # Business-ready aggregations
│   │   ├── customer_360_view.py          # Unified customer profile
│   │   ├── revenue_analytics.py          # Revenue metrics
│   │   ├── customer_segmentation.py      # RFM segmentation
│   │   └── churn_features.py             # ML feature table
│   │
│   ├── dlt_pipelines/              # Lakeflow Declarative Pipelines
│   │   ├── dlt_bronze_to_silver.py       # DLT: Bronze -> Silver
│   │   ├── dlt_silver_to_gold.py         # DLT: Silver -> Gold
│   │   └── dlt_customer_360_complete.py  # Full end-to-end DLT
│   │
│   ├── streaming/                  # Structured Streaming
│   │   ├── stream_transactions.py        # Real-time txn processing
│   │   └── stream_clickstream.py         # Real-time click processing
│   │
│   ├── quality/                    # Data quality framework
│   │   ├── expectations.py               # Reusable quality rules
│   │   └── quality_monitoring.py         # Quality dashboards
│   │
│   ├── utils/                      # Shared utilities
│   │   ├── delta_lake_features.py        # Time travel, schema evolution
│   │   ├── performance_optimization.py   # OPTIMIZE, Z-ORDER, caching
│   │   └── common_functions.py           # Shared helpers
│   │
│   ├── databricks_sql/             # Databricks SQL Analytics
│   │   ├── sql_warehouse_management.py   # SQL warehouse setup + config
│   │   ├── sql_queries_and_dashboards.py # Dashboard queries + KPIs
│   │   └── sql_alerts_monitoring.py      # SQL alerts + data monitoring
│   │
│   ├── delta_sharing/              # Delta Sharing & Federation
│   │   ├── delta_sharing_provider.py     # Share data (provider side)
│   │   ├── delta_sharing_recipient.py    # Consume shared data (recipient)
│   │   └── lakehouse_federation.py       # Federated queries (PostgreSQL, MySQL, etc.)
│   │
│   ├── databricks_connect/         # Databricks Connect
│   │   ├── remote_development.py         # Remote Spark session + IDE setup
│   │   └── local_testing.py              # Local testing with pytest + CI/CD
│   │
│   └── ml_operations/              # MLflow & ML Operations
│       ├── mlflow_experiment_tracking.py  # Experiment tracking + hyperparams
│       ├── model_registry_serving.py      # Model registry + REST serving
│       └── feature_store.py               # Feature Store + online serving
│
├── governance/                     # Unity Catalog governance
│   ├── unity_catalog_setup.py            # Catalog/schema/table setup
│   ├── access_control.py                 # Row/column security
│   ├── lineage_and_audit.py              # Lineage tracking + audit
│   ├── data_classification.py            # PII discovery + data tagging
│   └── compliance_policies.py            # GDPR/CCPA + data retention
│
├── asset_bundles/                  # Databricks Asset Bundles (DABs)
│   ├── databricks.yml                    # Bundle config (jobs, pipelines, targets)
│   └── bundle_deployment.py              # CI/CD patterns + GitHub Actions
│
├── orchestration/                  # Workflow definitions
│   ├── workflow_daily_batch.py           # Daily batch pipeline job
│   ├── workflow_streaming.py             # Streaming pipeline job
│   └── workflow_definitions.json         # Databricks Jobs API JSON
│
├── data_generator/                 # Sample data for testing
│   └── generate_sample_data.py           # Realistic test data
│
├── tests/                          # Unit + integration tests
│   ├── test_silver_transforms.py         # Transform logic tests
│   └── test_quality_rules.py             # Quality rule tests
│
├── interview_guide/                # Interview preparation
│   └── INTERVIEW_GUIDE.md                # Concepts + Q&A + talking points
│
└── docs/
    └── ARCHITECTURE.md                   # Deep architecture documentation
```

---

## Quick Start

### 1. Prerequisites
- Databricks workspace (Community Edition works for basics)
- Unity Catalog enabled (for governance features)
- Photon-enabled cluster (for performance features)

### 2. Generate Sample Data
```python
# Run in Databricks notebook
%run ./data_generator/generate_sample_data
```

### 3. Run the Full Pipeline
```python
# Option A: Run individual layers
%run ./src/bronze/ingest_crm_customers
%run ./src/silver/transform_customers
%run ./src/gold/customer_360_view

# Option B: Run the DLT pipeline (recommended)
# Configure in Workflows > Delta Live Tables > Create Pipeline
# Source: src/dlt_pipelines/dlt_customer_360_complete.py

# Option C: Run via Workflow orchestration
# Import orchestration/workflow_definitions.json into Databricks Jobs

# Option D: Deploy via Asset Bundles (CI/CD)
# cd asset_bundles && databricks bundle deploy -t dev
```

### 4. Explore Delta Lake Features
```python
%run ./src/utils/delta_lake_features
%run ./src/utils/performance_optimization
```

### 5. Explore SQL Analytics
```python
# Connect to a SQL Warehouse and run queries from:
# src/databricks_sql/sql_queries_and_dashboards.py
```

### 6. Explore ML Operations
```python
%run ./src/ml_operations/mlflow_experiment_tracking
%run ./src/ml_operations/feature_store
```

---

## Layer Details

### Bronze Layer (Raw Ingestion)
- **Auto Loader** with `cloudFiles` for schema inference and incremental processing
- Checkpoint-based exactly-once guarantees
- Supports JSON, CSV, Parquet, and Delta CDC formats
- Rescue data column for malformed records

### Silver Layer (Cleaned & Conformed)
- **MERGE INTO** for CDC / upsert patterns
- **SCD Type 2** for slowly changing dimensions
- **Schema enforcement** with `mergeSchema` for evolution
- **Structured Streaming** with watermarks and deduplication
- **Data quality expectations** that fail, drop, or warn on bad records

### Gold Layer (Business Aggregations)
- Materialized **Customer 360** profile combining all sources
- **RFM segmentation** (Recency, Frequency, Monetary)
- **Revenue analytics** with time-series rollups
- **Churn prediction features** for ML pipelines

### Databricks SQL (Analytics Layer)
- **SQL Warehouses** (Classic, Pro, Serverless) for compute
- **Dashboards** with KPI counters, trend charts, and segmentation views
- **SQL Alerts** for data freshness, quality, and business metric monitoring

### Delta Sharing & Lakehouse Federation
- **Delta Sharing** (provider + recipient) for cross-organization data sharing
- **Lakehouse Federation** for querying external databases (PostgreSQL, MySQL, Snowflake)
- Zero-copy sharing with full Unity Catalog governance

### MLflow & ML Operations
- **Experiment Tracking** with parameters, metrics, and artifacts
- **Model Registry** in Unity Catalog with versioning and aliases
- **Model Serving** via REST API with A/B testing support
- **Feature Store** with point-in-time lookups and online serving

### Asset Bundles & CI/CD
- **Databricks Asset Bundles** (DABs) for Infrastructure-as-Code
- Multi-environment deployment (dev, staging, prod)
- **GitHub Actions** integration for automated deployments
- Service principal-based production deployments

### Data Governance & Compliance
- **Data classification** with Unity Catalog tags (PII, PCI, sensitivity levels)
- **Automated PII discovery** via pattern matching
- **GDPR compliance** (Right to Erasure, Data Subject Access Requests)
- **CCPA compliance** (Do Not Sell opt-out)
- **Data retention policies** with automated VACUUM enforcement

---

## Key Interview Talking Points

1. **"Why Delta Lake over plain Parquet?"** - ACID transactions, time travel, schema enforcement, MERGE support, audit history
2. **"How do you handle late-arriving data?"** - Watermarks in Structured Streaming + MERGE upserts in Silver layer
3. **"How do you ensure data quality?"** - DLT Expectations (fail/drop/warn) + custom quality framework + quarantine tables
4. **"How do you optimize query performance?"** - OPTIMIZE + Z-ORDER on high-cardinality join/filter columns, Photon engine, caching
5. **"How do you handle schema changes?"** - Auto Loader schema hints + `mergeSchema` option + schema evolution in Delta Lake
6. **"How do you secure a multi-tenant lakehouse?"** - Unity Catalog with row-level security, column masking, data lineage, audit logs
7. **"How do you deploy pipelines to production?"** - Databricks Asset Bundles with multi-environment targets + GitHub Actions CI/CD
8. **"How do you share data across organizations?"** - Delta Sharing (open protocol) for cross-org + Lakehouse Federation for external DBs
9. **"How do you handle GDPR compliance?"** - PII tagging, MERGE-based deletion, VACUUM for physical removal, audit trail
10. **"How do you serve ML models?"** - MLflow Model Registry in Unity Catalog + Model Serving REST API with A/B testing
11. **"How do you develop locally?"** - Databricks Connect v2 with Spark Connect protocol for IDE debugging against remote clusters
12. **"How do you build dashboards?"** - Databricks SQL with Lakeview dashboards, scheduled refresh, and SQL Alerts for monitoring

See `interview_guide/INTERVIEW_GUIDE.md` for 50+ detailed Q&A pairs.
