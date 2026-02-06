# Architecture: Customer 360 Analytics Platform

## System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                                 │
├──────────────┬───────────────┬──────────────┬────────────────────────┤
│ CRM System   │ E-Commerce    │ Web Analytics│ Support System          │
│ (JSON files) │ (CSV files)   │ (Parquet)    │ (Delta CDC)            │
└──────┬───────┴───────┬───────┴──────┬───────┴──────────┬─────────────┘
       │               │              │                   │
       ▼               ▼              ▼                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                               │
│  ┌──────────────┐ ┌──────────┐ ┌────────────┐ ┌────────────────┐   │
│  │Auto Loader   │ │Auto Loader│ │Auto Loader │ │Delta CDF       │   │
│  │JSON→Delta    │ │CSV→Delta │ │Parquet→Delta│ │CDC Stream      │   │
│  │Schema Infer  │ │Headers   │ │Embedded    │ │Change Types    │   │
│  └──────────────┘ └──────────┘ └────────────┘ └────────────────┘   │
│  Append-only · Rescue column · Metadata · Checkpoints                │
└──────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (Cleaned)                            │
│  ┌──────────────┐ ┌──────────┐ ┌────────────┐ ┌────────────────┐   │
│  │MERGE/Upsert  │ │foreachBatch│ │Sessionize  │ │CDC Apply       │   │
│  │SCD Type 2    │ │Streaming │ │Window Funcs│ │MERGE Upserts   │   │
│  │Dedup+Clean   │ │Dedup     │ │Watermarks  │ │Soft Delete     │   │
│  └──────────────┘ └──────────┘ └────────────┘ └────────────────┘   │
│  Schema enforcement · Data quality · Standardization                 │
│  DLT Expectations: @expect, @expect_or_drop, @expect_or_fail        │
└──────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER (Business)                            │
│  ┌──────────────┐ ┌──────────┐ ┌────────────┐ ┌────────────────┐   │
│  │Customer 360  │ │Revenue   │ │RFM Segments│ │Churn Features  │   │
│  │Unified View  │ │Analytics │ │Business    │ │ML Feature Table│   │
│  │Multi-JOIN    │ │Time-series│ │Segments   │ │Scoring Model   │   │
│  └──────────────┘ └──────────┘ └────────────┘ └────────────────┘   │
│  Aggregations · Materialized views · Business logic                  │
└──────────────────────────────────────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                 ▼
        ┌──────────┐   ┌──────────┐    ┌──────────────┐
        │Dashboards│   │ML Models │    │BI / Reports  │
        │(SQL)     │   │(MLflow)  │    │(Partners)    │
        └──────────┘   └──────────┘    └──────────────┘
```

## Data Flow Patterns

### Pattern 1: Batch Pipeline (Daily)
```
Files land → Auto Loader → Bronze → Silver (MERGE) → Gold (MERGE) → Dashboard
Schedule: Daily at 6am via Databricks Workflows
Trigger: availableNow=True (cost-efficient)
```

### Pattern 2: Streaming Pipeline (Near Real-Time)
```
Files land → Auto Loader → Bronze → Silver (foreachBatch+MERGE) → Dashboard
Trigger: processingTime="10 seconds"
Always running on a streaming cluster
```

### Pattern 3: DLT Pipeline (Declarative)
```
Files land → DLT Bronze → DLT Silver (with Expectations) → DLT Gold
Managed by DLT engine: auto-retry, quality metrics, lineage
Schedule: Every 4 hours or continuous
```

## Technology Mapping

| Concern | Technology | Where |
|---------|-----------|-------|
| File ingestion | Auto Loader (cloudFiles) | Bronze layer |
| Incremental processing | Structured Streaming + checkpoints | All layers |
| CDC/Upserts | MERGE INTO (Delta Lake) | Silver layer |
| Data quality | DLT Expectations + custom framework | Silver layer |
| Orchestration | Databricks Workflows | `orchestration/` |
| Performance | OPTIMIZE + Z-ORDER + Photon | `src/utils/` |
| Governance | Unity Catalog | `governance/` |
| Security | Row-level + Column masking | `governance/access_control.py` |
| Audit | Delta History + System Tables | `governance/lineage_and_audit.py` |
| Data Classification | Unity Catalog Tags + PII discovery | `governance/data_classification.py` |
| Compliance | GDPR/CCPA deletion + retention policies | `governance/compliance_policies.py` |
| SQL Analytics | SQL Warehouses + Dashboards + Alerts | `src/databricks_sql/` |
| Data Sharing | Delta Sharing (open protocol) | `src/delta_sharing/` |
| Federation | Lakehouse Federation (external DBs) | `src/delta_sharing/lakehouse_federation.py` |
| ML Tracking | MLflow experiments + model registry | `src/ml_operations/` |
| Feature Engineering | Feature Store + online serving | `src/ml_operations/feature_store.py` |
| Model Serving | REST API + A/B testing | `src/ml_operations/model_registry_serving.py` |
| CI/CD Deployment | Databricks Asset Bundles (DABs) | `asset_bundles/` |
| Local Development | Databricks Connect v2 | `src/databricks_connect/` |

## Medallion Architecture Principles

### Bronze (Raw)
- **Append-only**: Never update or delete in Bronze
- **Schema-flexible**: Auto Loader handles schema evolution
- **Full fidelity**: Keep all source data including malformed records (`_rescued_data`)
- **Metadata**: Add `_ingested_at`, `_source_file`, `_datasource` for lineage

### Silver (Cleaned)
- **Schema-enforced**: Define and enforce the expected schema
- **Deduplicated**: Remove duplicate records using window functions
- **Conformed**: Standardize formats, enrich with business logic
- **Quality-gated**: Expectations validate data before it enters Silver

### Gold (Business)
- **Aggregated**: Pre-computed metrics and rollups for performance
- **Business-aligned**: Tables match business domain concepts
- **Consumption-ready**: Optimized for BI tools, ML models, and APIs
- **Documented**: Every column has a comment and business definition

## Scalability Design

### Small Scale (Dev/Test)
- Single-node cluster (Community Edition compatible)
- `availableNow=True` triggers
- No partitioning needed
- Directory listing for Auto Loader

### Production Scale
- Photon-enabled auto-scaling clusters
- Streaming with `processingTime` triggers
- Z-ORDER or Liquid Clustering on key columns
- File Notification mode for Auto Loader
- Unity Catalog for multi-team governance

### Enterprise Scale
- Separate clusters per workload (streaming, batch, SQL)
- DLT with continuous mode for critical pipelines
- Cross-workspace data sharing via Unity Catalog
- System tables for billing and usage monitoring
- Service principals for automated access

## Deployment Architecture (Asset Bundles)

```
┌─────────────────────────────────────────────────────────────────┐
│                    CI/CD Pipeline (GitHub Actions)                │
│                                                                  │
│  Push to main → Validate → Deploy Staging → Test → Deploy Prod  │
└──────────────────────────────┬──────────────────────────────────┘
                               │ databricks bundle deploy
                               ▼
┌─────────────┐    ┌─────────────────┐    ┌─────────────────────┐
│  DEV         │    │  STAGING         │    │  PRODUCTION          │
│  (developer  │    │  (integration    │    │  (service principal  │
│   identity)  │    │   testing)       │    │   identity)          │
│              │    │                  │    │                      │
│ customer_360 │    │ customer_360     │    │ customer_360         │
│ _dev catalog │    │ _staging catalog │    │ _prod catalog        │
└─────────────┘    └─────────────────┘    └─────────────────────┘
```

## Data Sharing Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Lakehouse (Unity Catalog)                      │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐│
│  │ Delta Lake    │  │ Delta Sharing │  │ Lakehouse Federation   ││
│  │ Tables        │  │ (Outbound)    │  │ (Inbound)              ││
│  │               │  │               │  │                        ││
│  │ Bronze/Silver │  │ Share Gold    │  │ Query external:        ││
│  │ /Gold layers  │  │ tables with   │  │ • PostgreSQL (CRM)     ││
│  │               │  │ partners      │  │ • MySQL (E-commerce)   ││
│  │               │  │               │  │ • Snowflake (DW)       ││
│  └──────────────┘  └──────────────┘  └────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## ML Operations Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ML Lifecycle                                   │
│                                                                  │
│  Feature Store ──> Experiment Tracking ──> Model Registry         │
│  (Gold layer)      (MLflow)                (Unity Catalog)        │
│       │                  │                       │                │
│       ▼                  ▼                       ▼                │
│  ┌──────────┐     ┌──────────┐           ┌──────────────┐       │
│  │ Offline   │     │ Compare  │           │ champion/     │       │
│  │ (Delta)   │     │ Runs     │           │ challenger    │       │
│  │ Online    │     │ Tune     │           │ aliases       │       │
│  │ (DynamoDB)│     │ Params   │           └──────┬───────┘       │
│  └──────────┘     └──────────┘                  │                │
│                                                  ▼                │
│                                         ┌──────────────┐         │
│                                         │Model Serving  │         │
│                                         │REST API       │         │
│                                         │A/B Testing    │         │
│                                         └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

## Governance & Compliance Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Governance                                │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐│
│  │Classification │  │ Access        │  │ Compliance              ││
│  │               │  │ Control       │  │                        ││
│  │ • PII tags    │  │ • GRANT/REVOKE│  │ • GDPR deletion        ││
│  │ • Sensitivity │  │ • Row-level   │  │ • CCPA opt-out         ││
│  │ • Auto-detect │  │ • Col masking │  │ • Data retention       ││
│  │               │  │ • Groups      │  │ • Audit trail          ││
│  └──────────────┘  └──────────────┘  └────────────────────────┘│
│                                                                  │
│  System Tables: access.audit · lineage · billing                 │
└─────────────────────────────────────────────────────────────────┘
```
