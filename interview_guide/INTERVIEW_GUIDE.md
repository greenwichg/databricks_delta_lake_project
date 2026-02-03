# Databricks Interview Guide - Customer 360 Platform

Use this guide to prepare for Databricks Data Engineer interviews. Each section maps to a project component you can walk through live.

---

## 1. Delta Lake

### Q: What is Delta Lake and why use it over plain Parquet?

**Answer**: Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It sits on top of Parquet files and adds a transaction log (`_delta_log/`) that tracks every change.

**Key advantages over Parquet:**

| Feature | Parquet | Delta Lake |
|---------|---------|------------|
| ACID transactions | No | Yes (atomic writes) |
| Schema enforcement | No (schema-on-read) | Yes (schema-on-write) |
| Time travel | No | Yes (version history) |
| MERGE / UPDATE / DELETE | No | Yes |
| Streaming + batch | Separate | Unified |
| Audit history | No | Full transaction log |

**Project reference**: Every table in our platform uses Delta format. See `src/utils/delta_lake_features.py` for hands-on examples.

### Q: Explain ACID properties in Delta Lake

- **Atomicity**: The transaction log ensures writes are all-or-nothing. A failed MERGE doesn't leave partial data.
- **Consistency**: Schema enforcement rejects writes that don't match the table schema.
- **Isolation**: Snapshot isolation via optimistic concurrency control. Readers see a consistent snapshot while writers operate independently.
- **Durability**: Data files (Parquet) and the transaction log are stored on durable cloud object storage (S3/ADLS/GCS).

### Q: How does Time Travel work?

Delta Lake maintains a version history through the transaction log. Each commit (write, update, delete, merge) creates a new version.

```sql
-- Query a specific version
SELECT * FROM my_table VERSION AS OF 5;

-- Query as of a timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-15';

-- Restore to a previous version
RESTORE TABLE my_table TO VERSION AS OF 5;
```

**Use cases**: Debugging data quality issues, auditing, reproducing ML experiments, rollback.

**Limitation**: VACUUM removes old data files, making time travel impossible beyond the retention period (default 7 days).

### Q: Explain the MERGE statement and its use cases

MERGE is Delta Lake's upsert operation - it handles INSERT, UPDATE, and DELETE in a single atomic operation.

```sql
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED AND source.op = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Use cases in our project**:
- Silver layer: Upsert cleaned data (`src/silver/transform_customers.py`)
- SCD Type 2: Close old records, insert new versions
- CDC: Apply insert/update/delete operations from Change Data Feed
- Gold layer: Incrementally update aggregations

### Q: What is schema enforcement vs schema evolution?

- **Enforcement** (default): Delta REJECTS writes with columns not in the table schema. Prevents accidental data corruption.
- **Evolution**: With `mergeSchema=true`, new columns in the source are automatically added to the table.

```python
# Schema evolution on write
df.write.option("mergeSchema", "true").mode("append").saveAsTable("my_table")
```

---

## 2. Auto Loader

### Q: What is Auto Loader and how does it differ from `spark.read`?

Auto Loader is Databricks' solution for incremental file ingestion. It uses the `cloudFiles` format to efficiently discover and process new files.

| Feature | `spark.read` (manual) | Auto Loader (`cloudFiles`) |
|---------|----------------------|---------------------------|
| Incremental | No (reads all files) | Yes (checkpoint-based) |
| Schema evolution | Manual | Automatic |
| Scalability | Degrades with file count | Handles billions of files |
| Exactly-once | Must implement yourself | Built-in via checkpoints |
| File formats | All | JSON, CSV, Parquet, Avro, etc. |

### Q: Explain the two Auto Loader modes

1. **Directory Listing** (default): Auto Loader periodically lists the directory to find new files. Simple setup, works for most cases.
2. **File Notification**: Uses cloud events (S3 SQS, Azure Event Grid) to get notified of new files. Better for very high volume (billions of files).

```python
# Directory listing (default)
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(path)

# File notification
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .load(path)
```

### Q: How does Auto Loader handle schema changes?

Auto Loader has a `schemaEvolutionMode` setting:
- `addNewColumns`: Automatically adds new columns to the schema
- `rescue`: Puts unrecognized data in a `_rescued_data` column
- `failOnNewColumns`: Fails the stream so you can handle it manually
- `none`: Ignores new columns

**Project reference**: See `src/bronze/ingest_crm_customers.py` for a complete Auto Loader implementation with schema hints.

---

## 3. Streaming + CDC

### Q: How does Structured Streaming provide unified batch + streaming?

Structured Streaming treats a stream as an unbounded table. The same DataFrame/SQL API works for both batch and streaming:

```python
# Batch
df = spark.read.format("delta").table("my_table")

# Streaming (same transformations, different read)
df = spark.readStream.format("delta").table("my_table")
```

The **trigger** controls the execution model:
- `processingTime="10 seconds"`: Micro-batch every 10 seconds (streaming)
- `availableNow=True`: Process all available data, then stop (batch-style)

### Q: Explain watermarks and why they're needed

Watermarks tell Spark how late data can arrive. Without watermarks, Spark must keep ALL state in memory forever, leading to OOM errors.

```python
df.withWatermark("event_timestamp", "10 minutes")
```

This means: "Data might be up to 10 minutes late. After that, drop it and clear the state."

**When required**: Any stateful streaming operation - aggregations, deduplication, joins.

### Q: What is Change Data Feed (CDF)?

CDF records row-level changes (insert, update, delete) in a Delta table. When enabled, you can read only the changes since a specific version.

```sql
ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

```python
changes = spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 5)
    .table("my_table")
# Returns: _change_type, _commit_version, _commit_timestamp columns
```

**Project reference**: `src/bronze/ingest_support_tickets.py` reads CDF, `src/silver/transform_support_tickets.py` applies the changes.

### Q: Explain the foreachBatch pattern

`foreachBatch` gives you a micro-batch DataFrame you can process with any logic, including MERGE:

```python
def merge_batch(batch_df, batch_id):
    target = DeltaTable.forName(spark, "my_table")
    target.merge(batch_df, "target.id = source.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()

stream.writeStream.foreachBatch(merge_batch).start()
```

This enables **idempotent streaming writes** - reprocessing the same data doesn't create duplicates.

---

## 4. DLT / Lakeflow Declarative Pipelines

### Q: What is DLT and how is it different from regular notebooks?

DLT (now "Lakeflow Spark Declarative Pipelines") is a **declarative** framework:
- You declare WHAT the table should contain (SQL or Python)
- DLT handles orchestration, dependencies, retries, monitoring
- Built-in data quality with Expectations
- Automatic dependency graph resolution

```python
@dlt.table(name="silver_customers")
@dlt.expect_or_drop("valid_id", "customer_id IS NOT NULL")
def silver_customers():
    return dlt.read_stream("bronze_customers").select(...)
```

### Q: Explain the three types of DLT Expectations

| Type | Behavior | Use Case |
|------|----------|----------|
| `@dlt.expect` | **Warn**: Record passes, metric logged | Quality monitoring |
| `@dlt.expect_or_drop` | **Drop**: Bad records silently removed | Filter invalid data |
| `@dlt.expect_or_fail` | **Fail**: Pipeline stops immediately | Critical integrity |

**Batch variants**: `expect_all`, `expect_all_or_drop`, `expect_all_or_fail` for multiple rules.

### Q: Streaming Table vs Materialized View in DLT?

- **Streaming Table** (`dlt.read_stream`): Append-only, processes only new data incrementally. Good for Bronze/Silver.
- **Materialized View** (`dlt.read`): Fully recomputed on each run. Good for Gold aggregations.

**Project reference**: `src/dlt_pipelines/dlt_customer_360_complete.py` shows both patterns.

---

## 5. Workflows & Jobs

### Q: How do you orchestrate a multi-step pipeline in Databricks?

Databricks Workflows supports:
- **Multi-task DAGs**: Tasks with dependencies (parallel and sequential)
- **Retry policies**: Per-task retry with configurable intervals
- **Schedules**: Cron-based scheduling
- **Parameters**: Job-level and task-level parameters
- **Repair & re-run**: Re-run only failed tasks (not the entire job)
- **Alerts**: Email/webhook/PagerDuty notifications

**Project reference**: `orchestration/workflow_definitions.json` defines the complete job DAG.

### Q: Batch vs streaming orchestration approaches?

| Approach | When to Use | Cost |
|----------|-------------|------|
| Scheduled batch job | Daily/hourly processing, cost-sensitive | $ |
| DLT pipeline (triggered) | Declarative, quality-focused | $$ |
| DLT pipeline (continuous) | Near real-time with quality gates | $$$ |
| Streaming job (always-on) | Low-latency requirements | $$$$ |
| availableNow trigger | Batch-style streaming (best of both) | $ |

---

## 6. Performance

### Q: Explain OPTIMIZE and Z-ORDER

**OPTIMIZE**: Compacts small files into larger ones (~1GB target). Reduces per-file overhead for reads.

**Z-ORDER**: Physically co-locates related data during OPTIMIZE. Uses a Z-curve (space-filling curve) to map multi-dimensional data into a linear order.

```sql
OPTIMIZE my_table ZORDER BY (customer_id, transaction_date);
```

**Best practices**:
- Z-ORDER on columns used in WHERE/JOIN clauses
- Limit to 2-4 columns
- High-cardinality columns benefit most
- Run after heavy write workloads

### Q: What is Liquid Clustering?

The recommended replacement for Z-ORDER + partitioning:
- Automatic incremental clustering (no manual OPTIMIZE needed)
- Can change clustering columns without full rewrite
- Replaces both PARTITION BY and ZORDER BY

```sql
CREATE TABLE my_table (...) CLUSTER BY (customer_id, date);
ALTER TABLE my_table CLUSTER BY (customer_id);  -- Change columns!
```

### Q: What is Photon?

Databricks' native vectorized query engine written in C++:
- Up to 3x faster for SQL/DataFrame operations
- No code changes needed (just enable on cluster)
- Best for: scans, aggregations, joins, window functions
- Enable via `runtime_engine: "PHOTON"` in cluster config

### Q: Explain caching strategies

| Method | Scope | Best For |
|--------|-------|----------|
| `df.cache()` | DataFrame, in-memory | Iterative analysis |
| Delta Cache | Cluster SSDs, automatic | Default for all reads |
| `CACHE TABLE` | Table, session-level | Frequently queried tables |
| Result Cache | Query, automatic | Repeated identical queries |

---

## 7. Unity Catalog

### Q: What is Unity Catalog?

Centralized governance for the Databricks lakehouse:
- **Three-level namespace**: `catalog.schema.table`
- **Access control**: GRANT/REVOKE on any object
- **Data lineage**: Automatic table and column-level tracking
- **Audit logging**: Who accessed what, when, from where
- **Data discovery**: Tags, comments, search across the lakehouse

### Q: How do you handle PII in the lakehouse?

1. **Column masking**: Functions that return masked values for unauthorized users
2. **Row-level security**: Dynamic views that filter based on user group
3. **Tags**: Classify columns as PII for governance policies
4. **Access control**: Restrict Bronze/Silver access; expose only Gold views

```sql
-- Column masking
CREATE FUNCTION mask_email(email STRING) RETURNS STRING
RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('pii_team') THEN email
       ELSE CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1]) END;

ALTER TABLE customers ALTER COLUMN email SET MASK mask_email;
```

### Q: Explain data lineage in Unity Catalog

Lineage is captured **automatically** whenever data flows through Spark SQL:
- Table-level: Which tables feed into which
- Column-level: Which source columns map to target columns
- Queryable via system tables: `system.lineage.table_lineage`
- Visualized in the Unity Catalog UI

---

## General Interview Tips

1. **Always reference your project**: "In my Customer 360 platform, I used MERGE to implement SCD Type 2..."
2. **Explain trade-offs**: "We chose availableNow trigger over continuous streaming because..."
3. **Know the costs**: Streaming is expensive. Batch is cheap. DLT is in between.
4. **Connect features**: "We used Auto Loader for ingestion, DLT expectations for quality, and Z-ORDER for performance."
5. **Mention monitoring**: "We tracked quality metrics in a Delta table and created Databricks SQL dashboards."
