# Databricks notebook source

# MAGIC %md
# MAGIC # Performance Optimization: OPTIMIZE, Z-ORDER, Caching, Photon
# MAGIC
# MAGIC This notebook covers every major performance tuning technique in Databricks.
# MAGIC
# MAGIC ## Topics
# MAGIC 1. OPTIMIZE (file compaction)
# MAGIC 2. Z-ORDER (data clustering for co-located access)
# MAGIC 3. Liquid Clustering (next-gen replacement for Z-ORDER)
# MAGIC 4. Data Skipping & Statistics
# MAGIC 5. Caching Strategies
# MAGIC 6. Photon Engine
# MAGIC 7. Auto Optimize & Auto Compact
# MAGIC 8. Partitioning Best Practices
# MAGIC 9. Query Optimization Patterns

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. OPTIMIZE (File Compaction)
# MAGIC
# MAGIC **Problem**: Over time, Delta tables accumulate many small files from
# MAGIC streaming writes, MERGE operations, and append batches. Many small files
# MAGIC = slow reads because of overhead per file.
# MAGIC
# MAGIC **Solution**: `OPTIMIZE` compacts small files into larger ones (target ~1GB).
# MAGIC
# MAGIC **When to run**:
# MAGIC - After heavy write workloads (streaming, frequent MERGEs)
# MAGIC - When query performance degrades
# MAGIC - Typically scheduled daily or weekly

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Basic OPTIMIZE: compact all small files
# MAGIC OPTIMIZE customer_360_catalog.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE a specific partition (more efficient for partitioned tables)
# MAGIC OPTIMIZE customer_360_catalog.silver.transactions
# MAGIC WHERE transaction_date >= '2024-01-01';

# COMMAND ----------

# Programmatic OPTIMIZE
def optimize_table(table_name, z_order_cols=None, where_clause=None):
    """
    Run OPTIMIZE with optional Z-ORDER and partition filter.

    Args:
        table_name: Fully qualified table name
        z_order_cols: List of columns to Z-ORDER by
        where_clause: Optional partition filter
    """
    sql = f"OPTIMIZE {table_name}"

    if where_clause:
        sql += f" WHERE {where_clause}"

    if z_order_cols:
        sql += f" ZORDER BY ({', '.join(z_order_cols)})"

    print(f"Running: {sql}")
    result = spark.sql(sql)
    display(result)
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Z-ORDER (Multi-dimensional Clustering)
# MAGIC
# MAGIC **Problem**: When you query with `WHERE customer_id = 'C-1001'`, Spark must
# MAGIC scan many files because `customer_id` values are spread randomly across files.
# MAGIC
# MAGIC **Solution**: Z-ORDER physically co-locates related data in the same files.
# MAGIC After Z-ORDER on `customer_id`, all records for `C-1001` will be in just
# MAGIC 1-2 files instead of 100+.
# MAGIC
# MAGIC **How it works**: Z-ORDER uses a space-filling curve (Z-curve) to map
# MAGIC multi-dimensional data into a linear order, keeping related values close together.
# MAGIC
# MAGIC **Best practices**:
# MAGIC - Z-ORDER on columns used in WHERE clauses and JOIN conditions
# MAGIC - Limit to 2-4 columns (effectiveness decreases with more columns)
# MAGIC - High-cardinality columns benefit most (customer_id > status)
# MAGIC - Z-ORDER is applied during OPTIMIZE (they run together)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE with Z-ORDER
# MAGIC OPTIMIZE customer_360_catalog.silver.transactions
# MAGIC ZORDER BY (customer_id, transaction_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-ORDER the Customer 360 table for fast lookups
# MAGIC OPTIMIZE customer_360_catalog.gold.customer_360
# MAGIC ZORDER BY (customer_id);

# COMMAND ----------

# Run OPTIMIZE + Z-ORDER on all key tables
def optimize_all_tables():
    """Run OPTIMIZE + Z-ORDER on all tables in the platform."""

    optimization_plan = {
        "customer_360_catalog.silver.customers": ["customer_id"],
        "customer_360_catalog.silver.transactions": ["customer_id", "transaction_date"],
        "customer_360_catalog.silver.clickstream_sessions": ["customer_id", "session_date"],
        "customer_360_catalog.gold.customer_360": ["customer_id"],
        "customer_360_catalog.gold.revenue_analytics": ["transaction_date", "product_category"],
    }

    for table, z_cols in optimization_plan.items():
        try:
            optimize_table(table, z_order_cols=z_cols)
            print(f"✓ Optimized {table}")
        except Exception as e:
            print(f"✗ Failed to optimize {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Liquid Clustering (Next-Gen Z-ORDER)
# MAGIC
# MAGIC **Liquid Clustering** is the recommended replacement for Z-ORDER + partitioning:
# MAGIC - Automatically clusters data on specified columns
# MAGIC - No need to run OPTIMIZE manually (incremental, automatic)
# MAGIC - Can change clustering columns without rewriting all data
# MAGIC - Better for tables with unpredictable query patterns
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a table with liquid clustering
# MAGIC CREATE TABLE my_table (
# MAGIC     customer_id STRING,
# MAGIC     txn_date DATE,
# MAGIC     amount DOUBLE
# MAGIC ) USING DELTA
# MAGIC CLUSTER BY (customer_id, txn_date);
# MAGIC
# MAGIC -- Change clustering columns later (no full rewrite!)
# MAGIC ALTER TABLE my_table CLUSTER BY (customer_id);
# MAGIC
# MAGIC -- Trigger clustering
# MAGIC OPTIMIZE my_table;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Create a table with Liquid Clustering
# MAGIC -- CREATE OR REPLACE TABLE customer_360_catalog.gold.customer_360_clustered
# MAGIC -- CLUSTER BY (customer_id)
# MAGIC -- AS SELECT * FROM customer_360_catalog.gold.customer_360;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Data Skipping & Statistics
# MAGIC
# MAGIC Delta Lake automatically collects **min/max statistics** on the first 32 columns.
# MAGIC These enable **data skipping**: reading only relevant files for a query.
# MAGIC
# MAGIC Example: `WHERE customer_id = 'C-1001'`
# MAGIC - Delta checks each file's min/max for customer_id
# MAGIC - Files where 'C-1001' can't possibly exist are SKIPPED
# MAGIC - Combined with Z-ORDER, this can skip 99%+ of files

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check how many files are in a table
# MAGIC DESCRIBE DETAIL customer_360_catalog.silver.transactions;
# MAGIC -- Look at numFiles and sizeInBytes columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze table statistics (helps the query optimizer)
# MAGIC ANALYZE TABLE customer_360_catalog.silver.transactions COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE customer_360_catalog.silver.transactions
# MAGIC   COMPUTE STATISTICS FOR COLUMNS customer_id, transaction_date, amount;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Caching Strategies
# MAGIC
# MAGIC | Method | Scope | Persistence | Best For |
# MAGIC |--------|-------|-------------|----------|
# MAGIC | `df.cache()` | DataFrame | Until unpersisted / session end | Iterative analysis |
# MAGIC | `CACHE TABLE` | Table | Session-level | Repeatedly queried tables |
# MAGIC | **Delta Cache** | Disk | Cluster-level (SSD) | Automatic, no code changes |
# MAGIC | **Result Cache** | Query | Session-level | Identical repeated queries |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cache a frequently-queried table in memory
# MAGIC CACHE TABLE customer_360_catalog.gold.customer_360;

# COMMAND ----------

# DataFrame-level caching
customer_360 = spark.table("customer_360_catalog.gold.customer_360")

# Cache in memory (MEMORY_AND_DISK is default)
customer_360.cache()

# Force materialization (cache is lazy)
customer_360.count()

# Now subsequent queries on customer_360 will be fast
display(customer_360.filter("loyalty_tier = 'gold'"))

# When done, unpersist to free memory
customer_360.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Cache (Automatic, Recommended)
# MAGIC
# MAGIC Delta Cache automatically caches data on the cluster's SSDs:
# MAGIC - **No code changes needed** - happens automatically
# MAGIC - Enabled by default on Photon clusters
# MAGIC - Caches Parquet data files on local SSDs
# MAGIC - Survives across queries (unlike DataFrame cache)
# MAGIC - Best with SSD-based instance types (i3, r5d)
# MAGIC
# MAGIC ```python
# MAGIC # Enable Delta Cache (usually on by default)
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "true")
# MAGIC spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
# MAGIC spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Photon Engine
# MAGIC
# MAGIC **What is Photon?**
# MAGIC - Databricks' native **vectorized query engine** written in C++
# MAGIC - Replaces the JVM-based Spark SQL engine for supported operations
# MAGIC - Up to **3x faster** for scans, aggregations, joins, and window functions
# MAGIC - **No code changes required** - just enable it on the cluster
# MAGIC
# MAGIC **When to use Photon:**
# MAGIC - Always enable for production workloads (cost savings > compute premium)
# MAGIC - Especially beneficial for: SQL workloads, ETL, BI queries
# MAGIC - Less benefit for: UDF-heavy code, ML training, Python-heavy workloads
# MAGIC
# MAGIC **How to enable:**
# MAGIC 1. Cluster config: `"runtime_engine": "PHOTON"`
# MAGIC 2. Or select "Photon acceleration" in the cluster creation UI
# MAGIC
# MAGIC **How to verify it's being used:**

# COMMAND ----------

# Check if Photon is enabled
print(f"Photon enabled: {spark.conf.get('spark.databricks.photon.enabled', 'false')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Auto Optimize & Auto Compact
# MAGIC
# MAGIC **Auto Optimize**: Automatically runs OPTIMIZE on small file writes
# MAGIC **Auto Compact**: Automatically compacts small files after writes
# MAGIC
# MAGIC These reduce the need for manual OPTIMIZE runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable Auto Optimize on a specific table
# MAGIC ALTER TABLE customer_360_catalog.silver.transactions
# MAGIC SET TBLPROPERTIES (
# MAGIC     delta.autoOptimize.optimizeWrite = true,    -- Coalesce small writes
# MAGIC     delta.autoOptimize.autoCompact = true        -- Auto compact after writes
# MAGIC );

# COMMAND ----------

# Enable at session level (applies to all writes)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Partitioning Best Practices
# MAGIC
# MAGIC **When to partition:**
# MAGIC - Table is **very large** (TBs of data)
# MAGIC - Queries almost always filter on the partition column
# MAGIC - Partition column has **low cardinality** (date, country - NOT customer_id)
# MAGIC
# MAGIC **When NOT to partition (use Z-ORDER or Liquid Clustering instead):**
# MAGIC - Table is < 1TB
# MAGIC - Queries filter on high-cardinality columns
# MAGIC - Multiple filter columns needed
# MAGIC
# MAGIC **Common mistake**: Over-partitioning creates many tiny files (small file problem)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Good: Partition by date for a large table with date-range queries
# MAGIC -- CREATE TABLE large_events (...)
# MAGIC -- USING DELTA
# MAGIC -- PARTITIONED BY (event_date);
# MAGIC
# MAGIC -- Bad: Partition by high-cardinality column
# MAGIC -- CREATE TABLE customers (...)
# MAGIC -- PARTITIONED BY (customer_id);  -- DON'T DO THIS!
# MAGIC
# MAGIC -- Better alternative: Use Liquid Clustering
# MAGIC -- CREATE TABLE customers (...)
# MAGIC -- USING DELTA
# MAGIC -- CLUSTER BY (customer_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Query Optimization Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Predicate Pushdown
# MAGIC Push filters down to the scan level so fewer files are read.

# COMMAND ----------

# Good: filter early (enables file skipping)
result = (
    spark.table("customer_360_catalog.silver.transactions")
    .filter("transaction_date >= '2024-01-01'")      # This filter is pushed down
    .filter("customer_id = 'C-1001'")                 # This too (if Z-ORDERed)
    .groupBy("product_category")
    .agg(F.sum("net_amount").alias("total"))
)

# Bad: filter late (reads all files first)
# result = spark.table(...).groupBy(...).agg(...).filter("total > 100")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast Join (for small dimension tables)

# COMMAND ----------

# When one table is small (< 10MB), broadcast it to avoid shuffle
from pyspark.sql.functions import broadcast

customers = spark.table("customer_360_catalog.silver.customers")     # Small dim table
transactions = spark.table("customer_360_catalog.silver.transactions")  # Large fact table

# Broadcast the small table to all executors
result = transactions.join(
    broadcast(customers),                                # Broadcast hint
    "customer_id"
)

# Auto-broadcast threshold (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adaptive Query Execution (AQE)

# COMMAND ----------

# AQE is ON by default in Databricks. Key features:
# 1. Dynamically coalesces shuffle partitions (reduces small tasks)
# 2. Converts sort-merge joins to broadcast joins at runtime
# 3. Optimizes skewed joins automatically

# Verify AQE is enabled
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Auto partition coalescing: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Performance Checklist (Interview Reference)
# MAGIC
# MAGIC | Technique | Impact | When to Use |
# MAGIC |-----------|--------|-------------|
# MAGIC | OPTIMIZE | High | After heavy writes, weekly maintenance |
# MAGIC | Z-ORDER | High | Columns in WHERE/JOIN, up to 4 columns |
# MAGIC | Liquid Clustering | High | New tables, replacing Z-ORDER + partitions |
# MAGIC | Photon | High | Always for production (3x speedup) |
# MAGIC | Auto Optimize | Medium | Enable on all tables |
# MAGIC | Delta Cache | Medium | Automatic on SSD instances |
# MAGIC | DataFrame Cache | Medium | Iterative analysis on same data |
# MAGIC | Broadcast Join | Medium | Small table joins (<10MB) |
# MAGIC | AQE | Medium | Always on (default), handles skew |
# MAGIC | Predicate Pushdown | High | Always filter early in query chain |
# MAGIC | Partition Pruning | High | Large tables with low-cardinality filter |
# MAGIC | ANALYZE TABLE | Low | After major data changes, for CBO |
