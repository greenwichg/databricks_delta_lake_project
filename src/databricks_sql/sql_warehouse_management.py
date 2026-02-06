# Databricks notebook source

# MAGIC %md
# MAGIC # Databricks SQL: Warehouse Management
# MAGIC
# MAGIC ## What is Databricks SQL?
# MAGIC Databricks SQL (DBSQL) provides a **SQL-native analytics experience** on the Lakehouse:
# MAGIC - **SQL Warehouses** (formerly SQL Endpoints) for compute
# MAGIC - **SQL Editor** for interactive queries
# MAGIC - **Dashboards** for visualizations
# MAGIC - **Alerts** for automated monitoring
# MAGIC - **Query History** for audit and optimization
# MAGIC
# MAGIC ## SQL Warehouse Types
# MAGIC | Type | Description | Use Case |
# MAGIC |------|------------|----------|
# MAGIC | **Classic** | Standard SQL compute | General-purpose SQL analytics |
# MAGIC | **Pro** | Enhanced with Query Profile, Predictive I/O | Production dashboards, complex queries |
# MAGIC | **Serverless** | Auto-managed compute, instant startup | Cost-efficient, variable workloads |
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
# MAGIC │  SQL Editor       │     │  Dashboards       │     │  BI Tools        │
# MAGIC │  (Interactive)    │     │  (Visualizations) │     │  (Tableau, PBI)  │
# MAGIC └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
# MAGIC          │                         │                         │
# MAGIC          └─────────────────────────┼─────────────────────────┘
# MAGIC                                    ▼
# MAGIC                      ┌──────────────────────────┐
# MAGIC                      │    SQL Warehouse           │
# MAGIC                      │  (Photon-powered compute)  │
# MAGIC                      └─────────────┬──────────────┘
# MAGIC                                    ▼
# MAGIC                      ┌──────────────────────────┐
# MAGIC                      │    Unity Catalog           │
# MAGIC                      │  (Delta Lake tables)       │
# MAGIC                      └──────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create and Configure SQL Warehouses

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    EndpointConfPair,
    CreateWarehouseRequestWarehouseType,
    SpotInstancePolicy,
    Channel,
    ChannelName,
)

def create_sql_warehouse():
    """
    Create a SQL Warehouse programmatically using the Databricks SDK.
    """
    w = WorkspaceClient()

    # Create a Pro SQL Warehouse for production dashboards
    warehouse = w.warehouses.create_and_wait(
        name="Customer 360 Analytics Warehouse",
        cluster_size="Medium",
        min_num_clusters=1,
        max_num_clusters=4,
        auto_stop_mins=30,
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        enable_photon=True,
        spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
        channel=Channel(name=ChannelName.CHANNEL_NAME_CURRENT),
        tags={
            "custom_tags": [
                {"key": "team", "value": "data-engineering"},
                {"key": "project", "value": "customer-360"},
                {"key": "environment", "value": "production"},
            ]
        },
    )

    print(f"Created warehouse: {warehouse.name} (ID: {warehouse.id})")
    print(f"State: {warehouse.state}")
    return warehouse.id


def create_serverless_warehouse():
    """
    Create a Serverless SQL Warehouse for cost-efficient analytics.
    Serverless warehouses start instantly and scale to zero.
    """
    w = WorkspaceClient()

    warehouse = w.warehouses.create_and_wait(
        name="Customer 360 Serverless Warehouse",
        cluster_size="Small",
        max_num_clusters=2,
        auto_stop_mins=10,
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        enable_serverless_compute=True,
        enable_photon=True,
    )

    print(f"Created serverless warehouse: {warehouse.name} (ID: {warehouse.id})")
    return warehouse.id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Manage SQL Warehouses

# COMMAND ----------

def manage_warehouses():
    """
    List, start, stop, and monitor SQL Warehouses.
    """
    w = WorkspaceClient()

    # --- List all warehouses ---
    warehouses = w.warehouses.list()
    print("=== SQL Warehouses ===")
    for wh in warehouses:
        print(f"  {wh.name}")
        print(f"    ID: {wh.id}")
        print(f"    State: {wh.state}")
        print(f"    Size: {wh.cluster_size}")
        print(f"    Clusters: {wh.num_active_sessions} active sessions")
        print(f"    Auto-stop: {wh.auto_stop_mins} minutes")
        print()

    return warehouses


def start_warehouse(warehouse_id):
    """Start a stopped SQL Warehouse."""
    w = WorkspaceClient()
    w.warehouses.start(warehouse_id)
    print(f"Starting warehouse {warehouse_id}...")


def stop_warehouse(warehouse_id):
    """Stop a running SQL Warehouse to save costs."""
    w = WorkspaceClient()
    w.warehouses.stop(warehouse_id)
    print(f"Stopping warehouse {warehouse_id}...")


def resize_warehouse(warehouse_id, new_size, max_clusters):
    """
    Resize a SQL Warehouse for different workload requirements.
    Sizes: 2X-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large
    """
    w = WorkspaceClient()
    w.warehouses.edit(
        id=warehouse_id,
        cluster_size=new_size,
        max_num_clusters=max_clusters,
    )
    print(f"Resized warehouse {warehouse_id} to {new_size} (max {max_clusters} clusters)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SQL Warehouse Configuration Best Practices

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Configure warehouse-level settings via SQL
# MAGIC -- These apply to all queries running on this warehouse
# MAGIC
# MAGIC -- Enable adaptive query execution
# MAGIC SET spark.sql.adaptive.enabled = true;
# MAGIC
# MAGIC -- Set default catalog for the warehouse
# MAGIC SET catalog = customer_360_catalog;
# MAGIC
# MAGIC -- Set default schema
# MAGIC USE SCHEMA gold;

# COMMAND ----------

def configure_warehouse_permissions():
    """
    Configure warehouse access permissions using the Databricks SDK.
    """
    w = WorkspaceClient()

    print("""
    === SQL Warehouse Permission Model ===

    Permission levels (applied per warehouse):
    ┌────────────────────┬──────────────────────────────────────┐
    │ Permission         │ Capabilities                          │
    ├────────────────────┼──────────────────────────────────────┤
    │ CAN USE            │ Run queries on the warehouse          │
    │ CAN MONITOR        │ View query history and metrics        │
    │ CAN MANAGE         │ Start/stop, resize, configure         │
    │ IS OWNER           │ Full control including delete         │
    └────────────────────┴──────────────────────────────────────┘

    Best practices:
    • Analysts: CAN USE on production warehouse
    • Data Engineers: CAN MANAGE on all warehouses
    • BI Tools (service principal): CAN USE on specific warehouse
    • Cost control: Set auto-stop (10-30 min) and max clusters
    """)


def show_warehouse_sizing_guide():
    """
    Guide for choosing the right SQL Warehouse size.
    """
    print("""
    === SQL Warehouse Sizing Guide ===

    ┌────────────┬───────────┬────────────────────────────────────────┐
    │ Size       │ DBUs/hour │ Recommended For                        │
    ├────────────┼───────────┼────────────────────────────────────────┤
    │ 2X-Small   │ ~2        │ Development, testing, small queries    │
    │ X-Small    │ ~4        │ Light analytics, single-user queries   │
    │ Small      │ ~8        │ Team dashboards, moderate queries      │
    │ Medium     │ ~16       │ Production dashboards, complex joins   │
    │ Large      │ ~32       │ Heavy analytics, large table scans     │
    │ X-Large    │ ~64       │ Data-intensive workloads, many users   │
    │ 2X-Large+  │ ~128+     │ Enterprise-scale, concurrent users     │
    └────────────┴───────────┴────────────────────────────────────────┘

    Multi-cluster scaling:
    • min_num_clusters=1, max_num_clusters=N
    • Each cluster handles ~10 concurrent queries
    • Scales up when queue time > 5 seconds
    • Scales down when idle for 15 minutes

    Serverless warehouses:
    • Instant startup (no cold start)
    • Scale to zero when idle (no cost)
    • Best for variable/unpredictable workloads
    """)

show_warehouse_sizing_guide()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Query History & Performance Monitoring

# COMMAND ----------

def analyze_query_history():
    """
    Analyze SQL Warehouse query history using system tables.
    """
    w = WorkspaceClient()

    # Get query history via API
    queries = w.query_history.list(
        filter_by={"warehouse_ids": []},
        max_results=100,
    )

    print("=== Recent Query Performance ===")
    for query in queries:
        print(f"  Query ID: {query.query_id}")
        print(f"  Status: {query.status}")
        print(f"  Duration: {query.duration}ms")
        print(f"  Rows: {query.rows_produced}")
        print(f"  User: {query.user_name}")
        print()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query performance analysis via system tables
# MAGIC SELECT
# MAGIC     query_id,
# MAGIC     statement_type,
# MAGIC     total_duration_ms,
# MAGIC     rows_produced,
# MAGIC     read_bytes,
# MAGIC     executed_by,
# MAGIC     warehouse_id,
# MAGIC     start_time
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
# MAGIC   AND total_duration_ms > 30000  -- Queries > 30 seconds
# MAGIC ORDER BY total_duration_ms DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: SQL Warehouse Management
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **Classic vs Pro vs Serverless?** | Classic: basic. Pro: query profiles, predictive I/O. Serverless: instant start, auto-scale to zero |
# MAGIC | **How do you size a warehouse?** | Based on concurrent users + query complexity. Start small, monitor, scale up |
# MAGIC | **Cost optimization?** | Auto-stop (10-30 min), serverless for variable workloads, right-size clusters |
# MAGIC | **Multi-cluster scaling?** | Set min/max clusters; auto-scales based on query queue depth |
# MAGIC | **Monitoring?** | System tables (system.query.history), Query Profile, SDK query_history API |
# MAGIC | **Security?** | CAN USE / CAN MANAGE permissions per warehouse; Unity Catalog for data access |
