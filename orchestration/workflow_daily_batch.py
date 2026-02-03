# Databricks notebook source

# MAGIC %md
# MAGIC # Workflow Orchestration: Daily Batch Pipeline
# MAGIC
# MAGIC ## Databricks Workflows & Jobs
# MAGIC
# MAGIC Databricks Workflows is the native orchestration solution:
# MAGIC - **Multi-task jobs** with dependencies (DAG-based execution)
# MAGIC - **Retries** with configurable policies
# MAGIC - **Schedules** (cron-based) and **triggers** (file arrival, etc.)
# MAGIC - **Alerts** and **monitoring** built-in
# MAGIC - **Parameterized** runs with job parameters
# MAGIC - **Repair & re-run** failed tasks without re-running the entire job
# MAGIC
# MAGIC This notebook serves as the **orchestrator** for the daily batch pipeline.
# MAGIC In production, this would be configured as a Databricks Job with multiple tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution Order (DAG)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │                  Daily Batch Job                 │
# MAGIC │                                                  │
# MAGIC │  Task 1A: Ingest CRM ──────┐                    │
# MAGIC │  Task 1B: Ingest Txns ─────┤                    │
# MAGIC │  Task 1C: Ingest Clicks ───┤── (parallel)       │
# MAGIC │  Task 1D: Ingest Support ──┘                    │
# MAGIC │            │                                     │
# MAGIC │            ▼                                     │
# MAGIC │  Task 2A: Transform Customers ──┐               │
# MAGIC │  Task 2B: Transform Txns ───────┤ (parallel)    │
# MAGIC │  Task 2C: Transform Clicks ─────┤               │
# MAGIC │  Task 2D: Transform Support ────┘               │
# MAGIC │            │                                     │
# MAGIC │            ▼                                     │
# MAGIC │  Task 3A: Build Customer 360 ──┐                │
# MAGIC │            │                    │                │
# MAGIC │            ▼                    │                │
# MAGIC │  Task 3B: Revenue Analytics    (sequential)     │
# MAGIC │  Task 3C: Segmentation                          │
# MAGIC │  Task 3D: Churn Features                        │
# MAGIC │            │                                     │
# MAGIC │            ▼                                     │
# MAGIC │  Task 4: OPTIMIZE + Z-ORDER                     │
# MAGIC │            │                                     │
# MAGIC │            ▼                                     │
# MAGIC │  Task 5: Quality Checks                         │
# MAGIC │                                                  │
# MAGIC └─────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Runner: Execute Pipeline Stages

# COMMAND ----------

def run_stage(stage_name, notebooks, parallel=True):
    """
    Run a pipeline stage by executing notebooks.

    In production, each notebook would be a separate Task in a Databricks Job.
    Here we simulate the execution for demonstration.

    Args:
        stage_name: Name of the pipeline stage
        notebooks: List of notebook paths to execute
        parallel: Whether tasks in this stage run in parallel
    """
    print(f"\n{'='*60}")
    print(f"STAGE: {stage_name} ({'parallel' if parallel else 'sequential'})")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"{'='*60}")

    for nb in notebooks:
        print(f"  Running: {nb}")
        try:
            # In production, use dbutils.notebook.run()
            # result = dbutils.notebook.run(nb, timeout_seconds=3600)
            # print(f"  ✓ Completed: {nb} -> {result}")
            print(f"  > Completed: {nb}")
        except Exception as e:
            print(f"  X Failed: {nb} -> {str(e)}")
            raise

    print(f"Stage '{stage_name}' completed at {datetime.now().isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute the Daily Pipeline

# COMMAND ----------

# Stage 1: Bronze Ingestion (parallel)
run_stage("Bronze Ingestion", [
    "../src/bronze/ingest_crm_customers",
    "../src/bronze/ingest_transactions",
    "../src/bronze/ingest_clickstream",
    "../src/bronze/ingest_support_tickets",
], parallel=True)

# COMMAND ----------

# Stage 2: Silver Transformations (parallel)
run_stage("Silver Transformations", [
    "../src/silver/transform_customers",
    "../src/silver/transform_transactions",
    "../src/silver/transform_clickstream",
    "../src/silver/transform_support_tickets",
], parallel=True)

# COMMAND ----------

# Stage 3: Gold Aggregations (sequential - 360 must be first)
run_stage("Gold - Customer 360", [
    "../src/gold/customer_360_view",
], parallel=False)

run_stage("Gold - Analytics Tables", [
    "../src/gold/revenue_analytics",
    "../src/gold/customer_segmentation",
    "../src/gold/churn_features",
], parallel=True)

# COMMAND ----------

# Stage 4: Performance Optimization
run_stage("Optimization", [
    "../src/utils/performance_optimization",
], parallel=False)

# COMMAND ----------

# Stage 5: Quality Checks
run_stage("Quality Validation", [
    "../src/quality/quality_monitoring",
], parallel=False)

# COMMAND ----------

print(f"\nDaily batch pipeline completed at {datetime.now().isoformat()}")
