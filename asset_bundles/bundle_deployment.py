# Databricks notebook source

# MAGIC %md
# MAGIC # Databricks Asset Bundles (DABs): Deployment & CI/CD
# MAGIC
# MAGIC ## What are Asset Bundles?
# MAGIC Databricks Asset Bundles (DABs) provide **Infrastructure-as-Code** for Databricks:
# MAGIC - **Version-controlled** deployment of jobs, pipelines, clusters, and notebooks
# MAGIC - **Multi-environment** support (dev, staging, prod) from a single config
# MAGIC - **CI/CD integration** with GitHub Actions, Azure DevOps, Jenkins
# MAGIC - **Parameterized** configurations using variables and target overrides
# MAGIC
# MAGIC ## Bundle Structure
# MAGIC ```
# MAGIC asset_bundles/
# MAGIC ├── databricks.yml           # Main bundle configuration
# MAGIC ├── bundle_deployment.py     # This file - deployment patterns & CI/CD
# MAGIC └── resources/               # (optional) Additional resource configs
# MAGIC ```
# MAGIC
# MAGIC ## Key CLI Commands
# MAGIC | Command | Description |
# MAGIC |---------|------------|
# MAGIC | `databricks bundle init` | Create a new bundle from a template |
# MAGIC | `databricks bundle validate` | Validate bundle configuration |
# MAGIC | `databricks bundle deploy -t <target>` | Deploy resources to a target |
# MAGIC | `databricks bundle run -t <target> <job>` | Run a specific job |
# MAGIC | `databricks bundle destroy -t <target>` | Tear down deployed resources |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bundle Validation & Deployment (Shell Commands)
# MAGIC
# MAGIC These commands are run from your **local terminal** or **CI/CD pipeline**:
# MAGIC ```bash
# MAGIC # Install / update the Databricks CLI
# MAGIC pip install databricks-cli --upgrade
# MAGIC
# MAGIC # Authenticate (one-time setup)
# MAGIC databricks configure --token
# MAGIC
# MAGIC # Validate the bundle (catches YAML errors, missing references)
# MAGIC cd asset_bundles/
# MAGIC databricks bundle validate
# MAGIC
# MAGIC # Deploy to dev (default target)
# MAGIC databricks bundle deploy -t dev
# MAGIC
# MAGIC # Deploy to production (requires service principal)
# MAGIC databricks bundle deploy -t prod
# MAGIC
# MAGIC # Run a specific job after deployment
# MAGIC databricks bundle run -t dev daily_batch_pipeline
# MAGIC
# MAGIC # Tear down dev resources
# MAGIC databricks bundle destroy -t dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CI/CD with GitHub Actions

# COMMAND ----------

# The following shows a GitHub Actions workflow for deploying DABs.
# In practice, this YAML lives at .github/workflows/deploy.yml

GITHUB_ACTIONS_WORKFLOW = """
name: Deploy Customer 360 Pipeline

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Validate Bundle
        working-directory: ./asset_bundles
        run: databricks bundle validate

  deploy-staging:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy to Staging
        working-directory: ./asset_bundles
        run: databricks bundle deploy -t staging

      - name: Run Integration Tests
        working-directory: ./asset_bundles
        run: databricks bundle run -t staging daily_batch_pipeline

  deploy-prod:
    needs: deploy-staging
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy to Production
        working-directory: ./asset_bundles
        run: databricks bundle deploy -t prod
"""

print("GitHub Actions workflow defined. Save to .github/workflows/deploy.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Programmatic Bundle Management with Databricks SDK

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, JobCluster,
    CronSchedule, JobEmailNotifications
)

def create_job_programmatically():
    """
    Create a Databricks job using the SDK.
    Useful when you need dynamic job creation beyond what DABs YAML supports.
    """
    w = WorkspaceClient()

    job = w.jobs.create(
        name="Customer 360 - Dynamic Pipeline",
        job_clusters=[
            JobCluster(
                job_cluster_key="main_cluster",
                new_cluster={
                    "spark_version": "14.3.x-photon-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "autoscale": {"min_workers": 2, "max_workers": 8},
                    "data_security_mode": "USER_ISOLATION",
                },
            )
        ],
        tasks=[
            Task(
                task_key="bronze_ingestion",
                job_cluster_key="main_cluster",
                notebook_task=NotebookTask(
                    notebook_path="/Workspace/customer_360/src/bronze/ingest_crm_customers"
                ),
            ),
            Task(
                task_key="silver_transform",
                depends_on=[{"task_key": "bronze_ingestion"}],
                job_cluster_key="main_cluster",
                notebook_task=NotebookTask(
                    notebook_path="/Workspace/customer_360/src/silver/transform_customers"
                ),
            ),
            Task(
                task_key="gold_aggregation",
                depends_on=[{"task_key": "silver_transform"}],
                job_cluster_key="main_cluster",
                notebook_task=NotebookTask(
                    notebook_path="/Workspace/customer_360/src/gold/customer_360_view"
                ),
            ),
        ],
        schedule=CronSchedule(
            quartz_cron_expression="0 0 6 * * ?",
            timezone_id="America/New_York",
        ),
        email_notifications=JobEmailNotifications(
            on_failure=["data-engineering@company.com"]
        ),
    )

    print(f"Created job: {job.job_id}")
    return job.job_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bundle Permissions & Service Principals

# COMMAND ----------

def setup_service_principal_for_bundles():
    """
    Configure a service principal for production DAB deployments.
    Service principals ensure non-interactive, secure CI/CD authentication.
    """
    w = WorkspaceClient()

    # List existing service principals
    sps = w.service_principals.list()
    for sp in sps:
        print(f"Service Principal: {sp.display_name} (ID: {sp.id})")

    print("""
    === Service Principal Setup for DABs ===

    1. Create a service principal in your identity provider (Entra ID / Okta)
    2. Add it to Databricks Account Console
    3. Grant workspace access:
       - Add to workspace
       - Grant 'CAN_MANAGE' on jobs and pipelines
       - Grant Unity Catalog privileges

    4. Generate an OAuth token or PAT for CI/CD:
       export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
       export DATABRICKS_TOKEN=dapi_xxx
       # Or use OAuth M2M:
       export DATABRICKS_CLIENT_ID=xxx
       export DATABRICKS_CLIENT_SECRET=xxx

    5. In databricks.yml, set run_as for production:
       targets:
         prod:
           run_as:
             service_principal_name: "customer-360-sp"

    This ensures prod jobs run with the SP's permissions, not a user's.
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Multi-Environment Configuration Pattern

# COMMAND ----------

def demonstrate_environment_pattern():
    """
    Shows how DABs handle multi-environment configuration.
    Each target (dev/staging/prod) can override any resource property.
    """
    environments = {
        "dev": {
            "catalog": "customer_360_dev",
            "mode": "development",
            "cluster_size": "1-4 workers",
            "dlt_mode": "development (relaxed quality)",
            "run_as": "developer's identity",
            "schedule": "manual / on-demand",
        },
        "staging": {
            "catalog": "customer_360_staging",
            "mode": "default",
            "cluster_size": "2-12 workers",
            "dlt_mode": "production (strict quality)",
            "run_as": "developer's identity",
            "schedule": "same as prod (for validation)",
        },
        "prod": {
            "catalog": "customer_360_prod",
            "mode": "production",
            "cluster_size": "4-16 workers",
            "dlt_mode": "production (strict quality)",
            "run_as": "service principal",
            "schedule": "daily at 6am ET",
        },
    }

    print("=" * 70)
    print("MULTI-ENVIRONMENT CONFIGURATION")
    print("=" * 70)

    for env_name, config in environments.items():
        print(f"\n--- {env_name.upper()} ---")
        for key, value in config.items():
            print(f"  {key:20s}: {value}")

    print("""
    Key DABs Features for Multi-Environment:
    ─────────────────────────────────────────
    • 'mode: development' - Prefixes resources with [dev <user>] to avoid conflicts
    • 'mode: production'  - Locks resources to prevent accidental modification
    • 'run_as'            - Controls execution identity (user vs service principal)
    • 'variables'         - Override catalog, paths, emails per target
    • Resource overrides  - Adjust cluster size, DLT config per target
    """)

demonstrate_environment_pattern()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Asset Bundles
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What are DABs?** | Infrastructure-as-Code for Databricks - define jobs, pipelines, clusters in YAML |
# MAGIC | **Why use DABs over UI?** | Version control, code review, multi-env support, reproducible deployments |
# MAGIC | **How do you handle dev vs prod?** | Targets in databricks.yml with variable overrides per environment |
# MAGIC | **CI/CD integration?** | GitHub Actions / Azure DevOps + `databricks bundle deploy` in pipeline |
# MAGIC | **What about permissions?** | Service principals for prod, `run_as` in bundle config |
# MAGIC | **How do you test before prod?** | Deploy to staging target, run integration test job, then promote |
