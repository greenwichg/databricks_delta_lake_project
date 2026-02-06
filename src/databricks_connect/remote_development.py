# Databricks notebook source

# MAGIC %md
# MAGIC # Databricks Connect: Remote Development
# MAGIC
# MAGIC ## What is Databricks Connect?
# MAGIC Databricks Connect lets you run PySpark code from your **local IDE** (VS Code, PyCharm,
# MAGIC IntelliJ) against a **remote Databricks cluster**. This enables:
# MAGIC - **Local development** with IDE features (debugging, autocomplete, breakpoints)
# MAGIC - **Remote execution** on Databricks compute (Spark runs in the cloud)
# MAGIC - **Interactive development** without Databricks notebooks
# MAGIC - **CI/CD integration** for running tests against real Spark clusters
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC ┌─────────────────┐     gRPC / Spark Connect      ┌──────────────────┐
# MAGIC │  Local IDE       │ ──────────────────────────────> │  Databricks      │
# MAGIC │  (VS Code /      │                                 │  Cluster         │
# MAGIC │   PyCharm)       │ <────────────────────────────── │  (Spark Runtime) │
# MAGIC │                  │     Results / DataFrames         │                  │
# MAGIC │  • Code editing  │                                 │  • Spark engine  │
# MAGIC │  • Debugging     │                                 │  • Delta Lake    │
# MAGIC │  • Breakpoints   │                                 │  • Unity Catalog │
# MAGIC └─────────────────┘                                 └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Databricks Connect v2 (Spark Connect protocol)
# MAGIC - Uses **gRPC-based Spark Connect** (not the legacy Thrift-based v1)
# MAGIC - Requires **DBR 13.3+** on the cluster
# MAGIC - Compatible with Unity Catalog and serverless compute

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Installation & Setup
# MAGIC
# MAGIC ```bash
# MAGIC # Install Databricks Connect (matches your DBR version)
# MAGIC pip install databricks-connect==14.3.*
# MAGIC
# MAGIC # Verify installation
# MAGIC databricks-connect test
# MAGIC
# MAGIC # Configure authentication (one-time)
# MAGIC # Option A: Databricks CLI profile
# MAGIC databricks configure --token
# MAGIC
# MAGIC # Option B: Environment variables
# MAGIC export DATABRICKS_HOST=https://adb-1234567890.azuredatabricks.net
# MAGIC export DATABRICKS_TOKEN=dapi_xxxxxxxxxx
# MAGIC export DATABRICKS_CLUSTER_ID=0123-456789-abcdefgh
# MAGIC
# MAGIC # Option C: OAuth (recommended for production)
# MAGIC export DATABRICKS_HOST=https://adb-1234567890.azuredatabricks.net
# MAGIC export DATABRICKS_CLIENT_ID=xxx
# MAGIC export DATABRICKS_CLIENT_SECRET=xxx
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Establishing a Remote Spark Session

# COMMAND ----------

from databricks.connect import DatabricksSession

# --- Method 1: Using environment variables / Databricks CLI profile ---
def get_spark_session_auto():
    """
    Create a remote Spark session using automatic authentication.
    Picks up DATABRICKS_HOST / DATABRICKS_TOKEN from environment or CLI profile.
    """
    spark = DatabricksSession.builder.getOrCreate()
    print(f"Connected to cluster: {spark.conf.get('spark.databricks.clusterUsageTags.clusterId')}")
    return spark


# --- Method 2: Explicit configuration ---
def get_spark_session_explicit(host, token, cluster_id):
    """
    Create a remote Spark session with explicit connection parameters.
    Useful for scripts that need to connect to different clusters.
    """
    spark = (
        DatabricksSession.builder
        .host(host)
        .token(token)
        .clusterId(cluster_id)
        .getOrCreate()
    )
    return spark


# --- Method 3: Using a Databricks CLI profile ---
def get_spark_session_profile(profile_name="DEFAULT"):
    """
    Create a remote Spark session using a named CLI profile.
    Profiles are defined in ~/.databrickscfg.
    """
    spark = (
        DatabricksSession.builder
        .profile(profile_name)
        .getOrCreate()
    )
    return spark


# --- Method 4: Serverless compute (no cluster needed) ---
def get_spark_session_serverless():
    """
    Connect to Databricks Serverless compute.
    No cluster ID needed - Databricks manages the compute.
    """
    spark = (
        DatabricksSession.builder
        .serverless(True)
        .getOrCreate()
    )
    return spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Running PySpark Locally Against Remote Cluster

# COMMAND ----------

from pyspark.sql import functions as F

def demonstrate_remote_spark_operations(spark):
    """
    Run PySpark operations locally that execute on the remote Databricks cluster.
    The code looks identical to notebook code - only the session creation differs.
    """

    # --- Read from Unity Catalog (runs on remote cluster) ---
    customers_df = spark.table("customer_360_catalog.silver.customers")

    print(f"Total customers: {customers_df.count()}")
    print(f"Schema: {customers_df.schema.simpleString()}")

    # --- Apply transformations (executed remotely) ---
    high_value_customers = (
        customers_df
        .filter(F.col("loyalty_tier").isin("Gold", "Platinum"))
        .groupBy("loyalty_tier")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("lifetime_value").alias("avg_ltv"),
        )
        .orderBy(F.desc("avg_ltv"))
    )

    # Results are pulled to local machine for display
    high_value_customers.show()

    # --- Write results back to Delta Lake (remote) ---
    (
        high_value_customers
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("customer_360_catalog.gold.high_value_summary")
    )

    print("Results written to customer_360_catalog.gold.high_value_summary")


def demonstrate_delta_operations(spark):
    """
    Perform Delta Lake-specific operations remotely via Databricks Connect.
    """
    from delta.tables import DeltaTable

    # --- Time Travel ---
    customers_v0 = (
        spark.read
        .format("delta")
        .option("versionAsOf", 0)
        .table("customer_360_catalog.silver.customers")
    )
    print(f"Customers at version 0: {customers_v0.count()}")

    # --- MERGE (Upsert) from local DataFrame ---
    updates = spark.createDataFrame([
        ("CUST-001", "Jane Doe", "jane.doe@newdomain.com"),
        ("CUST-999", "New Customer", "new@email.com"),
    ], ["customer_id", "full_name", "email"])

    target = DeltaTable.forName(spark, "customer_360_catalog.silver.customers")

    (
        target.alias("target")
        .merge(updates.alias("source"), "target.customer_id = source.customer_id")
        .whenMatchedUpdate(set={"email": "source.email"})
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("MERGE operation completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Using Databricks Connect with Unity Catalog

# COMMAND ----------

def explore_unity_catalog_remotely(spark):
    """
    Browse Unity Catalog metadata from your local IDE.
    All catalog operations are supported via Databricks Connect.
    """

    # List available catalogs
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print("Available Catalogs:")
    for row in catalogs:
        print(f"  - {row.catalog}")

    # List schemas in our catalog
    schemas = spark.sql("SHOW SCHEMAS IN customer_360_catalog").collect()
    print("\nSchemas in customer_360_catalog:")
    for row in schemas:
        print(f"  - {row.databaseName}")

    # List tables in a schema
    tables = spark.sql("SHOW TABLES IN customer_360_catalog.gold").collect()
    print("\nTables in Gold schema:")
    for row in tables:
        print(f"  - {row.tableName}")

    # Describe a table with detailed metadata
    spark.sql("DESCRIBE EXTENDED customer_360_catalog.gold.customer_360").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Configuration for VS Code & PyCharm

# COMMAND ----------

def show_ide_configuration():
    """
    Print IDE configuration tips for Databricks Connect.
    """
    print("""
    ══════════════════════════════════════════════════════════
    VS CODE SETUP
    ══════════════════════════════════════════════════════════

    1. Install the Databricks Extension for VS Code
       - Search "Databricks" in VS Code Extensions Marketplace
       - Provides cluster management, notebook sync, and debugging

    2. Configure launch.json for debugging:
       {
           "version": "0.2.0",
           "configurations": [
               {
                   "name": "Databricks Connect",
                   "type": "python",
                   "request": "launch",
                   "program": "${file}",
                   "env": {
                       "DATABRICKS_HOST": "https://adb-xxx.azuredatabricks.net",
                       "DATABRICKS_CLUSTER_ID": "0123-456789-abcdefgh"
                   }
               }
           ]
       }

    3. Set breakpoints in your PySpark code and debug normally.
       - Variables panel shows DataFrame schemas
       - Console allows interactive Spark queries

    ══════════════════════════════════════════════════════════
    PYCHARM / INTELLIJ SETUP
    ══════════════════════════════════════════════════════════

    1. Install databricks-connect in your project's virtual env
    2. Set environment variables in Run Configuration:
       DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
       DATABRICKS_TOKEN=dapi_xxx
       DATABRICKS_CLUSTER_ID=0123-456789-abcdefgh
    3. Run/debug as a normal Python script

    ══════════════════════════════════════════════════════════
    JUPYTER NOTEBOOK (LOCAL)
    ══════════════════════════════════════════════════════════

    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()

    # Now use spark as you would in a Databricks notebook
    df = spark.table("customer_360_catalog.gold.customer_360")
    df.display()  # Works in Jupyter with databricks-connect
    """)

show_ide_configuration()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Databricks Connect
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What is Databricks Connect?** | Lets you run PySpark from local IDE against a remote Databricks cluster via gRPC/Spark Connect |
# MAGIC | **v1 vs v2?** | v2 uses Spark Connect protocol (gRPC), more lightweight and compatible with Unity Catalog |
# MAGIC | **When would you use it?** | Local development/debugging, CI/CD test suites, IDE-based workflows |
# MAGIC | **Limitations?** | RDD APIs not fully supported, some streaming operations limited, requires cluster to be running |
# MAGIC | **How does it work with Unity Catalog?** | Full support - reads/writes go through Unity Catalog governance like any other client |
# MAGIC | **Serverless support?** | Yes, v2 supports serverless compute - no cluster management needed |
