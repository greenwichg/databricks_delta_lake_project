# Databricks notebook source

# MAGIC %md
# MAGIC # MLflow: Model Registry & Serving
# MAGIC
# MAGIC ## Model Lifecycle
# MAGIC ```
# MAGIC Training ──> Registration ──> Staging ──> Production ──> Monitoring
# MAGIC   │              │              │            │              │
# MAGIC   │  MLflow       │  Unity       │  A/B       │  REST API    │  Drift
# MAGIC   │  Tracking     │  Catalog     │  Testing   │  Batch       │  Detection
# MAGIC   └──────────────┴──────────────┴────────────┴──────────────┴──────────
# MAGIC ```
# MAGIC
# MAGIC ## Model Registry in Unity Catalog
# MAGIC Models are stored as three-level namespace objects:
# MAGIC ```
# MAGIC catalog.schema.model_name
# MAGIC   └── Version 1 (Staging)
# MAGIC   └── Version 2 (Production)
# MAGIC   └── Version 3 (Archived)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Register a Model in Unity Catalog

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

# Set registry to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

MODEL_NAME = "customer_360_catalog.gold.churn_prediction_model"

def register_best_model(experiment_name):
    """
    Find the best model from experiment runs and register it in Unity Catalog.
    """
    client = MlflowClient()

    # Find the best run by AUC-ROC
    experiment = mlflow.get_experiment_by_name(experiment_name)
    best_run = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.auc_roc DESC"],
        max_results=1,
    )

    run_id = best_run.iloc[0]["run_id"]
    auc = best_run.iloc[0]["metrics.auc_roc"]

    print(f"Best model - Run: {run_id}, AUC: {auc:.4f}")

    # Register the model
    model_uri = f"runs:/{run_id}/churn_model"
    result = mlflow.register_model(model_uri, MODEL_NAME)

    print(f"Registered model: {MODEL_NAME}")
    print(f"Version: {result.version}")

    return result.version

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Model Version Management

# COMMAND ----------

def manage_model_versions():
    """
    Manage model versions: set aliases, add descriptions, manage transitions.
    Unity Catalog uses aliases instead of stages.
    """
    client = MlflowClient()

    # List all versions
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    print("=== Model Versions ===")
    for v in versions:
        print(f"  Version {v.version}: status={v.status}, aliases={v.aliases}")

    # Set aliases (replaces staging/production stages)
    # "champion" = current production model
    # "challenger" = candidate for A/B testing
    latest_version = max(v.version for v in versions)

    client.set_registered_model_alias(MODEL_NAME, "champion", str(latest_version))
    print(f"\nSet 'champion' alias to version {latest_version}")

    # Add model description
    client.update_registered_model(
        name=MODEL_NAME,
        description="Customer churn prediction model. Predicts probability of customer "
                    "churning in the next 30 days based on behavioral features."
    )

    # Add version description
    client.update_model_version(
        name=MODEL_NAME,
        version=str(latest_version),
        description=f"GBT classifier with AUC-ROC improvement. Trained on churn_features table."
    )

    print("Model metadata updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load and Use Registered Models

# COMMAND ----------

def load_production_model(spark):
    """
    Load the production (champion) model for batch scoring.
    """

    # Load the champion model
    model_uri = f"models:/{MODEL_NAME}@champion"
    model = mlflow.spark.load_model(model_uri)

    print(f"Loaded model: {MODEL_NAME}@champion")

    # Score new data
    features_df = spark.table("customer_360_catalog.gold.churn_features")
    predictions = model.transform(features_df)

    # Show predictions
    predictions.select(
        "customer_id", "prediction", "probability"
    ).show(10, truncate=False)

    return predictions


def batch_score_and_save(spark):
    """
    Run batch scoring and save predictions to a Gold table.
    """
    from pyspark.sql import functions as F

    # Load champion model
    model_uri = f"models:/{MODEL_NAME}@champion"
    model = mlflow.spark.load_model(model_uri)

    # Score all customers
    features_df = spark.table("customer_360_catalog.gold.churn_features")
    predictions = model.transform(features_df)

    # Extract probability and save
    scored_df = predictions.select(
        "customer_id",
        "prediction",
        F.col("probability").getItem(1).alias("churn_probability"),
        F.current_timestamp().alias("scored_at"),
        F.lit(MODEL_NAME).alias("model_name"),
    )

    # Write to Gold table
    scored_df.write.format("delta").mode("overwrite").saveAsTable(
        "customer_360_catalog.gold.churn_predictions"
    )

    print(f"Scored {scored_df.count()} customers. Saved to gold.churn_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model Serving (REST API)

# COMMAND ----------

def setup_model_serving():
    """
    Deploy a model as a REST API endpoint on Databricks Model Serving.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
    )

    w = WorkspaceClient()

    # Create a serving endpoint
    endpoint = w.serving_endpoints.create_and_wait(
        name="customer-churn-prediction",
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_NAME,
                    entity_version="1",
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                )
            ]
        ),
    )

    print(f"Serving endpoint created: {endpoint.name}")
    print(f"State: {endpoint.state}")
    print(f"URL: https://<workspace-url>/serving-endpoints/{endpoint.name}/invocations")

    return endpoint.name


def query_serving_endpoint(endpoint_name, features):
    """
    Query a model serving endpoint for real-time predictions.
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    # Make a prediction request
    response = w.serving_endpoints.query(
        name=endpoint_name,
        dataframe_records=[features],
    )

    print(f"Prediction: {response.predictions}")
    return response.predictions


# Example: Real-time churn prediction
def predict_churn_realtime():
    """
    Example of real-time churn prediction via REST API.
    """
    features = {
        "days_since_last_activity": 45,
        "total_transactions": 12,
        "lifetime_value": 2500.0,
        "avg_transaction_amount": 208.33,
        "support_ticket_count": 3,
        "session_count": 25,
    }

    print("Input features:")
    for k, v in features.items():
        print(f"  {k}: {v}")

    prediction = query_serving_endpoint("customer-churn-prediction", features)
    print(f"\nChurn prediction: {prediction}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. A/B Testing with Model Serving

# COMMAND ----------

def setup_ab_test():
    """
    Configure A/B testing between champion and challenger models.
    Traffic is split between model versions.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
        TrafficConfig,
        Route,
    )

    w = WorkspaceClient()

    # Update endpoint with traffic split
    w.serving_endpoints.update_config_and_wait(
        name="customer-churn-prediction",
        served_entities=[
            ServedEntityInput(
                name="champion",
                entity_name=MODEL_NAME,
                entity_version="1",
                workload_size="Small",
                scale_to_zero_enabled=True,
            ),
            ServedEntityInput(
                name="challenger",
                entity_name=MODEL_NAME,
                entity_version="2",
                workload_size="Small",
                scale_to_zero_enabled=True,
            ),
        ],
        traffic_config=TrafficConfig(
            routes=[
                Route(served_model_name="champion", traffic_percentage=90),
                Route(served_model_name="challenger", traffic_percentage=10),
            ]
        ),
    )

    print("A/B test configured: 90% champion / 10% challenger")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Model Registry & Serving
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What is the Model Registry?** | Centralized model store in Unity Catalog with versioning, aliases, and governance |
# MAGIC | **Stages vs Aliases?** | UC uses aliases (champion/challenger) instead of stages (Staging/Production) |
# MAGIC | **Batch vs Real-time?** | Batch: load model, score DataFrame, save to table. Real-time: Model Serving REST API |
# MAGIC | **A/B testing?** | Traffic splitting on serving endpoints between model versions |
# MAGIC | **Governance?** | Models in Unity Catalog get same GRANT/REVOKE, lineage, and audit as tables |
# MAGIC | **Scale to zero?** | Model Serving supports scale-to-zero for cost optimization |
