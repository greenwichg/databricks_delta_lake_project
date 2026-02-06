# Databricks notebook source

# MAGIC %md
# MAGIC # MLflow: Experiment Tracking
# MAGIC
# MAGIC ## What is MLflow?
# MAGIC MLflow is an **open-source ML lifecycle platform** integrated natively into Databricks:
# MAGIC - **Tracking** - Log parameters, metrics, and artifacts for every experiment run
# MAGIC - **Models** - Package ML models with their dependencies
# MAGIC - **Registry** - Centralized model store with versioning and stage transitions
# MAGIC - **Serving** - Deploy models as REST endpoints
# MAGIC
# MAGIC ## MLflow Architecture on Databricks
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────┐
# MAGIC │                    MLflow on Databricks                          │
# MAGIC ├──────────────┬───────────────┬──────────────┬───────────────────┤
# MAGIC │  Experiments  │  Model        │  Model       │  Model            │
# MAGIC │  & Tracking   │  Registry     │  Serving     │  Monitoring       │
# MAGIC │              │              │              │                    │
# MAGIC │ • Parameters │ • Versioning │ • REST API   │ • Drift detection │
# MAGIC │ • Metrics    │ • Staging    │ • Batch      │ • Performance     │
# MAGIC │ • Artifacts  │ • Approval   │ • Streaming  │ • Alerts          │
# MAGIC │ • Tags       │ • Lineage    │ • A/B test   │ • Retraining      │
# MAGIC └──────────────┴───────────────┴──────────────┴───────────────────┘
# MAGIC                                │
# MAGIC                    ┌───────────┴───────────┐
# MAGIC                    │    Unity Catalog       │
# MAGIC                    │  (Model Governance)    │
# MAGIC                    └───────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: Create an Experiment

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.sql import functions as F

# Set the experiment (organizes all runs under one name)
EXPERIMENT_NAME = "/Workspace/customer_360/experiments/churn_prediction"
mlflow.set_experiment(EXPERIMENT_NAME)

print(f"Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Train a Churn Prediction Model with MLflow Tracking

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

def train_churn_model(spark, model_type="gbt", max_depth=5, num_trees=100):
    """
    Train a churn prediction model using the Gold layer feature table.
    All parameters, metrics, and the model are logged to MLflow.
    """

    # --- Load feature data from Gold layer ---
    features_df = spark.table("customer_360_catalog.gold.churn_features")

    # --- Prepare features ---
    feature_columns = [
        "days_since_last_activity",
        "total_transactions",
        "lifetime_value",
        "avg_transaction_amount",
        "support_ticket_count",
        "session_count",
    ]

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    # --- Select classifier ---
    if model_type == "gbt":
        classifier = GBTClassifier(
            featuresCol="features",
            labelCol="churned",
            maxDepth=max_depth,
            maxIter=num_trees,
        )
    else:
        classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="churned",
            maxDepth=max_depth,
            numTrees=num_trees,
        )

    pipeline = Pipeline(stages=[assembler, classifier])

    # --- Split data ---
    train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

    # --- Train with MLflow tracking ---
    with mlflow.start_run(run_name=f"churn_{model_type}_depth{max_depth}") as run:

        # Log parameters
        mlflow.log_param("model_type", model_type)
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("num_trees", num_trees)
        mlflow.log_param("feature_columns", str(feature_columns))
        mlflow.log_param("training_rows", train_df.count())
        mlflow.log_param("test_rows", test_df.count())
        mlflow.log_param("feature_table", "customer_360_catalog.gold.churn_features")

        # Train the model
        model = pipeline.fit(train_df)

        # Make predictions
        predictions = model.transform(test_df)

        # Evaluate
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="churned", metricName="areaUnderROC"
        )
        evaluator_pr = BinaryClassificationEvaluator(
            labelCol="churned", metricName="areaUnderPR"
        )

        auc = evaluator_auc.evaluate(predictions)
        pr_auc = evaluator_pr.evaluate(predictions)

        # Log metrics
        mlflow.log_metric("auc_roc", auc)
        mlflow.log_metric("auc_pr", pr_auc)

        # Log the model
        mlflow.spark.log_model(
            model,
            "churn_model",
            registered_model_name="customer_360_catalog.gold.churn_prediction_model",
        )

        # Log feature importance (for tree-based models)
        if hasattr(model.stages[-1], "featureImportances"):
            importances = model.stages[-1].featureImportances.toArray()
            for col_name, importance in zip(feature_columns, importances):
                mlflow.log_metric(f"importance_{col_name}", importance)

        # Set tags for organization
        mlflow.set_tag("project", "customer_360")
        mlflow.set_tag("team", "data_science")
        mlflow.set_tag("stage", "development")

        print(f"Run ID: {run.info.run_id}")
        print(f"AUC-ROC: {auc:.4f}")
        print(f"AUC-PR: {pr_auc:.4f}")

        return run.info.run_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hyperparameter Tuning with MLflow

# COMMAND ----------

def hyperparameter_search(spark):
    """
    Run multiple experiments with different hyperparameters.
    MLflow tracks all runs for comparison.
    """
    configurations = [
        {"model_type": "gbt", "max_depth": 3, "num_trees": 50},
        {"model_type": "gbt", "max_depth": 5, "num_trees": 100},
        {"model_type": "gbt", "max_depth": 7, "num_trees": 150},
        {"model_type": "rf",  "max_depth": 5, "num_trees": 100},
        {"model_type": "rf",  "max_depth": 7, "num_trees": 200},
    ]

    results = []
    for config in configurations:
        print(f"\nTraining: {config}")
        run_id = train_churn_model(spark, **config)
        results.append({"config": config, "run_id": run_id})

    # Find the best run
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    best_run = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.auc_roc DESC"],
        max_results=1,
    )

    print("\n" + "=" * 50)
    print("BEST MODEL:")
    print(f"  Run ID: {best_run.iloc[0]['run_id']}")
    print(f"  AUC-ROC: {best_run.iloc[0]['metrics.auc_roc']:.4f}")
    print(f"  Model Type: {best_run.iloc[0]['params.model_type']}")
    print(f"  Max Depth: {best_run.iloc[0]['params.max_depth']}")

    return best_run.iloc[0]["run_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compare Experiment Runs

# COMMAND ----------

def compare_runs():
    """
    Compare all experiment runs and display results.
    """
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)

    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["metrics.auc_roc DESC"],
    )

    print("=" * 80)
    print("EXPERIMENT COMPARISON")
    print("=" * 80)
    print(f"{'Run Name':<35} {'Model':<8} {'Depth':<7} {'Trees':<7} {'AUC-ROC':<10} {'AUC-PR':<10}")
    print("-" * 80)

    for _, row in runs.iterrows():
        print(
            f"{row.get('tags.mlflow.runName', 'N/A'):<35} "
            f"{row.get('params.model_type', 'N/A'):<8} "
            f"{row.get('params.max_depth', 'N/A'):<7} "
            f"{row.get('params.num_trees', 'N/A'):<7} "
            f"{row.get('metrics.auc_roc', 0):<10.4f} "
            f"{row.get('metrics.auc_pr', 0):<10.4f}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MLflow Autologging

# COMMAND ----------

def demonstrate_autologging(spark):
    """
    MLflow autologging automatically captures parameters, metrics,
    and models without explicit logging calls.
    """

    # Enable autologging for PySpark ML
    mlflow.pyspark.ml.autolog()

    # Now any PySpark ML pipeline will be automatically tracked
    features_df = spark.table("customer_360_catalog.gold.churn_features")

    assembler = VectorAssembler(
        inputCols=["days_since_last_activity", "total_transactions", "lifetime_value"],
        outputCol="features",
    )
    classifier = GBTClassifier(featuresCol="features", labelCol="churned", maxDepth=4)
    pipeline = Pipeline(stages=[assembler, classifier])

    train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

    # This automatically logs everything to MLflow!
    model = pipeline.fit(train_df)

    print("Model trained with autologging - check MLflow UI for parameters and metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: MLflow Experiment Tracking
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What is MLflow?** | Open-source ML lifecycle platform: tracking, models, registry, serving |
# MAGIC | **What gets tracked?** | Parameters, metrics, artifacts (model files, plots), tags, code version |
# MAGIC | **How do you compare models?** | MLflow UI experiment view, search_runs API, or programmatic comparison |
# MAGIC | **Autologging?** | mlflow.autolog() captures everything automatically for supported frameworks |
# MAGIC | **Unity Catalog integration?** | Models registered in UC namespace (catalog.schema.model_name) |
# MAGIC | **Team collaboration?** | Shared experiments, model registry with approval workflows |
