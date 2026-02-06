# Databricks notebook source

# MAGIC %md
# MAGIC # Feature Store: Feature Engineering & Serving
# MAGIC
# MAGIC ## What is Databricks Feature Store?
# MAGIC The Feature Store provides a **centralized repository** for ML features:
# MAGIC - **Discover** features created by other teams
# MAGIC - **Reuse** features across multiple models (avoid duplication)
# MAGIC - **Lineage** tracking from raw data to features to models
# MAGIC - **Online serving** for real-time model inference
# MAGIC - **Point-in-time lookups** to prevent data leakage
# MAGIC
# MAGIC ## Feature Store Architecture
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────┐
# MAGIC │                    Feature Store                                 │
# MAGIC ├──────────────┬───────────────┬──────────────┬───────────────────┤
# MAGIC │  Feature      │  Feature      │  Online      │  Feature          │
# MAGIC │  Tables       │  Functions    │  Store       │  Lineage          │
# MAGIC │              │              │              │                    │
# MAGIC │ Delta tables │ Python/SQL   │ Cosmos DB /  │ Table → Feature   │
# MAGIC │ in Unity     │ UDFs in UC   │ DynamoDB     │ → Model → Serving │
# MAGIC │ Catalog      │              │              │                    │
# MAGIC └──────────────┴───────────────┴──────────────┴───────────────────┘
# MAGIC
# MAGIC Data Pipeline:
# MAGIC ┌───────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
# MAGIC │ Silver     │───>│ Feature  │───>│ Training │───>│ Serving  │
# MAGIC │ Tables     │    │ Tables   │    │ Dataset  │    │ (Online/ │
# MAGIC │            │    │ (Gold)   │    │          │    │  Batch)  │
# MAGIC └───────────┘    └──────────┘    └──────────┘    └──────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Feature Tables

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()

def create_customer_behavior_features(spark):
    """
    Create a feature table for customer behavioral features.
    These features are derived from Silver layer tables.
    """

    # --- Compute features from transactions ---
    txn_features = spark.sql("""
        SELECT
            customer_id,
            COUNT(*) AS total_transactions,
            SUM(amount) AS lifetime_value,
            AVG(amount) AS avg_transaction_amount,
            MAX(amount) AS max_transaction_amount,
            MIN(amount) AS min_transaction_amount,
            STDDEV(amount) AS stddev_transaction_amount,
            DATEDIFF(CURRENT_DATE(), MAX(transaction_date)) AS days_since_last_transaction,
            DATEDIFF(CURRENT_DATE(), MIN(transaction_date)) AS days_since_first_transaction,
            COUNT(DISTINCT DATE(transaction_date)) AS active_days,
            COUNT(DISTINCT product_category) AS distinct_categories
        FROM customer_360_catalog.silver.transactions
        WHERE status = 'completed'
        GROUP BY customer_id
    """)

    # --- Compute features from clickstream ---
    click_features = spark.sql("""
        SELECT
            customer_id,
            COUNT(DISTINCT session_id) AS total_sessions,
            AVG(page_views) AS avg_pages_per_session,
            AVG(session_duration_minutes) AS avg_session_duration,
            MAX(session_start) AS last_session_date,
            DATEDIFF(CURRENT_DATE(), MAX(session_start)) AS days_since_last_session
        FROM customer_360_catalog.silver.clickstream_sessions
        GROUP BY customer_id
    """)

    # --- Compute features from support tickets ---
    support_features = spark.sql("""
        SELECT
            customer_id,
            COUNT(*) AS total_support_tickets,
            COUNT(CASE WHEN status = 'open' THEN 1 END) AS open_tickets,
            AVG(resolution_hours) AS avg_resolution_hours,
            COUNT(CASE WHEN priority = 'high' THEN 1 END) AS high_priority_tickets
        FROM customer_360_catalog.silver.support_tickets
        GROUP BY customer_id
    """)

    # --- Join all features ---
    customer_features = (
        txn_features
        .join(click_features, "customer_id", "left")
        .join(support_features, "customer_id", "left")
        .fillna(0)
        .withColumn("_feature_timestamp", F.current_timestamp())
    )

    # --- Register as a Feature Table in Unity Catalog ---
    fe.create_table(
        name="customer_360_catalog.gold.customer_behavior_features",
        primary_keys=["customer_id"],
        timestamp_keys=["_feature_timestamp"],
        df=customer_features,
        description="Customer behavioral features derived from transactions, clickstream, and support data",
    )

    print(f"Feature table created with {customer_features.count()} customers")
    print(f"Features: {len(customer_features.columns)} columns")

    return customer_features

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Feature Functions (SQL/Python UDFs)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feature Function: Calculate customer age bucket
# MAGIC -- Registered as a Unity Catalog function
# MAGIC CREATE OR REPLACE FUNCTION customer_360_catalog.gold.age_bucket(age INT)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Categorize customer age into buckets for ML features'
# MAGIC RETURN CASE
# MAGIC     WHEN age < 25 THEN '18-24'
# MAGIC     WHEN age < 35 THEN '25-34'
# MAGIC     WHEN age < 45 THEN '35-44'
# MAGIC     WHEN age < 55 THEN '45-54'
# MAGIC     WHEN age < 65 THEN '55-64'
# MAGIC     ELSE '65+'
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feature Function: Calculate engagement score
# MAGIC CREATE OR REPLACE FUNCTION customer_360_catalog.gold.engagement_score(
# MAGIC     total_sessions INT,
# MAGIC     avg_session_duration DOUBLE,
# MAGIC     total_transactions INT,
# MAGIC     days_since_last_activity INT
# MAGIC )
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Calculate a 0-100 engagement score for a customer'
# MAGIC RETURN ROUND(
# MAGIC     (LEAST(total_sessions, 100) / 100.0 * 25) +
# MAGIC     (LEAST(avg_session_duration, 30) / 30.0 * 25) +
# MAGIC     (LEAST(total_transactions, 50) / 50.0 * 25) +
# MAGIC     (GREATEST(0, 1 - days_since_last_activity / 365.0) * 25),
# MAGIC     2
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build Training Datasets with Feature Lookups

# COMMAND ----------

def create_training_dataset(spark):
    """
    Build a training dataset using Feature Store lookups.
    This ensures training and serving use the same feature computation.
    """

    # Labels (what we're predicting)
    labels_df = spark.sql("""
        SELECT
            customer_id,
            CASE WHEN days_since_last_activity > 90 THEN 1 ELSE 0 END AS churned,
            _feature_timestamp AS label_timestamp
        FROM customer_360_catalog.gold.churn_features
    """)

    # Define feature lookups
    feature_lookups = [
        FeatureLookup(
            table_name="customer_360_catalog.gold.customer_behavior_features",
            feature_names=[
                "total_transactions",
                "lifetime_value",
                "avg_transaction_amount",
                "total_sessions",
                "avg_session_duration",
                "total_support_tickets",
                "days_since_last_transaction",
            ],
            lookup_key="customer_id",
            timestamp_lookup_key="label_timestamp",  # Point-in-time correctness
        ),
    ]

    # Create the training set
    training_set = fe.create_training_set(
        df=labels_df,
        feature_lookups=feature_lookups,
        label="churned",
    )

    training_df = training_set.load_df()

    print(f"Training dataset: {training_df.count()} rows, {len(training_df.columns)} columns")
    print(f"Columns: {training_df.columns}")

    return training_set, training_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train Model with Feature Store

# COMMAND ----------

import mlflow
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline

def train_model_with_feature_store(spark):
    """
    Train a model that is linked to Feature Store tables.
    This enables automatic feature lookup during serving.
    """
    training_set, training_df = create_training_dataset(spark)

    # Prepare features
    feature_cols = [
        "total_transactions", "lifetime_value", "avg_transaction_amount",
        "total_sessions", "avg_session_duration", "total_support_tickets",
        "days_since_last_transaction",
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    classifier = GBTClassifier(featuresCol="features", labelCol="churned", maxDepth=5)
    pipeline = Pipeline(stages=[assembler, classifier])

    train_df, test_df = training_df.randomSplit([0.8, 0.2], seed=42)

    with mlflow.start_run(run_name="churn_with_feature_store") as run:
        model = pipeline.fit(train_df)

        # Log model with Feature Store metadata
        # This links the model to its feature tables for automatic serving
        fe.log_model(
            model=model,
            artifact_path="churn_model",
            flavor=mlflow.spark,
            training_set=training_set,
            registered_model_name="customer_360_catalog.gold.churn_model_fs",
        )

        print(f"Model logged with Feature Store linkage. Run: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Batch Scoring with Feature Store

# COMMAND ----------

def batch_score_with_feature_store(spark):
    """
    Score customers using Feature Store for automatic feature lookup.
    Only need to provide the lookup keys (customer_id) - features are looked up automatically.
    """

    # Only need the lookup keys
    customers_to_score = spark.sql("""
        SELECT DISTINCT customer_id
        FROM customer_360_catalog.silver.customers
    """)

    # Score using the Feature Store-linked model
    predictions = fe.score_batch(
        model_uri="models:/customer_360_catalog.gold.churn_model_fs@champion",
        df=customers_to_score,
    )

    # Save predictions
    predictions.select(
        "customer_id", "prediction",
        F.current_timestamp().alias("scored_at"),
    ).write.format("delta").mode("overwrite").saveAsTable(
        "customer_360_catalog.gold.churn_predictions_fs"
    )

    print(f"Scored {predictions.count()} customers using Feature Store")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Online Feature Serving

# COMMAND ----------

def setup_online_feature_serving():
    """
    Configure online feature serving for real-time inference.
    Features are synced from Delta tables to a low-latency online store.
    """
    from databricks.feature_engineering.online_store_spec import (
        AmazonDynamoDBSpec,
        AzureCosmosDBSpec,
    )

    # Option A: Sync to DynamoDB (AWS)
    dynamodb_spec = AmazonDynamoDBSpec(
        region="us-west-2",
        table_name="customer_behavior_features",
        read_secret_prefix="feature-store/dynamo",
        write_secret_prefix="feature-store/dynamo",
    )

    # Option B: Sync to Cosmos DB (Azure)
    cosmosdb_spec = AzureCosmosDBSpec(
        account_uri="https://customer360.documents.azure.com:443/",
        container_name="customer_behavior_features",
        read_secret_prefix="feature-store/cosmos",
        write_secret_prefix="feature-store/cosmos",
    )

    # Publish features to online store
    fe.publish_table(
        name="customer_360_catalog.gold.customer_behavior_features",
        online_store=dynamodb_spec,  # or cosmosdb_spec
    )

    print("Features published to online store for real-time serving")
    print("""
    Real-time Serving Flow:
    ┌──────────────┐     ┌───────────────┐     ┌──────────────┐
    │ API Request   │────>│ Model Serving  │────>│ Prediction    │
    │ {customer_id} │     │ + Feature      │     │ {churn_prob}  │
    │               │     │   Lookup       │     │               │
    └──────────────┘     └───────┬───────┘     └──────────────┘
                                 │
                        ┌────────┴────────┐
                        │  Online Store    │
                        │  (DynamoDB /     │
                        │   Cosmos DB)     │
                        └─────────────────┘
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interview Talking Points: Feature Store
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |----------|--------|
# MAGIC | **What is a Feature Store?** | Centralized repo for ML features with discovery, reuse, lineage, and serving |
# MAGIC | **Feature Tables vs regular tables?** | Feature tables have primary keys, timestamps, and are integrated with model serving |
# MAGIC | **Point-in-time lookups?** | timestamp_keys prevent data leakage by joining features as-of the label timestamp |
# MAGIC | **Online vs Offline?** | Offline: Delta tables for batch. Online: DynamoDB/CosmosDB for real-time (<10ms) |
# MAGIC | **Feature Functions?** | SQL/Python UDFs in Unity Catalog - computed on-demand during serving |
# MAGIC | **How does it connect to serving?** | fe.log_model links model to features; serving auto-lookups features by key |
