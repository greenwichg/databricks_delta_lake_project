# Databricks notebook source

# MAGIC %md
# MAGIC # Unity Catalog: Access Control & Security
# MAGIC
# MAGIC ## Security Model
# MAGIC Unity Catalog uses a **privilege-based** access control model:
# MAGIC - **GRANT / REVOKE** privileges on catalogs, schemas, tables, views
# MAGIC - **Row-Level Security** using dynamic views
# MAGIC - **Column Masking** to hide sensitive data
# MAGIC - **Groups** for team-based access management
# MAGIC
# MAGIC ## Privilege Hierarchy
# MAGIC ```
# MAGIC CATALOG owner
# MAGIC   ├── ALL PRIVILEGES on all schemas and tables
# MAGIC   ├── GRANT: can grant access to others
# MAGIC   │
# MAGIC SCHEMA owner
# MAGIC   ├── ALL PRIVILEGES on all tables in schema
# MAGIC   │
# MAGIC TABLE privileges
# MAGIC   ├── SELECT: read data
# MAGIC   ├── MODIFY: write data (INSERT, UPDATE, DELETE, MERGE)
# MAGIC   ├── CREATE: create tables
# MAGIC   └── ALL PRIVILEGES: everything
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Grant Access to Teams

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant schema-level access to the data analytics team
# MAGIC -- They can read all Gold tables but nothing in Bronze/Silver
# MAGIC GRANT USAGE ON CATALOG customer_360_catalog TO `analytics_team`;
# MAGIC GRANT USAGE ON SCHEMA customer_360_catalog.gold TO `analytics_team`;
# MAGIC GRANT SELECT ON SCHEMA customer_360_catalog.gold TO `analytics_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant data engineering team full access to all schemas
# MAGIC GRANT ALL PRIVILEGES ON CATALOG customer_360_catalog TO `data_engineering_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant data science team read access to Gold + quality schemas
# MAGIC GRANT USAGE ON CATALOG customer_360_catalog TO `data_science_team`;
# MAGIC GRANT USAGE ON SCHEMA customer_360_catalog.gold TO `data_science_team`;
# MAGIC GRANT USAGE ON SCHEMA customer_360_catalog.quality TO `data_science_team`;
# MAGIC GRANT SELECT ON SCHEMA customer_360_catalog.gold TO `data_science_team`;
# MAGIC GRANT SELECT ON SCHEMA customer_360_catalog.quality TO `data_science_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revoke access (if needed)
# MAGIC -- REVOKE SELECT ON SCHEMA customer_360_catalog.silver FROM `analytics_team`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row-Level Security (Dynamic Views)
# MAGIC
# MAGIC **Problem**: Different teams should see different rows.
# MAGIC Example: Regional managers should only see customers in their region.
# MAGIC
# MAGIC **Solution**: Create a view that dynamically filters based on the current user.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row-Level Security: Regional customer view
# MAGIC CREATE OR REPLACE VIEW customer_360_catalog.gold.regional_customers AS
# MAGIC SELECT *
# MAGIC FROM customer_360_catalog.gold.customer_360
# MAGIC WHERE
# MAGIC     -- Admins see everything
# MAGIC     IS_ACCOUNT_GROUP_MEMBER('admin_group')
# MAGIC     OR
# MAGIC     -- Regional managers see only their region
# MAGIC     (IS_ACCOUNT_GROUP_MEMBER('region_east') AND state IN ('NY', 'NJ', 'CT', 'MA', 'PA'))
# MAGIC     OR
# MAGIC     (IS_ACCOUNT_GROUP_MEMBER('region_west') AND state IN ('CA', 'WA', 'OR', 'NV', 'AZ'))
# MAGIC     OR
# MAGIC     (IS_ACCOUNT_GROUP_MEMBER('region_central') AND state IN ('TX', 'IL', 'OH', 'MI', 'MN'));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant access to the secure view (NOT the underlying table)
# MAGIC GRANT SELECT ON VIEW customer_360_catalog.gold.regional_customers TO `regional_managers`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Column Masking (Hide Sensitive Data)
# MAGIC
# MAGIC **Problem**: Some users need access to the table but shouldn't see PII.
# MAGIC
# MAGIC **Solution**: Column masking functions that return masked values for
# MAGIC unauthorized users.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a masking function for email
# MAGIC CREATE OR REPLACE FUNCTION customer_360_catalog.gold.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC     CASE
# MAGIC         WHEN IS_ACCOUNT_GROUP_MEMBER('pii_authorized')
# MAGIC         THEN email
# MAGIC         ELSE CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1])
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a masking function for phone
# MAGIC CREATE OR REPLACE FUNCTION customer_360_catalog.gold.mask_phone(phone STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC     CASE
# MAGIC         WHEN IS_ACCOUNT_GROUP_MEMBER('pii_authorized')
# MAGIC         THEN phone
# MAGIC         ELSE CONCAT('***-***-', RIGHT(phone, 4))
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply column masks to the table
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC ALTER COLUMN email SET MASK customer_360_catalog.gold.mask_email;
# MAGIC
# MAGIC ALTER TABLE customer_360_catalog.silver.customers
# MAGIC ALTER COLUMN phone SET MASK customer_360_catalog.gold.mask_phone;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Security Best Practices Summary
# MAGIC
# MAGIC | Practice | Implementation |
# MAGIC |----------|---------------|
# MAGIC | **Least privilege** | Grant SELECT only on Gold; never on Bronze |
# MAGIC | **Group-based access** | Use groups, not individual users |
# MAGIC | **PII protection** | Column masking on email, phone, name |
# MAGIC | **Regional isolation** | Row-level security via dynamic views |
# MAGIC | **Audit trail** | Enable audit logging (automatic in UC) |
# MAGIC | **No direct file access** | All access through UC tables/views |
# MAGIC | **Service principals** | Use for automated pipelines, not personal tokens |
