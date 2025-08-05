# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Intelligence Platform - Setup and Configuration
# MAGIC 
# MAGIC This notebook helps you set up the DataOps Intelligence Platform in your Databricks environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Packages

# COMMAND ----------

# MAGIC %pip install databricks-sdk pyyaml slack-sdk

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Target Catalog and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS main;
# MAGIC CREATE SCHEMA IF NOT EXISTS main.dataops_intelligence;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate System Tables Access

# COMMAND ----------

# Test access to system tables
tables_to_check = [
    "system.lakeflow.jobs",
    "system.lakeflow.job_run_timeline",
    "system.compute.node_timeline",
    "system.billing.usage"
]

for table in tables_to_check:
    try:
        count = spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0]["count"]
        print(f"✅ {table}: {count:,} records accessible")
    except Exception as e:
        print(f"❌ {table}: Access denied - {e}")

print("\n📋 If any tables show 'Access denied', contact your Databricks admin to enable system table access.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test Data Collection

# COMMAND ----------

# Simple test query for jobs
jobs_sample = spark.sql("""
    WITH latest_jobs AS (
        SELECT *,
            ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
        FROM system.lakeflow.jobs
    )
    SELECT 
        workspace_id,
        job_id,
        name,
        owned_by,
        created_time
    FROM latest_jobs
    WHERE rn = 1
    LIMIT 10
""")

print("Sample Jobs Data:")
jobs_sample.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Pipeline Monitoring Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.dataops_intelligence.pipeline_runs (
# MAGIC   run_id STRING,
# MAGIC   status STRING,
# MAGIC   start_time TIMESTAMP,
# MAGIC   end_time TIMESTAMP,
# MAGIC   duration_seconds BIGINT,
# MAGIC   records_processed STRING,
# MAGIC   tables_updated BIGINT,
# MAGIC   alerts_sent STRING,
# MAGIC   errors STRING,
# MAGIC   environment STRING,
# MAGIC   collection_window_days INT
# MAGIC ) USING DELTA
# MAGIC LOCATION '/mnt/delta/dataops_intelligence/pipeline_runs'

# COMMAND ----------

print("✅ Setup completed successfully!")
print("\n📋 Next Steps:")
print("1. Configure alerting channels in the settings.yaml file")
print("2. Schedule the data collection job to run regularly") 
print("3. Set up dashboards using the collected data")
print("4. Review and customize health scoring weights")
