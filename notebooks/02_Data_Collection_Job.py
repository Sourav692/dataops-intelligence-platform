# Databricks notebook source
# MAGIC %md
# MAGIC # DataOps Intelligence Platform - Data Collection Job
# MAGIC 
# MAGIC This notebook runs the main data collection pipeline for the DataOps Intelligence Platform.

# COMMAND ----------

# MAGIC %pip install databricks-sdk pyyaml slack-sdk

# COMMAND ----------

import sys
import os
from datetime import datetime
import uuid

# Add the source path
sys.path.append("/Workspace/path/to/dataops-intelligence-platform/src")

try:
    from dip.core.config import DIPConfig
    from dip.orchestrator import DataOpsIntelligencePlatform
    from dip.utils.logging import setup_logging
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure the source code is available at the specified path")

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

# Configuration parameters (can be passed as job parameters)
dbutils.widgets.text("workspace_url", "", "Databricks Workspace URL")
dbutils.widgets.text("token", "", "Personal Access Token") 
dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.text("schema", "dataops_intelligence", "Target Schema")
dbutils.widgets.text("collection_window_days", "30", "Collection Window (Days)")
dbutils.widgets.dropdown("environment", "development", ["development", "staging", "production"], "Environment")

# Get parameters
workspace_url = dbutils.widgets.get("workspace_url")
token = dbutils.widgets.get("token")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
collection_window_days = int(dbutils.widgets.get("collection_window_days"))
environment = dbutils.widgets.get("environment")

print(f"Configuration:")
print(f"  Workspace: {workspace_url}")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print(f"  Collection Window: {collection_window_days} days")
print(f"  Environment: {environment}")

# COMMAND ----------

# MAGIC %md ## Initialize Configuration

# COMMAND ----------

# Create configuration programmatically
try:
    config = DIPConfig(
        databricks=DIPConfig.DatabricksConfig(
            workspace_url=workspace_url,
            token=token
        ),
        storage=DIPConfig.StorageConfig(
            catalog=catalog,
            schema=schema
        ),
        processing=DIPConfig.ProcessingConfig(
            collection_window_days=collection_window_days
        ),
        alerting=DIPConfig.AlertingConfig(
            enabled=True,  # Enable alerting
            slack=DIPConfig.SlackConfig(
                enabled=False  # Disable by default, configure as needed
            ),
            email=DIPConfig.EmailConfig(
                enabled=False  # Disable by default, configure as needed
            )
        ),
        environment=environment
    )

    # Validate configuration
    issues = config.validate()
    if issues:
        print(f"Configuration warnings: {issues}")

    print("✅ Configuration initialized successfully")

except Exception as e:
    print(f"❌ Configuration error: {e}")
    raise

# COMMAND ----------

# MAGIC %md ## Run Data Collection Pipeline

# COMMAND ----------

# Initialize the platform
try:
    dip = DataOpsIntelligencePlatform(config)
    print("✅ DataOps Intelligence Platform initialized")
except Exception as e:
    print(f"❌ Platform initialization failed: {e}")
    raise

# COMMAND ----------

# Run full collection
try:
    print("🚀 Starting data collection pipeline...")
    results = dip.run_full_collection()

    # Display results
    print("=" * 60)
    print("📊 PIPELINE EXECUTION RESULTS")
    print("=" * 60)
    print(f"Status: {results['status']}")
    print(f"Start Time: {results['start_time']}")
    print(f"End Time: {results.get('end_time', 'N/A')}")

    if results.get('end_time'):
        duration = results['end_time'] - results['start_time']
        print(f"Duration: {duration}")

    print("\nRecords Processed:")
    for entity, count in results.get('records_processed', {}).items():
        print(f"  📈 {entity}: {count:,}")

    print("\nTables Updated:")
    for table in results.get('tables_updated', []):
        print(f"  ✅ {table}")

    print("\nAlerts Sent:")
    for alert_type, count in results.get('alerts_sent', {}).items():
        if count > 0:
            print(f"  🚨 {alert_type}: {count}")

    if results.get('errors'):
        print("\nErrors:")
        for error in results['errors']:
            print(f"  ❌ {error}")

    # Save results to monitoring table
    try:
        results_data = [{
            "run_id": str(uuid.uuid4()),
            "status": results['status'],
            "start_time": results['start_time'],
            "end_time": results.get('end_time'),
            "duration_seconds": int((results.get('end_time', datetime.now()) - results['start_time']).total_seconds()),
            "records_processed": str(results.get('records_processed', {})),
            "tables_updated": len(results.get('tables_updated', [])),
            "alerts_sent": str(results.get('alerts_sent', {})),
            "errors": str(results.get('errors', [])),
            "environment": environment,
            "collection_window_days": collection_window_days
        }]

        results_df = spark.createDataFrame(results_data)
        results_df.write.mode("append").saveAsTable(f"{catalog}.{schema}.pipeline_runs")
        print(f"\n💾 Run results saved to {catalog}.{schema}.pipeline_runs")

    except Exception as e:
        print(f"⚠️  Failed to save run results: {e}")

except Exception as e:
    print(f"❌ Pipeline failed: {e}")

    # Log failure
    try:
        failure_data = [{
            "run_id": str(uuid.uuid4()),
            "status": "failed",
            "start_time": datetime.now(),
            "end_time": datetime.now(),
            "duration_seconds": 0,
            "records_processed": "{}",
            "tables_updated": 0,
            "alerts_sent": "{}",
            "errors": str(e),
            "environment": environment,
            "collection_window_days": collection_window_days
        }]

        failure_df = spark.createDataFrame(failure_data)
        failure_df.write.mode("append").saveAsTable(f"{catalog}.{schema}.pipeline_runs")

    except Exception as log_error:
        print(f"Failed to log error: {log_error}")

    raise

# COMMAND ----------

# MAGIC %md ## Health Check Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent pipeline runs
# MAGIC SELECT 
# MAGIC   run_id,
# MAGIC   status,
# MAGIC   start_time,
# MAGIC   duration_seconds,
# MAGIC   records_processed,
# MAGIC   alerts_sent
# MAGIC FROM ${catalog}.${schema}.pipeline_runs
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View job health summary
# MAGIC SELECT 
# MAGIC   job_name,
# MAGIC   owned_by,
# MAGIC   health_score,
# MAGIC   health_category,
# MAGIC   requires_attention
# MAGIC FROM ${catalog}.${schema}.job_health
# MAGIC WHERE requires_attention = true
# MAGIC ORDER BY health_score ASC
# MAGIC LIMIT 20

# COMMAND ----------

print("✅ DataOps Intelligence Platform data collection completed!")
print("\n📋 Next Steps:")
print("1. Review the health scores and alerts")
print("2. Set up alerting channels (Slack, Email, etc.)")
print("3. Schedule this notebook to run regularly")
print("4. Create dashboards using the collected data")
