terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_workspace_url
  token = var.databricks_token
}

# Create catalog for DataOps Intelligence Platform
resource "databricks_catalog" "dataops_intelligence" {
  name    = var.catalog_name
  comment = "Catalog for DataOps Intelligence Platform monitoring data"
}

# Create schema for monitoring tables
resource "databricks_schema" "monitoring" {
  catalog_name = databricks_catalog.dataops_intelligence.name
  name         = var.schema_name
  comment      = "Schema containing job monitoring and performance metrics"
}

# Create job for data collection
resource "databricks_job" "data_collection" {
  name = "DataOps Intelligence - Data Collection"

  notebook_task {
    notebook_path = "/Shared/dataops-intelligence/02_Data_Collection_Job"
  }

  existing_cluster_id = var.cluster_id

  schedule {
    quartz_cron_expression = "0 */30 * * * ?" # Every 30 minutes
    timezone_id            = "UTC"
  }

  email_notifications {
    on_failure = var.notification_emails
  }

  tags = {
    environment = var.environment
    project     = "dataops-intelligence-platform"
  }
}
