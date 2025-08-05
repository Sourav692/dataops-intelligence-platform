variable "databricks_workspace_url" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "catalog_name" {
  description = "Name of the catalog to create"
  type        = string
  default     = "dataops_intelligence"
}

variable "schema_name" {
  description = "Name of the schema to create"
  type        = string
  default     = "monitoring"
}

variable "cluster_id" {
  description = "Existing cluster ID for job execution"
  type        = string
}

variable "notification_emails" {
  description = "List of email addresses for job failure notifications"
  type        = list(string)
  default     = []
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}
