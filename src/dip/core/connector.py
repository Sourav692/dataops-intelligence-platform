from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class DatabricksConnector:
    """Manages connections to Databricks workspace and Spark"""

    def __init__(self, config):
        self.config = config
        self._workspace_client: Optional[WorkspaceClient] = None
        self._spark: Optional[SparkSession] = None

    @property
    def workspace_client(self) -> WorkspaceClient:
        """Get authenticated workspace client"""
        if self._workspace_client is None:
            try:
                self._workspace_client = WorkspaceClient(
                    host=self.config.workspace_url,
                    token=self.config.token
                )
                # Test connection
                self._workspace_client.current_user.me()
                logger.info("Successfully connected to Databricks workspace")
            except Exception as e:
                logger.error(f"Failed to connect to Databricks: {e}")
                raise

        return self._workspace_client

    @property
    def spark(self) -> SparkSession:
        """Get Spark session"""
        if self._spark is None:
            try:
                self._spark = SparkSession.builder \
                    .appName("DataOps Intelligence Platform") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .getOrCreate()

                logger.info("Spark session created successfully")
            except Exception as e:
                logger.error(f"Failed to create Spark session: {e}")
                raise

        return self._spark

    def test_system_tables_access(self) -> Dict[str, bool]:
        """Test access to required system tables"""
        required_tables = [
            "system.lakeflow.jobs",
            "system.lakeflow.job_run_timeline", 
            "system.compute.node_timeline",
            "system.billing.usage"
        ]

        access_results = {}
        for table in required_tables:
            try:
                test_df = self.spark.sql(f"SELECT * FROM {table} LIMIT 1")
                test_df.collect()
                access_results[table] = True
                logger.info(f"✓ Access validated for {table}")
            except Exception as e:
                access_results[table] = False
                logger.warning(f"✗ Access denied for {table}: {e}")

        return access_results
