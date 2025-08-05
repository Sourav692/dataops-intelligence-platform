import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

class DeltaWriter:
    """Handles writing data to Delta Lake tables"""

    def __init__(self, connector, storage_config):
        self.connector = connector
        self.config = storage_config
        self.spark = connector.spark

    def write_table(self, df: DataFrame, table_name: str, mode: str = "overwrite", merge_keys: list = None):
        """Write DataFrame to Delta table"""
        full_table_name = f"{self.config.catalog}.{self.config.schema}.{table_name}"

        try:
            # Ensure catalog and schema exist
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.config.catalog}")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.config.catalog}.{self.config.schema}")

            # Write data
            df.write \
                .format("delta") \
                .mode(mode) \
                .option("mergeSchema", "true") \
                .saveAsTable(full_table_name)

            logger.info(f"Successfully wrote {df.count()} records to {full_table_name}")

        except Exception as e:
            logger.error(f"Failed to write to {full_table_name}: {e}")
            raise
