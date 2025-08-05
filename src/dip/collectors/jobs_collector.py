from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)

class JobsCollector:
    """Collects comprehensive job metadata and execution data"""

    def __init__(self, connector):
        self.connector = connector
        self.spark = connector.spark

    def collect_jobs_metadata(self) -> DataFrame:
        """Collect current job configurations and metadata"""
        logger.info("Collecting jobs metadata...")

        query = """
        WITH latest_jobs AS (
            SELECT *,
                ROW_NUMBER() OVER(
                    PARTITION BY workspace_id, job_id 
                    ORDER BY change_time DESC
                ) as rn
            FROM system.lakeflow.jobs
        )
        SELECT 
            workspace_id,
            job_id,
            name as job_name,
            owned_by,
            creator_email,
            created_time,
            CASE 
                WHEN settings.tasks IS NOT NULL THEN 'MULTI_TASK'
                WHEN settings.notebook_task IS NOT NULL THEN 'NOTEBOOK'
                WHEN settings.python_wheel_task IS NOT NULL THEN 'PYTHON_WHEEL'
                WHEN settings.jar_task IS NOT NULL THEN 'JAR'
                ELSE 'UNKNOWN'
            END as job_type,
            CURRENT_TIMESTAMP() as collected_at
        FROM latest_jobs
        WHERE rn = 1
        """

        return self.spark.sql(query)

    def collect_job_runs(self, days_back: int = 30) -> DataFrame:
        """Collect job execution history"""
        logger.info(f"Collecting job runs for last {days_back} days...")

        query = f"""
        SELECT 
            workspace_id,
            job_id,
            run_id,
            run_name,
            run_type,
            trigger_type,
            creator_email,
            period_start_time,
            period_end_time,
            result_state,
            lifecycle_state,
            state_message,
            termination_code,
            (period_end_time - period_start_time) as duration_ms,
            CURRENT_TIMESTAMP() as collected_at
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= CURRENT_TIMESTAMP() - INTERVAL {days_back} DAYS
        ORDER BY period_start_time DESC
        """

        return self.spark.sql(query)
