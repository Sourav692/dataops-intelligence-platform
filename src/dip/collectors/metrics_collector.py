from typing import Dict, List, Optional
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Collects performance and resource utilization metrics"""

    def __init__(self, connector):
        self.connector = connector
        self.spark = connector.spark

    def collect_resource_metrics(self, days_back: int = 7) -> DataFrame:
        """Collect CPU, memory, and network metrics for job executions"""
        logger.info(f"Collecting resource metrics for last {days_back} days...")

        query = f"""
        WITH per_cluster_daily AS (
            SELECT
                cluster_id,
                DATE_TRUNC('DAY', start_time) AS day,
                AVG(cpu_user_percent + cpu_system_percent) AS avg_cpu_usage_percent,
                AVG(cpu_idle_percent) AS avg_cpu_idle_percent,
                AVG(mem_used_percent) AS avg_memory_usage_percent,
                AVG(network_received_bytes + network_sent_bytes) as avg_network_bytes,
                COUNT(*) as metric_points
            FROM system.compute.node_timeline
            WHERE start_time >= CURRENT_DATE - INTERVAL {days_back} DAYS
            GROUP BY cluster_id, DATE_TRUNC('DAY', start_time)
        )
        SELECT 
            cluster_id,
            day,
            avg_cpu_usage_percent,
            avg_cpu_idle_percent,
            avg_memory_usage_percent,
            avg_network_bytes,
            metric_points,
            CURRENT_TIMESTAMP() as collected_at
        FROM per_cluster_daily
        ORDER BY cluster_id, day
        """

        return self.spark.sql(query)

    def calculate_performance_baselines(self, days_back: int = 30) -> DataFrame:
        """Calculate performance baselines for anomaly detection"""
        logger.info("Calculating performance baselines...")

        query = f"""
        WITH successful_runs AS (
            SELECT 
                workspace_id,
                job_id,
                run_id,
                SUM(period_end_time - period_start_time) as total_duration_ms
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= CURRENT_TIMESTAMP() - INTERVAL {days_back} DAYS
              AND result_state = 'SUCCESS'
            GROUP BY workspace_id, job_id, run_id
        ),
        job_stats AS (
            SELECT
                workspace_id,
                job_id,
                COUNT(*) as total_runs,
                AVG(total_duration_ms) as avg_duration_ms,
                STDDEV(total_duration_ms) as stddev_duration_ms,
                PERCENTILE(total_duration_ms, 0.5) as median_duration_ms,
                PERCENTILE(total_duration_ms, 0.9) as p90_duration_ms,
                MIN(total_duration_ms) as min_duration_ms,
                MAX(total_duration_ms) as max_duration_ms
            FROM successful_runs
            GROUP BY workspace_id, job_id
            HAVING COUNT(*) >= 5
        )
        SELECT 
            *,
            (avg_duration_ms + 2 * COALESCE(stddev_duration_ms, 0)) as upper_threshold_ms,
            (avg_duration_ms - 2 * COALESCE(stddev_duration_ms, 0)) as lower_threshold_ms,
            CASE 
                WHEN avg_duration_ms > 0 
                THEN COALESCE(stddev_duration_ms, 0) / avg_duration_ms 
                ELSE 0 
            END as coefficient_of_variation,
            CURRENT_TIMESTAMP() as calculated_at
        FROM job_stats
        """

        return self.spark.sql(query)
