from typing import Dict, List, Optional
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)

class AnomalyDetector:
    """Detects performance anomalies in job executions"""

    def __init__(self, config):
        self.config = config
        self.threshold = config.anomaly_detection_threshold

    def detect_performance_anomalies(self, runs_df: DataFrame, baselines_df: DataFrame) -> DataFrame:
        """Detect jobs with unusual performance patterns"""
        logger.info("Detecting performance anomalies...")

        # Calculate recent performance
        recent_cutoff = current_timestamp() - expr("INTERVAL 7 DAYS")

        recent_runs = runs_df.filter(col("period_start_time") >= recent_cutoff) \
            .groupBy("workspace_id", "job_id") \
            .agg(
                avg("duration_ms").alias("recent_avg_duration"),
                count("*").alias("recent_run_count")
            )

        # Join with baselines to detect anomalies
        anomalies = recent_runs.join(baselines_df, ["workspace_id", "job_id"], "inner") \
            .withColumn(
                "z_score",
                (col("recent_avg_duration") - col("avg_duration_ms")) / 
                greatest(col("stddev_duration_ms"), lit(1.0))
            ) \
            .withColumn(
                "anomaly_type",
                when(col("z_score") > self.threshold, "SLOW")
                .when(col("z_score") < -self.threshold, "FAST")
                .otherwise("NORMAL")
            ) \
            .withColumn(
                "anomaly_score",
                abs(col("z_score"))
            ) \
            .filter(col("anomaly_type") != "NORMAL") \
            .withColumn(
                "detected_at",
                current_timestamp()
            )

        return anomalies.select(
            "workspace_id", "job_id", "anomaly_type", "anomaly_score",
            "recent_avg_duration", "avg_duration_ms", "z_score", "detected_at"
        )
