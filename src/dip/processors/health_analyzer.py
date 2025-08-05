from typing import Dict, List, Optional
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)

class HealthAnalyzer:
    """Analyzes job health and calculates comprehensive health scores"""

    def __init__(self, config):
        self.config = config
        self.weights = config.health_score_weights

    def calculate_health_scores(self, 
                              jobs_df: DataFrame,
                              runs_df: DataFrame, 
                              metrics_df: DataFrame,
                              baselines_df: DataFrame) -> DataFrame:
        """Calculate comprehensive health scores for all jobs"""
        logger.info("Calculating job health scores...")

        # Success rate component
        success_rate_df = self._calculate_success_rate(runs_df)

        # Performance stability component  
        stability_df = self._calculate_performance_stability(baselines_df)

        # Combine components
        health_df = jobs_df.select("workspace_id", "job_id", "job_name", "owned_by") \
            .join(success_rate_df, ["workspace_id", "job_id"], "left") \
            .join(stability_df, ["workspace_id", "job_id"], "left")

        # Calculate final weighted health score
        return health_df.withColumn(
            "health_score",
            (
                coalesce(col("success_rate_score"), lit(0)) * self.weights["success_rate"] +
                coalesce(col("stability_score"), lit(0)) * self.weights["performance_stability"] +
                lit(0.8) * self.weights["cost_efficiency"] +  # Placeholder
                lit(0.7) * self.weights["resource_utilization"] +  # Placeholder
                lit(0.6) * self.weights["trend_analysis"]  # Placeholder
            ) * 100
        ).withColumn(
            "health_category",
            when(col("health_score") >= 85, "Excellent")
            .when(col("health_score") >= 70, "Good") 
            .when(col("health_score") >= 50, "Fair")
            .otherwise("Poor")
        ).withColumn(
            "requires_attention",
            col("health_score") < 70
        ).withColumn(
            "calculated_at",
            current_timestamp()
        )

    def _calculate_success_rate(self, runs_df: DataFrame) -> DataFrame:
        """Calculate success rate score component"""
        return runs_df.groupBy("workspace_id", "job_id") \
            .agg(
                count("*").alias("total_runs"),
                sum(when(col("result_state") == "SUCCESS", 1).otherwise(0)).alias("successful_runs")
            ) \
            .withColumn(
                "success_rate", 
                col("successful_runs") / col("total_runs")
            ) \
            .withColumn(
                "success_rate_score",
                when(col("success_rate") >= 0.95, 1.0)
                .when(col("success_rate") >= 0.90, 0.9)
                .when(col("success_rate") >= 0.80, 0.7)
                .when(col("success_rate") >= 0.70, 0.5)
                .otherwise(0.2)
            )

    def _calculate_performance_stability(self, baselines_df: DataFrame) -> DataFrame:
        """Calculate performance stability score"""
        return baselines_df.withColumn(
            "stability_score",
            when(col("coefficient_of_variation") <= 0.1, 1.0)
            .when(col("coefficient_of_variation") <= 0.2, 0.8)
            .when(col("coefficient_of_variation") <= 0.3, 0.6)
            .when(col("coefficient_of_variation") <= 0.5, 0.4)
            .otherwise(0.2)
        ).select("workspace_id", "job_id", "stability_score", "coefficient_of_variation")
