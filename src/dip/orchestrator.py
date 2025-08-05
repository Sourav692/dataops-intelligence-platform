import logging
from typing import Dict, Any, Optional
from datetime import datetime
from .core.config import DIPConfig
from .core.connector import DatabricksConnector
from .collectors.jobs_collector import JobsCollector
from .collectors.metrics_collector import MetricsCollector
from .processors.health_analyzer import HealthAnalyzer
from .processors.anomaly_detector import AnomalyDetector
from .storage.delta_writer import DeltaWriter
from .alerting.manager import AlertingManager
from .utils.logging import setup_logging
from .utils.retry import retry_on_failure

logger = logging.getLogger(__name__)

class DataOpsIntelligencePlatform:
    """Main orchestrator for the DataOps Intelligence Platform"""

    def __init__(self, config: DIPConfig):
        self.config = config
        self.connector = DatabricksConnector(config.databricks)

        # Initialize components
        self.jobs_collector = JobsCollector(self.connector)
        self.metrics_collector = MetricsCollector(self.connector)
        self.health_analyzer = HealthAnalyzer(config.processing)
        self.anomaly_detector = AnomalyDetector(config.processing)
        self.delta_writer = DeltaWriter(self.connector, config.storage)
        self.alerting_manager = AlertingManager(config.alerting)

        logger.info("DataOps Intelligence Platform initialized")

    @retry_on_failure(max_retries=3, delay=30)
    def run_full_collection(self) -> Dict[str, Any]:
        """Execute complete data collection and analysis pipeline"""
        logger.info("Starting full data collection pipeline...")

        results = {
            "start_time": datetime.now(),
            "status": "running",
            "tables_updated": [],
            "records_processed": {},
            "errors": [],
            "alerts_sent": {}
        }

        try:
            # Step 1: Validate system access
            self._validate_system_access()

            # Step 2: Collect job metadata
            jobs_df = self.jobs_collector.collect_jobs_metadata()
            job_count = jobs_df.count()
            logger.info(f"Collected metadata for {job_count} jobs")

            # Step 3: Collect job runs
            runs_df = self.jobs_collector.collect_job_runs(
                days_back=self.config.processing.collection_window_days
            )
            runs_count = runs_df.count()
            logger.info(f"Collected {runs_count} job runs")

            # Step 4: Collect performance metrics
            metrics_df = self.metrics_collector.collect_resource_metrics(days_back=7)
            metrics_count = metrics_df.count() if metrics_df else 0
            logger.info(f"Collected {metrics_count} metric records")

            # Step 5: Calculate performance baselines
            baselines_df = self.metrics_collector.calculate_performance_baselines()
            baselines_count = baselines_df.count()
            logger.info(f"Calculated baselines for {baselines_count} jobs")

            # Step 6: Analyze job health
            health_df = self.health_analyzer.calculate_health_scores(
                jobs_df, runs_df, metrics_df, baselines_df
            )
            health_count = health_df.count()
            logger.info(f"Calculated health scores for {health_count} jobs")

            # Step 7: Detect anomalies
            anomalies_df = self.anomaly_detector.detect_performance_anomalies(
                runs_df, baselines_df
            )
            anomalies_count = anomalies_df.count()
            logger.info(f"Detected {anomalies_count} performance anomalies")

            # Step 8: Write to Delta tables
            self._write_to_delta_tables({
                "jobs_metadata": jobs_df,
                "job_runs": runs_df,
                "job_metrics": metrics_df,
                "job_health": health_df,
                "job_baselines": baselines_df,
                "job_anomalies": anomalies_df
            })

            # Step 9: Process alerts
            alert_results = self._process_alerts(health_df, anomalies_df, runs_df)

            # Update results
            results.update({
                "status": "completed",
                "end_time": datetime.now(),
                "records_processed": {
                    "jobs": job_count,
                    "runs": runs_count,
                    "metrics": metrics_count,
                    "health_scores": health_count,
                    "anomalies": anomalies_count
                },
                "tables_updated": [
                    f"{self.config.storage.catalog}.{self.config.storage.schema}.{table}"
                    for table in ["jobs_metadata", "job_runs", "job_metrics", 
                                "job_health", "job_anomalies"]
                ],
                "alerts_sent": alert_results
            })

            logger.info("Full collection pipeline completed successfully")
            return results

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            results["status"] = "failed"
            results["errors"].append(str(e))
            results["end_time"] = datetime.now()
            raise

    def _validate_system_access(self):
        """Validate access to required system tables"""
        access_results = self.connector.test_system_tables_access()
        failed_tables = [table for table, accessible in access_results.items() if not accessible]

        if failed_tables:
            raise PermissionError(f"Access denied to system tables: {failed_tables}")

        logger.info("System tables access validated successfully")

    def _write_to_delta_tables(self, dataframes: Dict[str, Any]):
        """Write all dataframes to their respective Delta tables"""
        for table_name, df in dataframes.items():
            if df is not None and df.count() > 0:
                try:
                    self.delta_writer.write_table(df, table_name, mode="overwrite")
                    logger.info(f"Successfully wrote {df.count()} records to {table_name}")
                except Exception as e:
                    logger.error(f"Failed to write to {table_name}: {e}")
                    raise

    def _process_alerts(self, health_df, anomalies_df, runs_df) -> Dict[str, int]:
        """Process and send alerts based on analysis results"""
        alert_results = {"job_failures": 0, "health_alerts": 0, "anomaly_alerts": 0}

        try:
            # Job failure alerts
            failed_runs = runs_df.filter(runs_df.result_state == "FAILED").collect()
            for row in failed_runs[:10]:  # Limit to prevent spam
                alert = self.alerting_manager.create_job_failure_alert(
                    job_name=row.job_name or f"Job {row.job_id}",
                    job_id=row.job_id,
                    workspace_id=row.workspace_id,
                    error_message=row.state_message or "Unknown error"
                )

                self.alerting_manager.send_alert(
                    alert, 
                    channels=self.config.alerting.job_failure.channels
                )
                alert_results["job_failures"] += 1

            # Health score alerts
            if self.config.alerting.health_score_low.enabled:
                low_health_jobs = health_df.filter(
                    health_df.health_score < self.config.alerting.health_score_low.threshold
                ).collect()

                for row in low_health_jobs[:5]:  # Limit alerts
                    alert = self.alerting_manager.create_health_score_alert(
                        job_name=row.job_name,
                        job_id=row.job_id,
                        workspace_id=row.workspace_id,
                        health_score=row.health_score
                    )

                    self.alerting_manager.send_alert(
                        alert,
                        channels=self.config.alerting.health_score_low.channels
                    )
                    alert_results["health_alerts"] += 1

            logger.info(f"Processed alerts: {alert_results}")

        except Exception as e:
            logger.error(f"Error processing alerts: {e}")

        return alert_results
