from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import yaml
from pathlib import Path
import os

@dataclass
class DatabricksConfig:
    """Databricks workspace configuration"""
    workspace_url: str
    token: str
    cluster_id: Optional[str] = None

    def __post_init__(self):
        if not self.workspace_url.startswith('https://'):
            raise ValueError("Workspace URL must start with https://")
        self.workspace_url = self.workspace_url.rstrip('/')

@dataclass
class StorageConfig:
    """Delta Lake storage configuration"""
    catalog: str = "main"
    schema: str = "dataops_intelligence"
    checkpoint_location: str = "/tmp/dip_checkpoints"

    # Table names
    jobs_table: str = "jobs_metadata"
    runs_table: str = "job_runs"
    metrics_table: str = "job_metrics"
    health_table: str = "job_health"
    costs_table: str = "job_costs"
    alerts_table: str = "job_alerts"
    baselines_table: str = "job_baselines"
    anomalies_table: str = "job_anomalies"

@dataclass
class SlackConfig:
    """Slack alerting configuration"""
    enabled: bool = False
    webhook_url: Optional[str] = None
    channel: str = "#dataops-alerts"
    username: str = "DataOps Intelligence"
    icon_emoji: str = ":warning:"

@dataclass
class EmailConfig:
    """Email alerting configuration"""
    enabled: bool = False
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    username: Optional[str] = None
    password: Optional[str] = None
    from_address: str = "dataops@company.com"
    to_addresses: List[str] = field(default_factory=list)

@dataclass
class TeamsConfig:
    """Microsoft Teams alerting configuration"""
    enabled: bool = False
    webhook_url: Optional[str] = None

@dataclass
class WebhookConfig:
    """Custom webhook alerting configuration"""
    enabled: bool = False
    url: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)

@dataclass
class AlertRule:
    """Alert rule configuration"""
    enabled: bool = True
    threshold: float = 0.0
    severity: str = "medium"  # low, medium, high, critical
    channels: List[str] = field(default_factory=list)

@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    enabled: bool = True
    max_alerts_per_hour: int = 10
    max_alerts_per_day: int = 50
    cooldown_minutes: int = 30

@dataclass
class AlertingConfig:
    """Comprehensive alerting configuration"""
    enabled: bool = True

    # Channel configurations
    slack: SlackConfig = field(default_factory=SlackConfig)
    email: EmailConfig = field(default_factory=EmailConfig)
    teams: TeamsConfig = field(default_factory=TeamsConfig)
    webhook: WebhookConfig = field(default_factory=WebhookConfig)

    # Alert rules
    job_failure: AlertRule = field(default_factory=lambda: AlertRule(
        threshold=1, severity="high", channels=["slack", "email"]
    ))
    health_score_low: AlertRule = field(default_factory=lambda: AlertRule(
        threshold=50, severity="medium", channels=["slack"]
    ))
    performance_anomaly: AlertRule = field(default_factory=lambda: AlertRule(
        threshold=2.0, severity="medium", channels=["slack"]
    ))
    cost_spike: AlertRule = field(default_factory=lambda: AlertRule(
        threshold=1.5, severity="high", channels=["slack", "email"]
    ))

    # Rate limiting
    rate_limiting: RateLimitConfig = field(default_factory=RateLimitConfig)

@dataclass
class ProcessingConfig:
    """Data processing configuration"""
    collection_window_days: int = 30
    metrics_granularity_minutes: int = 5
    health_score_weights: Dict[str, float] = field(default_factory=lambda: {
        "success_rate": 0.3,
        "performance_stability": 0.25,
        "cost_efficiency": 0.2,
        "resource_utilization": 0.15,
        "trend_analysis": 0.1
    })
    anomaly_detection_threshold: float = 2.0
    max_concurrent_jobs: int = 10
    min_runs_for_baseline: int = 5

@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None
    max_file_size_mb: int = 100
    backup_count: int = 5

@dataclass
class DIPConfig:
    """Main DIP configuration"""
    databricks: DatabricksConfig
    storage: StorageConfig
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    alerting: AlertingConfig = field(default_factory=AlertingConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    environment: str = "development"
    debug: bool = False

    @classmethod
    def from_yaml(cls, config_path: str) -> 'DIPConfig':
        """Load configuration from YAML file with environment variable substitution"""
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        # Substitute environment variables
        config_data = cls._substitute_env_vars(config_data)

        return cls(
            databricks=DatabricksConfig(**config_data['databricks']),
            storage=StorageConfig(**config_data.get('storage', {})),
            processing=ProcessingConfig(**config_data.get('processing', {})),
            alerting=AlertingConfig(
                enabled=config_data.get('alerting', {}).get('enabled', True),
                slack=SlackConfig(**config_data.get('alerting', {}).get('channels', {}).get('slack', {})),
                email=EmailConfig(**config_data.get('alerting', {}).get('channels', {}).get('email', {})),
                teams=TeamsConfig(**config_data.get('alerting', {}).get('channels', {}).get('teams', {})),
                webhook=WebhookConfig(**config_data.get('alerting', {}).get('channels', {}).get('webhook', {})),
                job_failure=AlertRule(**config_data.get('alerting', {}).get('rules', {}).get('job_failure', {})),
                health_score_low=AlertRule(**config_data.get('alerting', {}).get('rules', {}).get('health_score_low', {})),
                performance_anomaly=AlertRule(**config_data.get('alerting', {}).get('rules', {}).get('performance_anomaly', {})),
                cost_spike=AlertRule(**config_data.get('alerting', {}).get('rules', {}).get('cost_spike', {})),
                rate_limiting=RateLimitConfig(**config_data.get('alerting', {}).get('rate_limiting', {}))
            ),
            logging=LoggingConfig(**config_data.get('logging', {})),
            environment=config_data.get('environment', 'development'),
            debug=config_data.get('debug', False)
        )

    @staticmethod
    def _substitute_env_vars(obj):
        """Recursively substitute environment variables in configuration"""
        if isinstance(obj, dict):
            return {k: DIPConfig._substitute_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [DIPConfig._substitute_env_vars(item) for item in obj]
        elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
            env_var = obj[2:-1]
            return os.getenv(env_var, obj)
        return obj

    def validate(self) -> List[str]:
        """Validate configuration and return any issues"""
        issues = []

        # Validate weights sum to 1.0
        weights_sum = sum(self.processing.health_score_weights.values())
        if not 0.95 <= weights_sum <= 1.05:
            issues.append(f"Health score weights sum to {weights_sum}, should be 1.0")

        # Validate alerting configuration
        if self.alerting.enabled:
            if not any([
                self.alerting.slack.enabled and self.alerting.slack.webhook_url,
                self.alerting.email.enabled and self.alerting.email.username,
                self.alerting.teams.enabled and self.alerting.teams.webhook_url,
                self.alerting.webhook.enabled and self.alerting.webhook.url
            ]):
                issues.append("Alerting is enabled but no valid channels are configured")

        return issues

    def get_databricks_config(self) -> Dict[str, str]:
        """Get Databricks SDK configuration"""
        return {
            "host": self.databricks.workspace_url,
            "token": self.databricks.token
        }
