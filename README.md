# DataOps Intelligence Platform (DIP)

Enterprise-grade Databricks job monitoring and intelligence platform with automated alerting.

## Features

- 🚀 **Comprehensive Job Monitoring**: Track all Databricks jobs, runs, and tasks
- 🚀 **Real-Time Metrics**: CPU/Memory/Network utilization monitoring  
- 🚀 **Health Scoring**: AI-powered job health analysis
- 🚀 **Anomaly Detection**: Automatic performance anomaly detection
- 🚀 **Cost Analysis**: Detailed cost tracking and optimization
- 🚀 **Automated Alerting**: Slack/Email/Webhook/Teams alerting
- 🚀 **Delta Lake Storage**: Scalable data storage
- 🚀 **Easy Deployment**: Ready-to-use notebooks and configuration

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure settings in `config/settings.yaml`
4. Run the setup notebook `notebooks/01_Setup_and_Configuration.py`
5. Schedule the data collection job

## Quick Start

```python
from dip.core.config import DIPConfig
from dip.orchestrator import DataOpsIntelligencePlatform

# Create configuration
config = DIPConfig(
    databricks=DIPConfig.DatabricksConfig(
        workspace_url="https://your-workspace.cloud.databricks.com",
        token="your-pat-token"
    ),
    storage=DIPConfig.StorageConfig(
        catalog="main",
        schema="dataops_intelligence"
    )
)

# Initialize and run
dip = DataOpsIntelligencePlatform(config)
results = dip.run_full_collection()
```

## Architecture

The platform follows a modular architecture:

1. **Collectors**: Gather data from Databricks system tables
2. **Processors**: Analyze data and calculate metrics
3. **Storage**: Write data to Delta Lake tables
4. **Alerting**: Send notifications based on rules
5. **Orchestrator**: Coordinate all components

## Alerting System

The enhanced alerting system supports:

- **Multiple Channels**: Slack, Email, Microsoft Teams, Webhooks
- **Smart Rules**: Health score thresholds, anomaly detection, cost alerts
- **Escalation**: Multi-level alerting with escalation paths
- **Rate Limiting**: Prevent alert fatigue with intelligent throttling
- **Rich Content**: Detailed alerts with charts and actionable insights

## Documentation

See the `docs/` folder for detailed documentation.

## License

Apache 2.0 License
