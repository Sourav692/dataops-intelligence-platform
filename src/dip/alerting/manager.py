from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
import time
from collections import defaultdict

try:
    import requests
    from slack_sdk.webhook import WebhookClient
    import smtplib
    from email.mime.text import MimeText
    from email.mime.multipart import MimeMultipart
    import pymsteams
    from jinja2 import Template
except ImportError as e:
    logging.warning(f"Some alerting dependencies not installed: {e}")

from ..core.config import AlertingConfig, AlertRule

logger = logging.getLogger(__name__)

@dataclass
class Alert:
    '''Alert data structure'''
    id: str
    alert_type: str
    severity: str
    title: str
    message: str
    job_id: Optional[str] = None
    job_name: Optional[str] = None
    workspace_id: Optional[str] = None
    metadata: Dict[str, Any] = None
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.metadata is None:
            self.metadata = {}

class AlertChannel(ABC):
    '''Abstract base class for alert channels'''

    @abstractmethod
    def send_alert(self, alert: Alert) -> bool:
        '''Send alert through this channel'''
        pass

class SlackChannel(AlertChannel):
    '''Slack alerting channel'''

    def __init__(self, webhook_url: str, channel: str, username: str, icon_emoji: str):
        self.webhook_url = webhook_url
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.client = WebhookClient(webhook_url)

    def send_alert(self, alert: Alert) -> bool:
        '''Send alert to Slack'''
        try:
            color = self._get_color_for_severity(alert.severity)

            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"🚨 {alert.title}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": alert.message
                    }
                }
            ]

            if alert.job_name:
                blocks.append({
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Job:* {alert.job_name}"
                        },
                        {
                            "type": "mrkdwn", 
                            "text": f"*Severity:* {alert.severity.upper()}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Time:* {alert.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                        }
                    ]
                })

            response = self.client.send(
                text=alert.title,
                channel=self.channel,
                username=self.username,
                icon_emoji=self.icon_emoji,
                blocks=blocks,
                attachments=[{
                    "color": color,
                    "fields": []
                }]
            )

            if response.status_code == 200:
                logger.info(f"Slack alert sent successfully: {alert.id}")
                return True
            else:
                logger.error(f"Failed to send Slack alert: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error sending Slack alert: {e}")
            return False

    def _get_color_for_severity(self, severity: str) -> str:
        colors = {
            "low": "#36a64f",
            "medium": "#ff9500",
            "high": "#ff0000",
            "critical": "#8b0000"
        }
        return colors.get(severity, "#ff9500")

class EmailChannel(AlertChannel):
    '''Email alerting channel'''

    def __init__(self, smtp_server: str, smtp_port: int, username: str, 
                 password: str, from_address: str, to_addresses: List[str]):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_address = from_address
        self.to_addresses = to_addresses

    def send_alert(self, alert: Alert) -> bool:
        '''Send alert via email'''
        try:
            msg = MimeMultipart('alternative')
            msg['Subject'] = f"[DataOps Alert] {alert.title}"
            msg['From'] = self.from_address
            msg['To'] = ', '.join(self.to_addresses)

            # Simple text content
            text_content = f'''
            DataOps Intelligence Alert

            Title: {alert.title}
            Severity: {alert.severity.upper()}
            Message: {alert.message}
            Job: {alert.job_name or 'N/A'}
            Time: {alert.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}

            Additional Details:
            {json.dumps(alert.metadata, indent=2) if alert.metadata else 'None'}
            '''

            text_part = MimeText(text_content, 'plain')
            msg.attach(text_part)

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)

            logger.info(f"Email alert sent successfully: {alert.id}")
            return True

        except Exception as e:
            logger.error(f"Error sending email alert: {e}")
            return False

class AlertingManager:
    '''Main alerting manager that coordinates all alert channels'''

    def __init__(self, config: AlertingConfig):
        self.config = config
        self.channels = {}
        self._initialize_channels()

    def _initialize_channels(self):
        '''Initialize all configured alert channels'''

        if self.config.slack.enabled and self.config.slack.webhook_url:
            self.channels['slack'] = SlackChannel(
                self.config.slack.webhook_url,
                self.config.slack.channel,
                self.config.slack.username,
                self.config.slack.icon_emoji
            )
            logger.info("Slack alerting channel initialized")

        if (self.config.email.enabled and self.config.email.username 
            and self.config.email.password and self.config.email.to_addresses):
            self.channels['email'] = EmailChannel(
                self.config.email.smtp_server,
                self.config.email.smtp_port,
                self.config.email.username,
                self.config.email.password,
                self.config.email.from_address,
                self.config.email.to_addresses
            )
            logger.info("Email alerting channel initialized")

    def send_alert(self, alert: Alert, channels: List[str] = None) -> Dict[str, bool]:
        '''Send alert through specified channels'''
        if not self.config.enabled:
            logger.debug("Alerting is disabled, skipping alert")
            return {}

        if channels is None:
            channels = list(self.channels.keys())

        results = {}
        for channel_name in channels:
            if channel_name in self.channels:
                try:
                    success = self.channels[channel_name].send_alert(alert)
                    results[channel_name] = success
                except Exception as e:
                    logger.error(f"Error sending alert through {channel_name}: {e}")
                    results[channel_name] = False
            else:
                logger.warning(f"Channel {channel_name} not configured")
                results[channel_name] = False

        return results

    def create_job_failure_alert(self, job_name: str, job_id: str, 
                               workspace_id: str, error_message: str) -> Alert:
        '''Create job failure alert'''
        return Alert(
            id=f"job_failure_{job_id}_{int(time.time())}",
            alert_type="job_failure",
            severity=self.config.job_failure.severity,
            title=f"Job Failure: {job_name}",
            message=f"Job '{job_name}' has failed with error: {error_message}",
            job_id=job_id,
            job_name=job_name,
            workspace_id=workspace_id,
            metadata={
                "error_message": error_message,
                "rule_threshold": self.config.job_failure.threshold
            }
        )

    def create_health_score_alert(self, job_name: str, job_id: str, 
                                workspace_id: str, health_score: float) -> Alert:
        '''Create low health score alert'''
        return Alert(
            id=f"health_low_{job_id}_{int(time.time())}",
            alert_type="health_score_low",
            severity=self.config.health_score_low.severity,
            title=f"Low Health Score: {job_name}",
            message=f"Job '{job_name}' has a health score of {health_score:.1f}, below threshold of {self.config.health_score_low.threshold}",
            job_id=job_id,
            job_name=job_name,
            workspace_id=workspace_id,
            metadata={
                "health_score": health_score,
                "threshold": self.config.health_score_low.threshold
            }
        )
