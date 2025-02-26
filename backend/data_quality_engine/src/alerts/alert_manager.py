import json
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Any, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='alerts.log'
)
logger = logging.getLogger('alert_manager')


class AlertManager:
    """Manages alerts and notifications for data quality issues"""

    def __init__(self, config_path="./alert_config.json"):
        """Initialize the alert manager with configuration"""
        self.config_path = config_path
        self.config = self._load_config()
        self.alert_history = []

    def _load_config(self) -> Dict:
        """Load alert configuration from file, or return defaults"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {self.config_path} not found. Using defaults.")
            # Return default configuration
            return {
                "email": {
                    "enabled": False,
                    "smtp_server": "smtp.example.com",
                    "smtp_port": 587,
                    "username": "",
                    "password": "",
                    "from_address": "sparvi@example.com",
                    "recipients": []
                },
                "slack": {
                    "enabled": False,
                    "webhook_url": ""
                },
                "webhook": {
                    "enabled": False,
                    "url": ""
                },
                "alert_thresholds": {
                    "row_count_change_pct": 10,
                    "null_rate_change_pct": 5,
                    "duplicate_rate_threshold": 5,
                    "validation_failure_threshold": 1
                },
                "severity_levels": ["info", "warning", "error", "critical"]
            }

    def process_profile_for_alerts(self, current_profile: Dict, previous_profile: Optional[Dict] = None) -> List[Dict]:
        """
        Process a profile to check for conditions that should trigger alerts
        Returns a list of alert objects
        """
        alerts = []

        # Check for anomalies already detected by the profiler
        for anomaly in current_profile.get('anomalies', []):
            alerts.append({
                "type": "anomaly",
                "description": anomaly['description'],
                "severity": anomaly['severity'],
                "timestamp": current_profile['timestamp'],
                "table": current_profile['table'],
                "details": anomaly
            })

        # Check for schema shifts
        for shift in current_profile.get('schema_shifts', []):
            alerts.append({
                "type": "schema_shift",
                "description": shift['description'],
                "severity": "warning" if shift['type'] == "column_added" else "error",
                "timestamp": current_profile['timestamp'],
                "table": current_profile['table'],
                "details": shift
            })

        # Check for validation failures
        for result in current_profile.get('validation_results', []):
            if not result['is_valid']:
                alerts.append({
                    "type": "validation_failure",
                    "description": f"Validation rule '{result['rule_name']}' failed: Expected {result['expected_value']}, got {result['actual_value']}",
                    "severity": "error",
                    "timestamp": current_profile['timestamp'],
                    "table": current_profile['table'],
                    "details": result
                })

        # Only proceed with trend-based alerts if we have previous data
        if not previous_profile:
            return alerts

        # Check for significant row count changes
        row_count_change_pct = self.config['alert_thresholds']['row_count_change_pct']
        if previous_profile['row_count'] > 0:
            pct_change = abs(current_profile['row_count'] - previous_profile['row_count']) / previous_profile[
                'row_count'] * 100
            if pct_change > row_count_change_pct:
                alerts.append({
                    "type": "row_count_change",
                    "description": f"Row count changed by {pct_change:.1f}% (threshold: {row_count_change_pct}%)",
                    "severity": "warning" if pct_change < row_count_change_pct * 2 else "error",
                    "timestamp": current_profile['timestamp'],
                    "table": current_profile['table'],
                    "details": {
                        "previous_count": previous_profile['row_count'],
                        "current_count": current_profile['row_count'],
                        "percent_change": pct_change
                    }
                })

        # Check for significant null rate changes
        null_rate_change_pct = self.config['alert_thresholds']['null_rate_change_pct']
        for col, stats in current_profile['completeness'].items():
            if col in previous_profile['completeness']:
                prev_null_pct = previous_profile['completeness'][col]['null_percentage']
                curr_null_pct = stats['null_percentage']

                pct_diff = abs(curr_null_pct - prev_null_pct)
                if pct_diff > null_rate_change_pct:
                    alerts.append({
                        "type": "null_rate_change",
                        "description": f"Null rate for column '{col}' changed by {pct_diff:.1f}% (threshold: {null_rate_change_pct}%)",
                        "severity": "warning" if pct_diff < null_rate_change_pct * 2 else "error",
                        "timestamp": current_profile['timestamp'],
                        "table": current_profile['table'],
                        "column": col,
                        "details": {
                            "previous_null_pct": prev_null_pct,
                            "current_null_pct": curr_null_pct,
                            "percent_change": pct_diff
                        }
                    })

        # Store alerts in history
        self.alert_history.extend(alerts)

        return alerts

    def send_alerts(self, alerts: List[Dict]) -> Dict[str, int]:
        """
        Send alerts through configured channels
        Returns counts of alerts sent by channel
        """
        if not alerts:
            return {"email": 0, "slack": 0, "webhook": 0}

        results = {"email": 0, "slack": 0, "webhook": 0}

        # Group alerts by severity for better notification
        alerts_by_severity = {}
        for alert in alerts:
            severity = alert.get('severity', 'info')
            if severity not in alerts_by_severity:
                alerts_by_severity[severity] = []
            alerts_by_severity[severity].append(alert)

        # Send email notifications
        if self.config['email']['enabled'] and self.config['email']['recipients']:
            try:
                self._send_email_alerts(alerts_by_severity)
                results['email'] = sum(len(alerts) for alerts in alerts_by_severity.values())
                logger.info(f"Sent {results['email']} alerts via email")
            except Exception as e:
                logger.error(f"Failed to send email alerts: {str(e)}")

        # Send Slack notifications
        if self.config['slack']['enabled'] and self.config['slack']['webhook_url']:
            try:
                self._send_slack_alerts(alerts_by_severity)
                results['slack'] = sum(len(alerts) for alerts in alerts_by_severity.values())
                logger.info(f"Sent {results['slack']} alerts via Slack")
            except Exception as e:
                logger.error(f"Failed to send Slack alerts: {str(e)}")

        # Send webhook notifications
        if self.config['webhook']['enabled'] and self.config['webhook']['url']:
            try:
                self._send_webhook_alerts(alerts)
                results['webhook'] = len(alerts)
                logger.info(f"Sent {results['webhook']} alerts via webhook")
            except Exception as e:
                logger.error(f"Failed to send webhook alerts: {str(e)}")

        return results

    def _send_email_alerts(self, alerts_by_severity: Dict[str, List[Dict]]):
        """Send alerts via email"""
        # Skip if no alerts to send
        if not any(alerts_by_severity.values()):
            return

        # Create email message
        msg = MIMEMultipart()
        msg['From'] = self.config['email']['from_address']
        msg['To'] = ', '.join(self.config['email']['recipients'])

        # Set subject based on highest severity
        highest_severity = 'info'
        for severity in ['critical', 'error', 'warning']:
            if severity in alerts_by_severity and alerts_by_severity[severity]:
                highest_severity = severity
                break

        # Count alerts by severity for subject line
        alert_counts = {sev: len(alerts) for sev, alerts in alerts_by_severity.items() if alerts}
        total_alerts = sum(alert_counts.values())

        msg['Subject'] = f"Sparvi Data Quality Alert: {total_alerts} issues detected ({highest_severity.upper()})"

        # Create the email body
        body = f"<h2>Sparvi Data Quality Alerts</h2>\n"
        body += f"<p>Total alerts: {total_alerts}</p>\n"

        # Add each alert group
        for severity in ['critical', 'error', 'warning', 'info']:
            if severity in alerts_by_severity and alerts_by_severity[severity]:
                body += f"<h3>{severity.upper()} ({len(alerts_by_severity[severity])})</h3>\n"
                body += "<ul>\n"

                for alert in alerts_by_severity[severity]:
                    body += f"<li><strong>{alert['table']}:</strong> {alert['description']}</li>\n"

                body += "</ul>\n"

        # Add HTML formatting
        msg.attach(MIMEText(body, 'html'))

        # Connect to SMTP server and send
        with smtplib.SMTP(self.config['email']['smtp_server'], self.config['email']['smtp_port']) as server:
            server.starttls()
            if self.config['email']['username'] and self.config['email']['password']:
                server.login(self.config['email']['username'], self.config['email']['password'])
            server.send_message(msg)

    def _send_slack_alerts(self, alerts_by_severity: Dict[str, List[Dict]]):
        """Send alerts to Slack via webhook"""
        # Skip if no alerts to send
        if not any(alerts_by_severity.values()):
            return

        # Count alerts
        total_alerts = sum(len(alerts) for alerts in alerts_by_severity.values())

        # Create Slack message blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🦅 Sparvi Data Quality Alert: {total_alerts} issues detected",
                    "emoji": True
                }
            },
            {
                "type": "divider"
            }
        ]

        # Add alert sections by severity
        severity_emojis = {
            "critical": "🔴",
            "error": "🟠",
            "warning": "🟡",
            "info": "🔵"
        }

        for severity in ['critical', 'error', 'warning', 'info']:
            if severity in alerts_by_severity and alerts_by_severity[severity]:
                # Add section for this severity
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{severity_emojis.get(severity, '')} {severity.upper()} ({len(alerts_by_severity[severity])})*"
                    }
                })

                # Add alerts as a bulleted list
                alert_text = ""
                for alert in alerts_by_severity[severity]:
                    alert_text += f"• *{alert['table']}:* {alert['description']}\n"

                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": alert_text
                    }
                })

                blocks.append({"type": "divider"})

        # Send the message to Slack
        message = {
            "blocks": blocks
        }

        response = requests.post(
            self.config['slack']['webhook_url'],
            json=message
        )
        response.raise_for_status()

    def _send_webhook_alerts(self, alerts: List[Dict]):
        """Send alerts to a custom webhook"""
        if not alerts:
            return

        # Send alerts as JSON
        payload = {
            "source": "sparvi",
            "timestamp": alerts[0]['timestamp'],  # Use timestamp from first alert
            "alert_count": len(alerts),
            "alerts": alerts
        }

        response = requests.post(
            self.config['webhook']['url'],
            json=payload
        )
        response.raise_for_status()

    def save_config(self):
        """Save the current configuration to file"""
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=2)

    def update_config(self, new_config: Dict):
        """Update the alert configuration"""
        self.config.update(new_config)
        self.save_config()
        logger.info("Alert configuration updated")