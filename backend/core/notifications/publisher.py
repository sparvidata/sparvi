# core/notifications/publisher.py - Multi-tenant version

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List, Optional
import os
import requests
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


class NotificationPublisher:
    """
    Multi-tenant notification publisher that gets notification settings
    per organization from the database
    """

    def __init__(self):
        self.supabase = SupabaseManager()

    def publish_event(self, event_type: str, payload: Dict[str, Any]) -> bool:
        """
        Publish notification events for a specific organization

        Args:
            event_type: Type of event ('anomaly', etc.)
            payload: Event payload with notification data

        Returns:
            Success status
        """
        try:
            organization_id = payload.get("organization_id")
            if not organization_id:
                logger.error("No organization_id in payload, cannot send notifications")
                return False

            if event_type == "anomaly" and payload.get("type") == "anomaly_detected":
                return self._send_anomaly_notification(organization_id, payload)

            return True
        except Exception as e:
            logger.error(f"Error publishing notification: {str(e)}")
            return False

    def _send_anomaly_notification(self, organization_id: str, payload: Dict[str, Any]) -> bool:
        """
        Send notifications for detected anomalies to organization-specific channels

        Args:
            organization_id: Organization ID
            payload: Anomaly event payload

        Returns:
            Success status
        """
        try:
            # Get notification settings for this organization
            notification_settings = self._get_notification_settings(organization_id)

            if not notification_settings:
                logger.info(f"No notification settings for organization {organization_id}")
                return True

            data = payload.get("data", {})

            # Check if we should send notifications based on severity
            high_severity = data.get("high_severity_count", 0)
            medium_severity = data.get("medium_severity_count", 0)
            low_severity = data.get("low_severity_count", 0)

            # Get notification preferences
            notify_high = notification_settings.get("notify_high_severity", True)
            notify_medium = notification_settings.get("notify_medium_severity", True)
            notify_low = notification_settings.get("notify_low_severity", False)

            should_notify = (
                    (high_severity > 0 and notify_high) or
                    (medium_severity > 0 and notify_medium) or
                    (low_severity > 0 and notify_low)
            )

            if not should_notify:
                logger.info(f"Severity levels don't meet notification criteria for org {organization_id}")
                return True

            # Build notification message
            table_name = data.get("table_name", "Unknown")
            column_name = data.get("column_name", "")
            metric_name = data.get("metric_name", "Unknown")
            total_anomalies = data.get("anomaly_count", 0)

            subject = f"🚨 Anomaly Alert: {table_name}"

            message = f"""
Data Quality Alert - Anomalies Detected

Table: {table_name}
{f"Column: {column_name}" if column_name else ""}
Metric: {metric_name}

Anomalies Found:
• High Severity: {high_severity}
• Medium Severity: {medium_severity}
• Low Severity: {low_severity}
• Total: {total_anomalies}

Detection Method: {data.get("detection_method", "Unknown")}
Time: {payload.get("timestamp", "Unknown")}

View details in your Sparvi dashboard.
            """.strip()

            # Send via configured channels
            success = True

            # Email notifications
            if notification_settings.get("email_enabled", False):
                success &= self._send_email_notification(
                    notification_settings, subject, message
                )

            # Slack notifications  
            if notification_settings.get("slack_enabled", False):
                success &= self._send_slack_notification(
                    notification_settings, subject, message, data
                )

            # Webhook notifications
            if notification_settings.get("webhook_enabled", False):
                success &= self._send_webhook_notification(
                    notification_settings, payload
                )

            return success

        except Exception as e:
            logger.error(f"Error sending anomaly notification for org {organization_id}: {str(e)}")
            return False

    def _get_notification_settings(self, organization_id: str) -> Optional[Dict[str, Any]]:
        """
        Get notification settings for an organization from the database

        Args:
            organization_id: Organization ID

        Returns:
            Notification settings dictionary or None
        """
        try:
            response = self.supabase.supabase.table("notification_settings") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .execute()

            if response.data and len(response.data) > 0:
                return response.data[0]

            return None

        except Exception as e:
            logger.error(f"Error getting notification settings for org {organization_id}: {str(e)}")
            return None

    def _send_email_notification(self, settings: Dict[str, Any], subject: str, message: str) -> bool:
        """Send email notification using organization-specific settings"""
        try:
            smtp_config = settings.get("email_config", {})

            smtp_host = smtp_config.get("smtp_host")
            smtp_port = smtp_config.get("smtp_port", 587)
            smtp_user = smtp_config.get("smtp_user")
            smtp_password = smtp_config.get("smtp_password")
            from_email = smtp_config.get("from_email", smtp_user)
            to_emails = smtp_config.get("to_emails", [])

            if not all([smtp_host, smtp_user, smtp_password]) or not to_emails:
                logger.warning(f"Incomplete email configuration for organization")
                return True

            # Create message
            msg = MIMEMultipart()
            msg['From'] = from_email
            msg['To'] = ", ".join(to_emails)
            msg['Subject'] = subject
            msg.attach(MIMEText(message, 'plain'))

            # Send email
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                if smtp_config.get("use_tls", True):
                    server.starttls()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)

            logger.info(f"Email notification sent to {len(to_emails)} recipients")
            return True

        except Exception as e:
            logger.error(f"Error sending email notification: {str(e)}")
            return False

    def _send_slack_notification(self, settings: Dict[str, Any], subject: str, message: str,
                                 data: Dict[str, Any]) -> bool:
        """Send Slack notification using organization-specific settings"""
        try:
            slack_config = settings.get("slack_config", {})
            webhook_url = slack_config.get("webhook_url")

            if not webhook_url:
                logger.warning("No Slack webhook URL configured")
                return True

            # Format for Slack
            high_severity = data.get("high_severity_count", 0)
            medium_severity = data.get("medium_severity_count", 0)

            color = "danger" if high_severity > 0 else "warning"

            payload = {
                "text": subject,
                "attachments": [
                    {
                        "color": color,
                        "fields": [
                            {
                                "title": "Table",
                                "value": data.get("table_name", "Unknown"),
                                "short": True
                            },
                            {
                                "title": "Metric",
                                "value": data.get("metric_name", "Unknown"),
                                "short": True
                            },
                            {
                                "title": "High Severity",
                                "value": str(high_severity),
                                "short": True
                            },
                            {
                                "title": "Medium Severity",
                                "value": str(medium_severity),
                                "short": True
                            }
                        ]
                    }
                ]
            }

            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()

            logger.info("Slack notification sent successfully")
            return True

        except Exception as e:
            logger.error(f"Error sending Slack notification: {str(e)}")
            return False

    def _send_webhook_notification(self, settings: Dict[str, Any], payload: Dict[str, Any]) -> bool:
        """Send webhook notification using organization-specific settings"""
        try:
            webhook_config = settings.get("webhook_config", {})
            webhook_url = webhook_config.get("url")

            if not webhook_url:
                return True  # No webhook configured

            # Add any custom headers
            headers = {"Content-Type": "application/json"}
            custom_headers = webhook_config.get("headers", {})
            headers.update(custom_headers)

            # Send the full payload
            response = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()

            logger.info("Webhook notification sent successfully")
            return True

        except Exception as e:
            logger.error(f"Error sending webhook notification: {str(e)}")
            return False


# Global instance
notification_publisher = NotificationPublisher()


def publish_event(event_type: str, payload: Dict[str, Any]) -> bool:
    """
    Convenience function to publish events using the global publisher

    Args:
        event_type: Type of event
        payload: Event payload (must include organization_id)

    Returns:
        Success status
    """
    return notification_publisher.publish_event(event_type, payload)