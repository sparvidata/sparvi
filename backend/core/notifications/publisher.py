# core/notifications/publisher.py - Multi-tenant version with automation support

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
            event_type: Type of event ('anomaly', 'automation', etc.)
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
            elif event_type == "automation":
                return self._send_automation_notification(organization_id, payload)

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

    def _send_automation_notification(self, organization_id: str, payload: Dict[str, Any]) -> bool:
        """
        Send notifications for automation events

        Args:
            organization_id: Organization ID
            payload: Automation event payload

        Returns:
            Success status
        """
        try:
            # Get notification settings for this organization
            notification_settings = self._get_notification_settings(organization_id)

            if not notification_settings:
                logger.info(f"No notification settings for organization {organization_id}")
                return True

            # Check if automation notifications are enabled
            if not notification_settings.get("automation_notifications_enabled", True):
                logger.info(f"Automation notifications disabled for organization {organization_id}")
                return True

            event_type = payload.get("type", "")
            data = payload.get("data", {})

            # Only send notifications for important automation events
            important_events = [
                "automation_job_failed",
                "schema_changes_detected",
                "validation_failures_detected"
            ]

            if event_type not in important_events:
                logger.debug(f"Skipping notification for non-important event: {event_type}")
                return True

            # Build notification content based on event type
            subject, message = self._build_automation_message(event_type, data, payload)

            if not subject or not message:
                logger.warning(f"Could not build message for automation event: {event_type}")
                return True

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
            logger.error(f"Error sending automation notification for org {organization_id}: {str(e)}")
            return False

    def _build_automation_message(self, event_type: str, data: Dict[str, Any], payload: Dict[str, Any]) -> tuple:
        """Build notification message for automation events"""
        try:
            if event_type == "automation_job_failed":
                job_type = data.get("job_type", "automation")
                error = data.get("error", "Unknown error")
                connection_id = data.get("connection_id", "Unknown")

                subject = f"🔧 Automation Job Failed"
                message = f"""
Automation Job Failure

Job Type: {job_type}
Connection: {connection_id}
Error: {error}
Time: {payload.get("timestamp", "Unknown")}

Please check your automation settings in the Sparvi dashboard.
                """.strip()

            elif event_type == "schema_changes_detected":
                changes_count = data.get("changes_detected", 0)
                connection_id = data.get("connection_id", "Unknown")
                important = data.get("important", False)

                if important:
                    subject = f"⚠️ Important Schema Changes Detected"
                    message = f"""
Important Database Schema Changes Detected

Connection: {connection_id}
Changes Detected: {changes_count}
Time: {payload.get("timestamp", "Unknown")}

Minor schema changes have been detected and logged in your Sparvi dashboard.
                    """.strip()

            elif event_type == "validation_failures_detected":
                failed_rules = data.get("failed_rules", 0)
                tables = data.get("tables", [])
                connection_id = data.get("connection_id", "Unknown")

                subject = f"❌ Validation Failures Detected"
                message = f"""
Data Validation Failures Detected

Connection: {connection_id}
Failed Rules: {failed_rules}
Affected Tables: {len(tables)}
Time: {payload.get("timestamp", "Unknown")}

Please review the failed validation rules in your Sparvi dashboard.
                """.strip()

            else:
                return None, None

            return subject, message

        except Exception as e:
            logger.error(f"Error building automation message: {str(e)}")
            return None, None

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

            # Format for Slack - determine color based on event type and severity
            color = "danger"  # Default to danger

            # Determine color based on content
            if "Schema Changes" in subject:
                color = "warning" if "Important" in subject else "good"
            elif "Validation Failures" in subject:
                color = "danger"
            elif "Job Failed" in subject:
                color = "danger"
            elif "Anomaly Alert" in subject:
                high_severity = data.get("high_severity_count", 0)
                color = "danger" if high_severity > 0 else "warning"

            # Build Slack payload
            slack_payload = {
                "text": subject,
                "attachments": [
                    {
                        "color": color,
                        "text": message,
                        "fields": self._build_slack_fields(data),
                        "footer": "Sparvi Data Quality Platform"
                    }
                ]
            }

            response = requests.post(webhook_url, json=slack_payload, timeout=10)
            response.raise_for_status()

            logger.info("Slack notification sent successfully")
            return True

        except Exception as e:
            logger.error(f"Error sending Slack notification: {str(e)}")
            return False

    def _build_slack_fields(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build Slack fields based on data content"""
        fields = []

        # Add relevant fields based on what's in the data
        if "table_name" in data:
            fields.append({
                "title": "Table",
                "value": data["table_name"],
                "short": True
            })

        if "connection_id" in data:
            fields.append({
                "title": "Connection",
                "value": data["connection_id"],
                "short": True
            })

        if "high_severity_count" in data:
            fields.extend([
                {
                    "title": "High Severity",
                    "value": str(data["high_severity_count"]),
                    "short": True
                },
                {
                    "title": "Medium Severity",
                    "value": str(data.get("medium_severity_count", 0)),
                    "short": True
                }
            ])

        if "failed_rules" in data:
            fields.append({
                "title": "Failed Rules",
                "value": str(data["failed_rules"]),
                "short": True
            })

        if "changes_detected" in data:
            fields.append({
                "title": "Changes Detected",
                "value": str(data["changes_detected"]),
                "short": True
            })

        if "job_type" in data:
            fields.append({
                "title": "Job Type",
                "value": data["job_type"],
                "short": True
            })

        return fields

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