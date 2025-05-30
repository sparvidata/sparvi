import logging
from enum import Enum
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class AutomationEventType(Enum):
    """Event types for automation system"""
    CONFIG_CREATED = "automation_config_created"
    CONFIG_UPDATED = "automation_config_updated"
    CONFIG_DELETED = "automation_config_deleted"

    JOB_SCHEDULED = "automation_job_scheduled"
    JOB_STARTED = "automation_job_started"
    JOB_COMPLETED = "automation_job_completed"
    JOB_FAILED = "automation_job_failed"
    JOB_CANCELLED = "automation_job_cancelled"

    METADATA_REFRESHED = "metadata_refreshed"
    SCHEMA_CHANGES_DETECTED = "schema_changes_detected"
    VALIDATION_FAILURES_DETECTED = "validation_failures_detected"

    AUTOMATION_ENABLED = "automation_enabled"
    AUTOMATION_DISABLED = "automation_disabled"
    MANUAL_TRIGGER = "automation_manual_trigger"


def publish_automation_event(event_type: AutomationEventType,
                             data: Dict[str, Any],
                             connection_id: Optional[str] = None,
                             organization_id: Optional[str] = None,
                             user_id: Optional[str] = None) -> bool:
    """
    Publish an automation-related event

    Args:
        event_type: Type of event (from AutomationEventType)
        data: Event data
        connection_id: Optional connection ID
        organization_id: Optional organization ID
        user_id: Optional user ID for user-triggered events

    Returns:
        Success status
    """
    try:
        # Create event payload
        payload = {
            "type": event_type.value,
            "data": data,
            "connection_id": connection_id,
            "organization_id": organization_id,
            "user_id": user_id,
            "timestamp": datetime.now().isoformat()
        }

        # Log the event
        logger.info(f"Automation event: {event_type.value}")
        logger.debug(f"Automation event payload: {payload}")

        # Try to publish to the notification system if available
        try:
            from core.notifications.publisher import publish_event

            # Determine if this should trigger notifications
            notification_events = [
                AutomationEventType.JOB_FAILED,
                AutomationEventType.SCHEMA_CHANGES_DETECTED,
                AutomationEventType.VALIDATION_FAILURES_DETECTED
            ]

            if event_type in notification_events:
                return publish_event("automation", payload)
            else:
                # Just log non-notification events
                return True

        except ImportError:
            logger.warning("Notification system not available, event logged only")
            return True

    except Exception as e:
        logger.error(f"Error publishing automation event: {str(e)}")
        return False


class AutomationEventHandler:
    """Handler for automation events that may trigger additional actions"""

    def __init__(self, supabase_manager=None):
        self.supabase = supabase_manager

    def handle_event(self, event_type: AutomationEventType, data: Dict[str, Any],
                     connection_id: str = None, organization_id: str = None):
        """
        Handle automation events and trigger appropriate actions

        Args:
            event_type: Type of event
            data: Event data
            connection_id: Connection ID
            organization_id: Organization ID
        """
        try:
            if event_type == AutomationEventType.SCHEMA_CHANGES_DETECTED:
                self._handle_schema_changes(data, connection_id, organization_id)
            elif event_type == AutomationEventType.VALIDATION_FAILURES_DETECTED:
                self._handle_validation_failures(data, connection_id, organization_id)
            elif event_type == AutomationEventType.JOB_FAILED:
                self._handle_job_failure(data, connection_id, organization_id)

        except Exception as e:
            logger.error(f"Error handling automation event: {str(e)}")

    def _handle_schema_changes(self, data: Dict[str, Any], connection_id: str, organization_id: str):
        """Handle schema changes detected event"""
        try:
            changes_count = data.get("changes", 0)
            important = data.get("important", False)

            if important and changes_count > 0:
                # Create notification for important schema changes
                notification_data = {
                    "title": "Schema Changes Detected",
                    "message": f"Detected {changes_count} schema changes in your database",
                    "connection_id": connection_id,
                    "severity": "medium" if changes_count < 5 else "high",
                    "action_required": True,
                    "details": data
                }

                # Store notification in database
                if self.supabase:
                    self._store_notification(organization_id, notification_data)

        except Exception as e:
            logger.error(f"Error handling schema changes event: {str(e)}")

    def _handle_validation_failures(self, data: Dict[str, Any], connection_id: str, organization_id: str):
        """Handle validation failures detected event"""
        try:
            failed_rules = data.get("failed_rules", 0)
            tables = data.get("tables", [])

            if failed_rules > 0:
                # Create notification for validation failures
                notification_data = {
                    "title": "Validation Failures Detected",
                    "message": f"Found {failed_rules} validation failures across {len(tables)} tables",
                    "connection_id": connection_id,
                    "severity": "high" if failed_rules > 10 else "medium",
                    "action_required": True,
                    "details": data
                }

                # Store notification in database
                if self.supabase:
                    self._store_notification(organization_id, notification_data)

        except Exception as e:
            logger.error(f"Error handling validation failures event: {str(e)}")

    def _handle_job_failure(self, data: Dict[str, Any], connection_id: str, organization_id: str):
        """Handle automation job failure event"""
        try:
            job_id = data.get("job_id")
            job_type = data.get("job_type")
            error = data.get("error", "Unknown error")

            # Create notification for job failure
            notification_data = {
                "title": f"Automation Job Failed",
                "message": f"Job {job_type} failed: {error}",
                "connection_id": connection_id,
                "severity": "medium",
                "action_required": False,
                "details": data
            }

            # Store notification in database
            if self.supabase:
                self._store_notification(organization_id, notification_data)

        except Exception as e:
            logger.error(f"Error handling job failure event: {str(e)}")

    def _store_notification(self, organization_id: str, notification_data: Dict[str, Any]):
        """Store notification in database"""
        try:
            notification_record = {
                "organization_id": organization_id,
                "type": "automation",
                "title": notification_data["title"],
                "message": notification_data["message"],
                "severity": notification_data.get("severity", "medium"),
                "connection_id": notification_data.get("connection_id"),
                "action_required": notification_data.get("action_required", False),
                "data": notification_data.get("details", {}),
                "read": False,
                "created_at": datetime.now().isoformat()
            }

            # Insert into notifications table
            self.supabase.supabase.table("notifications").insert(notification_record).execute()
            logger.info(f"Stored automation notification for organization {organization_id}")

        except Exception as e:
            logger.error(f"Error storing notification: {str(e)}")


# Global event handler instance
event_handler = AutomationEventHandler()


def set_event_handler_supabase(supabase_manager):
    """Set the Supabase manager for the global event handler"""
    global event_handler
    event_handler.supabase = supabase_manager