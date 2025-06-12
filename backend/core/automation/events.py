import logging
from enum import Enum
from typing import Dict, Any, Optional, List  # Added List import
from datetime import datetime, timezone
import traceback

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
    Publish an automation-related event - FIXED VERSION that stores in database

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
        # If organization_id is not provided but connection_id is, try to get it
        if not organization_id and connection_id:
            try:
                from core.storage.supabase_manager import SupabaseManager
                supabase = SupabaseManager()
                connection = supabase.get_connection(connection_id)
                if connection and connection.get("organization_id"):
                    organization_id = connection["organization_id"]
                    logger.debug(f"Retrieved organization_id {organization_id} from connection {connection_id}")
            except Exception as e:
                logger.warning(f"Could not get organization_id from connection {connection_id}: {str(e)}")

        # Create event payload
        payload = {
            "type": event_type.value,
            "data": data,
            "connection_id": connection_id,
            "organization_id": organization_id,
            "user_id": user_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Log the event
        logger.info(f"Automation event: {event_type.value}")
        logger.debug(f"Automation event payload: {payload}")

        # Store event in database
        success = _store_automation_event(payload)

        if success:
            logger.info(f"Successfully stored automation event: {event_type.value}")
        else:
            logger.error(f"Failed to store automation event: {event_type.value}")

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
                # Add organization_id to payload for notifications
                notification_payload = payload.copy()
                if organization_id:
                    notification_payload["organization_id"] = organization_id
                elif connection_id:
                    # Try to get organization_id from connection
                    try:
                        from core.storage.supabase_manager import SupabaseManager
                        supabase = SupabaseManager()
                        connection = supabase.get_connection(connection_id)
                        if connection and connection.get("organization_id"):
                            notification_payload["organization_id"] = connection["organization_id"]
                    except Exception as e:
                        logger.warning(f"Could not get organization_id for notifications: {str(e)}")

                publish_event("automation", notification_payload)
                logger.info(f"Published notification for event: {event_type.value}")

        except ImportError:
            logger.warning("Notification system not available, event stored only")
        except Exception as e:
            logger.error(f"Error publishing to notification system: {str(e)}")

        return success

    except Exception as e:
        logger.error(f"Error publishing automation event: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def _store_automation_event(payload: Dict[str, Any]) -> bool:
    """Store automation event in the database"""
    try:
        from core.storage.supabase_manager import SupabaseManager

        supabase = SupabaseManager()

        # Extract automation_type from the event_type or data
        automation_type = None
        event_type = payload["type"]

        # Map event types to automation types
        if "metadata" in event_type.lower():
            automation_type = "metadata_refresh"
        elif "schema" in event_type.lower():
            automation_type = "schema_detection"
        elif "validation" in event_type.lower():
            automation_type = "validation_run"
        elif "job" in event_type.lower():
            # Try to get from the event data
            job_data = payload.get("data", {})
            automation_type = job_data.get("automation_type", "general")
        else:
            automation_type = "general"

        # Prepare event record for database matching your schema
        event_record = {
            "event_type": event_type,
            "automation_type": automation_type,  # This was missing!
            "connection_id": payload.get("connection_id"),
            "organization_id": payload.get("organization_id"),
            "user_id": payload.get("user_id"),
            "created_by": payload.get("user_id"),  # Use user_id as created_by
            "event_data": payload.get("data", {}),
            "created_at": payload["timestamp"]
        }

        # Remove None values except for nullable fields
        filtered_record = {}
        for k, v in event_record.items():
            # Keep nullable fields even if None
            if k in ["organization_id", "user_id", "created_by", "event_data", "created_at"]:
                filtered_record[k] = v
            # Only keep non-nullable fields if they have values
            elif v is not None:
                filtered_record[k] = v

        # Insert into automation_events table
        response = supabase.supabase.table("automation_events").insert(filtered_record).execute()

        if response.data:
            logger.debug(f"Stored automation event in database: {event_type}")
            return True
        else:
            logger.error(f"Failed to store automation event: no data returned")
            return False

    except Exception as e:
        logger.error(f"Error storing automation event in database: {str(e)}")
        logger.error(traceback.format_exc())
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
                "created_at": datetime.now(timezone.utc).isoformat()
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


def get_automation_events(connection_id: str = None, organization_id: str = None,
                          event_type: str = None, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get automation events from the database

    Args:
        connection_id: Optional connection ID filter
        organization_id: Optional organization ID filter
        event_type: Optional event type filter
        limit: Maximum number of events to return

    Returns:
        List of automation events
    """
    try:
        from core.storage.supabase_manager import SupabaseManager

        supabase = SupabaseManager()

        # Build query
        query = supabase.supabase.table("automation_events").select("*")

        if connection_id:
            query = query.eq("connection_id", connection_id)

        if organization_id:
            query = query.eq("organization_id", organization_id)

        if event_type:
            query = query.eq("event_type", event_type)

        # Order by created_at descending and limit
        response = query.order("created_at", desc=True).limit(limit).execute()

        return response.data if response.data else []

    except Exception as e:
        logger.error(f"Error getting automation events: {str(e)}")
        return []


def get_automation_event_stats(organization_id: str, days: int = 7) -> Dict[str, Any]:
    """
    Get automation event statistics for an organization

    Args:
        organization_id: Organization ID
        days: Number of days to look back

    Returns:
        Dictionary with event statistics
    """
    try:
        from core.storage.supabase_manager import SupabaseManager
        from datetime import timedelta

        supabase = SupabaseManager()

        # Calculate date threshold
        since_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        # Get events since the threshold
        response = supabase.supabase.table("automation_events") \
            .select("event_type, created_at") \
            .eq("organization_id", organization_id) \
            .gte("created_at", since_date) \
            .execute()

        events = response.data if response.data else []

        # Calculate statistics
        stats = {
            "total_events": len(events),
            "events_by_type": {},
            "recent_events": len([e for e in events if
                                  (datetime.now(timezone.utc) - datetime.fromisoformat(
                                      e["created_at"].replace('Z', '+00:00'))).days < 1]),
            "days_analyzed": days
        }

        # Count by event type
        for event in events:
            event_type = event["event_type"]
            if event_type not in stats["events_by_type"]:
                stats["events_by_type"][event_type] = 0
            stats["events_by_type"][event_type] += 1

        return stats

    except Exception as e:
        logger.error(f"Error getting automation event stats: {str(e)}")
        return {"error": str(e)}