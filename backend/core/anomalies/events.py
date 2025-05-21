# core/anomalies/events.py

import logging
from enum import Enum
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class AnomalyEventType(Enum):
    """Event types for anomaly detection system"""
    ANOMALY_DETECTED = "anomaly_detected"
    ANOMALY_RESOLVED = "anomaly_resolved"
    ANOMALY_ACKNOWLEDGED = "anomaly_acknowledged"
    CONFIG_CREATED = "anomaly_config_created"
    CONFIG_UPDATED = "anomaly_config_updated"


def publish_anomaly_event(event_type: AnomalyEventType,
                          data: Dict[str, Any],
                          organization_id: str,
                          user_id: Optional[str] = None) -> bool:
    """
    Publish an anomaly-related event

    Args:
        event_type: Type of event (from AnomalyEventType)
        data: Event data
        organization_id: Organization ID
        user_id: Optional user ID for user-triggered events

    Returns:
        Success status
    """
    try:
        # For MVP, we'll implement a simple version that logs events
        # and publishes to notification system if available

        # Create event payload
        payload = {
            "type": event_type.value,
            "data": data,
            "organization_id": organization_id,
            "user_id": user_id,
            "timestamp": get_current_timestamp()
        }

        # Log the event
        logger.info(f"Anomaly event: {event_type.value}")
        logger.debug(f"Anomaly event payload: {payload}")

        # Try to publish to the notification system if available
        try:
            from core.notifications.publisher import publish_event
            return publish_event("anomaly", payload)
        except ImportError:
            logger.warning("Notification system not available, event logged only")
            return True

    except Exception as e:
        logger.error(f"Error publishing anomaly event: {str(e)}")
        return False


def get_current_timestamp() -> str:
    """Get current timestamp in ISO format"""
    from datetime import datetime
    return datetime.now().isoformat()