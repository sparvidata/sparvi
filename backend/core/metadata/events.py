import logging
from typing import Dict, Any, Optional
from enum import Enum, auto

# Configure logging
logger = logging.getLogger(__name__)


class MetadataEventType(Enum):
    """Types of events that can trigger metadata updates"""
    VALIDATION_FAILURE = auto()
    VALIDATION_SUCCESS = auto()
    PROFILE_COMPLETION = auto()
    SCHEMA_CHANGE = auto()
    USER_REQUEST = auto()
    SYSTEM_REFRESH = auto()


class MetadataEvent:
    """Represents a metadata event that might trigger updates"""

    def __init__(
            self,
            event_type: MetadataEventType,
            connection_id: str,
            details: Optional[Dict[str, Any]] = None,
            organization_id: Optional[str] = None,
            user_id: Optional[str] = None
    ):
        """
        Initialize a metadata event

        Args:
            event_type: Type of event
            connection_id: Database connection ID
            details: Additional event details (table_name, reason, etc.)
            organization_id: Organization ID (if applicable)
            user_id: User ID (if initiated by a user)
        """
        self.event_type = event_type
        self.connection_id = connection_id
        self.details = details or {}
        self.organization_id = organization_id
        self.user_id = user_id

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for storage"""
        return {
            "event_type": self.event_type.name,
            "connection_id": self.connection_id,
            "details": self.details,
            "organization_id": self.organization_id,
            "user_id": self.user_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MetadataEvent':
        """Create event from dictionary"""
        return cls(
            event_type=MetadataEventType[data["event_type"]],
            connection_id=data["connection_id"],
            details=data.get("details", {}),
            organization_id=data.get("organization_id"),
            user_id=data.get("user_id")
        )


class MetadataEventPublisher:
    """Publishes metadata events to handlers"""

    _instance = None

    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initialize the event publisher"""
        self.handlers = []
        self.task_manager = None

    def register_handler(self, handler):
        """Register a handler for events"""
        if handler not in self.handlers:
            self.handlers.append(handler)

    def set_task_manager(self, task_manager):
        """Set the metadata task manager for direct processing"""
        self.task_manager = task_manager

    def publish_event(self, event: MetadataEvent) -> Optional[str]:
        """
        Publish an event to all handlers

        Args:
            event: The event to publish

        Returns:
            Optional task ID if a task was created
        """
        logger.info(f"Publishing metadata event: {event.event_type.name}")

        # First try to handle directly with task manager for efficiency
        if self.task_manager:
            try:
                # Handle with task manager
                task_id = self.task_manager.handle_metadata_event(
                    event.event_type,
                    event.connection_id,
                    event.details
                )

                if task_id:
                    logger.info(f"Event handled by task manager, created task: {task_id}")
                    return task_id
            except Exception as e:
                logger.error(f"Error handling event with task manager: {str(e)}")

        # Fall back to other handlers if task manager didn't handle it
        for handler in self.handlers:
            try:
                handler.handle_event(event)
            except Exception as e:
                logger.error(f"Error in event handler: {str(e)}")

        return None


# Create a global instance
event_publisher = MetadataEventPublisher.get_instance()


def publish_metadata_event(
        event_type: MetadataEventType,
        connection_id: str,
        details: Optional[Dict[str, Any]] = None,
        organization_id: Optional[str] = None,
        user_id: Optional[str] = None
) -> Optional[str]:
    """
    Helper function to publish a metadata event

    Args:
        event_type: Type of event
        connection_id: Database connection ID
        details: Additional event details
        organization_id: Organization ID
        user_id: User ID

    Returns:
        Optional task ID if a task was created
    """
    event = MetadataEvent(
        event_type=event_type,
        connection_id=connection_id,
        details=details,
        organization_id=organization_id,
        user_id=user_id
    )

    return event_publisher.publish_event(event)