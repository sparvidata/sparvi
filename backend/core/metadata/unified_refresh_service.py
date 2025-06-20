import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
from enum import Enum

logger = logging.getLogger(__name__)


class RefreshTrigger(Enum):
    """Types of refresh triggers"""
    MANUAL_USER = "manual_user"
    AUTOMATION_SCHEDULED = "automation_scheduled"
    EVENT_DRIVEN = "event_driven"
    SYSTEM_MAINTENANCE = "system_maintenance"


class UnifiedMetadataRefreshService:
    """
    Unified service that handles all metadata refresh requests
    Both manual and automated refreshes go through this service
    """

    def __init__(self, metadata_task_manager, supabase_manager):
        """
        Initialize the unified refresh service

        Args:
            metadata_task_manager: Instance of MetadataTaskManager
            supabase_manager: Instance of SupabaseManager
        """
        self.metadata_task_manager = metadata_task_manager
        self.supabase_manager = supabase_manager

    def refresh_metadata(self,
                         connection_id: str,
                         metadata_type: str = "schema",
                         table_name: str = None,
                         trigger: RefreshTrigger = RefreshTrigger.MANUAL_USER,
                         automation_job_id: str = None,
                         user_id: str = None,
                         organization_id: str = None,
                         priority: str = "medium") -> Dict[str, Any]:
        """
        Universal metadata refresh method used by both manual and automated systems

        Args:
            connection_id: Database connection ID
            metadata_type: Type of metadata to refresh (schema, statistics, full)
            table_name: Optional specific table name
            trigger: What triggered this refresh
            automation_job_id: ID of automation job if triggered by automation
            user_id: User ID if triggered by user
            organization_id: Organization ID
            priority: Task priority (high, medium, low)

        Returns:
            Dictionary with refresh results and task ID
        """
        try:
            # Create refresh tracking record
            refresh_id = str(uuid.uuid4())

            refresh_record = {
                "id": refresh_id,
                "connection_id": connection_id,
                "metadata_type": metadata_type,
                "table_name": table_name,
                "trigger_type": trigger.value,
                "automation_job_id": automation_job_id,
                "user_id": user_id,
                "organization_id": organization_id,
                "priority": priority,
                "status": "initiated",
                "created_at": datetime.now(timezone.utc).isoformat()
            }

            # Store refresh record
            self._store_refresh_record(refresh_record)

            # Determine task parameters based on metadata type and trigger
            task_params = self._determine_task_parameters(
                metadata_type, table_name, trigger, automation_job_id
            )

            # Submit appropriate task based on metadata type
            task_id = None
            message = ""

            if metadata_type == "schema":
                if table_name:
                    # Refresh specific table schema
                    task_id = self.metadata_task_manager.submit_table_metadata_task(
                        connection_id, table_name, priority
                    )
                    message = f"Scheduled schema refresh for table {table_name}"
                else:
                    # Refresh all tables schema
                    task_id = self.metadata_task_manager.submit_collection_task(
                        connection_id, task_params, priority
                    )
                    message = "Scheduled schema refresh for all tables"

            elif metadata_type == "statistics":
                if table_name:
                    # Refresh statistics for specific table
                    task_id = self.metadata_task_manager.submit_statistics_refresh_task(
                        connection_id, table_name, priority
                    )
                    message = f"Scheduled statistics refresh for table {table_name}"
                else:
                    # Refresh statistics for multiple tables
                    task_id = self.metadata_task_manager.submit_collection_task(
                        connection_id, task_params, priority
                    )
                    message = "Scheduled statistics refresh"

            elif metadata_type == "full":
                # Comprehensive refresh
                task_id = self.metadata_task_manager.submit_collection_task(
                    connection_id, task_params, priority
                )
                message = "Scheduled comprehensive metadata refresh"

            else:
                raise ValueError(f"Unknown metadata type: {metadata_type}")

            # Update refresh record with task ID
            if task_id:
                self._update_refresh_record(refresh_id, {
                    "task_id": task_id,
                    "status": "task_submitted",
                    "message": message
                })

                # Publish metadata event for tracking
                self._publish_refresh_event(
                    connection_id=connection_id,
                    metadata_type=metadata_type,
                    table_name=table_name,
                    trigger=trigger,
                    task_id=task_id,
                    user_id=user_id,
                    organization_id=organization_id
                )

                return {
                    "success": True,
                    "refresh_id": refresh_id,
                    "task_id": task_id,
                    "status": "scheduled",
                    "message": message,
                    "trigger": trigger.value,
                    "estimated_completion": self._estimate_completion_time(metadata_type, task_params)
                }
            else:
                # No task was submitted
                self._update_refresh_record(refresh_id, {
                    "status": "no_action",
                    "message": "No refresh action needed at this time"
                })

                return {
                    "success": True,
                    "refresh_id": refresh_id,
                    "status": "no_action",
                    "message": "No refresh action needed at this time",
                    "trigger": trigger.value
                }

        except Exception as e:
            logger.error(f"Error in unified metadata refresh: {str(e)}")

            # Update refresh record with error
            if 'refresh_id' in locals():
                self._update_refresh_record(refresh_id, {
                    "status": "failed",
                    "error_message": str(e)
                })

            return {
                "success": False,
                "error": str(e),
                "trigger": trigger.value if trigger else "unknown"
            }

    def refresh_for_automation(self,
                               connection_id: str,
                               automation_job_id: str,
                               metadata_types: List[str] = None,
                               organization_id: str = None,
                               wait_for_completion: bool = False,
                               timeout_minutes: int = 30) -> Dict[str, Any]:
        """
        Specialized method for automation-triggered refreshes

        Args:
            connection_id: Database connection ID
            automation_job_id: ID of the automation job
            metadata_types: List of metadata types to refresh
            organization_id: Organization ID
            wait_for_completion: Whether to wait for task completion
            timeout_minutes: Timeout for waiting

        Returns:
            Dictionary with refresh results
        """
        try:
            if not metadata_types:
                metadata_types = ["tables", "columns"]

            logger.info(f"Starting automation metadata refresh for connection {connection_id}, "
                        f"job {automation_job_id}, types: {metadata_types}")

            # Use the enhanced automation metadata refresh method
            task_id = self.metadata_task_manager.submit_automation_metadata_refresh(
                connection_id=connection_id,
                metadata_types=metadata_types,
                priority="medium",
                automation_job_id=automation_job_id
            )

            result = {
                "success": True,
                "task_id": task_id,
                "automation_job_id": automation_job_id,
                "metadata_types": metadata_types,
                "status": "task_submitted"
            }

            # Wait for completion if requested
            if wait_for_completion:
                logger.info(f"Waiting for automation task {task_id} to complete")

                completion_result = self.metadata_task_manager.wait_for_task_completion_sync(
                    task_id, timeout_minutes
                )

                result.update({
                    "completion_result": completion_result,
                    "completed": completion_result.get("completed", False),
                    "task_success": completion_result.get("success", False),
                    "elapsed_seconds": completion_result.get("elapsed_seconds", 0)
                })

                if completion_result.get("success"):
                    result["status"] = "completed_successfully"
                    logger.info(f"Automation metadata refresh completed successfully for job {automation_job_id}")
                else:
                    result["status"] = "completed_with_errors"
                    result["error"] = completion_result.get("error", "Unknown error")
                    logger.error(f"Automation metadata refresh failed for job {automation_job_id}: {result['error']}")

            return result

        except Exception as e:
            logger.error(f"Error in automation metadata refresh: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "automation_job_id": automation_job_id,
                "metadata_types": metadata_types
            }

    def get_refresh_status(self, refresh_id: str = None, task_id: str = None) -> Dict[str, Any]:
        """
        Get status of a metadata refresh operation

        Args:
            refresh_id: Refresh tracking ID
            task_id: Task ID

        Returns:
            Status information
        """
        try:
            status = {}

            # Get refresh record if ID provided
            if refresh_id:
                refresh_record = self._get_refresh_record(refresh_id)
                if refresh_record:
                    status["refresh_info"] = refresh_record
                    task_id = refresh_record.get("task_id")

            # Get task status if task ID available
            if task_id:
                task_status = self.metadata_task_manager.get_task_status(task_id)
                status["task_status"] = task_status

            return status

        except Exception as e:
            logger.error(f"Error getting refresh status: {str(e)}")
            return {"error": str(e)}

    def _determine_task_parameters(self, metadata_type: str, table_name: str,
                                   trigger: RefreshTrigger, automation_job_id: str = None) -> Dict[str, Any]:
        """Determine task parameters based on refresh context"""
        base_params = {
            "automation_trigger": trigger == RefreshTrigger.AUTOMATION_SCHEDULED,
            "automation_job_id": automation_job_id,
            "verify_storage": True,
            "max_retries": 3
        }

        if metadata_type == "schema":
            if trigger == RefreshTrigger.AUTOMATION_SCHEDULED:
                # Automation: lighter collection for performance
                return {**base_params, "depth": "low", "table_limit": 100}
            else:
                # Manual: more thorough collection
                return {**base_params, "depth": "medium", "table_limit": 75}

        elif metadata_type == "statistics":
            if trigger == RefreshTrigger.AUTOMATION_SCHEDULED:
                # Automation: focus on key tables
                return {**base_params, "depth": "medium", "table_limit": 20, "focus": "statistics"}
            else:
                # Manual: more comprehensive
                return {**base_params, "depth": "high", "table_limit": 50}

        elif metadata_type == "full":
            if trigger == RefreshTrigger.AUTOMATION_SCHEDULED:
                # Automation: balanced approach
                return {**base_params, "depth": "medium", "table_limit": 50}
            else:
                # Manual: comprehensive
                return {**base_params, "depth": "high", "table_limit": 75}

        # Default parameters
        return {**base_params, "depth": "medium", "table_limit": 50}

    def _estimate_completion_time(self, metadata_type: str, task_params: Dict[str, Any]) -> str:
        """Estimate task completion time"""
        try:
            depth = task_params.get("depth", "medium")
            table_limit = task_params.get("table_limit", 50)

            if metadata_type == "schema":
                if depth == "low":
                    base_time = 30  # seconds
                else:
                    base_time = 60
            elif metadata_type == "statistics":
                base_time = 120  # Statistics take longer
            else:
                base_time = 90

            # Adjust for table count
            estimated_seconds = base_time + (table_limit * 2)

            if estimated_seconds < 60:
                return f"{estimated_seconds} seconds"
            elif estimated_seconds < 3600:
                return f"{estimated_seconds // 60} minutes"
            else:
                return f"{estimated_seconds // 3600} hours"

        except Exception:
            return "2-5 minutes"

    def _store_refresh_record(self, refresh_record: Dict[str, Any]):
        """Store refresh tracking record"""
        try:
            self.supabase_manager.supabase.table("metadata_refresh_tracking").insert(refresh_record).execute()
        except Exception as e:
            logger.warning(f"Could not store refresh record: {str(e)}")

    def _update_refresh_record(self, refresh_id: str, updates: Dict[str, Any]):
        """Update refresh tracking record"""
        try:
            updates["updated_at"] = datetime.now(timezone.utc).isoformat()
            self.supabase_manager.supabase.table("metadata_refresh_tracking") \
                .update(updates) \
                .eq("id", refresh_id) \
                .execute()
        except Exception as e:
            logger.warning(f"Could not update refresh record: {str(e)}")

    def _get_refresh_record(self, refresh_id: str) -> Optional[Dict[str, Any]]:
        """Get refresh tracking record"""
        try:
            response = self.supabase_manager.supabase.table("metadata_refresh_tracking") \
                .select("*") \
                .eq("id", refresh_id) \
                .single() \
                .execute()
            return response.data
        except Exception as e:
            logger.warning(f"Could not get refresh record: {str(e)}")
            return None

    def _publish_refresh_event(self, connection_id: str, metadata_type: str, table_name: str,
                               trigger: RefreshTrigger, task_id: str, user_id: str = None,
                               organization_id: str = None):
        """Publish metadata refresh event"""
        try:
            from core.metadata.events import publish_metadata_event, MetadataEventType

            if trigger == RefreshTrigger.MANUAL_USER:
                event_type = MetadataEventType.USER_REQUEST
            else:
                event_type = MetadataEventType.SYSTEM_REFRESH

            publish_metadata_event(
                event_type=event_type,
                connection_id=connection_id,
                details={
                    "metadata_type": metadata_type,
                    "table_name": table_name,
                    "trigger": trigger.value,
                    "task_id": task_id
                },
                organization_id=organization_id,
                user_id=user_id
            )

        except Exception as e:
            logger.warning(f"Could not publish refresh event: {str(e)}")

    def get_refresh_history(self, connection_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get refresh history for a connection"""
        try:
            response = self.supabase_manager.supabase.table("metadata_refresh_tracking") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .order("created_at", desc=True) \
                .limit(limit) \
                .execute()

            return response.data or []

        except Exception as e:
            logger.error(f"Error getting refresh history: {str(e)}")
            return []


# Factory function to create unified refresh service
def create_unified_refresh_service():
    """Create and return a unified metadata refresh service instance"""
    try:
        from core.metadata.manager import MetadataTaskManager
        from core.storage.supabase_manager import SupabaseManager

        metadata_task_manager = MetadataTaskManager.get_instance()
        supabase_manager = SupabaseManager()

        return UnifiedMetadataRefreshService(metadata_task_manager, supabase_manager)

    except Exception as e:
        logger.error(f"Error creating unified refresh service: {str(e)}")
        raise


# Global service instance for reuse
_unified_refresh_service = None


def get_unified_refresh_service():
    """Get or create the global unified refresh service instance"""
    global _unified_refresh_service

    if _unified_refresh_service is None:
        _unified_refresh_service = create_unified_refresh_service()

    return _unified_refresh_service