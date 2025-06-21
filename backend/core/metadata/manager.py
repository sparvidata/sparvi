import logging
import os
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

# Configure logging
logger = logging.getLogger(__name__)


class MetadataTaskManager:
    """Enhanced metadata task manager with automation integration"""

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, storage_service=None, supabase_manager=None):
        """Get the singleton instance"""
        with cls._lock:
            if cls._instance is None:
                if storage_service is None:
                    # Try different import strategies to find the storage service
                    try:
                        from .storage_service import MetadataStorageService
                        storage_service = MetadataStorageService()
                        logger.info("Imported MetadataStorageService from local module")
                    except ImportError:
                        try:
                            from core.metadata.storage_service import MetadataStorageService
                            storage_service = MetadataStorageService()
                            logger.info("Imported MetadataStorageService from core.metadata")
                        except ImportError:
                            current_dir = os.path.dirname(os.path.abspath(__file__))
                            parent_dir = os.path.dirname(current_dir)
                            grand_parent_dir = os.path.dirname(parent_dir)

                            if grand_parent_dir not in sys.path:
                                sys.path.append(grand_parent_dir)

                            try:
                                from core.metadata.storage_service import MetadataStorageService
                                storage_service = MetadataStorageService()
                                logger.info("Imported MetadataStorageService using modified path")
                            except ImportError as e:
                                logger.error(f"Failed to import MetadataStorageService: {e}")
                                raise ImportError(
                                    "Could not import MetadataStorageService. Please provide a storage_service instance.")

                cls._instance = cls(storage_service, supabase_manager)

                # Connect to the event system
                try:
                    from .events import event_publisher
                    event_publisher.set_task_manager(cls._instance)
                    logger.info("Connected metadata task manager to event system")
                except ImportError:
                    logger.warning("Could not import event_publisher, event-driven updates may not work")
                except Exception as e:
                    logger.error(f"Error connecting to event system: {str(e)}")

            return cls._instance

    def __init__(self, storage_service, supabase_manager=None):
        """Initialize the task manager"""
        from .worker import PriorityTaskQueue, MetadataWorker
        from .connector_factory import ConnectorFactory

        self.storage_service = storage_service
        self.supabase_manager = supabase_manager

        # Create connector factory
        self.connector_factory = ConnectorFactory(supabase_manager)

        # Create task queue
        self.task_queue = PriorityTaskQueue()

        # Create worker
        self.worker = MetadataWorker(
            self.task_queue,
            self.storage_service,
            self.connector_factory,
            max_workers=3
        )

        # Override worker methods that need external dependencies
        self.worker._get_connection_details = self._get_connection_details

        # Tasks by status (for lookup)
        self.pending_tasks = {}
        self.recent_tasks = {}
        self.max_recent_tasks = 100

        # Start worker
        self.worker.start()

        # Start scheduled refresh thread
        self.scheduled_refresh_thread = threading.Thread(
            target=self._scheduled_refresh_loop,
            name="ScheduledRefresh",
            daemon=True
        )
        self.scheduled_refresh_thread.start()

        logger.info("MetadataTaskManager initialized with automation integration")

    def _get_connection_details(self, connection_id):
        """Get connection details from Supabase"""
        if not self.supabase_manager:
            raise ValueError("supabase_manager is required to get connection details")

        connection = self.supabase_manager.get_connection(connection_id)
        if not connection:
            raise ValueError(f"Connection not found: {connection_id}")

        return connection

    # Enhanced collection methods for automation integration
    def submit_automation_metadata_refresh(self, connection_id: str, metadata_types: List[str] = None,
                                           priority: str = "medium", automation_job_id: str = None) -> str:
        """
        Submit metadata refresh task for automation system with enhanced tracking

        Args:
            connection_id: Connection ID
            metadata_types: List of metadata types to refresh (tables, columns, statistics)
            priority: Task priority
            automation_job_id: ID of the automation job that triggered this

        Returns:
            Task ID
        """
        try:
            if not metadata_types:
                metadata_types = ["tables", "columns"]

            # Map metadata types to collection depth
            if "statistics" in metadata_types:
                depth = "high"
                table_limit = 25  # Reduced for automation performance
            elif "columns" in metadata_types:
                depth = "medium"
                table_limit = 50
            else:
                depth = "low"
                table_limit = 100

            # Enhanced parameters for automation
            params = {
                "depth": depth,
                "table_limit": table_limit,
                "automation_trigger": True,
                "automation_job_id": automation_job_id,
                "metadata_types": metadata_types,
                "verify_storage": True,  # Always verify for automation
                "max_retries": 3  # Retry failed operations
            }

            task_id = self.submit_collection_task(connection_id, params, priority)

            logger.info(f"Submitted automation metadata refresh task {task_id} for connection {connection_id}")
            return task_id

        except Exception as e:
            logger.error(f"Error submitting automation metadata refresh: {str(e)}")
            raise

    def get_metadata_collection_status_for_automation(self, connection_id: str) -> Dict[str, Any]:
        """
        Get comprehensive metadata collection status for automation system

        Args:
            connection_id: Connection ID

        Returns:
            Enhanced status dictionary for automation
        """
        try:
            # Get base status
            base_status = self.get_metadata_collection_status(connection_id)

            # Add automation-specific information
            automation_status = {
                **base_status,
                "automation_ready": False,
                "last_automation_run": None,
                "automation_job_history": []
            }

            # Check for recent automation jobs
            try:
                if self.supabase_manager:
                    recent_jobs = self.supabase_manager.supabase.table("automation_jobs") \
                        .select("id, job_type, status, completed_at, result_summary") \
                        .eq("connection_id", connection_id) \
                        .eq("job_type", "metadata_refresh") \
                        .order("scheduled_at", desc=True) \
                        .limit(5) \
                        .execute()

                    if recent_jobs.data:
                        automation_status["automation_job_history"] = recent_jobs.data

                        # Find last successful automation run
                        for job in recent_jobs.data:
                            if job.get("status") == "completed":
                                automation_status["last_automation_run"] = job.get("completed_at")
                                break

                        # Check if metadata is ready for automation (has recent successful collection)
                        recent_successful = any(
                            job.get("status") == "completed"
                            for job in recent_jobs.data[:2]  # Check last 2 jobs
                        )

                        automation_status["automation_ready"] = (
                                automation_status["overall_status"] in ["fresh", "recent"] and
                                recent_successful
                        )

            except Exception as job_error:
                logger.warning(f"Error getting automation job history: {str(job_error)}")

            return automation_status

        except Exception as e:
            logger.error(f"Error getting automation metadata status: {str(e)}")
            return {
                "connection_id": connection_id,
                "overall_status": "error",
                "automation_ready": False,
                "error": str(e)
            }

    def wait_for_task_completion_sync(self, task_id: str, timeout_minutes: int = 30) -> Dict[str, Any]:
        """
        Synchronously wait for a task to complete (for automation use)

        Args:
            task_id: Task ID to wait for
            timeout_minutes: Maximum time to wait

        Returns:
            Task completion status and results
        """
        try:
            timeout_seconds = timeout_minutes * 60
            poll_interval = 2  # Check every 2 seconds
            elapsed = 0

            logger.info(f"Waiting for task {task_id} to complete (timeout: {timeout_minutes}m)")

            while elapsed < timeout_seconds:
                task_status = self.get_task_status(task_id)

                if "error" in task_status:
                    # Check if error is "Task not found" - might still be processing
                    if "Task not found" in str(task_status.get("error", "")):
                        logger.debug(f"Task {task_id} not yet in history, continuing to wait...")
                    else:
                        return {
                            "completed": False,
                            "success": False,
                            "error": task_status.get("error"),
                            "elapsed_seconds": elapsed
                        }

                else:
                    task_info = task_status.get("task", {})
                    status = task_info.get("status", "unknown")

                    if status == "completed":
                        return {
                            "completed": True,
                            "success": True,
                            "result": task_status.get("result"),
                            "elapsed_seconds": elapsed,
                            "task_info": task_info
                        }
                    elif status == "failed":
                        return {
                            "completed": True,
                            "success": False,
                            "error": task_info.get("error", "Task failed"),
                            "elapsed_seconds": elapsed,
                            "task_info": task_info
                        }

                # Task still running or not found yet, wait
                time.sleep(poll_interval)
                elapsed += poll_interval

                # Log progress every 30 seconds
                if elapsed % 30 == 0:
                    logger.info(
                        f"Task {task_id} still waiting after {elapsed}s (status: {status if 'status' in locals() else 'unknown'})")

            # Timeout reached
            logger.warning(f"Task {task_id} did not complete within {timeout_minutes} minutes")
            return {
                "completed": False,
                "success": False,
                "error": f"Task timeout after {timeout_minutes} minutes",
                "elapsed_seconds": elapsed,
                "final_status": status if 'status' in locals() else "unknown"
            }

        except Exception as e:
            logger.error(f"Error waiting for task completion: {str(e)}")
            return {
                "completed": False,
                "success": False,
                "error": str(e),
                "elapsed_seconds": elapsed if 'elapsed' in locals() else 0
            }

    # Keep all existing methods and add automation enhancements
    def submit_collection_task(self, connection_id, params=None, priority="medium"):
        """Submit a comprehensive metadata collection task"""
        return self.worker.submit_task("full_collection", connection_id, params or {}, priority)

    def submit_table_metadata_task(self, connection_id, table_name, priority="medium"):
        """Submit a task to collect metadata for a specific table"""
        params = {"table_name": table_name}
        return self.worker.submit_task("table_metadata", connection_id, params, priority)

    def submit_statistics_refresh_task(self, connection_id, table_name, priority="low"):
        """Submit a task to refresh statistics for a specific table"""
        params = {"table_name": table_name}
        return self.worker.submit_task("refresh_statistics", connection_id, params, priority)

    def submit_usage_update_task(self, connection_id, table_name, priority="low"):
        """Submit a task to update usage patterns for a specific table"""
        params = {"table_name": table_name}
        return self.worker.submit_task("update_usage", connection_id, params, priority)

    def get_task_status(self, task_id):
        """Get status of a task"""
        return self.worker.get_task_status(task_id)

    def get_worker_stats(self):
        """Get worker statistics"""
        return self.worker.get_stats()

    def get_recent_tasks(self, limit=10):
        """Get recent tasks"""
        return self.worker.get_task_history(limit)

    def handle_metadata_event(self, event_type, connection_id, details=None):
        """Process events that might trigger metadata updates"""
        logger.info(f"Processing metadata event: {event_type} for connection {connection_id}")

        if not hasattr(self, 'worker') or not self.worker:
            logger.error("Task manager not fully initialized, cannot process event")
            return None

        # Import here to avoid circular imports
        try:
            from .events import MetadataEventType
            if isinstance(event_type, str):
                try:
                    event_type = getattr(MetadataEventType, event_type)
                except (AttributeError, TypeError):
                    pass
        except ImportError:
            pass

        # Process validation failure events
        if (hasattr(event_type,
                    'name') and event_type.name == "VALIDATION_FAILURE") or event_type == "VALIDATION_FAILURE":
            reason = details.get("reason") if details else None

            if reason == "schema_mismatch":
                table_name = details.get("table_name") if details else None
                if table_name:
                    return self.submit_table_metadata_task(connection_id, table_name, "high")
                else:
                    return self.submit_collection_task(
                        connection_id,
                        {"depth": "low", "table_limit": 100},
                        "high"
                    )

            table_name = details.get("table_name") if details else None
            if table_name:
                return self.submit_statistics_refresh_task(connection_id, table_name, "medium")

        # Profile completion events
        elif (hasattr(event_type,
                      'name') and event_type.name == "PROFILE_COMPLETION") or event_type == "PROFILE_COMPLETION":
            table_name = details.get("table_name") if details else None

            if table_name:
                if not self.check_metadata_freshness(connection_id, "tables", 24):
                    return self.submit_table_metadata_task(connection_id, table_name, "high")
                return self.submit_statistics_refresh_task(connection_id, table_name, "medium")

        # Schema change events
        elif (hasattr(event_type, 'name') and event_type.name == "SCHEMA_CHANGE") or event_type == "SCHEMA_CHANGE":
            table_name = details.get("table_name") if details else None

            if table_name:
                return self.submit_table_metadata_task(connection_id, table_name, "high")
            else:
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "low", "table_limit": 100},
                    "high"
                )

        # User requests
        elif (hasattr(event_type, 'name') and event_type.name == "USER_REQUEST") or event_type == "USER_REQUEST":
            metadata_type = details.get("metadata_type") if details else None

            if not metadata_type:
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "medium", "table_limit": 50},
                    "high"
                )

            if metadata_type == "schema":
                if details and details.get("table_name"):
                    return self.submit_table_metadata_task(
                        connection_id,
                        details.get("table_name"),
                        "high"
                    )
                else:
                    return self.submit_collection_task(
                        connection_id,
                        {"depth": "low", "table_limit": 100},
                        "high"
                    )

            elif metadata_type == "statistics":
                if details and details.get("table_name"):
                    return self.submit_statistics_refresh_task(
                        connection_id,
                        details.get("table_name"),
                        "high"
                    )
                else:
                    return self.submit_collection_task(
                        connection_id,
                        {"depth": "high", "table_limit": 50},
                        "high"
                    )

            elif metadata_type == "usage":
                if details and details.get("table_name"):
                    return self.submit_usage_update_task(
                        connection_id,
                        details.get("table_name"),
                        "high"
                    )

        # System-initiated refresh (e.g., scheduled)
        elif (hasattr(event_type, 'name') and event_type.name == "SYSTEM_REFRESH") or event_type == "SYSTEM_REFRESH":
            metadata_type = details.get("metadata_type") if details else "all"
            depth = details.get("depth", "medium") if details else "medium"
            priority = details.get("priority", "low") if details else "low"

            if metadata_type == "all":
                return self.submit_collection_task(
                    connection_id,
                    {"depth": depth, "table_limit": 100},
                    priority
                )
            elif metadata_type == "schema":
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "low", "table_limit": 100},
                    priority
                )
            elif metadata_type == "statistics":
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "high", "table_limit": 50},
                    priority
                )

        return None

    def check_metadata_freshness(self, connection_id, metadata_type="tables", max_age_hours=24):
        """Check if metadata is fresh"""
        metadata = self.storage_service.get_metadata(connection_id, metadata_type)

        if not metadata:
            return False

        freshness = metadata.get("freshness", {})
        age_hours = freshness.get("age_hours", float('inf'))

        return age_hours < max_age_hours

    def schedule_refresh_if_needed(self, connection_id, metadata_type="tables", max_age_hours=24):
        """Schedule refresh if metadata is not fresh"""
        if not self.check_metadata_freshness(connection_id, metadata_type, max_age_hours):
            if metadata_type == "tables":
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "low", "table_limit": 100},
                    "medium"
                )
            elif metadata_type == "statistics":
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "high", "table_limit": 50},
                    "low"
                )

        return None

    def get_metadata_collection_status(self, connection_id: str) -> Dict[str, Any]:
        """Get status of metadata collection for automation system"""
        try:
            # Check freshness of different metadata types
            tables_metadata = self.storage_service.get_metadata(connection_id, "tables")
            columns_metadata = self.storage_service.get_metadata(connection_id, "columns")
            statistics_metadata = self.storage_service.get_metadata(connection_id, "statistics")

            status = {
                "connection_id": connection_id,
                "tables": self._get_metadata_status(tables_metadata),
                "columns": self._get_metadata_status(columns_metadata),
                "statistics": self._get_metadata_status(statistics_metadata),
                "overall_status": "unknown"
            }

            # Determine overall status
            statuses = [status["tables"]["status"], status["columns"]["status"], status["statistics"]["status"]]
            if "fresh" in statuses:
                status["overall_status"] = "fresh"
            elif "recent" in statuses:
                status["overall_status"] = "recent"
            elif "stale" in statuses:
                status["overall_status"] = "stale"
            else:
                status["overall_status"] = "unknown"

            return status

        except Exception as e:
            logger.error(f"Error getting metadata collection status: {str(e)}")
            return {
                "connection_id": connection_id,
                "overall_status": "error",
                "error": str(e)
            }

    def _get_metadata_status(self, metadata: Optional[Dict]) -> Dict[str, Any]:
        """Get status info for a metadata object"""
        if not metadata:
            return {"status": "missing", "age_hours": None, "last_collected": None}

        freshness = metadata.get("freshness", {})
        return {
            "status": freshness.get("status", "unknown"),
            "age_hours": freshness.get("age_hours"),
            "last_collected": metadata.get("collected_at")
        }

    def _scheduled_refresh_loop(self):
        """Background thread for scheduled metadata refreshes"""
        logger.info("Started scheduled refresh thread")

        # Add simple circuit breaker
        consecutive_errors = 0
        max_errors = 3

        # Create schema change detector if available
        schema_detector = None
        try:
            from .schema_change_detector import SchemaChangeDetector
            schema_detector = SchemaChangeDetector(self.storage_service)
            logger.info("Successfully created schema change detector")
        except ImportError:
            logger.warning("Could not import SchemaChangeDetector, schema change detection disabled")
        except Exception as e:
            logger.error(f"Error creating schema change detector: {str(e)}")

        last_schema_check = {}
        schema_check_interval = 86400  # 24 hours in seconds

        while True:
            try:
                # Circuit breaker: if too many errors, slow down
                if consecutive_errors >= max_errors:
                    logger.warning(f"Too many consecutive errors ({consecutive_errors}), sleeping for 10 minutes")
                    time.sleep(600)  # 10 minutes
                    consecutive_errors = 0
                    continue

                # Get all connections
                if self.supabase_manager:
                    connections = self._get_all_connections()
                    current_time = time.time()

                    # Check each connection for stale metadata
                    for connection in connections:
                        connection_id = connection.get("id")
                        try:
                            # Check tables metadata (refresh if older than 1 day)
                            self.schedule_refresh_if_needed(connection_id, "tables", 24)

                            # Check statistics metadata (refresh if older than 3 days)
                            self.schedule_refresh_if_needed(connection_id, "statistics", 72)

                            # Run schema change detection if it's time and detector is available
                            if (schema_detector and
                                    (connection_id not in last_schema_check or
                                     current_time - last_schema_check[connection_id] > schema_check_interval)):

                                logger.info(f"Running schema change detection for connection {connection_id}")
                                try:
                                    # Detect schema changes
                                    changes, important_changes = schema_detector.detect_changes_for_connection(
                                        connection_id,
                                        self.connector_factory,
                                        self.supabase_manager
                                    )

                                    # Update last check time
                                    last_schema_check[connection_id] = current_time

                                    # If important changes were detected, publish events
                                    if important_changes and changes:
                                        logger.info(f"Important schema changes detected for connection {connection_id}")

                                        # Get organization ID if possible
                                        organization_id = connection.get("organization_id")

                                        # Log detected changes for visibility
                                        change_types = {}
                                        for change in changes:
                                            change_type = change.get("type")
                                            if change_type not in change_types:
                                                change_types[change_type] = 0
                                            change_types[change_type] += 1

                                        logger.info(f"Change types detected: {change_types}")

                                except Exception as e:
                                    logger.error(f"Error in schema change detection for {connection_id}: {str(e)}")

                        except Exception as e:
                            logger.error(f"Error checking connection {connection_id}: {str(e)}")

                # Reset error counter on success
                consecutive_errors = 0

                # Sleep for a while before next check (1 hour)
                time.sleep(3600)

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error in scheduled refresh loop: {str(e)}")
                # Sleep progressively longer with more errors
                sleep_time = min(300, 60 * consecutive_errors)  # Max 5 minutes
                time.sleep(sleep_time)

    def _get_all_connections(self):
        """Get all database connections from Supabase"""
        try:
            if not self.supabase_manager:
                return []

            connections = []
            organizations = self._get_all_organizations()

            for org in organizations:
                org_id = org.get("id")

                org_connections = self.supabase_manager.supabase.table("database_connections") \
                    .select("id, name, connection_type, organization_id") \
                    .eq("organization_id", org_id) \
                    .execute()

                if org_connections.data:
                    connections.extend(org_connections.data)

            return connections

        except Exception as e:
            logger.error(f"Error getting connections: {str(e)}")
            return []

    def _get_all_organizations(self):
        """Get all organizations from Supabase"""
        try:
            if not self.supabase_manager:
                return []

            orgs = self.supabase_manager.supabase.table("organizations") \
                .select("id, name") \
                .execute()

            return orgs.data if orgs.data else []

        except Exception as e:
            logger.error(f"Error getting organizations: {str(e)}")
            return []