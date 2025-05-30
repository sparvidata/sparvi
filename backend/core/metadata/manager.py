import logging
import os
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Configure logging
logger = logging.getLogger(__name__)


class MetadataTaskManager:
    """Manages metadata tasks and the worker pool"""

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
                        # Try direct import first
                        from .storage_service import MetadataStorageService
                        storage_service = MetadataStorageService()
                        logger.info("Imported MetadataStorageService from local module")
                    except ImportError:
                        try:
                            # Try absolute import with 'core'
                            from core.metadata.storage_service import MetadataStorageService
                            storage_service = MetadataStorageService()
                            logger.info("Imported MetadataStorageService from core.metadata")
                        except ImportError:
                            # Try with parent directory import
                            current_dir = os.path.dirname(os.path.abspath(__file__))
                            parent_dir = os.path.dirname(current_dir)
                            grand_parent_dir = os.path.dirname(parent_dir)

                            if grand_parent_dir not in sys.path:
                                sys.path.append(grand_parent_dir)

                            try:
                                # Try to import from the parent directory structure
                                from core.metadata.storage_service import MetadataStorageService
                                storage_service = MetadataStorageService()
                                logger.info("Imported MetadataStorageService using modified path")
                            except ImportError as e:
                                logger.error(f"Failed to import MetadataStorageService: {e}")
                                logger.error("Please provide a storage_service instance")
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
        """
        Initialize the task manager

        Args:
            storage_service: Storage service for metadata
            supabase_manager: Supabase manager for database operations
        """
        # Import locally to avoid circular imports
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

        logger.info("MetadataTaskManager initialized")

    def _get_connection_details(self, connection_id):
        """Get connection details from Supabase"""
        if not self.supabase_manager:
            raise ValueError("supabase_manager is required to get connection details")

        # Get connection details
        connection = self.supabase_manager.get_connection(connection_id)
        if not connection:
            raise ValueError(f"Connection not found: {connection_id}")

        return connection

    def submit_collection_task(self, connection_id, params=None, priority="medium"):
        """
        Submit a comprehensive metadata collection task

        Args:
            connection_id: Connection ID
            params: Additional parameters (table_limit, depth, etc.)
            priority: Task priority

        Returns:
            Task ID
        """
        return self.worker.submit_task("full_collection", connection_id, params or {}, priority)

    def submit_table_metadata_task(self, connection_id, table_name, priority="medium"):
        """
        Submit a task to collect metadata for a specific table

        Args:
            connection_id: Connection ID
            table_name: Table name
            priority: Task priority

        Returns:
            Task ID
        """
        params = {"table_name": table_name}
        return self.worker.submit_task("table_metadata", connection_id, params, priority)

    def submit_statistics_refresh_task(self, connection_id, table_name, priority="low"):
        """
        Submit a task to refresh statistics for a specific table

        Args:
            connection_id: Connection ID
            table_name: Table name
            priority: Task priority

        Returns:
            Task ID
        """
        params = {"table_name": table_name}
        return self.worker.submit_task("refresh_statistics", connection_id, params, priority)

    def submit_usage_update_task(self, connection_id, table_name, priority="low"):
        """
        Submit a task to update usage patterns for a specific table

        Args:
            connection_id: Connection ID
            table_name: Table name
            priority: Task priority

        Returns:
            Task ID
        """
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
        """
        Process events that might trigger metadata updates

        Args:
            event_type: Type of event (string or enum)
            connection_id: Connection ID
            details: Additional event details

        Returns:
            Task ID if a task was submitted, None otherwise
        """
        logger.info(f"Processing metadata event: {event_type} for connection {connection_id}")

        if not hasattr(self, 'worker') or not self.worker:
            logger.error("Task manager not fully initialized, cannot process event")
            return None

        # Import here to avoid circular imports
        try:
            from .events import MetadataEventType
            # Convert string event type to enum if needed
            if isinstance(event_type, str):
                try:
                    event_type = getattr(MetadataEventType, event_type)
                except (AttributeError, TypeError):
                    # Keep it as a string if not found in enum
                    pass
        except ImportError:
            # Continue with string event types if events module not available
            pass

        # Process validation failure events
        if (hasattr(event_type,
                    'name') and event_type.name == "VALIDATION_FAILURE") or event_type == "VALIDATION_FAILURE":
            # Check for schema mismatch reason
            reason = details.get("reason") if details else None

            if reason == "schema_mismatch":
                # Schema mismatch detected, refresh schema metadata
                table_name = details.get("table_name") if details else None
                if table_name:
                    # Refresh specific table
                    return self.submit_table_metadata_task(connection_id, table_name, "high")
                else:
                    # Refresh all tables
                    return self.submit_collection_task(
                        connection_id,
                        {"depth": "low", "table_limit": 100},
                        "high"
                    )

            # General validation failure - could be a data issue
            # Refresh statistics for the affected table
            table_name = details.get("table_name") if details else None
            if table_name:
                return self.submit_statistics_refresh_task(connection_id, table_name, "medium")

        # Profile completion events
        elif (hasattr(event_type,
                      'name') and event_type.name == "PROFILE_COMPLETION") or event_type == "PROFILE_COMPLETION":
            # Profile completed, update statistics
            table_name = details.get("table_name") if details else None

            if table_name:
                # First check if schema metadata exists
                if not self.check_metadata_freshness(connection_id, "tables", 24):
                    # Collect table metadata first
                    return self.submit_table_metadata_task(connection_id, table_name, "high")

                # Then refresh statistics
                return self.submit_statistics_refresh_task(connection_id, table_name, "medium")

        # Schema change events
        elif (hasattr(event_type, 'name') and event_type.name == "SCHEMA_CHANGE") or event_type == "SCHEMA_CHANGE":
            # Schema change detected, refresh schema metadata
            table_name = details.get("table_name") if details else None

            if table_name:
                # Refresh specific table
                return self.submit_table_metadata_task(connection_id, table_name, "high")
            else:
                # Refresh all tables
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "low", "table_limit": 100},
                    "high"
                )

        # User requests
        elif (hasattr(event_type, 'name') and event_type.name == "USER_REQUEST") or event_type == "USER_REQUEST":
            # User manually requested refresh
            metadata_type = details.get("metadata_type") if details else None

            # If no specific type requested, collect everything
            if not metadata_type:
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "medium", "table_limit": 50},
                    "high"
                )

            if metadata_type == "schema":
                # Refresh schema
                if details and details.get("table_name"):
                    # Refresh specific table
                    return self.submit_table_metadata_task(
                        connection_id,
                        details.get("table_name"),
                        "high"
                    )
                else:
                    # Refresh all tables
                    return self.submit_collection_task(
                        connection_id,
                        {"depth": "low", "table_limit": 100},
                        "high"
                    )

            elif metadata_type == "statistics":
                # Refresh statistics
                if details and details.get("table_name"):
                    # Refresh specific table
                    return self.submit_statistics_refresh_task(
                        connection_id,
                        details.get("table_name"),
                        "high"
                    )
                else:
                    # Refresh all tables
                    return self.submit_collection_task(
                        connection_id,
                        {"depth": "high", "table_limit": 50},
                        "high"
                    )

            elif metadata_type == "usage":
                # Refresh usage patterns
                if details and details.get("table_name"):
                    # Refresh specific table
                    return self.submit_usage_update_task(
                        connection_id,
                        details.get("table_name"),
                        "high"
                    )

        # System-initiated refresh (e.g., scheduled)
        elif (hasattr(event_type, 'name') and event_type.name == "SYSTEM_REFRESH") or event_type == "SYSTEM_REFRESH":
            # System scheduled refresh
            metadata_type = details.get("metadata_type") if details else "all"
            depth = details.get("depth", "medium") if details else "medium"
            priority = details.get("priority", "low") if details else "low"

            if metadata_type == "all":
                # Refresh everything
                return self.submit_collection_task(
                    connection_id,
                    {"depth": depth, "table_limit": 100},
                    priority
                )
            elif metadata_type == "schema":
                # Refresh schema only
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "low", "table_limit": 100},
                    priority
                )
            elif metadata_type == "statistics":
                # Refresh statistics only
                return self.submit_collection_task(
                    connection_id,
                    {"depth": "high", "table_limit": 50},
                    priority
                )

        # No action taken
        return None

    def check_metadata_freshness(self, connection_id, metadata_type="tables", max_age_hours=24):
        """
        Check if metadata is fresh

        Args:
            connection_id: Connection ID
            metadata_type: Type of metadata to check
            max_age_hours: Maximum age in hours for fresh metadata

        Returns:
            True if metadata is fresh, False otherwise
        """
        # Get metadata
        metadata = self.storage_service.get_metadata(connection_id, metadata_type)

        if not metadata:
            return False

        # Check freshness
        freshness = metadata.get("freshness", {})
        age_hours = freshness.get("age_hours", float('inf'))

        return age_hours < max_age_hours

    def schedule_refresh_if_needed(self, connection_id, metadata_type="tables", max_age_hours=24):
        """
        Schedule refresh if metadata is not fresh

        Args:
            connection_id: Connection ID
            metadata_type: Type of metadata to check
            max_age_hours: Maximum age in hours for fresh metadata

        Returns:
            Task ID if a task was submitted, None otherwise
        """
        # Check freshness
        if not self.check_metadata_freshness(connection_id, metadata_type, max_age_hours):
            # Schedule refresh
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

    def submit_automation_metadata_refresh(self, connection_id: str, metadata_types: List[str] = None,
                                           priority: str = "medium"):
        """
        Submit metadata refresh task for automation system

        Args:
            connection_id: Connection ID
            metadata_types: List of metadata types to refresh (tables, columns, statistics)
            priority: Task priority

        Returns:
            Task ID
        """
        if not metadata_types:
            metadata_types = ["tables", "columns"]

        # Map metadata types to collection depth
        if "statistics" in metadata_types:
            depth = "high"
            table_limit = 50
        elif "columns" in metadata_types:
            depth = "medium"
            table_limit = 75
        else:
            depth = "low"
            table_limit = 100

        return self.submit_collection_task(
            connection_id,
            {"depth": depth, "table_limit": table_limit, "automation_trigger": True},
            priority
        )

    def get_metadata_collection_status(self, connection_id: str) -> Dict[str, Any]:
        """
        Get status of metadata collection for automation system

        Args:
            connection_id: Connection ID

        Returns:
            Status dictionary
        """
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

        # Track when we last ran schema change detection for each connection
        last_schema_check = {}
        schema_check_interval = 86400  # 24 hours in seconds

        while True:
            try:
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
                                        organization_id = None
                                        if hasattr(connection, "organization_id"):
                                            organization_id = connection.organization_id
                                        else:
                                            # Try to get organization ID from connection object directly
                                            organization_id = connection.get("organization_id")

                                        # Log detected changes for visibility
                                        change_types = {}
                                        for change in changes:
                                            change_type = change.get("type")
                                            if change_type not in change_types:
                                                change_types[change_type] = 0
                                            change_types[change_type] += 1

                                        logger.info(f"Change types detected: {change_types}")

                                        # Store changes in the database
                                        if hasattr(schema_detector, '_store_schema_changes'):
                                            schema_detector._store_schema_changes(
                                                connection_id,
                                                changes,
                                                self.supabase_manager
                                            )
                                            logger.info(f"Stored {len(changes)} schema changes in database")

                                        # Publish events
                                        task_ids = schema_detector.publish_changes_as_events(
                                            connection_id,
                                            changes,
                                            organization_id
                                        )

                                        logger.info(f"Published {len(task_ids)} schema change events")

                                        # Publish automation events for schema changes
                                        try:
                                            from core.automation.events import AutomationEventType, \
                                                publish_automation_event

                                            publish_automation_event(
                                                event_type=AutomationEventType.SCHEMA_CHANGES_DETECTED,
                                                data={
                                                    "connection_id": connection_id,
                                                    "changes_detected": len(changes),
                                                    "important": important_changes,
                                                    "change_types": change_types
                                                },
                                                connection_id=connection_id,
                                                organization_id=organization_id
                                            )
                                            logger.info("Published automation event for schema changes")

                                        except ImportError:
                                            logger.warning("Automation events not available")
                                        except Exception as e:
                                            logger.error(f"Error publishing automation event: {str(e)}")

                                        # Process specific high-priority change types immediately
                                        high_priority_changes = [
                                            c for c in changes
                                            if
                                            c.get("type") in ["table_removed", "column_removed", "column_type_changed"]
                                        ]

                                        for change in high_priority_changes:
                                            table_name = change.get("table")
                                            change_type = change.get("type")

                                            # For table-level changes
                                            if change_type == "table_removed":
                                                logger.info(
                                                    f"Processing high-priority change: Table {table_name} removed")
                                                # Handle table removal (e.g., mark dependent objects as affected)
                                                pass

                                            # For column-level changes
                                            elif change_type in ["column_removed", "column_type_changed"]:
                                                column_name = change.get("column")
                                                logger.info(
                                                    f"Processing high-priority change: Column {column_name} in table {table_name} {change_type}")
                                                # Handle column change (e.g., check for dependent validations)

                                                # Check for validations that might be affected
                                                if self.supabase_manager:
                                                    # Try to find affected validations
                                                    affected_validations = self.supabase_manager.get_validations_by_column(
                                                        organization_id,
                                                        table_name,
                                                        column_name,
                                                        connection_id
                                                    )

                                                    if affected_validations:
                                                        logger.info(
                                                            f"Found {len(affected_validations)} validations potentially affected by schema change")
                                                        # Mark these validations for recheck or notify users

                                except Exception as e:
                                    logger.error(f"Error in schema change detection for {connection_id}: {str(e)}")
                                    logger.error(traceback.format_exc())

                            # Check for other types of metadata that might need refreshing
                            # For example, usage statistics
                            if hasattr(self, 'schedule_usage_refresh_if_needed'):
                                self.schedule_usage_refresh_if_needed(connection_id, 168)  # Weekly

                        except Exception as e:
                            logger.error(f"Error checking connection {connection_id}: {str(e)}")
                            logger.error(traceback.format_exc())

                # Sleep for a while before next check (1 hour)
                time.sleep(3600)

            except Exception as e:
                logger.error(f"Error in scheduled refresh loop: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(300)  # Sleep for 5 minutes after error

    def _get_all_connections(self):
        """Get all database connections from Supabase"""
        try:
            if not self.supabase_manager:
                return []

            # Get all connections
            connections = []
            organizations = self._get_all_organizations()

            for org in organizations:
                org_id = org.get("id")

                # Get connections for this organization
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

            # Get all organizations
            orgs = self.supabase_manager.supabase.table("organizations") \
                .select("id, name") \
                .execute()

            return orgs.data if orgs.data else []

        except Exception as e:
            logger.error(f"Error getting organizations: {str(e)}")
            return []

    def determine_refresh_strategy(self, object_type, object_name=None, last_refreshed=None, change_frequency=None):
        """
        Determine optimal refresh strategy for metadata

        Args:
            object_type: Type of metadata object (table_list, column_metadata, statistics, etc.)
            object_name: Name of the object (optional)
            last_refreshed: When the metadata was last refreshed (ISO format datetime string)
            change_frequency: Historical change frequency (low, medium, high) or None to calculate dynamically

        Returns:
            Dictionary with schedule and priority
        """
        # Default strategies by object type
        strategies = {
            "table_list": {"schedule": "daily", "priority": "high", "hours": 24},
            "column_metadata": {"schedule": "weekly", "priority": "medium", "hours": 168},
            "statistics": {"schedule": "weekly", "priority": "low", "hours": 168},
            "relationships": {"schedule": "weekly", "priority": "medium", "hours": 168},
            "usage_patterns": {"schedule": "monthly", "priority": "low", "hours": 720}
        }

        # If object type is not recognized, use default strategy
        if object_type not in strategies:
            return {"schedule": "weekly", "priority": "low", "hours": 168}

        # Get base strategy
        strategy = strategies[object_type].copy()

        # Try to dynamically determine change frequency if not provided
        try:
            if change_frequency is None and object_name and hasattr(self, 'connection_id'):
                # Import dynamically to avoid circular imports
                try:
                    from .change_analytics import MetadataChangeAnalytics
                    analytics = MetadataChangeAnalytics(self.supabase_manager)

                    # Get current interval from strategy
                    current_interval_hours = strategy.get("hours", 24)

                    # Get refresh suggestion
                    suggestion = analytics.suggest_refresh_interval(
                        self.connection_id,
                        object_type,
                        object_name,
                        current_interval_hours
                    )

                    # Update change frequency based on analytics
                    change_frequency = suggestion.get("frequency")

                    # Update hours based on suggestion
                    strategy["hours"] = suggestion.get("suggested_interval_hours", current_interval_hours)

                    # Map hours to schedule
                    if strategy["hours"] <= 24:
                        strategy["schedule"] = "daily"
                    elif strategy["hours"] <= 168:
                        strategy["schedule"] = "weekly"
                    else:
                        strategy["schedule"] = "monthly"

                    logger.info(f"Updated refresh strategy for {object_type} {object_name} "
                                f"to {strategy['schedule']} ({strategy['hours']} hours) "
                                f"based on analytics")

                except (ImportError, Exception) as e:
                    logger.warning(f"Could not use change analytics for refresh strategy: {str(e)}")
        except Exception as e:
            # Log but continue with manual strategy
            logger.warning(f"Error in dynamic strategy determination: {str(e)}")

        # Adjust based on change frequency
        if change_frequency == "high":
            # More frequent refresh for high-change objects
            if strategy["schedule"] == "monthly":
                strategy["schedule"] = "weekly"
                strategy["hours"] = min(strategy["hours"], 168)
            elif strategy["schedule"] == "weekly":
                strategy["schedule"] = "daily"
                strategy["hours"] = min(strategy["hours"], 24)
            strategy["priority"] = "high"
        elif change_frequency == "low":
            # Less frequent refresh for low-change objects
            if strategy["schedule"] == "daily":
                strategy["schedule"] = "weekly"
                strategy["hours"] = max(strategy["hours"], 168)
            elif strategy["schedule"] == "weekly":
                strategy["schedule"] = "monthly"
                strategy["hours"] = max(strategy["hours"], 720)

        # Adjust if last refresh was too recent (within a day) - prioritize less
        if last_refreshed:
            try:
                refresh_date = datetime.fromisoformat(last_refreshed.replace('Z', '+00:00'))
                now = datetime.now(datetime.timezone.utc)

                # If refreshed very recently, lower priority
                age_hours = (now - refresh_date).total_seconds() / 3600

                # If refreshed less than 20% of the way through the schedule, lower priority
                if age_hours < (strategy["hours"] * 0.2):
                    if strategy["priority"] == "high":
                        strategy["priority"] = "medium"
                    elif strategy["priority"] == "medium":
                        strategy["priority"] = "low"

                # If nearly due for refresh (>80% of schedule elapsed), raise priority
                elif age_hours > (strategy["hours"] * 0.8):
                    if strategy["priority"] == "low":
                        strategy["priority"] = "medium"
                    elif strategy["priority"] == "medium":
                        strategy["priority"] = "high"
            except:
                pass

        return strategy