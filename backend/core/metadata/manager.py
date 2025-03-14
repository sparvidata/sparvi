import logging
import os
import sys
import threading
import time
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
        return self.worker.submit_task("full_collection", connection_id, params, priority)

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
            event_type: Type of event
            connection_id: Connection ID
            details: Additional event details

        Returns:
            Task ID if a task was submitted, None otherwise
        """
        logger.info(f"Processing metadata event: {event_type} for connection {connection_id}")

        if event_type == "validation_failure" and details and details.get("reason") == "schema_mismatch":
            # Schema mismatch detected, refresh schema metadata
            table_name = details.get("table_name")
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

        elif event_type == "profile_completion" and details and details.get("table_name"):
            # Profile completed, update statistics
            table_name = details.get("table_name")
            return self.submit_statistics_refresh_task(connection_id, table_name, "medium")

        elif event_type == "user_request" and details and details.get("metadata_type"):
            # User manually requested refresh
            metadata_type = details.get("metadata_type")

            if metadata_type == "schema":
                # Refresh schema
                if details.get("table_name"):
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
                if details.get("table_name"):
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
                if details.get("table_name"):
                    # Refresh specific table
                    return self.submit_usage_update_task(
                        connection_id,
                        details.get("table_name"),
                        "high"
                    )

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

    def _scheduled_refresh_loop(self):
        """Background thread for scheduled metadata refreshes"""
        logger.info("Started scheduled refresh thread")

        while True:
            try:
                # Get all connections
                if self.supabase_manager:
                    connections = self._get_all_connections()

                    # Check each connection for stale metadata
                    for connection in connections:
                        connection_id = connection.get("id")
                        try:
                            # Check tables metadata (refresh if older than 1 day)
                            self.schedule_refresh_if_needed(connection_id, "tables", 24)

                            # Check statistics metadata (refresh if older than 3 days)
                            self.schedule_refresh_if_needed(connection_id, "statistics", 72)
                        except Exception as e:
                            logger.error(f"Error checking connection {connection_id}: {str(e)}")

                # Sleep for a while before next check (1 hour)
                time.sleep(3600)

            except Exception as e:
                logger.error(f"Error in scheduled refresh loop: {str(e)}")
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
                    .select("id, name, connection_type") \
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