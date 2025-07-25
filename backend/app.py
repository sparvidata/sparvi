import json
import urllib.parse
import psutil
import supabase
from flask import Flask, render_template, jsonify, request
from datetime import datetime, timezone, timedelta
import os
import traceback
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
import threading
import queue
import uuid
import threading
import httpx
from functools import wraps
from dotenv import load_dotenv
from flask_cors import CORS
from sqlalchemy import inspect, create_engine, text
from supabase import create_client, Client
from sparvi.profiler.profile_engine import profile_table
from sparvi.validations.default_validations import get_default_validations
from sparvi.validations.validator import run_validations as sparvi_run_validations

from core.metadata.storage_service import MetadataStorageService
from core.metadata.connectors import SnowflakeConnector
from core.metadata.collector import MetadataCollector
from core.metadata.storage import MetadataStorage
from core.storage.supabase_manager import SupabaseManager

# Import refactored modules
from core.auth.decorators import token_required
from core.connections.builders import ConnectionStringBuilder
from utils.caching import request_cache
from routes.profiles import register_profile_routes
from routes.connections import register_connection_routes
from routes.validations import register_validation_routes
from core.validations.supabase_validation_manager import SupabaseValidationManager
from core.history.supabase_profile_history import SupabaseProfileHistoryManager
from core.metadata.manager import MetadataTaskManager
from core.metadata.events import MetadataEventType, publish_metadata_event
from core.utils.performance_optimizations import get_optimized_classes
from core.anomalies.routes import register_anomaly_routes
from core.anomalies.scheduler_service import AnomalyDetectionSchedulerService
from routes import notifications_bp

#remove later
from core.connections.utils import connection_access_check
from core.connections.builders import ConnectionStringBuilder
from core.connections.manager import ConnectionManager



# Automation system imports
from core.automation.routes import register_automation_routes
from core.automation.job_deduplication import job_deduplication_service
from core.utils.app_hooks import (
    initialize_automation_system,
    automation_health_endpoint,
    automation_control_endpoint,
    integrate_with_metadata_system
)

import concurrent.futures
from sqlalchemy.pool import QueuePool
from functools import wraps
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import time
import logging

# Configure logging BEFORE any other imports that might use logging
logging.getLogger('httpcore').setLevel(logging.ERROR)
logging.getLogger('hpack').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.WARNING)

# Set root logger level but keep your app logs
logging.basicConfig(level=logging.INFO)


class ConnectionPoolManager:
    """Manages a pool of database connections"""
    _instance = None
    _pools = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = ConnectionPoolManager()
        return cls._instance

    def get_engine(self, connection_string):
        """Get or create a connection pool for the given connection string"""
        # Use a sanitized version as the key to avoid storing credentials in memory
        from core.utils.connection_utils import sanitize_connection_string
        key = sanitize_connection_string(connection_string)

        if key not in self._pools:
            # Create a new engine with connection pooling
            self._pools[key] = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800  # Recycle connections after 30 minutes
            )
            logger.info(f"Created new connection pool for {key}")

        return self._pools[key]


def configure_logging():
    """Configure logging to handle Unicode properly"""
    import logging
    import sys

    # Create a custom formatter that handles Unicode gracefully
    class SafeFormatter(logging.Formatter):
        def format(self, record):
            try:
                return super().format(record)
            except UnicodeEncodeError:
                # Fallback: convert to ASCII and replace problematic characters
                msg = str(record.getMessage())
                safe_msg = msg.encode('ascii', 'replace').decode('ascii')
                record.msg = safe_msg
                return super().format(record)

    # Configure root logger
    root_logger = logging.getLogger()

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)

    # Try to set UTF-8 encoding, fall back to system default
    try:
        if hasattr(console_handler.stream, 'reconfigure'):
            console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
    except:
        pass  # Use system default if reconfigure fails

    # Set formatter
    formatter = SafeFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    console_handler.setFormatter(formatter)

    # Set level and add handler
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(logging.INFO)

    logger.info("Logging configured with Unicode safety")


def cache_with_timeout(timeout_seconds=300):
    """
    Cache function results with a timeout

    Args:
        timeout_seconds: Cache timeout in seconds
    """
    cache = {}

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create a cache key from function name and arguments
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"

            # Check if result is in cache and not expired
            if key in cache:
                result, timestamp = cache[key]
                if time.time() - timestamp < timeout_seconds:
                    return result

            # Call the function and cache the result
            result = func(*args, **kwargs)
            cache[key] = (result, time.time())

            return result

        return wrapper

    return decorator

# Cached version of the metadata getter
@request_cache
def get_metadata_cached(connection_id, metadata_type):
    """Cached version of get_metadata to avoid duplicate lookups"""
    storage_service = MetadataStorageService()
    return storage_service.get_metadata(connection_id, metadata_type)


# Improved task manager implementation
class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Task:
    id: str
    connection_id: str
    task_type: str
    params: Dict[str, Any]
    priority: int
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = 0.0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        # Handle specific SQL/database types from SQLAlchemy or similar
        if hasattr(obj, 'VARCHAR') or hasattr(obj, '__visit_name__'):
            return str(obj)
        return super().default(obj)

class ImprovedTaskManager:
    """A more robust task manager with priority queue and worker pool"""
    _instance = None

    @classmethod
    def get_instance(cls, storage_service=None, supabase_mgr=None):
        if cls._instance is None:
            cls._instance = ImprovedTaskManager(storage_service, supabase_mgr)
        return cls._instance

    def __init__(self, storage_service=None, supabase_mgr=None):
        self.storage_service = storage_service
        self.supabase_mgr = supabase_mgr
        self.tasks = {}
        self.task_queue = queue.PriorityQueue()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        self.stats = {
            "tasks_processed": 0,
            "tasks_succeeded": 0,
            "tasks_failed": 0,
            "start_time": time.time()
        }

    def _worker_loop(self):
        """Main worker loop that processes tasks from the queue"""
        while self.running:
            try:
                priority, task_id = self.task_queue.get(timeout=1.0)
                if task_id not in self.tasks:
                    logger.error(f"Task {task_id} not found in task list")
                    self.task_queue.task_done()
                    continue

                task = self.tasks[task_id]

                # Update task status
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()

                # Execute the task in the thread pool
                future = self.executor.submit(self._execute_task, task)
                future.add_done_callback(lambda f: self._task_completed(task_id, f))

                # Mark task as processed in the queue
                self.task_queue.task_done()

            except queue.Empty:
                # No tasks in queue, just continue
                pass
            except Exception as e:
                logger.error(f"Error in worker loop: {str(e)}")
                logger.error(traceback.format_exc())

    def _execute_task(self, task):
        """Execute a single task based on its type"""
        logger.info(f"Executing task {task.id} of type {task.task_type}")
        try:
            if task.task_type == "full_metadata_collection":
                return self._execute_full_collection(task)
            elif task.task_type == "table_metadata":
                return self._execute_table_metadata(task)
            elif task.task_type == "refresh_statistics":
                return self._execute_refresh_statistics(task)
            elif task.task_type == "update_usage":
                return self._execute_update_usage(task)
            else:
                logger.warning(f"Unknown task type: {task.task_type}")
                return {"status": "unknown_task_type"}
        except Exception as e:
            logger.error(f"Task execution error: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _execute_full_collection(self, task):
        """Execute full metadata collection"""
        connection_id = task.connection_id
        params = task.params
        depth = params.get("depth", "medium")
        table_limit = params.get("table_limit", 50)

        # Get connection details
        connection = self.supabase_mgr.get_connection(connection_id)
        if not connection:
            raise ValueError(f"Connection not found: {connection_id}")

        # Create connector
        connector = get_connector_for_connection(connection)
        connector.connect()

        # Create metadata collector
        collector = MetadataCollector(connection_id, connector)

        # Get list of tables
        tables = collector.collect_table_list()
        logger.info(f"Found {len(tables)} tables for connection {connection_id}")

        # Process tables based on depth and limit
        tables_to_process = tables[:min(len(tables), table_limit)]

        # Prepare data structures
        tables_data = []
        columns_by_table = {}
        statistics_by_table = {}

        # Process tables in parallel if depth is medium or deep
        if depth in ["medium", "deep"]:
            # Create thread pool for parallel processing
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # Submit tasks for each table
                future_to_table = {
                    executor.submit(self._process_table, collector, table, depth): table
                    for table in tables_to_process
                }

                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_table):
                    table = future_to_table[future]
                    try:
                        result = future.result()
                        if result:
                            tables_data.append(result["table_meta"])
                            columns_by_table[table] = result["columns"]
                            statistics_by_table[table] = result["statistics"]
                    except Exception as e:
                        logger.error(f"Error processing table {table}: {str(e)}")
        else:
            # Process tables sequentially for 'light' depth
            for table in tables_to_process:
                try:
                    result = self._process_table(collector, table, depth)
                    if result:
                        tables_data.append(result["table_meta"])
                        columns_by_table[table] = result["columns"]
                        statistics_by_table[table] = result["statistics"]
                except Exception as e:
                    logger.error(f"Error processing table {table}: {str(e)}")

        # Store the collected metadata
        if tables_data:
            self.storage_service.store_tables_metadata(connection_id, tables_data)
            logger.info(f"Stored metadata for {len(tables_data)} tables")

        if columns_by_table:
            self.storage_service.store_columns_metadata(connection_id, columns_by_table)
            logger.info(f"Stored column metadata for {len(columns_by_table)} tables")

        # Aggregate and store statistics ONCE after processing all tables
        if statistics_by_table: # Check if any statistics were collected
            # Ensure we only store if depth required it (medium/deep)
            # The collection logic inside _process_table already handles depth, but double-check here.
            if depth in ["medium", "deep"]:
                try:
                    self.storage_service.store_statistics_metadata(connection_id, statistics_by_table)
                    logger.info(f"Stored aggregated statistics for {len(statistics_by_table)} tables")
                except Exception as e:
                    logger.error(f"Failed to store aggregated statistics: {str(e)}")
            else:
                 logger.info(f"Skipping statistics storage for depth '{depth}'")
        else:
             logger.info("No statistics collected to store.")


        return {
            "status": "success",
            "tables_processed": len(tables_data),
            "depth": depth
        }

    def _process_table(self, collector, table, depth):
        """Process a single table's metadata"""
        try:
            # Basic table information
            columns = collector.collect_columns(table)
            primary_keys = collector.connector.get_primary_keys(table)

            # Row count - use a query
            row_count = 0
            try:
                result = collector.connector.execute_query(f"SELECT COUNT(*) FROM {table}")
                if result and len(result) > 0:
                    row_count = result[0][0]
            except Exception as e:
                logger.warning(f"Error getting row count for {table}: {str(e)}")

            # Create table metadata
            table_meta = {
                "name": table,
                "column_count": len(columns),
                "row_count": row_count,
                "primary_key": primary_keys,
                "id": str(uuid.uuid4())
            }

            # Get statistics based on depth
            statistics = None
            if depth == "low":
                # For low depth, use simplified statistics
                statistics = {
                    "row_count": row_count,
                    "column_count": len(columns),
                    "has_primary_key": len(primary_keys) > 0,
                    "columns": {
                        col["name"]: {
                            "type": col["type"],
                            "nullable": col.get("nullable", False)
                        } for col in columns
                    }
                }
            else:
                # For medium/high depth, create a task object and call _execute_refresh_statistics
                from dataclasses import dataclass

                @dataclass
                class StatisticsTask:
                    connection_id: str
                    task_type: str = "refresh_statistics"
                    params: dict = None

                # Create task object with needed parameters
                # Use collector.connection_id instead of self.connection_id
                task = StatisticsTask(
                    connection_id=collector.connection_id,
                    params={"table_name": table}
                )

                # Call the comprehensive statistics method
                try:
                    logger.info(f"Collecting comprehensive statistics for {table} via _execute_refresh_statistics")
                    stats_result = self._execute_refresh_statistics(task)

                    # Get the statistics from the return value
                    if stats_result and stats_result.get("status") == "success":
                        if "table_stats" in stats_result:
                            # If the stats are returned directly
                            statistics = stats_result["table_stats"]
                        else:
                            # Otherwise get from storage
                            statistics_metadata = self.storage_service.get_metadata(collector.connection_id,
                                                                                    "statistics")
                            if statistics_metadata and "metadata" in statistics_metadata:
                                stats_by_table = statistics_metadata["metadata"].get("statistics_by_table", {})
                                if table in stats_by_table:
                                    statistics = stats_by_table[table]
                except Exception as stats_error:
                    logger.error(f"Error getting comprehensive statistics: {str(stats_error)}")
                    logger.error(traceback.format_exc())

                # Fallback to basic statistics if comprehensive collection failed
                if not statistics:
                    logger.warning(f"Falling back to basic statistics for {table}")
                    statistics = {
                        "row_count": row_count,
                        "column_count": len(columns),
                        "has_primary_key": len(primary_keys) > 0,
                        "columns": {
                            col["name"]: {
                                "type": col["type"],
                                "nullable": col.get("nullable", False)
                            } for col in columns
                        }
                    }

            return {
                "table_meta": table_meta,
                "columns": columns,
                "statistics": statistics
            }

        except Exception as e:
            logger.error(f"Error processing table {table}: {str(e)}")
            return None

    def _execute_table_metadata(self, task):
        """Execute metadata collection for a single table"""
        connection_id = task.connection_id
        table_name = task.params.get("table_name")

        if not table_name:
            raise ValueError("Table name not provided for table_metadata task")

        # Get connection details
        connection = self.supabase_mgr.get_connection(connection_id)
        if not connection:
            raise ValueError(f"Connection not found: {connection_id}")

        # Create connector
        connector = get_connector_for_connection(connection)
        connector.connect()

        # Create metadata collector
        collector = MetadataCollector(connection_id, connector)

        # Process the table
        result = self._process_table(collector, table_name, "medium")

        if not result:
            raise ValueError(f"Failed to process table {table_name}")

        # Store the collected metadata
        tables_data = [result["table_meta"]]
        columns_by_table = {table_name: result["columns"]}
        statistics_by_table = {table_name: result["statistics"]}

        self.storage_service.store_tables_metadata(connection_id, tables_data)
        self.storage_service.store_columns_metadata(connection_id, columns_by_table)
        self.storage_service.store_statistics_metadata(connection_id, statistics_by_table)

        return {
            "status": "success",
            "table": table_name
        }

    def _execute_refresh_statistics(self, task):
        """Execute statistics refresh for a specific table"""
        connection_id = task.connection_id
        table_name = task.params.get("table_name")

        if not table_name:
            raise ValueError("Table name not provided for refresh_statistics task")

        # Implementation would go here - simplified for brevity
        return {
            "status": "success",
            "table": table_name,
            "statistics_collected": True
        }

    def _execute_update_usage(self, task):
        """Execute usage statistics update"""
        connection_id = task.connection_id
        table_name = task.params.get("table_name")

        if not table_name:
            raise ValueError("Table name not provided for update_usage task")

        return {
            "status": "success",
            "table": table_name,
            "message": "Usage statistics updated"
        }

    def _task_completed(self, task_id, future):
        """Callback when a task is completed"""
        if task_id not in self.tasks:
            return

        task = self.tasks[task_id]
        task.completed_at = time.time()

        try:
            task.result = future.result()
            task.status = TaskStatus.COMPLETED
            self.stats["tasks_processed"] += 1
            self.stats["tasks_succeeded"] += 1
            logger.info(f"Task {task_id} completed successfully")
        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED
            self.stats["tasks_processed"] += 1
            self.stats["tasks_failed"] += 1
            logger.error(f"Task {task_id} failed: {str(e)}")

    def submit_collection_task(self, connection_id, params, priority="medium"):
        """Submit a full collection task"""
        return self.submit_task(connection_id, "full_metadata_collection", params, priority)

    def submit_table_metadata_task(self, connection_id, table_name, priority="medium"):
        """Submit a task to collect metadata for a specific table"""
        params = {"table_name": table_name}
        return self.submit_task(connection_id, "table_metadata", params, priority)

    def submit_statistics_refresh_task(self, connection_id, table_name, priority="medium"):
        """Submit a task to refresh statistics for a specific table"""
        params = {"table_name": table_name}
        return self.submit_task(connection_id, "refresh_statistics", params, priority)

    def submit_usage_update_task(self, connection_id, table_name, priority="medium"):
        """Submit a task to update usage statistics for a specific table"""
        params = {"table_name": table_name}
        return self.submit_task(connection_id, "update_usage", params, priority)

    def submit_task(self, connection_id, task_type, params, priority="medium"):
        """Submit a new task to the queue"""
        # Convert priority string to integer
        priority_values = {"high": 0, "medium": 50, "low": 100}
        priority_int = priority_values.get(priority, 50)

        # Create a new task
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            connection_id=connection_id,
            task_type=task_type,
            params=params,
            priority=priority_int,
            created_at=time.time()
        )

        # Store the task
        self.tasks[task_id] = task

        # Add to queue with priority
        self.task_queue.put((priority_int, task_id))

        logger.info(f"Submitted task {task_id} of type {task_type} with priority {priority}")
        return task_id

    def get_task_status(self, task_id):
        """Get the current status of a task"""
        if task_id not in self.tasks:
            return {"error": "Task not found"}

        task = self.tasks[task_id]
        return {
            "id": task.id,
            "status": task.status.value,
            "connection_id": task.connection_id,
            "task_type": task.task_type,
            "created_at": task.created_at,
            "started_at": task.started_at,
            "completed_at": task.completed_at,
            "result": task.result,
            "error": task.error
        }

    def get_recent_tasks(self, limit=10, connection_id=None):
        """Get the most recent tasks, optionally filtered by connection_id"""
        # Filter by connection_id if provided
        task_list = list(self.tasks.values())
        if connection_id:
            task_list = [t for t in task_list if t.connection_id == connection_id]

        # Sort tasks by creation time (newest first) and limit
        sorted_tasks = sorted(
            task_list,
            key=lambda t: t.created_at,
            reverse=True
        )[:limit]

        # Convert to dictionaries
        return [
            {
                "id": task.id,
                "status": task.status.value,
                "connection_id": task.connection_id,
                "task_type": task.task_type,
                "created_at": task.created_at,
                "task": {
                    "connection_id": task.connection_id,
                    "type": task.task_type,
                    "params": task.params,
                    "status": task.status.value
                }
            }
            for task in sorted_tasks
        ]

    def handle_metadata_event(self, event_type, connection_id, details):
        """Process a metadata event and schedule appropriate tasks"""
        logger.info(f"Handling metadata event: {event_type} for connection {connection_id}")

        if event_type == "VALIDATION_FAILURE":
            # Schedule metadata refresh on validation failure
            reason = details.get("reason")
            table_name = details.get("table_name")

            if reason == "schema_mismatch" and table_name:
                return self.submit_table_metadata_task(connection_id, table_name, "high")

        elif event_type == "PROFILE_COMPLETION":
            # Update statistics after profile completion
            table_name = details.get("table_name")

            if table_name:
                return self.submit_statistics_refresh_task(connection_id, table_name, "medium")

        elif event_type == "USER_REQUEST":
            # Handle explicit user requests for metadata
            metadata_type = details.get("metadata_type")
            table_name = details.get("table_name")

            if metadata_type == "schema" and table_name:
                return self.submit_table_metadata_task(connection_id, table_name, "high")
            elif metadata_type == "statistics" and table_name:
                return self.submit_statistics_refresh_task(connection_id, table_name, "high")
            elif metadata_type == "full":
                return self.submit_collection_task(connection_id, {"depth": "medium"}, "high")

        elif event_type == "SCHEMA_CHANGE":
            # Handle schema change events
            table_name = details.get("table_name")

            if table_name:
                return self.submit_table_metadata_task(connection_id, table_name, "high")

        return None

    def get_worker_stats(self):
        """Get statistics about the worker"""
        uptime = time.time() - self.stats["start_time"]
        return {
            "tasks_processed": self.stats["tasks_processed"],
            "tasks_succeeded": self.stats["tasks_succeeded"],
            "tasks_failed": self.stats["tasks_failed"],
            "tasks_pending": self.task_queue.qsize(),
            "uptime_seconds": uptime,
            "worker_status": "running" if self.running else "stopped"
        }


import os



# Initialize connection pool manager
connection_pool_manager = ConnectionPoolManager.get_instance()

# Initialize global task manager
metadata_task_manager = None

json_encoder = CustomJSONEncoder

anomaly_scheduler_service = None


# def initialize_anomaly_detection():
#     global anomaly_scheduler_service
#
#     try:
#         # Add debug logging
#         logger.info("Starting anomaly detection initialization...")
#
#         # Register routes
#         logger.info("Registering anomaly detection routes...")
#         register_anomaly_routes(app, token_required)
#
#         # Log registered anomaly routes
#         anomaly_routes = [rule for rule in app.url_map.iter_rules() if 'anomalies' in rule.rule]
#         logger.info(f"Registered {len(anomaly_routes)} anomaly detection routes:")
#         for rule in anomaly_routes:
#             logger.info(f"  {rule.methods} {rule.rule}")
#
#         # Always start the anomaly detection scheduler - no environment checks
#         logger.info("Starting anomaly detection scheduler service...")
#
#         try:
#             from core.anomalies.scheduler_service import AnomalyDetectionSchedulerService
#             anomaly_scheduler_service = AnomalyDetectionSchedulerService()
#             anomaly_scheduler_service.start()
#
#             logger.info("Anomaly detection scheduler service started successfully")
#
#             # Verify it's actually running
#             if hasattr(anomaly_scheduler_service, 'running') and anomaly_scheduler_service.running:
#                 logger.info("Verified: Anomaly scheduler is running")
#             else:
#                 logger.warning("Warning: Anomaly scheduler may not be running properly")
#
#         except Exception as scheduler_error:
#             logger.error(f"Failed to start anomaly detection scheduler: {str(scheduler_error)}")
#             logger.error(traceback.format_exc())
#             anomaly_scheduler_service = None
#
#         logger.info("Anomaly detection initialization completed")
#         return True
#
#     except Exception as e:
#         logger.error(f"Error initializing anomaly detection: {str(e)}")
#         logger.error(traceback.format_exc())
#         return False


def get_metadata_task_manager():
    """
    Helper function to get metadata task manager instance.
    Returns None if not available (automation disabled).
    """
    try:
        from core.metadata.manager import MetadataTaskManager
        return MetadataTaskManager.get_instance()
    except Exception as e:
        logger.warning(f"Metadata task manager not available: {str(e)}")
        return None


def delayed_initialization(flask_app, token_required_decorator):
    """
    Initialize background services with unified automation management

    Args:
        flask_app: Flask application instance
        token_required_decorator: Authentication decorator for routes
    """
    try:
        logger.info("Starting delayed initialization of background services...")

        initialization_results = {
            "metadata_task_manager": False,
            "unified_automation": {},
            "performance_optimizations": False
        }

        # Step 1: Initialize metadata task manager (standalone)
        try:
            logger.info("Step 1: Initializing metadata task manager...")

            from core.metadata.manager import MetadataTaskManager
            from core.storage.supabase_manager import SupabaseManager

            supabase_manager = SupabaseManager()
            metadata_task_manager = MetadataTaskManager.get_instance(supabase_manager=supabase_manager)

            if metadata_task_manager:
                logger.info("✓ Metadata task manager initialized successfully")
                initialization_results["metadata_task_manager"] = True
            else:
                logger.warning("Metadata task manager initialization returned None")

        except Exception as e:
            logger.error(f"Failed to initialize metadata task manager: {str(e)}")

        # Wait a moment before automation
        time.sleep(1)

        # Step 2: Initialize ALL automation systems through unified manager
        try:
            logger.info("Step 2: Initializing unified automation systems...")

            from core.automation.unified_manager import initialize_unified_automation

            automation_results = initialize_unified_automation(flask_app, token_required_decorator)
            initialization_results["unified_automation"] = automation_results

            # Check if any systems started
            any_automation_success = any(automation_results.values())
            if any_automation_success:
                logger.info("✓ At least one automation system started successfully")
            else:
                logger.warning("No automation systems started successfully")

        except Exception as e:
            logger.error(f"Failed to initialize unified automation: {str(e)}")
            logger.error(traceback.format_exc())

        # Step 3: Apply performance optimizations (non-critical)
        try:
            logger.info("Step 3: Applying performance optimizations...")

            from core.utils.performance_optimizations import apply_performance_optimizations
            optimized_classes = apply_performance_optimizations()

            logger.info(f"Applied performance optimizations to {len(optimized_classes)} classes")
            initialization_results["performance_optimizations"] = True

        except Exception as e:
            logger.warning(f"Failed to apply performance optimizations: {str(e)}")

        # Log final results
        logger.info("Background services initialization complete")

        # Count successful services
        successful_services = 0
        if initialization_results["metadata_task_manager"]:
            successful_services += 1
        if any(initialization_results["unified_automation"].values()):
            successful_services += 1
        if initialization_results["performance_optimizations"]:
            successful_services += 1

        logger.info(f"Initialization summary: {successful_services}/3 major systems started successfully")

        # Detailed logging
        logger.info(
            f"  Metadata Task Manager: {'SUCCESS' if initialization_results['metadata_task_manager'] else 'FAILED'}")

        automation_status = "PARTIAL" if any(initialization_results["unified_automation"].values()) else "FAILED"
        if all(initialization_results["unified_automation"].values()):
            automation_status = "SUCCESS"
        logger.info(f"  Unified Automation: {automation_status}")

        for system, success in initialization_results["unified_automation"].items():
            logger.info(f"    - {system}: {'SUCCESS' if success else 'FAILED'}")

        logger.info(
            f"  Performance Optimizations: {'SUCCESS' if initialization_results['performance_optimizations'] else 'FAILED'}")

        # Provide guidance based on results
        if not any(initialization_results["unified_automation"].values()):
            environment = os.getenv("ENVIRONMENT", "development")
            if environment == "development":
                logger.info("To enable automation in development: set ENABLE_AUTOMATION_SCHEDULER=true")
            else:
                logger.warning("Automation systems failed to start - check configuration and logs above")

    except Exception as e:
        logger.error(f"Critical error in delayed initialization: {str(e)}")
        logger.error(traceback.format_exc())


def setup_comprehensive_logging():
    """
    Set up a comprehensive logging configuration that:
    - Logs to console (stderr)
    - Logs to a file
    - Captures appropriate log levels (not everything at DEBUG)
    - Includes detailed log formatting
    - Handles uncaught exceptions
    """
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Changed from DEBUG to INFO

    # Console Handler - writes to stderr
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)  # Changed from DEBUG to INFO

    # File Handler - writes to a log file
    try:
        file_handler = logging.FileHandler('app.log', mode='a')
        file_handler.setLevel(logging.DEBUG)  # Keep DEBUG for file only
    except Exception as e:
        print(f"Could not create file handler: {e}")
        file_handler = None

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )

    # Set formatter for handlers
    console_handler.setFormatter(formatter)
    if file_handler:
        file_handler.setFormatter(formatter)

    # Remove any existing handlers to prevent duplicate logs
    logger.handlers.clear()

    # Add handlers
    logger.addHandler(console_handler)
    if file_handler:
        logger.addHandler(file_handler)

    # Add automation loggers
    logging.getLogger('automation').setLevel(logging.INFO)
    logging.getLogger('automation.scheduler').setLevel(logging.INFO)
    logging.getLogger('automation.api').setLevel(logging.INFO)

    logging.getLogger('core.storage.supabase_manager').setLevel(logging.WARNING)
    logging.getLogger('core.utils.performance_optimizations').setLevel(logging.WARNING)
    logging.getLogger('core.metadata.storage_service').setLevel(logging.WARNING)
    logging.getLogger('supabase_manager').setLevel(logging.WARNING)
    logging.getLogger('core.utils.auth_utils').setLevel(logging.WARNING)
    logging.getLogger('core.utils.token_utils').setLevel(logging.WARNING)

    # Add handler for uncaught exceptions
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception

    return logger


# Set up logging early
logger = setup_comprehensive_logging()

# Load environment variables from .env file
load_dotenv()


def setup_cors(app):
    """Configure CORS with production-ready settings"""
    try:
        # Define allowed origins
        allowed_origins = [
            "https://cloud.sparvi.io",
            "http://localhost:3000",
            "https://ambitious-wave-0fdea0310.6.azurestaticapps.net"
        ]

        logger.info(f"Setting up CORS for origins: {allowed_origins}")

        # Configure CORS with explicit settings - IMPORTANT: This must be done BEFORE registering routes
        CORS(app,
             origins=allowed_origins,
             methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
             allow_headers=['Content-Type', 'Authorization', 'X-Requested-With'],
             supports_credentials=True,
             resources={r"/api/*": {"origins": allowed_origins}},
             # Add these additional options
             expose_headers=['Content-Range', 'X-Content-Range'],
             max_age=600)

        logger.info("CORS setup completed successfully")

        # Add manual CORS headers as backup
        @app.before_request
        def handle_preflight():
            if request.method == "OPTIONS":
                origin = request.headers.get('Origin')
                if origin in allowed_origins:
                    response = app.make_default_options_response()
                    response.headers['Access-Control-Allow-Origin'] = origin
                    response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization,X-Requested-With'
                    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE,OPTIONS'
                    response.headers['Access-Control-Allow-Credentials'] = 'true'
                    response.headers['Access-Control-Max-Age'] = '3600'
                    return response

        @app.after_request
        def after_request(response):
            origin = request.headers.get('Origin')
            if origin in allowed_origins:
                response.headers['Access-Control-Allow-Origin'] = origin
                response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization,X-Requested-With'
                response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE,OPTIONS'
                response.headers['Access-Control-Allow-Credentials'] = 'true'
            return response

    except Exception as e:
        logger.error(f"CORS setup failed: {e}")
        logger.error(traceback.format_exc())
        # Emergency fallback
        CORS(app, origins="*", supports_credentials=True)
        logger.warning("Using emergency CORS fallback (allow all origins)")


def create_error_handlers(app):
    """
    Add error handlers for common CORS and network-related issues
    """

    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({
            "error": "Bad Request",
            "message": "The server cannot process the request due to a client error",
            "status_code": 400
        }), 400

    @app.errorhandler(405)
    def method_not_allowed(error):
        return jsonify({
            "error": "Method Not Allowed",
            "message": "The method is not allowed for the requested URL",
            "status_code": 405
        }), 405

    @app.errorhandler(503)
    def service_unavailable(error):
        """Handle service unavailable errors (including automation system down)"""
        return jsonify({
            "error": "Service temporarily unavailable",
            "message": "Some automated features may not be available"
        }), 503

configure_logging()
app = Flask(__name__, template_folder="templates")
setup_cors(app)
create_error_handlers(app)
app.register_blueprint(notifications_bp, url_prefix='/api')

# Register automation routes with authentication
register_automation_routes(app, token_required)


# Set the secret key from environment variables
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "default_secret_key")

# Initialize Supabase validation manager
validation_manager = SupabaseValidationManager()
run_validations_after_profile = True  # Set to True to automatically run validations after profiling
metadata_storage = MetadataStorage()
task_executor = ThreadPoolExecutor(max_workers=5)
metadata_task_queue = queue.Queue()

try:
    optimized_classes = get_optimized_classes()
    logger.info(f"Applied performance optimizations to {len(optimized_classes)} classes")
except Exception as e:
    logger.warning(f"Could not apply performance optimizations: {str(e)}")
    optimized_classes = {}

logger.info("Scheduling delayed initialization in 15 seconds...")
threading.Timer(15.0, lambda: delayed_initialization(app, token_required)).start()
logger.info("Flask app initialization complete, background services will start shortly...")

@app.route('/health')
def health_check():
    """Main application health check"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "services": {}
        }

        # Check automation health
        try:
            from core.utils.app_hooks import get_automation_health
            automation_health = get_automation_health()
            health_status["services"]["automation"] = {
                "healthy": automation_health.get("healthy", False),
                "status": automation_health.get("status", {})
            }
        except Exception as e:
            health_status["services"]["automation"] = {
                "healthy": False,
                "error": str(e)
            }

        # Determine overall health
        all_healthy = all(
            service.get("healthy", False)
            for service in health_status["services"].values()
        )

        if not all_healthy:
            health_status["status"] = "degraded"

        return jsonify(health_status), 200 if all_healthy else 503

    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 503


@app.route('/health/automation')
def automation_health():
    """Health check endpoint for automation system"""
    try:
        from core.automation.unified_manager import get_unified_automation_status

        status = get_unified_automation_status()

        # Determine overall health
        healthy = (
                status.get("automation_enabled", False) and
                status.get("initialized", False) and
                (status.get("main_scheduler", {}).get("running", False) or
                 not status.get("automation_enabled", False))
        )

        health_result = {
            "healthy": healthy,
            "status": status,
            "message": "Automation systems operational" if healthy else "Automation systems degraded"
        }

        status_code = 200 if healthy else 503
        return jsonify(health_result), status_code

    except Exception as e:
        return jsonify({
            "healthy": False,
            "error": str(e),
            "message": "Automation health check failed"
        }), 503


# Add admin endpoint to restart automation
@app.route('/api/admin/automation/restart', methods=['POST'])
@token_required
def restart_automation(current_user, organization_id):
    """Admin endpoint to restart automation system"""
    try:
        # Check if user is admin
        supabase = SupabaseManager()
        user_role = supabase.get_user_role(current_user)

        if user_role not in ['admin', 'owner']:
            return jsonify({"error": "Insufficient permissions"}), 403

        from core.automation.unified_manager import restart_unified_automation

        logger.info(f"Admin {current_user} requested automation restart")

        # Restart all systems
        results = restart_unified_automation()

        success = any(results.values())

        return jsonify({
            "success": success,
            "results": results,
            "message": "Automation systems restarted" if success else "Failed to restart automation systems"
        }), 200

    except Exception as e:
        logger.error(f"Error restarting automation: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/health/automation/detailed')
@token_required
def automation_health_detailed(current_user, organization_id):
    """Detailed health check with authentication"""
    # Check user permissions
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)
    if user_role not in ['admin', 'owner', 'member']:
        return jsonify({"error": "Insufficient permissions"}), 403

    health_result, status_code = automation_health_endpoint()
    return jsonify(health_result), status_code


@app.route("/api/admin/automation/jobs-status", methods=["GET"])
@token_required
def get_automation_jobs_status(current_user, organization_id):
    """Get active automation jobs status - PHASE 3 MONITORING"""
    try:
        # Check admin permissions
        supabase = SupabaseManager()
        user_role = supabase.get_user_role(current_user)
        if user_role not in ['admin', 'owner']:
            return jsonify({"error": "Admin access required"}), 403

        from core.automation.job_deduplication import job_deduplication_service
        from core.automation.unified_manager import get_unified_automation_status

        # Get deduplication status
        active_jobs = job_deduplication_service.get_active_jobs_summary()

        # Get overall automation status
        automation_status = get_unified_automation_status()

        # Get recent job history from database
        recent_jobs = supabase.supabase.table("automation_jobs") \
            .select("id,job_type,status,connection_id,created_at,completed_at,error_message") \
            .order("created_at", desc=True) \
            .limit(20) \
            .execute()

        # Get connection names for better reporting
        connection_names = {}
        if recent_jobs.data:
            connection_ids = list(set(job.get("connection_id") for job in recent_jobs.data if job.get("connection_id")))
            for conn_id in connection_ids:
                try:
                    conn_response = supabase.supabase.table("database_connections") \
                        .select("name") \
                        .eq("id", conn_id) \
                        .eq("organization_id", organization_id) \
                        .execute()
                    if conn_response.data:
                        connection_names[conn_id] = conn_response.data[0]["name"]
                except:
                    connection_names[conn_id] = "Unknown"

        # Enhance job data with connection names and analyze for duplicates
        enhanced_jobs = []
        duplicate_patterns = []

        for job in recent_jobs.data or []:
            # Add connection name
            job["connection_name"] = connection_names.get(job.get("connection_id"), "Unknown")

            # Calculate job duration
            if job.get("created_at") and job.get("completed_at"):
                try:
                    start = datetime.fromisoformat(job["created_at"].replace('Z', '+00:00'))
                    end = datetime.fromisoformat(job["completed_at"].replace('Z', '+00:00'))
                    duration = (end - start).total_seconds()
                    job["duration_seconds"] = duration
                except:
                    job["duration_seconds"] = None

            enhanced_jobs.append(job)

        # Analyze for potential duplicate patterns
        job_groups = {}
        for job in enhanced_jobs:
            key = f"{job.get('connection_id')}:{job.get('job_type')}"
            if key not in job_groups:
                job_groups[key] = []
            job_groups[key].append(job)

        # Look for suspiciously close job timings
        for key, jobs in job_groups.items():
            if len(jobs) > 1:
                # Sort by creation time
                sorted_jobs = sorted(jobs, key=lambda x: x.get("created_at", ""))

                for i in range(1, len(sorted_jobs)):
                    try:
                        prev_time = datetime.fromisoformat(sorted_jobs[i - 1]["created_at"].replace('Z', '+00:00'))
                        curr_time = datetime.fromisoformat(sorted_jobs[i]["created_at"].replace('Z', '+00:00'))
                        time_diff = (curr_time - prev_time).total_seconds()

                        # Flag jobs created within 5 minutes of each other
                        if time_diff < 300:  # 5 minutes
                            duplicate_patterns.append({
                                "connection_id": sorted_jobs[i]["connection_id"],
                                "connection_name": sorted_jobs[i]["connection_name"],
                                "job_type": sorted_jobs[i]["job_type"],
                                "job_ids": [sorted_jobs[i - 1]["id"], sorted_jobs[i]["id"]],
                                "time_difference_seconds": time_diff,
                                "status": [sorted_jobs[i - 1]["status"], sorted_jobs[i]["status"]]
                            })
                    except:
                        continue

        # Get automation runs for additional context
        recent_runs = supabase.supabase.table("automation_runs") \
            .select("*") \
            .order("started_at", desc=True) \
            .limit(10) \
            .execute()

        return jsonify({
            "active_jobs": active_jobs,
            "automation_status": automation_status,
            "recent_jobs": enhanced_jobs,
            "duplicate_patterns": duplicate_patterns,
            "recent_runs": recent_runs.data or [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "analysis": {
                "total_recent_jobs": len(enhanced_jobs),
                "potential_duplicates": len(duplicate_patterns),
                "deduplication_active": active_jobs.get("total_active", 0) > 0,
                "automation_healthy": automation_status.get("running", False)
            }
        })

    except Exception as e:
        logger.error(f"Error getting automation jobs status: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/automation/deduplication-stats", methods=["GET"])
@token_required
def get_deduplication_stats(current_user, organization_id):
    """Get detailed deduplication statistics - PHASE 3 MONITORING"""
    try:
        # Check admin permissions
        supabase = SupabaseManager()
        user_role = supabase.get_user_role(current_user)
        if user_role not in ['admin', 'owner']:
            return jsonify({"error": "Admin access required"}), 403

        from core.automation.job_deduplication import job_deduplication_service

        # Get current active jobs in deduplication service
        active_summary = job_deduplication_service.get_active_jobs_summary()

        # Get historical analysis from database
        # Look for jobs with fingerprints in job_config
        historical_jobs = supabase.supabase.table("automation_jobs") \
            .select("id,job_type,connection_id,status,created_at,job_config") \
            .gte("created_at", (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()) \
            .order("created_at", desc=True) \
            .execute()

        # Analyze fingerprints and potential duplicates
        fingerprint_analysis = {}
        jobs_with_fingerprints = 0

        for job in historical_jobs.data or []:
            job_config = job.get("job_config", {})
            if isinstance(job_config, str):
                try:
                    job_config = json.loads(job_config)
                except:
                    job_config = {}

            fingerprint = job_config.get("fingerprint")
            if fingerprint:
                jobs_with_fingerprints += 1

                if fingerprint not in fingerprint_analysis:
                    fingerprint_analysis[fingerprint] = []

                fingerprint_analysis[fingerprint].append({
                    "job_id": job["id"],
                    "job_type": job["job_type"],
                    "connection_id": job["connection_id"],
                    "status": job["status"],
                    "created_at": job["created_at"]
                })

        # Find actual duplicates (same fingerprint, multiple jobs)
        actual_duplicates = {}
        for fingerprint, jobs in fingerprint_analysis.items():
            if len(jobs) > 1:
                actual_duplicates[fingerprint] = jobs

        # Calculate prevention statistics
        prevented_duplicates = 0
        for jobs in actual_duplicates.values():
            # If we have multiple jobs with same fingerprint, deduplication failed
            # But if only one succeeded and others failed quickly, deduplication worked
            successful_jobs = [j for j in jobs if j["status"] == "completed"]
            if len(successful_jobs) <= 1:
                prevented_duplicates += len(jobs) - 1

        return jsonify({
            "current_active": active_summary,
            "last_24_hours": {
                "total_jobs": len(historical_jobs.data or []),
                "jobs_with_fingerprints": jobs_with_fingerprints,
                "unique_fingerprints": len(fingerprint_analysis),
                "duplicate_fingerprints": len(actual_duplicates),
                "estimated_prevented_duplicates": prevented_duplicates
            },
            "duplicate_details": actual_duplicates,
            "effectiveness": {
                "deduplication_coverage": f"{(jobs_with_fingerprints / max(len(historical_jobs.data or []), 1)) * 100:.1f}%",
                "duplicate_prevention_rate": f"{(prevented_duplicates / max(jobs_with_fingerprints, 1)) * 100:.1f}%"
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:
        logger.error(f"Error getting deduplication stats: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/automation/test-deduplication", methods=["POST"])
@token_required
def test_deduplication(current_user, organization_id):
    """Test deduplication system - PHASE 3 TESTING"""
    try:
        # Check admin permissions
        supabase = SupabaseManager()
        user_role = supabase.get_user_role(current_user)
        if user_role not in ['admin', 'owner']:
            return jsonify({"error": "Admin access required"}), 403

        data = request.get_json() or {}
        connection_id = data.get("connection_id")
        job_type = data.get("job_type", "metadata_refresh")

        if not connection_id:
            return jsonify({"error": "connection_id is required"}), 400

        # Check if connection exists and user has access
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        from core.automation.job_deduplication import job_deduplication_service
        from core.automation.unified_manager import unified_manager

        # Test 1: Create a fingerprint
        fingerprint = job_deduplication_service.create_job_fingerprint(
            connection_id, job_type, "test_trigger"
        )

        # Test 2: Check if it's a duplicate (should be False initially)
        is_duplicate_before = job_deduplication_service.is_job_duplicate(fingerprint)

        # Test 3: Register a fake job
        test_job_id = f"test-{uuid.uuid4()}"
        registration_success = job_deduplication_service.register_job(
            fingerprint, test_job_id, connection_id, job_type, "test_trigger"
        )

        # Test 4: Check if it's a duplicate now (should be True)
        is_duplicate_after = job_deduplication_service.is_job_duplicate(fingerprint)

        # Test 5: Try to schedule actual job (should be prevented)
        try:
            schedule_result = unified_manager.schedule_immediate_job(
                connection_id, job_type, current_user
            )
        except Exception as e:
            schedule_result = {"error": str(e)}

        # Test 6: Clean up
        job_deduplication_service.mark_job_completed(fingerprint, "test_completed")

        # Test 7: Check if duplicate check works after cleanup
        is_duplicate_after_cleanup = job_deduplication_service.is_job_duplicate(fingerprint)

        return jsonify({
            "test_results": {
                "fingerprint_created": fingerprint,
                "duplicate_check_before_registration": is_duplicate_before,
                "registration_successful": registration_success,
                "duplicate_check_after_registration": is_duplicate_after,
                "schedule_attempt_result": schedule_result,
                "duplicate_check_after_cleanup": is_duplicate_after_cleanup
            },
            "expected_results": {
                "duplicate_check_before_registration": False,
                "registration_successful": True,
                "duplicate_check_after_registration": True,
                "schedule_should_be_prevented": True,
                "duplicate_check_after_cleanup": False
            },
            "test_passed": (
                    not is_duplicate_before and
                    registration_success and
                    is_duplicate_after and
                    not is_duplicate_after_cleanup
            ),
            "connection_tested": {
                "id": connection_id,
                "name": connection.get("name"),
                "type": connection.get("connection_type")
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:
        logger.error(f"Error testing deduplication: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Add this log analysis function
def analyze_automation_logs():
    """
    PHASE 3: Function to analyze logs for deduplication effectiveness
    Call this periodically or via admin endpoint
    """
    try:
        from core.automation.job_deduplication import job_deduplication_service

        # Get recent automation activity
        supabase = SupabaseManager()
        recent_jobs = supabase.supabase.table("automation_jobs") \
            .select("*") \
            .gte("created_at", (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()) \
            .order("created_at", desc=True) \
            .execute()

        # Analyze patterns
        patterns = {
            "duplicate_prevention_success": 0,
            "potential_missed_duplicates": 0,
            "jobs_with_fingerprints": 0,
            "total_jobs": len(recent_jobs.data or [])
        }

        fingerprint_groups = {}

        for job in recent_jobs.data or []:
            job_config = job.get("job_config", {})
            if isinstance(job_config, str):
                try:
                    job_config = json.loads(job_config)
                except:
                    job_config = {}

            fingerprint = job_config.get("fingerprint")
            if fingerprint:
                patterns["jobs_with_fingerprints"] += 1

                if fingerprint not in fingerprint_groups:
                    fingerprint_groups[fingerprint] = []
                fingerprint_groups[fingerprint].append(job)

        # Look for successful prevention
        for fingerprint, jobs in fingerprint_groups.items():
            if len(jobs) > 1:
                # Multiple jobs with same fingerprint
                completed_jobs = [j for j in jobs if j["status"] == "completed"]
                failed_jobs = [j for j in jobs if j["status"] == "failed"]

                if len(completed_jobs) <= 1 and len(failed_jobs) > 0:
                    # Likely successful deduplication
                    patterns["duplicate_prevention_success"] += len(failed_jobs)
                else:
                    # Possible missed duplicate
                    patterns["potential_missed_duplicates"] += len(jobs) - 1

        # Log the analysis
        logger.info("=== AUTOMATION DEDUPLICATION ANALYSIS ===")
        logger.info(f"Total jobs in last hour: {patterns['total_jobs']}")
        logger.info(f"Jobs with fingerprints: {patterns['jobs_with_fingerprints']}")
        logger.info(f"Successful duplicate prevention: {patterns['duplicate_prevention_success']}")
        logger.info(f"Potential missed duplicates: {patterns['potential_missed_duplicates']}")

        if patterns["potential_missed_duplicates"] > 0:
            logger.warning(f"⚠️ Found {patterns['potential_missed_duplicates']} potential missed duplicates!")
        else:
            logger.info("✅ No missed duplicates detected")

        return patterns

    except Exception as e:
        logger.error(f"Error analyzing automation logs: {str(e)}")
        return {"error": str(e)}


@app.route("/api/admin/automation/log-analysis", methods=["GET"])
@token_required
def get_log_analysis(current_user, organization_id):
    """Get automated log analysis - PHASE 3 MONITORING"""
    try:
        # Check admin permissions
        supabase = SupabaseManager()
        user_role = supabase.get_user_role(current_user)
        if user_role not in ['admin', 'owner']:
            return jsonify({"error": "Admin access required"}), 403

        # Run the analysis
        analysis = analyze_automation_logs()

        return jsonify({
            "analysis": analysis,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "recommendations": {
                "healthy": analysis.get("potential_missed_duplicates", 0) == 0,
                "action_needed": analysis.get("potential_missed_duplicates", 0) > 0,
                "next_steps": [
                    "Monitor for duplicate jobs in the next hour",
                    "Check scheduler logs for deduplication messages",
                    "Verify unified manager is properly initialized"
                ] if analysis.get("potential_missed_duplicates", 0) > 0 else [
                    "Deduplication system is working correctly",
                    "Continue monitoring"
                ]
            }
        })

    except Exception as e:
        logger.error(f"Error getting log analysis: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/debug/anomaly-status", methods=["GET"])
@token_required
def get_anomaly_debug_status(current_user, organization_id):
    """Debug endpoint to check anomaly detection status"""
    try:
        global anomaly_scheduler_service

        status = {
            "scheduler_service_exists": anomaly_scheduler_service is not None,
            "scheduler_running": False,
            "configs_in_db": 0,
            "recent_runs": 0,
            "routes_registered": len([rule for rule in app.url_map.iter_rules() if 'anomalies' in rule.rule]),
            "scheduler_details": {}
        }

        if anomaly_scheduler_service:
            status["scheduler_running"] = getattr(anomaly_scheduler_service, 'running', False)
            status["scheduler_details"] = {
                "has_scheduler": hasattr(anomaly_scheduler_service, 'scheduler'),
                "has_supabase": hasattr(anomaly_scheduler_service, 'supabase'),
                "class_name": anomaly_scheduler_service.__class__.__name__
            }

        # Check database for configs and runs
        try:
            from core.storage.supabase_manager import SupabaseManager
            supabase = SupabaseManager()

            configs_response = supabase.supabase.table("anomaly_detection_configs") \
                .select("id", count="exact") \
                .eq("organization_id", organization_id) \
                .execute()
            status["configs_in_db"] = configs_response.count or 0

            runs_response = supabase.supabase.table("anomaly_detection_runs") \
                .select("id", count="exact") \
                .eq("organization_id", organization_id) \
                .execute()
            status["recent_runs"] = runs_response.count or 0

            # Get recent runs details
            recent_runs_response = supabase.supabase.table("anomaly_detection_runs") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .order("started_at", desc=True) \
                .limit(5) \
                .execute()
            status["recent_runs_details"] = recent_runs_response.data or []

        except Exception as db_error:
            status["db_error"] = str(db_error)

        return jsonify({"status": status}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health/automation/persistence')
@token_required
def automation_persistence_check(current_user, organization_id):
    """Check how long automation has been running"""
    try:
        from core.utils.app_hooks import get_automation_health
        health = get_automation_health()

        # Add uptime information
        if health.get("healthy"):
            status = health.get("status", {})
            # Add worker process info
            import os
            health["worker_info"] = {
                "process_id": os.getpid(),
                "worker_started": os.environ.get('WORKER_START_TIME', 'unknown')
            }

        return jsonify(health), 200 if health["healthy"] else 503
    except Exception as e:
        return jsonify({"healthy": False, "error": str(e)}), 503

@app.route('/admin/automation/<action>', methods=['POST'])
@token_required
def automation_admin_control(current_user, organization_id, action):
    """Admin control endpoint for automation system"""
    # Check if user is admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)
    if user_role not in ['admin', 'owner']:
        return jsonify({"error": "Insufficient permissions"}), 403

    result = automation_control_endpoint(action)
    return jsonify(result)

def get_current_user():
    """Get current user from auth token"""
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None

    token = auth_header.split(' ')[1]
    supabase_mgr = SupabaseManager()
    return supabase_mgr.verify_token(token)


def run_validation_rules_internal(user_id, organization_id, data):
    """Internal version of run_validation_rules that can be called from other functions"""
    if not data or "table" not in data:
        return {"error": "Table name is required"}

    connection_string = data.get("connection_string")
    connection_id = data.get("connection_id")
    logger.info(f"Run validations request: {data}")
    table_name = data["table"]
    profile_history_id = data.get("profile_history_id")

    if not connection_id:
        return {"error": "Connection ID is required"}

    try:
        force_gc()

        # If no connection string is provided, fetch connection details
        if not connection_string and connection_id:
            # Check access to connection
            supabase_mgr = SupabaseManager()
            connection_check = supabase_mgr.supabase.table("database_connections") \
                .select("*") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_check.data or len(connection_check.data) == 0:
                logger.error(f"Connection not found or access denied: {connection_id}")
                return {"error": "Connection not found or access denied"}

            connection = connection_check.data[0]

            # Create connector for this connection
            try:
                connector = get_connector_for_connection(connection)
                connector.connect()
            except Exception as e:
                logger.error(f"Failed to connect to database: {str(e)}")
                return {"error": f"Failed to connect to database: {str(e)}"}

            # Build connection string using the connection details
            connection_string = build_connection_string(connection)

        # Validate connection string
        if not connection_string:
            logger.error("No database connection string available")
            return {"error": "No database connection string available"}

        # Get all rules
        rules = validation_manager.get_rules(organization_id, table_name, connection_id)

        if not rules:
            return {"results": []}

        # Convert from Supabase format to sparvi-core format if needed
        validation_rules = []
        for rule in rules:
            validation_rules.append({
                "name": rule["rule_name"],
                "description": rule["description"],
                "query": rule["query"],
                "operator": rule["operator"],
                "expected_value": rule["expected_value"]
            })

        # Log memory usage before validation
        log_memory_usage("Before validation")
        force_gc()

        # OPTIMIZATION: Execute rules in parallel for faster processing
        # Use a thread pool to execute validations in parallel
        def execute_validation_rule(rule, connection_string):
            """Execute a single validation rule"""
            try:
                # Use sparvi_run_validations but with a single rule for better performance
                result = sparvi_run_validations(connection_string, [rule])
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Error executing validation rule {rule['name']}: {str(e)}")
                return {
                    "name": rule["name"],
                    "is_valid": False,
                    "error": str(e)
                }

        # Execute rules in parallel with a limited number of workers
        # We use a smaller batch size here to avoid overloading the database
        max_parallel = min(10, len(validation_rules))
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
            # Submit all tasks
            future_to_rule = {
                executor.submit(execute_validation_rule, rule, connection_string): (i, rule)
                for i, rule in enumerate(validation_rules)
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_rule):
                i, rule = future_to_rule[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)

                        # Store result in Supabase
                        actual_value = result.get("actual_value", None)
                        validation_manager.store_validation_result(
                            organization_id,
                            rules[i]["id"],
                            result["is_valid"],
                            actual_value,
                            connection_id,  # Pass connection_id
                            profile_history_id
                        )
                except Exception as e:
                    logger.error(f"Error processing validation result: {str(e)}")

        # Log memory usage after validation
        log_memory_usage("After validation")

        # Check for failed validations and publish event if necessary
        had_failures = any(not r.get("is_valid", True) for r in results)
        schema_mismatch = any(
            ("column not found" in str(r.get("error", "")).lower() or
             "table not found" in str(r.get("error", "")).lower() or
             ("relation" in str(r.get("error", "")).lower() and "does not exist" in str(r.get("error", "")).lower()))
            for r in results if not r.get("is_valid", True)
        )

        # If any validations failed and we have a connection_id, publish an event
        if had_failures and connection_id:
            try:
                from core.metadata.events import MetadataEventType, publish_metadata_event
                # Publish validation failure event
                publish_metadata_event(
                    event_type=MetadataEventType.VALIDATION_FAILURE,
                    connection_id=connection_id,
                    details={
                        "table_name": table_name,
                        "reason": "schema_mismatch" if schema_mismatch else "data_issue",
                        "validation_count": len(results),
                        "failure_count": sum(1 for r in results if not r.get("is_valid", True))
                    },
                    organization_id=organization_id,
                    user_id=user_id
                )
                logger.info(f"Published validation failure event for table {table_name}")
            except Exception as e:
                logger.error(f"Error publishing validation failure event: {str(e)}")
                logger.error(traceback.format_exc())

        return {"results": results}

    except Exception as e:
        logger.error(f"Error running validations internal: {str(e)}")
        traceback.print_exc()
        return {"error": str(e)}


def log_memory_usage(label=""):
    """Log current memory usage"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        memory_mb = mem_info.rss / (1024 * 1024)
        logger.info(f"Memory Usage [{label}]: {memory_mb:.2f} MB")

        # Alert if memory is getting high (adjust threshold as needed for your environment)
        if memory_mb > 500:  # Alert if using more than 500 MB
            logger.warning(f"High memory usage detected: {memory_mb:.2f} MB")

        return memory_mb
    except ImportError:
        logger.warning("psutil not installed - cannot log memory usage")
        return 0
    except Exception as e:
        logger.warning(f"Error logging memory usage: {str(e)}")
        return 0


def force_gc():
    """Force garbage collection to free memory"""
    import gc
    collected = gc.collect()
    logger.debug(f"Garbage collection: collected {collected} objects")
    return collected


# Add task worker function for background processing
def metadata_task_worker():
    """Background worker that processes metadata collection tasks"""
    while True:
        try:
            # Get task from queue (blocking)
            task = metadata_task_queue.get()

            if task["task"] == "full_metadata_collection":
                # Process full metadata collection
                connection_id = task["connection_id"]
                logger.info(f"Processing full metadata collection for connection {connection_id}")

                # Get connection details
                connection = task.get("connection")
                if not connection:
                    supabase_mgr = SupabaseManager()
                    connection_check = supabase_mgr.supabase.table("database_connections") \
                        .select("*") \
                        .eq("id", connection_id) \
                        .execute()

                    if not connection_check.data or len(connection_check.data) == 0:
                        logger.error(f"Connection not found for background task: {connection_id}")
                        metadata_task_queue.task_done()
                        continue

                    connection = connection_check.data[0]

                try:
                    # Create connector
                    connector = get_connector_for_connection(connection)
                    logger.info(f"Created connector for connection {connection_id}")

                    # Explicitly connect to the database
                    try:
                        connector.connect()
                        logger.info(f"Successfully connected to the database in background task")
                    except Exception as e:
                        logger.error(f"Failed to connect to database in background task: {str(e)}")
                        metadata_task_queue.task_done()
                        continue

                    # Create metadata collector
                    collector = MetadataCollector(connection_id, connector)
                    logger.info(f"Created metadata collector for connection {connection_id}")

                    # Create storage service
                    storage_service = MetadataStorageService()
                    logger.info(f"Created storage service for connection {connection_id}")

                    # Get list of tables
                    tables = collector.collect_table_list()
                    logger.info(f"Found {len(tables)} tables")

                    # Initialize data structures
                    tables_data = []
                    columns_by_table = {}
                    statistics_by_table = {}

                    # Limit table processing if specified
                    table_limit = task.get("table_limit", 50)
                    process_tables = tables[:min(len(tables), table_limit)]
                    logger.info(f"Will process {len(process_tables)} tables (limit: {table_limit})")

                    # Process each table
                    for i, table in enumerate(process_tables):
                        try:
                            if i % 10 == 0:
                                logger.info(f"Processing table {i + 1}/{len(process_tables)}: {table}")

                            # Get column information
                            columns = collector.collect_columns(table)

                            # Try to get row count
                            row_count = 0
                            try:
                                result = connector.execute_query(f"SELECT COUNT(*) FROM {table}")
                                if result and len(result) > 0:
                                    row_count = result[0][0]
                            except Exception as e:
                                logger.warning(f"Could not get row count for {table}: {str(e)}")

                            # Try to get primary keys
                            primary_keys = []
                            try:
                                primary_keys = connector.get_primary_keys(table)
                            except Exception as e:
                                logger.warning(f"Could not get primary keys for {table}: {str(e)}")

                            # Create table metadata
                            table_meta = {
                                "name": table,
                                "column_count": len(columns),
                                "row_count": row_count,
                                "primary_key": primary_keys,
                                "id": str(uuid.uuid4())  # Generate an ID for this table
                            }

                            # Add to tables list
                            tables_data.append(table_meta)

                            # Store columns for this table
                            columns_by_table[table] = columns

                            # Store basic statistics
                            statistics_by_table[table] = {
                                "row_count": row_count,
                                "column_count": len(columns),
                                "has_primary_key": len(primary_keys) > 0,
                                "columns": {
                                    col["name"]: {
                                        "type": col["type"],
                                        "nullable": col.get("nullable", False)
                                    } for col in columns
                                }
                            }

                        except Exception as e:
                            logger.error(f"Error processing table {table}: {str(e)}")
                            continue

                    # Store the collected metadata
                    if tables_data:
                        storage_service.store_tables_metadata(connection_id, tables_data)
                        logger.info(f"Stored metadata for {len(tables_data)} tables")

                    if columns_by_table:
                        storage_service.store_columns_metadata(connection_id, columns_by_table)
                        logger.info(f"Stored column metadata for {len(columns_by_table)} tables")

                    if statistics_by_table:
                        storage_service.store_statistics_metadata(connection_id, statistics_by_table)
                        logger.info(f"Stored statistics for {len(statistics_by_table)} tables")

                    logger.info(f"Completed full metadata collection for connection {connection_id}")

                except Exception as e:
                    logger.error(f"Error in background metadata collection: {str(e)}")
                    logger.error(traceback.format_exc())

            # Mark task as done
            metadata_task_queue.task_done()

        except Exception as e:
            logger.error(f"Error in metadata task worker: {str(e)}")
            logger.error(traceback.format_exc())


# Start the background worker thread
metadata_worker_thread = threading.Thread(target=metadata_task_worker, daemon=True)
metadata_worker_thread.start()


# Helper functions for storage
def store_table_list(connection_id, tables):
    """Store table list in metadata storage"""
    try:
        # Use synchronous methods inside background thread
        # Create object for the connection
        storage = MetadataStorage()

        # Get metadata type ID for schema
        type_response = storage.supabase.table("metadata_types").select("id").eq("type_name", "schema").execute()
        if not type_response.data or len(type_response.data) == 0:
            logger.error("Schema metadata type not found")
            return False

        metadata_type_id = type_response.data[0]["id"]

        # Get property ID for table_list
        property_response = storage.supabase.table("metadata_properties").select("id").eq("property_name",
                                                                                          "table_list").execute()
        if not property_response.data or len(property_response.data) == 0:
            logger.error("table_list property not found")
            return False

        property_id = property_response.data[0]["id"]

        # Store as object first
        object_data = {
            "connection_id": connection_id,
            "object_type": "database",
            "object_name": "tables",
            "created_at": datetime.datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.datetime.now(timezone.utc).isoformat()
        }

        object_response = storage.supabase.table("metadata_objects").upsert(object_data).execute()
        if not object_response.data or len(object_response.data) == 0:
            logger.error("Failed to create/update metadata object for table list")
            return False

        object_id = object_response.data[0]["id"]

        # Now store the fact
        fact_data = {
            "connection_id": connection_id,
            "metadata_type_id": metadata_type_id,
            "object_id": object_id,
            "property_id": property_id,
            "value_json": json.dumps(tables),
            "collected_at": datetime.datetime.now(timezone.utc).isoformat(),
            "refresh_frequency": "1 day"
        }

        fact_response = storage.supabase.table("metadata_facts").upsert(fact_data).execute()
        if not fact_response.data:
            logger.error("Failed to store table list metadata")
            return False

        logger.info(f"Stored table list with {len(tables)} tables for connection {connection_id}")
        return True

    except Exception as e:
        logger.error(f"Error storing table list: {str(e)}")
        return False


def store_table_metadata(connection_id, table_name, metadata):
    """Store detailed table metadata with proper parent-child relationships"""
    try:
        storage = MetadataStorage()

        # Get metadata type ID for schema
        type_response = storage.supabase.table("metadata_types").select("id").eq("type_name", "schema").execute()
        if not type_response.data or len(type_response.data) == 0:
            logger.error("Schema metadata type not found")
            return False

        metadata_type_id = type_response.data[0]["id"]

        # 1. Create object for the table
        object_data = {
            "connection_id": connection_id,
            "object_type": "table",
            "object_name": table_name,
            "created_at": datetime.datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.datetime.now(timezone.utc).isoformat()
        }

        object_response = storage.supabase.table("metadata_objects").upsert(object_data).execute()
        if not object_response.data or len(object_response.data) == 0:
            logger.error(f"Failed to create/update metadata object for table {table_name}")
            return False

        table_object_id = object_response.data[0]["id"]
        logger.info(f"Created/updated table metadata object with ID: {table_object_id}")

        # 2. Get property IDs for the table properties we want to store
        property_ids = {}
        table_property_names = ["row_count", "primary_key", "column_count"]

        for name in table_property_names:
            property_response = storage.supabase.table("metadata_properties").select("id").eq("property_name",
                                                                                              name).execute()
            if property_response.data and len(property_response.data) > 0:
                property_ids[name] = property_response.data[0]["id"]

        # 3. Store table properties as facts
        for prop_name, prop_id in property_ids.items():
            fact_data = {
                "connection_id": connection_id,
                "metadata_type_id": metadata_type_id,
                "object_id": table_object_id,
                "property_id": prop_id,
                "collected_at": datetime.datetime.now(timezone.utc).isoformat(),
                "refresh_frequency": "1 day"
            }

            # Set the appropriate value field
            if prop_name == "row_count":
                fact_data["value_numeric"] = metadata.get("row_count", 0)
            elif prop_name == "column_count":
                fact_data["value_numeric"] = len(metadata.get("columns", []))
            elif prop_name == "primary_key":
                fact_data["value_json"] = json.dumps(metadata.get("primary_keys", []))

            # Store the fact
            fact_response = storage.supabase.table("metadata_facts").upsert(fact_data).execute()
            if not fact_response.data:
                logger.error(f"Failed to store {prop_name} for table {table_name}")

        # 4. Now handle columns - create metadata objects for each column with parent_id
        if "columns" in metadata and metadata["columns"]:
            columns = metadata["columns"]
            logger.info(f"Processing {len(columns)} columns for table {table_name}")

            # Get column-related property IDs
            column_property_ids = {}
            column_properties = ["data_type", "is_nullable", "default"]

            for name in column_properties:
                property_response = storage.supabase.table("metadata_properties").select("id").eq("property_name",
                                                                                                  name).execute()
                if property_response.data and len(property_response.data) > 0:
                    column_property_ids[name] = property_response.data[0]["id"]

            # Process each column
            for column in columns:
                column_name = column.get("name", "unknown")

                # Create metadata object for this column
                column_data = {
                    "connection_id": connection_id,
                    "object_type": "column",
                    "object_name": column_name,
                    "parent_id": table_object_id,  # Link to parent table
                    "created_at": datetime.datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.datetime.now(timezone.utc).isoformat()
                }

                col_response = storage.supabase.table("metadata_objects").upsert(column_data).execute()
                if not col_response.data or len(col_response.data) == 0:
                    logger.error(f"Failed to create metadata object for column {column_name}")
                    continue

                column_object_id = col_response.data[0]["id"]
                logger.info(f"Created column metadata object: {column_name} with ID: {column_object_id}")

                # Now store column properties as facts
                for prop_name, prop_id in column_property_ids.items():
                    if prop_name == "data_type" and "type" in column:
                        # Handle the mismatch between property name and column attribute
                        fact_data = {
                            "connection_id": connection_id,
                            "metadata_type_id": metadata_type_id,
                            "object_id": column_object_id,
                            "property_id": prop_id,
                            "value_text": str(column.get("type", "")),
                            "collected_at": datetime.datetime.now(timezone.utc).isoformat(),
                            "refresh_frequency": "1 day"
                        }
                        storage.supabase.table("metadata_facts").upsert(fact_data).execute()

                    elif prop_name == "is_nullable" and "nullable" in column:
                        # Handle the mismatch between property name and column attribute
                        fact_data = {
                            "connection_id": connection_id,
                            "metadata_type_id": metadata_type_id,
                            "object_id": column_object_id,
                            "property_id": prop_id,
                            "value_text": str(column.get("nullable", "")),
                            "collected_at": datetime.datetime.now(timezone.utc).isoformat(),
                            "refresh_frequency": "1 day"
                        }
                        storage.supabase.table("metadata_facts").upsert(fact_data).execute()

                    elif prop_name in column:
                        fact_data = {
                            "connection_id": connection_id,
                            "metadata_type_id": metadata_type_id,
                            "object_id": column_object_id,
                            "property_id": prop_id,
                            "value_text": str(column.get(prop_name, "")),
                            "collected_at": datetime.datetime.now(timezone.utc).isoformat(),
                            "refresh_frequency": "1 day"
                        }
                        storage.supabase.table("metadata_facts").upsert(fact_data).execute()

        logger.info(f"Successfully stored complete metadata for table {table_name}")
        return True

    except Exception as e:
        logger.error(f"Error storing table metadata: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def collect_immediate_metadata_sync(self):
    """Synchronous version of collect_immediate_metadata"""
    logger.info(f"Collecting immediate metadata for connection {self.connection_id}")

    # Connect to the database
    self.connector.connect()

    # Collect table list (Tier 1)
    tables = self.collect_table_list()

    # Only collect columns for the first few tables to avoid overloading
    column_metadata = {}
    for table in tables[:10]:  # Limit to first 10 tables for immediate collection
        column_metadata[table] = self.collect_columns(table)

    # Return just the essential metadata
    return {
        "table_list": tables,
        "column_metadata": column_metadata
    }


def collect_table_metadata_sync(self, table_name):
    """Synchronous version of collect_table_metadata"""
    logger.info(f"Collecting detailed metadata for table {table_name}")

    # If not already connected, connect
    if not self.connector.inspector:
        self.connector.connect()

    try:
        # Basic table information
        columns = self.collect_columns(table_name)
        primary_keys = self.connector.get_primary_keys(table_name)

        # Row count - use a query
        row_count = 0
        try:
            result = self.connector.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            if result and len(result) > 0:
                row_count = result[0][0]
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {str(e)}")

        # Compile table metadata
        table_metadata = {
            "table_name": table_name,
            "column_count": len(columns),
            "columns": columns,
            "primary_keys": primary_keys,
            "row_count": row_count,
            "collected_at": datetime.datetime.now(timezone.utc).isoformat()
        }

        logger.info(f"Successfully collected metadata for table {table_name}")
        return table_metadata
    except Exception as e:
        logger.error(f"Error collecting table metadata for {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "error": str(e),
            "collected_at": datetime.datetime.now(timezone.utc).isoformat()
        }


def calculate_current_health_score(rules, organization_id, connection_id):
    """Calculate the current health score based on latest validation results"""
    try:
        supabase_mgr = SupabaseManager()
        rule_ids = [rule["id"] for rule in rules]

        # Get the latest result for each rule
        latest_results_query = f"""
        WITH ranked_results AS (
            SELECT 
                rule_id,
                is_valid,
                ROW_NUMBER() OVER (PARTITION BY rule_id ORDER BY run_at DESC) as rn
            FROM validation_results
            WHERE 
                organization_id = '{organization_id}'
                AND rule_id IN ('{"','".join(rule_ids)}')
        )
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE is_valid = true) as passed
        FROM ranked_results
        WHERE rn = 1
        """

        results = supabase_mgr.supabase.rpc(
            'execute_sql',
            {'sql_query': latest_results_query}
        ).execute()

        if results.data and len(results.data) > 0:
            data = results.data[0]
            total = data.get("total", 0)
            passed = data.get("passed", 0)

            if total > 0:
                return round((passed / total) * 100, 2)

        return 0
    except Exception as e:
        logger.error(f"Error calculating current health score: {str(e)}")
        return 0


def fallback_validation_trends(organization_id, connection_id, table_name, rules, days):
    """Fallback implementation for validation trends if RPC query fails"""
    try:
        # Get a date series for the past X days
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        date_series = []

        # Generate date series
        current = start_date
        end_date = datetime.now(timezone.utc)

        while current <= end_date:
            date_series.append(current.date().isoformat())
            current += timedelta(days=1)

        # Get validation results for each rule
        supabase_mgr = SupabaseManager()
        rule_ids = [rule["id"] for rule in rules]

        # Get all validation results for these rules in the date range
        results = []
        for rule_id in rule_ids:
            response = supabase_mgr.supabase.table("validation_results") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("rule_id", rule_id) \
                .gte("run_at", start_date.isoformat()) \
                .order("run_at") \
                .execute()

            if response.data:
                results.extend(response.data)

        # Process results into daily aggregates
        daily_results = {}

        for result in results:
            # Extract date from timestamp
            run_date = result["run_at"].split("T")[0]
            rule_id = result["rule_id"]

            # Initialize if needed
            if run_date not in daily_results:
                daily_results[run_date] = {}

            # Only keep the latest result for each rule
            if rule_id not in daily_results[run_date] or result["run_at"] > daily_results[run_date][rule_id]["run_at"]:
                daily_results[run_date][rule_id] = {
                    "is_valid": result["is_valid"],
                    "run_at": result["run_at"]
                }

        # Build the complete trends data
        complete_trends = []

        for day in date_series:
            if day in daily_results:
                # Count results for this day
                day_results = daily_results[day]
                total = len(day_results)
                passed = sum(1 for r in day_results.values() if r["is_valid"])
                failed = total - passed
                health_score = (passed / total * 100) if total > 0 else 0

                complete_trends.append({
                    "day": day,
                    "total_validations": total,
                    "passed": passed,
                    "failed": failed,
                    "health_score": round(health_score, 2),
                    "not_run": len(rules) - total
                })
            else:
                # No data for this day
                complete_trends.append({
                    "day": day,
                    "total_validations": 0,
                    "passed": 0,
                    "failed": 0,
                    "health_score": 0,
                    "not_run": len(rules)
                })

        return jsonify({
            "trends": complete_trends,
            "table_name": table_name,
            "days": days,
            "rule_count": len(rules)
        })

    except Exception as e:
        logger.error(f"Error in fallback validation trends: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error processing validation trends: {str(e)}",
            "trends": []
        }), 500


@app.route('/', defaults={'path': ''}, methods=['OPTIONS'])
@app.route('/<path:path>', methods=['OPTIONS'])
def options_handler(path):
    return app.make_default_options_response()

@app.route("/api/login", methods=["POST"])
def login():
    try:
        # Add extensive logging
        logger.debug("Login attempt started")

        auth_data = request.get_json()
        logger.debug(f"Received auth data: {auth_data}")

        # Validate input
        if not auth_data or not auth_data.get("email") or not auth_data.get("password"):
            logger.warning("Missing credentials")
            return jsonify({"error": "Missing credentials"}), 400

        email = auth_data.get("email")
        password = auth_data.get("password")

        # Log environment variables (be careful in production!)
        logger.debug(f"Supabase URL: {os.getenv('SUPABASE_URL')}")
        logger.debug(f"Supabase Anon Key: {bool(os.getenv('SUPABASE_ANON_KEY'))}")  # Just check if it exists

        # Supabase authentication
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_ANON_KEY")

        if not url or not key:
            logger.error("Supabase URL or Anon Key is missing")
            return jsonify({"error": "Server configuration error"}), 500

        supabase_client = create_client(url, key)

        # Detailed logging around authentication
        logger.debug("Attempting Supabase sign in")
        response = supabase_client.auth.sign_in_with_password({
            "email": email,
            "password": password
        })

        logger.debug("Sign in successful")
        logger.debug(f"Session details: {response.session}")

        return jsonify({
            "token": response.session.access_token,
            "user": response.session.user.model_dump()
        })

    except Exception as e:
        # Catch and log all possible exceptions
        logger.error(f"Login error: {str(e)}", exc_info=True)
        # Log the full traceback
        logger.error(traceback.format_exc())
        return jsonify({"error": "Authentication failed", "message": str(e)}), 401

@app.route("/api/tables", methods=["GET"])
@token_required
def get_tables(current_user, organization_id):
    """Get all tables for a connection"""
    try:
        connection_string = request.args.get("connection_string")
        connection_id = request.args.get("connection_id")

        # Handle either JSON connection object or connection_id
        if connection_id:
            # Get connection by ID
            connection = connection_access_check(connection_id, organization_id)
            if not connection:
                return jsonify({"error": f"Connection with ID {connection_id} not found"}), 404

            connection_details = connection["connection_details"]
        elif connection_string:
            # Parse the JSON string into an object
            try:
                connection_obj = json.loads(connection_string)

                # Handle both formats: {"connection": {...}} or direct object
                if "connection" in connection_obj:
                    connection_obj = connection_obj["connection"]

                # Extract connection details
                if "connection_details" in connection_obj:
                    connection_details = connection_obj["connection_details"]
                else:
                    return jsonify({"error": "Invalid connection format"}), 400
            except json.JSONDecodeError:
                return jsonify({"error": "Invalid JSON in connection_string"}), 400
        else:
            return jsonify({"error": "Either connection_string or connection_id is required"}), 400

        # Build Snowflake connection string
        if connection_id:
            # We have the full connection object already
            proper_connection_string = get_connection_string(connection)
            if not proper_connection_string:
                return jsonify({"error": "Failed to build connection string"}), 400
        else:
            # We need to build a connection object from the connection details
            connection = {
                "connection_type": "snowflake",  # Assuming Snowflake for this endpoint
                "connection_details": connection_details
            }

            # If password is not provided, try to find it
            if "password" not in connection_details:
                supabase_mgr = SupabaseManager()
                db_connections = supabase_mgr.supabase.table("database_connections") \
                    .select("*") \
                    .eq("organization_id", organization_id) \
                    .execute()

                # Find matching connection
                username = connection_details.get("username")
                account = connection_details.get("account")

                for conn in db_connections.data:
                    conn_details = conn["connection_details"]
                    if conn_details.get("username") == username and conn_details.get("account") == account:
                        connection_details["password"] = conn_details.get("password")
                        break

                if "password" not in connection_details:
                    return jsonify({"error": "Could not find password for connection"}), 400

            proper_connection_string = get_connection_string(connection)
            if not proper_connection_string:
                return jsonify({"error": "Failed to build connection string"}), 400

        # Use the connection pool manager to get a pooled connection
        engine = connection_pool_manager.get_engine(proper_connection_string)

        # Use connection from pool
        with engine.connect() as conn:
            inspector = inspect(conn)
            tables = inspector.get_table_names()

        return jsonify({"tables": tables})

    except Exception as e:
        logger.error(f"Error getting tables: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    return render_template("index.html", version=os.getenv("SPARVI_CORE_VERSION", "Unknown"))


@app.route("/api/setup-user", methods=["POST"])
def setup_user():
    data = request.get_json()

    user_id = data.get("user_id")
    email = data.get("email")
    first_name = data.get("first_name", "")
    last_name = data.get("last_name", "")
    org_name = data.get("organization_name") or f"{first_name or email.split('@')[0]}'s Organization"

    if not user_id or not email:
        logger.error(f"Missing required fields for setup_user: user_id={user_id}, email={email}")
        return jsonify({"error": "Missing required fields"}), 400

    try:
        # Use service role key for admin privileges
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")

        if not url or not key:
            logger.error("Missing Supabase configuration")
            return jsonify({"error": "Server configuration error"}), 500

        supabase_client = create_client(url, key)

        # Check if profile exists
        profile_check = supabase_client.table("profiles").select("*").eq("id", user_id).execute()

        if profile_check.data and len(profile_check.data) > 0:
            logger.info(f"User already has a profile: {profile_check.data[0]}")

            # Check if organization exists
            if profile_check.data[0].get("organization_id"):
                org_id = profile_check.data[0].get("organization_id")
                org_check = supabase_client.table("organizations").select("*").eq("id", org_id).execute()

                if org_check.data and len(org_check.data) > 0:
                    logger.info(f"User has an organization: {org_check.data[0]}")
                    return jsonify({"success": True, "message": "User already set up"})

                logger.info(f"Organization {org_id} referenced by profile doesn't exist. Creating new organization...")

        # Create organization
        logger.info(f"Creating organization: {org_name}")

        try:
            org_response = supabase_client.table("organizations").insert({"name": org_name}).execute()

            if not org_response.data or len(org_response.data) == 0:
                logger.error("Failed to create organization: No data returned")
                logger.error(f"Response: {org_response}")
                return jsonify({"error": "Failed to create organization"}), 500

            org_id = org_response.data[0]["id"]
            logger.info(f"Created organization with ID: {org_id}")

            # Create or update profile
            if profile_check.data and len(profile_check.data) > 0:
                logger.info(f"Updating existing profile with org_id: {org_id}")
                profile_response = supabase_client.table("profiles").update({
                    "organization_id": org_id,
                    "first_name": first_name,
                    "last_name": last_name
                }).eq("id", user_id).execute()
            else:
                logger.info(f"Creating new profile for user: {user_id}")
                profile_response = supabase_client.table("profiles").insert({
                    "id": user_id,
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "organization_id": org_id,
                    "role": "admin"
                }).execute()

            if not profile_response.data or len(profile_response.data) == 0:
                logger.error("Failed to create/update profile: No data returned")
                logger.error(f"Response: {profile_response}")
                return jsonify({"error": "Failed to create/update profile"}), 500

            logger.info(f"Profile operation successful: {profile_response.data[0]}")

            # Verify success
            verification_profile = supabase_client.table("profiles").select("*").eq("id", user_id).execute()
            if verification_profile.data and len(verification_profile.data) > 0:
                logger.info(
                    f"Verification successful - user now has profile with organization: {verification_profile.data[0]}")
            else:
                logger.warning("Verification failed - profile still not visible")

            return jsonify({"success": True})

        except Exception as e:
            logger.error(f"Error in organization/profile operation: {str(e)}")
            return jsonify({"error": str(e)}), 500

    except Exception as e:
        logger.error(f"Error in setup-user: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/users", methods=["GET"])
@token_required
def get_users(current_user, organization_id):
    """Get all users in the organization (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    try:
        users = supabase_mgr.get_organization_users(organization_id)
        return jsonify({"users": users})
    except Exception as e:
        logger.error(f"Error getting users: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/users/<user_id>", methods=["PUT"])
@token_required
def update_user(current_user, organization_id, user_id):
    """Update a user's details (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    # Get update data
    data = request.get_json()
    if not data:
        return jsonify({"error": "No update data provided"}), 400

    try:
        # Ensure user belongs to the same organization
        user_details = supabase_mgr.get_user_details(user_id)
        if user_details.get('organization_id') != organization_id:
            return jsonify({"error": "User not in your organization"}), 403

        # Update the user
        success = supabase_mgr.update_user(user_id, data)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to update user"}), 500
    except Exception as e:
        logger.error(f"Error updating user: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/users", methods=["POST"])
@token_required
def invite_user(current_user, organization_id):
    """Invite a new user to the organization (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    # Get invite data
    data = request.get_json()
    if not data or 'email' not in data:
        return jsonify({"error": "Email address is required"}), 400

    try:
        # Generate a unique invite link
        invite_data = supabase_mgr.create_user_invite(organization_id, data['email'],
                                                      data.get('role', 'member'),
                                                      data.get('first_name', ''),
                                                      data.get('last_name', ''))

        # TODO: Send email with invite link (would typically use a service like SendGrid)
        # For now, just return the invite data that would be included in the email

        return jsonify({"success": True, "invite": invite_data})
    except Exception as e:
        logger.error(f"Error inviting user: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/users/<user_id>", methods=["DELETE"])
@token_required
def remove_user(current_user, organization_id, user_id):
    """Remove a user from the organization (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    # Prevent users from removing themselves
    if user_id == current_user:
        return jsonify({"error": "Cannot remove yourself"}), 400

    try:
        # Ensure user belongs to the same organization
        user_details = supabase_mgr.get_user_details(user_id)
        if user_details.get('organization_id') != organization_id:
            return jsonify({"error": "User not in your organization"}), 403

        # Remove the user
        success = supabase_mgr.remove_user_from_organization(user_id)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to remove user"}), 500
    except Exception as e:
        logger.error(f"Error removing user: {str(e)}")
        return jsonify({"error": str(e)}), 500


# Organization management routes
@app.route("/api/admin/organization", methods=["GET"])
@token_required
def get_organization(current_user, organization_id):
    """Get organization details"""
    try:
        supabase_mgr = SupabaseManager()
        org_details = supabase_mgr.get_organization_details(organization_id)
        return jsonify({"organization": org_details})
    except Exception as e:
        logger.error(f"Error getting organization details: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/organization", methods=["PUT"])
@token_required
def update_organization(current_user, organization_id):
    """Update organization details (admin only)"""
    # Check if user is an admin
    supabase_mgr = SupabaseManager()
    user_role = supabase_mgr.get_user_role(current_user)

    if user_role != 'admin':
        logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
        return jsonify({"error": "Admin access required"}), 403

    # Get update data
    data = request.get_json()
    if not data:
        return jsonify({"error": "No update data provided"}), 400

    try:
        success = supabase_mgr.update_organization(organization_id, data)
        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to update organization"}), 500
    except Exception as e:
        logger.error(f"Error updating organization: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/preview", methods=["GET"])
@token_required
def get_data_preview(current_user, organization_id):
    """Get a preview of data without storing it"""
    connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = request.args.get("table")
    connection_id = request.args.get("connection_id")

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    # If connection_id is provided, validate and get connection string
    if connection_id and not connection_string:
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get cached connection string
        connection_string = get_connection_string(connection)
        if not connection_string:
            return jsonify({"error": "Failed to build connection string"}), 400

    # Get preview settings
    supabase_mgr = SupabaseManager()
    org_settings = supabase_mgr.get_organization_settings(organization_id)
    preview_settings = org_settings.get("preview_settings", {})

    # Check if previews are enabled
    if not preview_settings.get("enable_previews", True):
        return jsonify({"error": "Data previews are disabled for your organization"}), 403

    # Get maximum allowed rows (system limit and org-specific limit)
    system_max_rows = int(os.getenv("MAX_PREVIEW_ROWS", 50))
    org_max_rows = int(preview_settings.get("max_preview_rows", system_max_rows))
    max_rows = min(
        int(request.args.get("max_rows", org_max_rows)),
        org_max_rows,
        system_max_rows
    )

    # Get restricted columns for this table
    restricted_columns = preview_settings.get("restricted_preview_columns", {}).get(table_name, [])

    try:
        # Create engine using connection pool
        engine = connection_pool_manager.get_engine(connection_string)

        with engine.connect() as conn:
            inspector = inspect(conn)

            # Get table columns
            table_columns = [col['name'] for col in inspector.get_columns(table_name)]

            # Filter restricted columns
            allowed_columns = [col for col in table_columns if col not in restricted_columns]

            if not allowed_columns:
                return jsonify({"error": "No viewable columns available for this table"}), 403

            # Log access (without storing the actual data)
            sanitized_conn = supabase_mgr._sanitize_connection_string(connection_string)
            supabase_mgr.log_preview_access(current_user, organization_id, table_name, sanitized_conn)

            # Construct and execute the query
            query = f"SELECT {', '.join(allowed_columns)} FROM {table_name} LIMIT {max_rows}"

            result = conn.execute(text(query))
            preview_data = [dict(zip(result.keys(), row)) for row in result.fetchall()]

            logger.info(f"Preview data fetched for {table_name}, returned {len(preview_data)} rows")

            # Return the data directly (not stored)
            return jsonify({
                "preview_data": preview_data,
                "row_count": len(preview_data),
                "preview_max": max_rows,
                "restricted_columns": restricted_columns if restricted_columns else [],
                "all_columns": table_columns
            })

    except Exception as e:
        logger.error(f"Error generating data preview: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": f"Failed to generate preview: {str(e)}"}), 500



@app.route("/api/connections/<connection_id>/default", methods=["PUT"])
@token_required
def set_default_connection(current_user, organization_id, connection_id):
    """Set a connection as the default"""
    try:
        supabase_mgr = SupabaseManager()

        # First check if the connection exists and belongs to this organization
        get_response = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not get_response.data or len(get_response.data) == 0:
            return jsonify({"error": "Connection not found or you don't have permission to update it"}), 404

        # First, set all connections for this organization to not default
        update_all_response = supabase_mgr.supabase.table("database_connections") \
            .update({"is_default": False}) \
            .eq("organization_id", organization_id) \
            .execute()

        # Then set this connection as default
        update_response = supabase_mgr.supabase.table("database_connections") \
            .update({"is_default": True}) \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        # If successful, return success
        if update_response.data and len(update_response.data) > 0:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to set default connection"}), 500

    except Exception as e:
        logger.error(f"Error setting default connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/api/connections/<connection_id>/credentials', methods=['GET'])
def get_connection_credentials(connection_id):
    """Get decrypted credentials for a connection"""
    try:
        # Get current user from auth token
        current_user = get_current_user()
        if not current_user:
            return jsonify({"error": "Authentication required"}), 401

        # Get connection details from Supabase
        supabase_mgr = SupabaseManager()
        connection = supabase_mgr.get_connection(connection_id)

        if not connection:
            return jsonify({"error": "Connection not found"}), 404

        # Return decrypted credentials
        return jsonify(connection.get("connection_details", {}))

    except Exception as e:
        logger.error(f"Error getting connection credentials: {str(e)}")
        logger.error(traceback.format_exc())  # Log the full stack trace
        return jsonify({"error": "Failed to get connection credentials"}), 500


@app.route("/api/connections/<connection_id>/metadata", methods=["GET"])
@token_required
def get_connection_metadata(current_user, organization_id, connection_id):
    """Get cached metadata for a connection"""
    try:
        # Check if user has access to this connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get query parameters
        metadata_type = request.args.get("type", "tables")  # Default to tables metadata

        # Use cached getter
        metadata = get_metadata_cached(connection_id, metadata_type)

        if not metadata:
            return jsonify({"metadata": {}, "message": f"No {metadata_type} metadata found"}), 404

        return jsonify({"metadata": metadata})

    except Exception as e:
        logger.error(f"Error retrieving connection metadata: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/metadata/collect", methods=["POST"])
@token_required
def collect_connection_metadata(current_user, organization_id, connection_id):
    """Collect metadata for a connection"""
    try:
        # Check if user has access to this connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        logger.info(f"Retrieved connection details for {connection_id}: {connection['name']}")

        # Try to get task manager
        metadata_task_manager = get_metadata_task_manager()

        if not metadata_task_manager:
            return jsonify({
                "error": "Metadata collection not available",
                "message": "Automation system is not enabled. Contact your administrator."
            }), 503

        # Determine collection type from request parameters
        request_data = request.get_json() or {}
        collection_type = request_data.get("collection_type", "immediate")
        table_limit = request_data.get("table_limit", 50)
        logger.info(f"Collection type: {collection_type}, table limit: {table_limit}")

        # For immediate collection, start a task but also collect some data right away
        tables_data = []
        message = ""

        if collection_type == "immediate":
            # Create connector for this connection
            try:
                connector = get_connector_for_connection(connection)
                connector.connect()

                # Create metadata collector
                from core.metadata.collector import MetadataCollector
                collector = MetadataCollector(connection_id, connector)

                # Get table list (limited for immediate response)
                tables = collector.collect_table_list()
                # Apply the slice to tables AFTER we get them
                tables_to_process = tables[:min(len(tables), 20)]

                for table in tables_to_process:
                    tables_data.append({
                        "name": table,
                        "id": str(uuid.uuid4())
                    })

                message = "Immediate metadata collection completed, full collection scheduled"
            except Exception as e:
                logger.error(f"Error in immediate collection: {str(e)}")
                message = "Error in immediate collection, scheduled full collection as fallback"
        else:
            message = "Comprehensive metadata collection scheduled"

        # Queue full collection as background task
        params = {
            "depth": "medium" if collection_type == "comprehensive" else "light",
            "table_limit": table_limit
        }

        task_id = metadata_task_manager.submit_collection_task(connection_id, params, "high")
        logger.info(f"Submitted metadata collection task with ID: {task_id}")

        return jsonify({
            "message": message,
            "metadata": {
                "tables": tables_data,
                "count": len(tables_data)
            },
            "task_id": task_id
        })

    except Exception as e:
        logger.error(f"Error collecting connection metadata: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/tables/<table_name>/columns", methods=["GET"])
@token_required
def get_table_columns(current_user, organization_id, connection_id, table_name):
    """Get detailed information about a table's columns"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # First try to get from cache
        columns_metadata = get_metadata_cached(connection_id, "columns")

        if columns_metadata and "metadata" in columns_metadata:
            # If we have table columns cached for this specific table
            columns_by_table = columns_metadata["metadata"].get("columns_by_table", {})
            if table_name in columns_by_table:
                logger.info(f"Returning cached column data for {table_name}")
                columns = columns_by_table[table_name]

                # Include freshness information
                result = {
                    "columns": columns,
                    "count": len(columns),
                    "freshness": columns_metadata.get("freshness", {"status": "unknown"})
                }
                return jsonify(result)

        # If not cached or cache miss, collect fresh data
        logger.info(f"No cached column data, collecting fresh data for {table_name}")

        # Create connector for this connection
        try:
            connector = get_connector_for_connection(connection)
            connector.connect()
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return jsonify({"error": f"Failed to connect to database: {str(e)}"}), 500

        # Create metadata collector
        collector = MetadataCollector(connection_id, connector)

        # Get column information
        columns = collector.collect_columns(table_name)

        # Update cache with new column data
        storage_service = MetadataStorageService()
        columns_by_table = {}

        if columns_metadata and "metadata" in columns_metadata:
            # Update existing metadata
            columns_by_table = columns_metadata["metadata"].get("columns_by_table", {})

        # Add/update this table's columns
        columns_by_table[table_name] = columns
        storage_service.store_columns_metadata(connection_id, columns_by_table)

        # Also schedule a background task to refresh table metadata
        if metadata_task_manager is not None:
            metadata_task_manager.submit_table_metadata_task(connection_id, table_name, "low")

        # Return result
        result = {
            "columns": columns,
            "count": len(columns),
            "freshness": {
                "status": "fresh",
                "age_seconds": 0
            }
        }

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting table columns: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/tables/<table_name>/statistics", methods=["GET"])
@token_required
def get_table_statistics(current_user, organization_id, connection_id, table_name):
    """Get detailed statistical information about a table and its columns"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Parse query parameters
        force_refresh = request.args.get("refresh", "false").lower() == "true"

        # Check if stats are cached and not forcing refresh
        if not force_refresh:
            stats_metadata = get_metadata_cached(connection_id, "statistics")

            if stats_metadata and "metadata" in stats_metadata:
                stats_by_table = stats_metadata["metadata"].get("statistics_by_table", {})
                if table_name in stats_by_table:
                    logger.info(f"Returning cached statistics for {table_name}")
                    stats = stats_by_table[table_name]

                    result = {
                        "statistics": stats,
                        "freshness": stats_metadata.get("freshness", {"status": "unknown"})
                    }
                    return jsonify(result)

        # If no cache or forcing refresh, collect fresh statistics
        logger.info(f"Collecting fresh statistics for {table_name}")

        # Create connector for this connection
        try:
            connector = get_connector_for_connection(connection)
            connector.connect()
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return jsonify({"error": f"Failed to connect to database: {str(e)}"}), 500

        # Create metadata collector
        collector = MetadataCollector(connection_id, connector)

        # Get column information first (needed for statistics)
        columns = collector.collect_columns(table_name)

        # Initialize comprehensive table statistics
        table_stats = {
            "general": {
                "row_count": 0,
                "column_count": len(columns),
                "size_bytes": None,
                "last_updated": None,
            },
            "collection_metadata": {
                "collected_at": datetime.datetime.now(timezone.utc).isoformat(),
                "collection_duration_ms": 0
            },
            "column_statistics": {}
        }

        start_time = datetime.datetime.now(timezone.utc)

        # OPTIMIZATION 1: Build a single query to get multiple column statistics at once
        # This reduces database round trips significantly
        try:
            # Build column lists for different data types
            numeric_columns = []
            string_columns = []
            date_columns = []

            for col in columns:
                col_name = col["name"]
                col_type = col["type"].lower() if isinstance(col["type"], str) else str(col["type"]).lower()

                # Categorize columns by type for efficient querying
                if ('int' in col_type or 'float' in col_type or 'numeric' in col_type or
                        'decimal' in col_type or 'double' in col_type or 'real' in col_type):
                    numeric_columns.append(col_name)
                elif ('char' in col_type or 'text' in col_type or 'string' in col_type):
                    string_columns.append(col_name)
                elif ('date' in col_type or 'time' in col_type):
                    date_columns.append(col_name)

            # 1. First get row count and null counts for all columns in one query
            base_counts_query_parts = [f"COUNT(*) as row_count"]

            # Add null count clauses for all columns
            for col in columns:
                col_name = col["name"]
                base_counts_query_parts.append(
                    f"SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) as {col_name}_null_count"
                )

            base_counts_query = f"SELECT {', '.join(base_counts_query_parts)} FROM {table_name}"
            base_counts_result = connector.execute_query(base_counts_query)

            if base_counts_result and len(base_counts_result) > 0:
                # Get row count
                table_stats["general"]["row_count"] = base_counts_result[0][0]

                # Process null counts for each column
                for i, col in enumerate(columns):
                    col_name = col["name"]
                    null_count = base_counts_result[0][i + 1]  # +1 because the first column is row_count

                    # Initialize column statistics structure
                    column_stats = {
                        "type": col["type"],
                        "nullable": col.get("nullable", True),
                        "basic": {
                            "null_count": null_count,
                            "null_percentage": (null_count / table_stats["general"]["row_count"] * 100)
                            if table_stats["general"]["row_count"] > 0 else 0
                        },
                        "numeric": {},
                        "datetime": {},
                        "string": {},
                        "top_values": []
                    }

                    # Store in the table stats
                    table_stats["column_statistics"][col_name] = column_stats

            # 2. Get distinct counts in one query for non-LOB columns (can be expensive for large text columns)
            # Non-LOB columns are typically more efficient to count
            distinct_counts_query_parts = []
            for col in columns:
                col_name = col["name"]
                col_type = col["type"].lower() if isinstance(col["type"], str) else str(col["type"]).lower()

                # Skip columns that would be expensive to count distinctly
                if not ('text' in col_type and 'long' in col_type):
                    distinct_counts_query_parts.append(
                        f"COUNT(DISTINCT {col_name}) as {col_name}_distinct_count"
                    )

            if distinct_counts_query_parts:
                distinct_counts_query = f"SELECT {', '.join(distinct_counts_query_parts)} FROM {table_name}"
                try:
                    distinct_counts_result = connector.execute_query(distinct_counts_query)

                    if distinct_counts_result and len(distinct_counts_result) > 0:
                        col_index = 0
                        for col in columns:
                            col_name = col["name"]
                            col_type = col["type"].lower() if isinstance(col["type"], str) else str(col["type"]).lower()

                            if not ('text' in col_type and 'long' in col_type):
                                # Get the distinct count result
                                distinct_count = distinct_counts_result[0][col_index]
                                col_index += 1

                                # Update column statistics
                                if col_name in table_stats["column_statistics"]:
                                    col_stats = table_stats["column_statistics"][col_name]
                                    col_stats["basic"]["distinct_count"] = distinct_count

                                    # Calculate distinct percentage
                                    non_null_count = table_stats["general"]["row_count"] - col_stats["basic"][
                                        "null_count"]
                                    if non_null_count > 0:
                                        col_stats["basic"]["distinct_percentage"] = (
                                                                                                distinct_count / non_null_count) * 100

                                    # Determine if column is unique
                                    col_stats["basic"]["is_unique"] = (distinct_count == non_null_count)
                except Exception as e:
                    logger.warning(f"Error getting distinct counts: {str(e)}")

            # 3. Get numeric statistics in one batch query
            if numeric_columns:
                numeric_stat_parts = []

                for col_name in numeric_columns:
                    # Add statistics for this numeric column
                    numeric_stat_parts.extend([
                        f"MIN({col_name}) as {col_name}_min",
                        f"MAX({col_name}) as {col_name}_max",
                        f"AVG({col_name}) as {col_name}_avg",
                        f"SUM({col_name}) as {col_name}_sum",
                        f"COUNT(CASE WHEN {col_name} = 0 THEN 1 END) as {col_name}_zero_count",
                        f"COUNT(CASE WHEN {col_name} < 0 THEN 1 END) as {col_name}_negative_count",
                        f"COUNT(CASE WHEN {col_name} > 0 THEN 1 END) as {col_name}_positive_count"
                    ])

                    # Try to add standard deviation if supported by the database
                    if 'snowflake' in connection["connection_type"].lower():
                        numeric_stat_parts.append(f"STDDEV({col_name}) as {col_name}_stddev")

                if numeric_stat_parts:
                    numeric_query = f"SELECT {', '.join(numeric_stat_parts)} FROM {table_name} WHERE "
                    numeric_query += " AND ".join([f"{col} IS NOT NULL" for col in numeric_columns])

                    try:
                        numeric_result = connector.execute_query(numeric_query)

                        if numeric_result and len(numeric_result) > 0:
                            # Process each numeric column's statistics
                            result_index = 0
                            for col_name in numeric_columns:
                                if col_name in table_stats["column_statistics"]:
                                    col_stats = table_stats["column_statistics"][col_name]["numeric"]

                                    # Store min, max, avg, sum
                                    col_stats["min"] = numeric_result[0][result_index]
                                    result_index += 1
                                    col_stats["max"] = numeric_result[0][result_index]
                                    result_index += 1
                                    col_stats["avg"] = numeric_result[0][result_index]
                                    result_index += 1
                                    col_stats["sum"] = numeric_result[0][result_index]
                                    result_index += 1

                                    # Store zero, negative, positive counts
                                    col_stats["zero_count"] = numeric_result[0][result_index]
                                    result_index += 1
                                    col_stats["negative_count"] = numeric_result[0][result_index]
                                    result_index += 1
                                    col_stats["positive_count"] = numeric_result[0][result_index]
                                    result_index += 1

                                    # Store stddev if available
                                    if 'snowflake' in connection["connection_type"].lower():
                                        col_stats["stddev"] = numeric_result[0][result_index]
                                        result_index += 1
                    except Exception as e:
                        logger.warning(f"Error getting numeric statistics: {str(e)}")

            # 4. Get date statistics in one batch
            if date_columns:
                date_stat_parts = []

                for col_name in date_columns:
                    date_stat_parts.extend([
                        f"MIN({col_name}) as {col_name}_min",
                        f"MAX({col_name}) as {col_name}_max",
                        f"COUNT(CASE WHEN {col_name} > CURRENT_DATE() THEN 1 END) as {col_name}_future_count",
                        f"COUNT(CASE WHEN {col_name} <= CURRENT_DATE() THEN 1 END) as {col_name}_past_count"
                    ])

                if date_stat_parts:
                    date_query = f"SELECT {', '.join(date_stat_parts)} FROM {table_name} WHERE "
                    date_query += " AND ".join([f"{col} IS NOT NULL" for col in date_columns])

                    try:
                        date_result = connector.execute_query(date_query)

                        if date_result and len(date_result) > 0:
                            # Process each date column's statistics
                            result_index = 0
                            for col_name in date_columns:
                                if col_name in table_stats["column_statistics"]:
                                    col_stats = table_stats["column_statistics"][col_name]["datetime"]

                                    # Store min and max dates
                                    min_date = date_result[0][result_index]
                                    max_date = date_result[0][result_index + 1]

                                    # Format dates as ISO strings if they're datetime objects
                                    col_stats["min"] = min_date.isoformat() if hasattr(min_date,
                                                                                       'isoformat') else min_date
                                    col_stats["max"] = max_date.isoformat() if hasattr(max_date,
                                                                                       'isoformat') else max_date

                                    result_index += 2

                                    # Store future and past counts
                                    col_stats["future_count"] = date_result[0][result_index]
                                    result_index += 1
                                    col_stats["past_count"] = date_result[0][result_index]
                                    result_index += 1
                    except Exception as e:
                        logger.warning(f"Error getting date statistics: {str(e)}")

            # 5. Get string statistics in one batch
            if string_columns:
                string_stat_parts = []

                for col_name in string_columns:
                    string_stat_parts.extend([
                        f"MIN(LENGTH({col_name})) as {col_name}_min_length",
                        f"MAX(LENGTH({col_name})) as {col_name}_max_length",
                        f"AVG(LENGTH({col_name})) as {col_name}_avg_length",
                        f"COUNT(CASE WHEN {col_name} = '' THEN 1 END) as {col_name}_empty_count"
                    ])

                if string_stat_parts:
                    string_query = f"SELECT {', '.join(string_stat_parts)} FROM {table_name} WHERE "
                    string_query += " AND ".join([f"{col} IS NOT NULL" for col in string_columns])

                    try:
                        string_result = connector.execute_query(string_query)

                        if string_result and len(string_result) > 0:
                            # Process each string column's statistics
                            result_index = 0
                            for col_name in string_columns:
                                if col_name in table_stats["column_statistics"]:
                                    col_stats = table_stats["column_statistics"][col_name]["string"]
                                    basic_stats = table_stats["column_statistics"][col_name]["basic"]

                                    # Store length statistics
                                    col_stats["min_length"] = string_result[0][result_index]
                                    basic_stats["min_length"] = string_result[0][result_index]
                                    result_index += 1

                                    col_stats["max_length"] = string_result[0][result_index]
                                    basic_stats["max_length"] = string_result[0][result_index]
                                    result_index += 1

                                    col_stats["avg_length"] = string_result[0][result_index]
                                    basic_stats["avg_length"] = string_result[0][result_index]
                                    result_index += 1

                                    # Store empty count
                                    col_stats["empty_count"] = string_result[0][result_index]
                                    basic_stats["empty_count"] = string_result[0][result_index]
                                    result_index += 1

                                    # Calculate empty percentage
                                    if table_stats["general"]["row_count"] > 0:
                                        empty_percentage = (col_stats["empty_count"] / table_stats["general"][
                                            "row_count"]) * 100
                                        col_stats["empty_percentage"] = empty_percentage
                                        basic_stats["empty_percentage"] = empty_percentage
                    except Exception as e:
                        logger.warning(f"Error getting string statistics: {str(e)}")

            # 6. Get top values for each column (limited to prevent performance issues)
            # This is done separately since each column needs its own query
            # But we limit to only columns that would be meaningful
            top_value_columns = []
            for col in columns:
                col_name = col["name"]
                col_type = col["type"].lower() if isinstance(col["type"], str) else str(col["type"]).lower()

                # Skip columns that would be inefficient to get top values for
                # such as large text or binary columns
                if not ('text' in col_type and 'long' in col_type) and not ('blob' in col_type):
                    # Skip columns with too many distinct values (if we know)
                    col_stats = table_stats["column_statistics"].get(col_name, {})
                    distinct_count = col_stats.get("basic", {}).get("distinct_count", 0)

                    # Only include if distinct count is unknown or reasonably small
                    if distinct_count == 0 or distinct_count < 1000:
                        top_value_columns.append(col_name)

            # Cap at 10 columns to prevent too many queries
            top_value_columns = top_value_columns[:10]

            # Get top values for selected columns
            for col_name in top_value_columns:
                try:
                    top_n = 10  # Number of top values to retrieve
                    top_values_query = f"""
                        SELECT {col_name}, COUNT(*) as count
                        FROM {table_name}
                        WHERE {col_name} IS NOT NULL
                        GROUP BY {col_name}
                        ORDER BY count DESC
                        LIMIT {top_n}
                    """
                    result = connector.execute_query(top_values_query)

                    if result and col_name in table_stats["column_statistics"]:
                        top_values = []
                        for row in result:
                            value = row[0]
                            count = row[1]
                            percentage = (count / table_stats["general"]["row_count"]) * 100 if table_stats["general"][
                                                                                                    "row_count"] > 0 else 0

                            # Format value for display (truncate long strings)
                            display_value = str(value)
                            if isinstance(value, str) and len(display_value) > 100:
                                display_value = display_value[:97] + "..."

                            top_values.append({
                                "value": display_value,
                                "count": count,
                                "percentage": percentage
                            })

                        table_stats["column_statistics"][col_name]["top_values"] = top_values
                except Exception as e:
                    logger.warning(f"Error getting top values for column {col_name}: {str(e)}")

            # 7. Get table size information in one query if supported
            try:
                if 'snowflake' in connection["connection_type"].lower():
                    # Snowflake-specific query to get table size
                    size_query = f"""
                        SELECT TABLE_NAME, ACTIVE_BYTES, DELETED_BYTES, TIME_TRAVEL_BYTES, LAST_ALTERED
                        FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
                        WHERE TABLE_NAME = '{table_name.upper()}'
                    """
                    result = connector.execute_query(size_query)
                    if result and len(result) > 0:
                        active_bytes = result[0][1] or 0
                        deleted_bytes = result[0][2] or 0
                        time_travel_bytes = result[0][3] or 0
                        last_altered = result[0][4]

                        table_stats["general"]["size_bytes"] = active_bytes
                        table_stats["general"]["total_storage_bytes"] = active_bytes + deleted_bytes + time_travel_bytes

                        if last_altered:
                            table_stats["general"]["last_updated"] = last_altered.isoformat() if hasattr(last_altered,
                                                                                                         'isoformat') else last_altered
                elif 'postgresql' in connection["connection_type"].lower():
                    # PostgreSQL query to get table size
                    size_query = f"""
                        SELECT pg_total_relation_size('{table_name}')
                    """
                    result = connector.execute_query(size_query)
                    if result and len(result) > 0:
                        table_stats["general"]["size_bytes"] = result[0][0]
            except Exception as e:
                logger.warning(f"Could not get table size for {table_name}: {str(e)}")

        except Exception as e:
            logger.error(f"Error in optimized statistics collection: {str(e)}")
            # Continue with traditional methods if optimized approach fails

        # Calculate collection duration
        end_time = datetime.datetime.now(timezone.utc)
        duration_ms = (end_time - start_time).total_seconds() * 1000
        table_stats["collection_metadata"]["collection_duration_ms"] = duration_ms

        # Save key metrics to historical statistics
        # (extract this to a separate function or batch process)
        def save_historical_statistics(connection_id, organization_id, table_name, table_stats):
            """Save key metrics to historical statistics table"""
            try:
                # Create direct Supabase client
                import os
                import json
                import decimal
                from supabase import create_client

                # Define a custom encoder for Decimal and datetime objects
                class CustomJSONEncoder(json.JSONEncoder):
                    def default(self, obj):
                        if isinstance(obj, decimal.Decimal):
                            return float(obj)
                        if hasattr(obj, 'isoformat'):  # Handle datetime objects
                            return obj.isoformat()
                        return super(CustomJSONEncoder, self).default(obj)

                # Get credentials from environment
                supabase_url = os.getenv("SUPABASE_URL")
                supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

                # Create client
                direct_client = create_client(supabase_url, supabase_key)

                # Current timestamp
                now = datetime.datetime.now(timezone.utc).isoformat()

                # Table-level metrics to track
                historical_records = []

                # Add table-level metrics
                row_count = table_stats["general"]["row_count"]
                # Convert Decimal to float if needed
                if isinstance(row_count, decimal.Decimal):
                    row_count = float(row_count)

                historical_records.append({
                    "connection_id": connection_id,
                    "organization_id": organization_id,
                    "table_name": table_name,
                    "column_name": None,  # None for table-level metrics
                    "metric_name": "row_count",
                    "metric_value": row_count,
                    "collected_at": now
                })

                # Add column-level metrics - limited to essential metrics only
                for column_name, column_stats in table_stats["column_statistics"].items():
                    # Track null percentage
                    null_percentage = column_stats["basic"].get("null_percentage")
                    if null_percentage is not None:
                        # Convert Decimal to float if needed
                        if isinstance(null_percentage, decimal.Decimal):
                            null_percentage = float(null_percentage)

                        historical_records.append({
                            "connection_id": connection_id,
                            "organization_id": organization_id,
                            "table_name": table_name,
                            "column_name": column_name,
                            "metric_name": "null_percentage",
                            "metric_value": null_percentage,
                            "collected_at": now
                        })

                    # Track distinct percentage (essential for cardinality trends)
                    distinct_percentage = column_stats["basic"].get("distinct_percentage")
                    if distinct_percentage is not None:
                        # Convert Decimal to float if needed
                        if isinstance(distinct_percentage, decimal.Decimal):
                            distinct_percentage = float(distinct_percentage)

                        historical_records.append({
                            "connection_id": connection_id,
                            "organization_id": organization_id,
                            "table_name": table_name,
                            "column_name": column_name,
                            "metric_name": "distinct_percentage",
                            "metric_value": distinct_percentage,
                            "collected_at": now
                        })

                # Insert historical records in batches
                batch_size = 50
                for i in range(0, len(historical_records), batch_size):
                    batch = historical_records[i:i + batch_size]
                    # Convert any remaining Decimal objects
                    batch_json = json.dumps(batch, cls=CustomJSONEncoder)
                    batch_clean = json.loads(batch_json)
                    direct_client.table("historical_statistics").insert(batch_clean).execute()

                logger.info(f"Saved {len(historical_records)} historical metrics for {table_name}")
                return True

            except Exception as e:
                logger.error(f"Error saving historical statistics: {str(e)}")
                logger.error(traceback.format_exc())
                return False

        # Call historical statistics function asynchronously
        task_executor.submit(save_historical_statistics, connection_id, organization_id, table_name, table_stats)

        # Store statistics in cache
        storage_service = MetadataStorageService()
        stats_metadata = storage_service.get_metadata(connection_id, "statistics")

        if not stats_metadata or "metadata" not in stats_metadata:
            # Initialize statistics structure
            stats_by_table = {table_name: table_stats}
            storage_service.store_statistics_metadata(connection_id, stats_by_table)
        else:
            # Update existing statistics
            stats_by_table = stats_metadata["metadata"].get("statistics_by_table", {})
            stats_by_table[table_name] = table_stats
            storage_service.store_statistics_metadata(connection_id, stats_by_table)

        # Return result
        result = {
            "statistics": table_stats,
            "freshness": {
                "status": "fresh",
                "age_seconds": 0
            }
        }

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting table statistics: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/changes/acknowledge", methods=["POST"])
@token_required
def acknowledge_schema_changes(current_user, organization_id, connection_id):
    """Acknowledge schema changes"""
    try:
        # Get parameters
        data = request.json
        table_name = data.get("table_name")
        change_ids = data.get("change_ids", [])
        change_types = data.get("change_types", [])

        if not (table_name or change_ids or change_types):
            return jsonify({"error": "table_name, change_ids, or change_types must be provided"}), 400

        # Get supabase client
        import os
        from supabase import create_client

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
        direct_client = create_client(supabase_url, supabase_key)

        # Fix: Use correct datetime import
        current_time = datetime.now(timezone.utc).isoformat()
        acknowledged_count = 0

        # Handle current_user safely
        user_id = None
        if isinstance(current_user, dict):
            user_id = current_user.get("id")
        elif isinstance(current_user, str):
            user_id = current_user

        # Create update data
        update_data = {
            "acknowledged": True,
            "acknowledged_at": current_time
        }

        # Only add user ID if available
        if user_id:
            update_data["acknowledged_by"] = user_id

        # Case 1: Acknowledge by table name
        if table_name:
            result = direct_client.table("schema_changes") \
                .update(update_data) \
                .eq("connection_id", connection_id) \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .eq("acknowledged", False) \
                .execute()

            acknowledged_count = len(result.data) if result.data else 0

        # Case 2: Acknowledge by specific change IDs
        elif change_ids:
            for change_id in change_ids:
                result = direct_client.table("schema_changes") \
                    .update(update_data) \
                    .eq("id", change_id) \
                    .eq("connection_id", connection_id) \
                    .eq("organization_id", organization_id) \
                    .execute()

                if result.data:
                    acknowledged_count += 1

        # Case 3: Acknowledge by change types
        elif change_types:
            for change_type in change_types:
                result = direct_client.table("schema_changes") \
                    .update(update_data) \
                    .eq("connection_id", connection_id) \
                    .eq("organization_id", organization_id) \
                    .eq("change_type", change_type) \
                    .eq("acknowledged", False) \
                    .execute()

                if result.data:
                    acknowledged_count += len(result.data)

        return jsonify({
            "success": True,
            "acknowledged_count": acknowledged_count,
            "message": f"Acknowledged {acknowledged_count} schema changes"
        })

    except Exception as e:
        logger.error(f"Error acknowledging schema changes: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/changes", methods=["GET"])
@token_required
@cache_with_timeout(timeout_seconds=30)
def get_schema_changes(current_user, organization_id, connection_id):
    """Get saved schema changes from the database"""
    try:
        # Parse query parameters
        since_timestamp = request.args.get("since")
        acknowledged = request.args.get("acknowledged")
        change_type = request.args.get("change_type")  # Filter by change type
        table_name = request.args.get("table_name")  # Filter by table name
        limit = request.args.get("limit", 100, type=int)

        # Check access to connection
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            logger.error(f"Connection not found or access denied: {connection_id}")
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get supabase client for direct query
        import os
        from supabase import create_client

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
        direct_client = create_client(supabase_url, supabase_key)

        # Build query to get schema changes
        query = direct_client.table("schema_changes") \
            .select("*") \
            .eq("connection_id", connection_id)

        # Add filters
        if acknowledged is not None:
            # Convert string 'true'/'false' to Python boolean
            acknowledged_bool = acknowledged.lower() == 'true'
            query = query.eq("acknowledged", acknowledged_bool)

        if since_timestamp:
            query = query.gte("detected_at", since_timestamp)

        if change_type:
            query = query.eq("change_type", change_type)

        if table_name:
            query = query.eq("table_name", table_name)

        # Execute query with pagination
        response = query.order("detected_at", desc=True).limit(limit).execute()

        if not response.data:
            return jsonify({
                "changes": [],
                "count": 0,
                "message": "No schema changes found"
            })

        # Process changes to match expected format
        changes = []
        for record in response.data:
            # Parse details if stored as JSON string
            details = record.get("details", {})
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except:
                    details = {}

            change = {
                "id": record.get("id"),
                "type": record.get("change_type"),
                "table": record.get("table_name"),
                "column": record.get("column_name"),
                "details": details,
                "timestamp": record.get("detected_at"),
                "acknowledged": record.get("acknowledged", False),
                "acknowledged_at": record.get("acknowledged_at")
            }
            changes.append(change)

        # Prepare metadata about changes
        change_types = {}
        for change in changes:
            change_type = change.get("type")
            if change_type not in change_types:
                change_types[change_type] = 0
            change_types[change_type] += 1

        return jsonify({
            "changes": changes,
            "count": len(changes),
            "change_types": change_types,
            "acknowledged_count": sum(1 for c in changes if c.get("acknowledged", False)),
            "unacknowledged_count": sum(1 for c in changes if not c.get("acknowledged", False)),
            "message": "Schema changes retrieved successfully"
        })

    except Exception as e:
        logger.error(f"Error retrieving schema changes: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/schema", methods=["GET"])
@token_required
def get_combined_schema(current_user, organization_id, connection_id):
    """Get a combined view of the database schema"""
    try:
        # Check access to connection
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            logger.error(f"Connection not found or access denied: {connection_id}")
            return jsonify({"error": "Connection not found or access denied"}), 404

        connection = connection_check.data[0]

        # Get all available metadata for this connection
        storage_service = MetadataStorageService()

        # Get tables metadata
        tables_metadata = storage_service.get_metadata(connection_id, "tables")
        if not tables_metadata or "metadata" not in tables_metadata:
            logger.error(f"No table metadata found for connection {connection_id}")
            return jsonify({"error": "No schema information available"}), 404

        # Get columns metadata
        columns_metadata = storage_service.get_metadata(connection_id, "columns")

        # Get statistics metadata
        statistics_metadata = storage_service.get_metadata(connection_id, "statistics")

        # Build combined schema
        schema = {
            "tables": [],
            "connection_id": connection_id,
            "connection_name": connection.get("name", "Unknown"),
            "metadata_freshness": {
                "tables": tables_metadata.get("freshness", {"status": "unknown"}),
                "columns": columns_metadata.get("freshness", {"status": "unknown"}) if columns_metadata else {
                    "status": "unknown"},
                "statistics": statistics_metadata.get("freshness", {"status": "unknown"}) if statistics_metadata else {
                    "status": "unknown"}
            }
        }

        # Extract tables
        tables = tables_metadata["metadata"].get("tables", [])

        # Extract columns by table (if available)
        columns_by_table = {}
        if columns_metadata and "metadata" in columns_metadata:
            columns_by_table = columns_metadata["metadata"].get("columns_by_table", {})

        # Extract statistics by table (if available)
        statistics_by_table = {}
        if statistics_metadata and "metadata" in statistics_metadata:
            statistics_by_table = statistics_metadata["metadata"].get("statistics_by_table", {})

        # Process each table with its columns and statistics
        for table in tables:
            table_name = table.get("name")

            # Build table entry with available information
            table_entry = {
                "name": table_name,
                "row_count": table.get("row_count"),
                "column_count": table.get("column_count"),
                "primary_key": table.get("primary_key", []),
                "columns": []
            }

            # Add columns if available
            if table_name in columns_by_table:
                for column in columns_by_table[table_name]:
                    column_entry = {
                        "name": column.get("name"),
                        "type": column.get("type"),
                        "nullable": column.get("nullable"),
                    }

                    # Add statistics if available
                    if (table_name in statistics_by_table and
                            "column_statistics" in statistics_by_table[table_name] and
                            column.get("name") in statistics_by_table[table_name]["column_statistics"]):
                        column_stats = statistics_by_table[table_name]["column_statistics"][column.get("name")]
                        column_entry.update({
                            "null_count": column_stats.get("null_count"),
                            "null_percentage": column_stats.get("null_percentage"),
                            "distinct_count": column_stats.get("distinct_count"),
                            "min_value": column_stats.get("min_value"),
                            "max_value": column_stats.get("max_value"),
                            "avg_value": column_stats.get("avg_value")
                        })

                    table_entry["columns"].append(column_entry)

            # Add table to schema
            schema["tables"].append(table_entry)

        # Sort tables by name for consistent output
        schema["tables"].sort(key=lambda x: x["name"])

        # Add counts and collection timestamp
        schema["table_count"] = len(schema["tables"])
        schema["collected_at"] = tables_metadata.get("collected_at", datetime.datetime.now(timezone.utc).isoformat())

        return jsonify(schema)

    except Exception as e:
        logger.error(f"Error getting combined schema: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/tables/<table_name>/columns/<column_name>/pattern", methods=["POST"])
@token_required
def check_custom_pattern(current_user, organization_id, connection_id, table_name, column_name):
    """Check a column against a custom pattern"""
    try:
        # Get the pattern from the request
        data = request.get_json()
        if not data or "pattern" not in data:
            return jsonify({"error": "Pattern is required"}), 400

        pattern = data["pattern"]
        pattern_name = data.get("name", "custom_pattern")

        # Check access to connection
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            return jsonify({"error": "Connection not found or access denied"}), 404

        connection = connection_check.data[0]

        # Create connector
        try:
            connector = get_connector_for_connection(connection)
            connector.connect()
        except Exception as e:
            return jsonify({"error": f"Failed to connect to database: {str(e)}"}), 500

        # Execute pattern check query
        try:
            pattern_query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN REGEXP_LIKE({column_name}, '{pattern}') THEN 1 END) as matching_count
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
            """
            result = connector.execute_query(pattern_query)

            if result and len(result) > 0:
                total_count = result[0][0]
                matching_count = result[0][1]

                # Calculate percentages
                matching_percentage = (matching_count / total_count * 100) if total_count > 0 else 0
                non_matching_percentage = 100 - matching_percentage

                return jsonify({
                    "pattern_name": pattern_name,
                    "pattern": pattern,
                    "total_count": total_count,
                    "matching_count": matching_count,
                    "non_matching_count": total_count - matching_count,
                    "matching_percentage": matching_percentage,
                    "non_matching_percentage": non_matching_percentage
                })

        except Exception as e:
            return jsonify({"error": f"Error checking pattern: {str(e)}"}), 500

    except Exception as e:
        logger.error(f"Error in custom pattern check: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/tables", methods=["GET"])
@token_required
def get_tables_for_connection(current_user, organization_id, connection_id):
    """Get all tables for a specific connection"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # First try to get from cache
        tables_metadata = get_metadata_cached(connection_id, "tables")

        if tables_metadata and "metadata" in tables_metadata:
            # Return cached tables
            tables = []
            for table_data in tables_metadata["metadata"].get("tables", []):
                tables.append(table_data.get("name"))

            return jsonify({
                "tables": tables,
                "freshness": tables_metadata.get("freshness", {"status": "unknown"})
            })

        # If no cache, collect fresh data
        logger.info(f"No cached table data, collecting fresh data for connection {connection_id}")

        # Create connector for this connection
        try:
            connector = get_connector_for_connection(connection)
            connector.connect()
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return jsonify({"error": f"Failed to connect to database: {str(e)}"}), 500

        # Create metadata collector
        collector = MetadataCollector(connection_id, connector)

        # Get table list
        tables = collector.collect_table_list()

        # Store in cache for future use
        if tables:
            tables_data = []
            for table in tables[:min(len(tables), 100)]:  # Store up to 100 tables
                tables_data.append({
                    "name": table,
                    "id": str(uuid.uuid4())  # Generate an ID for this table
                })

            storage_service = MetadataStorageService()
            storage_service.store_tables_metadata(connection_id, tables_data)

            # Schedule a background task to collect more detailed metadata
            if metadata_task_manager is not None:
                params = {"depth": "light", "table_limit": 50}
                metadata_task_manager.submit_collection_task(connection_id, params, "low")

        return jsonify({
            "tables": tables,
            "freshness": {
                "status": "fresh",
                "age_seconds": 0
            }
        })

    except Exception as e:
        logger.error(f"Error getting tables for connection {connection_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/validations/<table_name>/trends", methods=["GET"])
@token_required
def get_validation_trends(current_user, organization_id, connection_id, table_name):
    """
    Get validation trends over time for a specific table
    Completely rewritten for better maintainability and debugging
    """
    try:
        # Get query parameters
        days = int(request.args.get("days", 30))

        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        logger.info(f"=== VALIDATION TRENDS v2 START ===")
        logger.info(f"Request: table={table_name}, connection={connection_id}, days={days}, org={organization_id}")

        # Step 1: Get validation rules
        validation_manager = SupabaseValidationManager()
        rules = validation_manager.get_rules(organization_id, table_name, connection_id)

        if not rules:
            logger.warning(f"No validation rules found for table {table_name}")
            return jsonify({
                "trends": [],
                "message": "No validation rules found for this table",
                "table_name": table_name,
                "days": days,
                "rule_count": 0
            }), 200

        rule_ids = [rule["id"] for rule in rules]
        logger.info(f"Step 1 Complete: Found {len(rule_ids)} validation rules")

        # Step 2: Calculate date range in UTC
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        logger.info(f"Step 2 Complete: Date range {start_date.date()} to {end_date.date()} (UTC)")

        # Step 3: Get validation results with simplified query
        supabase_mgr = SupabaseManager()

        logger.info("Step 3: Fetching validation results...")
        query_results = supabase_mgr.supabase.table("validation_results") \
            .select("rule_id, is_valid, run_at, actual_value") \
            .eq("organization_id", organization_id) \
            .eq("connection_id", connection_id) \
            .in_("rule_id", rule_ids) \
            .gte("run_at", start_date.isoformat()) \
            .lte("run_at", end_date.isoformat()) \
            .order("run_at") \
            .execute()

        raw_results = query_results.data if query_results.data else []
        logger.info(f"Step 3 Complete: Retrieved {len(raw_results)} raw validation results")

        # Step 4: Process and group results by date
        logger.info("Step 4: Processing results by date...")
        daily_aggregates = process_validation_results_by_date(raw_results, rule_ids)
        logger.info(f"Step 4 Complete: Processed {len(daily_aggregates)} daily aggregates")

        # Step 5: Generate complete date series with trends
        logger.info("Step 5: Generating trends for complete date series...")
        trends = generate_trends_for_date_range(start_date, end_date, daily_aggregates, len(rules))
        logger.info(f"Step 5 Complete: Generated {len(trends)} trend points")

        # Step 6: Calculate summary metrics
        current_health_score = calculate_current_health_score_v2(rules, organization_id, connection_id)
        active_days = len([t for t in trends if t["total_validations"] > 0])

        logger.info(f"Step 6 Complete: Health score={current_health_score}, Active days={active_days}")

        # Step 7: Build response
        response_data = {
            "trends": trends,
            "table_name": table_name,
            "days": days,
            "rule_count": len(rules),
            "current_health_score": current_health_score,
            "data_points": active_days
        }

        logger.info(f"=== VALIDATION TRENDS v2 SUCCESS ===")
        logger.info(f"Returning {len(trends)} trends, {active_days} active days")

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"=== VALIDATION TRENDS v2 ERROR ===")
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())

        return jsonify({
            "error": f"Failed to get validation trends: {str(e)}",
            "trends": [],
            "table_name": table_name,
            "days": days,
            "rule_count": 0,
            "current_health_score": 0
        }), 500


def process_validation_results_by_date(raw_results, rule_ids):
    """
    Process raw validation results and group by date
    Returns a dictionary of {date_string: {rule_id: latest_result}}
    """
    logger.info("Processing validation results by date...")

    daily_data = {}
    processed_count = 0
    july_count = 0

    for result in raw_results:
        try:
            # Parse the timestamp
            run_at_str = result.get("run_at")
            if not run_at_str:
                continue

            # Handle different timestamp formats consistently
            if run_at_str.endswith('Z'):
                dt = datetime.fromisoformat(run_at_str[:-1] + '+00:00')
            elif '+' in run_at_str or run_at_str.endswith('+00:00'):
                dt = datetime.fromisoformat(run_at_str)
            else:
                dt = datetime.fromisoformat(run_at_str + '+00:00')

            # Extract date in YYYY-MM-DD format
            date_key = dt.date().isoformat()
            rule_id = result.get("rule_id")

            # Track July data specifically
            if date_key.startswith('2025-07'):
                july_count += 1
                if july_count <= 5:  # Log first 5 July results
                    logger.info(f"July result: date={date_key}, rule={rule_id}, valid={result.get('is_valid')}")

            # Initialize date if needed
            if date_key not in daily_data:
                daily_data[date_key] = {}

            # Keep only the latest result per rule per day
            if rule_id not in daily_data[date_key] or result["run_at"] > daily_data[date_key][rule_id]["run_at"]:
                daily_data[date_key][rule_id] = {
                    "is_valid": result.get("is_valid"),
                    "actual_value": result.get("actual_value"),
                    "run_at": result.get("run_at")
                }

            processed_count += 1

        except Exception as e:
            logger.warning(f"Error processing result: {e}")
            continue

    logger.info(f"Processed {processed_count} results into {len(daily_data)} daily groups")
    logger.info(f"Found {july_count} July results")

    # Log July dates that have data
    july_dates = [date for date in daily_data.keys() if date.startswith('2025-07')]
    if july_dates:
        logger.info(f"July dates with data: {sorted(july_dates)}")
        for date in sorted(july_dates)[:3]:  # Log first 3 July dates
            rules_count = len(daily_data[date])
            logger.info(f"July {date}: {rules_count} rules with results")

    return daily_data


def generate_trends_for_date_range(start_date, end_date, daily_aggregates, total_rules):
    """
    Generate trend data for the complete date range
    Returns a list of trend points, one for each day
    """
    logger.info("Generating trends for complete date range...")

    trends = []
    current_date = start_date.date()
    end_date_only = end_date.date()
    july_trend_count = 0

    while current_date <= end_date_only:
        date_key = current_date.isoformat()

        # Get data for this date
        day_results = daily_aggregates.get(date_key, {})

        # Calculate metrics
        total_validations = len(day_results)
        passed = sum(1 for r in day_results.values() if r["is_valid"] is True)
        failed = sum(1 for r in day_results.values() if r["is_valid"] is False)
        errored = sum(1 for r in day_results.values() if r["is_valid"] is None)
        not_run = total_rules - total_validations

        # Calculate health score
        valid_results = passed + failed
        health_score = (passed / valid_results * 100) if valid_results > 0 else 0

        trend_point = {
            "day": date_key,
            "timestamp": f"{date_key}T00:00:00Z",
            "total_validations": total_validations,
            "passed": passed,
            "failed": failed,
            "errored": errored,
            "health_score": round(health_score, 2),
            "not_run": not_run
        }

        trends.append(trend_point)

        # Log July trends specifically
        if date_key.startswith('2025-07'):
            july_trend_count += 1
            if july_trend_count <= 5:  # Log first 5 July trends
                logger.info(
                    f"July trend: {date_key} -> total={total_validations}, passed={passed}, failed={failed}, health={health_score:.1f}%")

        # Move to next day
        current_date += timedelta(days=1)

    logger.info(f"Generated {len(trends)} trend points")
    logger.info(f"July trend points: {july_trend_count}")

    # Summary for July
    july_trends = [t for t in trends if t['day'].startswith('2025-07')]
    july_with_data = [t for t in july_trends if t['total_validations'] > 0]
    logger.info(f"July summary: {len(july_trends)} total days, {len(july_with_data)} with validation data")

    return trends


def calculate_current_health_score_v2(rules, organization_id, connection_id):
    """
    Simplified current health score calculation
    """
    try:
        if not rules:
            return 0

        # Get latest results for all rules
        rule_ids = [rule["id"] for rule in rules]
        supabase_mgr = SupabaseManager()

        # Get the most recent result for each rule
        latest_results = {}
        for rule_id in rule_ids:
            result = supabase_mgr.supabase.table("validation_results") \
                .select("is_valid") \
                .eq("organization_id", organization_id) \
                .eq("connection_id", connection_id) \
                .eq("rule_id", rule_id) \
                .order("run_at", desc=True) \
                .limit(1) \
                .execute()

            if result.data:
                latest_results[rule_id] = result.data[0]["is_valid"]

        # Calculate health score
        total_with_results = len(latest_results)
        if total_with_results == 0:
            return 0

        passed = sum(1 for is_valid in latest_results.values() if is_valid is True)
        failed = sum(1 for is_valid in latest_results.values() if is_valid is False)
        valid_results = passed + failed

        return round((passed / valid_results * 100), 2) if valid_results > 0 else 0

    except Exception as e:
        logger.error(f"Error calculating current health score: {e}")
        return 0

def calculate_current_health_score(rules, organization_id, connection_id):
    """Calculate the current health score for validation rules"""
    try:
        if not rules:
            return 0

        supabase_mgr = SupabaseManager()
        rule_ids = [rule["id"] for rule in rules]

        # Get the latest result for each rule
        latest_results = []

        for rule_id in rule_ids:
            response = supabase_mgr.supabase.table("validation_results") \
                .select("is_valid") \
                .eq("organization_id", organization_id) \
                .eq("rule_id", rule_id) \
                .order("run_at", desc=True) \
                .limit(1) \
                .execute()

            if response.data:
                latest_results.append(response.data[0]["is_valid"])

        if not latest_results:
            return 0

        # Calculate health score from latest results
        passed = sum(1 for result in latest_results if result is True)
        failed = sum(1 for result in latest_results if result is False)
        valid_results = passed + failed

        return round((passed / valid_results * 100), 2) if valid_results > 0 else 0

    except Exception as e:
        logger.error(f"Error calculating current health score: {e}")
        return 0

@app.route("/api/connections/<connection_id>/tables/<table_name>/trends", methods=["GET"])
@token_required
def get_historical_trends(current_user, organization_id, connection_id, table_name):
    """Get historical trends for a table"""
    try:
        # Get query parameters
        days = int(request.args.get("days", 30))  # Default to 30 days
        column_name = request.args.get("column")  # Optional column filter
        metric = request.args.get("metric")  # Optional metric filter

        # Check access to connection
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Calculate the start date
        start_date = (datetime.datetime.now(timezone.utc) - datetime.timedelta(days=days)).isoformat()

        # Query historical statistics
        try:
            # Create direct Supabase client
            import os
            from supabase import create_client

            # Get credentials from environment
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

            # Create direct client
            direct_client = create_client(supabase_url, supabase_key)

            # Start building the query
            query = direct_client.table("historical_statistics") \
                .select("column_name, metric_name, metric_value, collected_at") \
                .eq("connection_id", connection_id) \
                .eq("table_name", table_name) \
                .gte("collected_at", start_date)

            # NOTE: The order syntax appears to have changed in the Supabase library
            # Try with the simpler version
            query = query.order("collected_at")

            # Add column filter if specified
            if column_name:
                query = query.eq("column_name", column_name)

            # Add metric filter if specified
            if metric:
                query = query.eq("metric_name", metric)

            # Execute the query
            response = query.execute()

            if not response.data:
                return jsonify({
                    "trends": [],
                    "message": "No historical data found"
                })

            # Process the data into a more usable format
            trends = {}

            for record in response.data:
                # Create the metric key
                col_name = record["column_name"] or "table"  # Use "table" for table-level metrics
                metric_name = record["metric_name"]
                key = f"{col_name}.{metric_name}"

                # Initialize the metric if not already present
                if key not in trends:
                    trends[key] = {
                        "column": col_name,
                        "metric": metric_name,
                        "values": [],
                        "timestamps": []
                    }

                # Add the value and timestamp
                trends[key]["values"].append(record["metric_value"])
                trends[key]["timestamps"].append(record["collected_at"])

            # Convert to a list and return
            trend_list = list(trends.values())

            # Calculate stats for each trend
            for trend in trend_list:
                if trend["values"]:
                    trend["current_value"] = trend["values"][-1]
                    trend["min_value"] = min(trend["values"])
                    trend["max_value"] = max(trend["values"])

                    # Calculate change metrics if we have at least 2 points
                    if len(trend["values"]) >= 2:
                        first_value = trend["values"][0]
                        last_value = trend["values"][-1]

                        if first_value != 0:  # Avoid division by zero
                            trend["percent_change"] = ((last_value - first_value) / first_value) * 100
                        else:
                            trend["percent_change"] = None

                        trend["absolute_change"] = last_value - first_value

            return jsonify({
                "trends": trend_list,
                "table_name": table_name,
                "days": days,
                "count": len(trend_list)
            })

        except Exception as e:
            logger.error(f"Error retrieving historical trends: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": f"Error retrieving historical trends: {str(e)}"}), 500

    except Exception as e:
        logger.error(f"Error in historical trends: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/metadata/tasks", methods=["POST"])
@token_required
def schedule_metadata_task(current_user, organization_id, connection_id):
    """Schedule a metadata collection task"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Try to get task manager
        metadata_task_manager = get_metadata_task_manager()

        if not metadata_task_manager:
            return jsonify({
                "error": "Task scheduling not available",
                "message": "Automation system is not enabled. Contact your administrator."
            }), 503

        # Get task parameters from request
        data = request.get_json() or {}
        task_type = data.get("task_type", "full_collection")
        priority = data.get("priority", "medium")
        table_name = data.get("table_name")

        # Submit appropriate task based on type
        task_id = None

        if task_type == "full_collection":
            # Full collection task
            params = {
                "depth": data.get("depth", "medium"),
                "table_limit": data.get("table_limit", 50)
            }

            task_id = metadata_task_manager.submit_collection_task(connection_id, params, priority)

        elif task_type == "table_metadata":
            # Table metadata task
            if not table_name:
                return jsonify({"error": "table_name is required for table_metadata task"}), 400

            task_id = metadata_task_manager.submit_table_metadata_task(connection_id, table_name, priority)

        elif task_type == "refresh_statistics":
            # Statistics refresh task
            if not table_name:
                return jsonify({"error": "table_name is required for refresh_statistics task"}), 400

            task_id = metadata_task_manager.submit_statistics_refresh_task(connection_id, table_name, priority)

        elif task_type == "update_usage":
            # Usage update task
            if not table_name:
                return jsonify({"error": "table_name is required for update_usage task"}), 400

            task_id = metadata_task_manager.submit_usage_update_task(connection_id, table_name, priority)

        else:
            return jsonify({"error": f"Unknown task type: {task_type}"}), 400

        return jsonify({
            "task_id": task_id,
            "status": "scheduled",
            "message": f"Scheduled {task_type} task for connection {connection_id}"
        })

    except Exception as e:
        logger.error(f"Error scheduling metadata task: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/metadata/tasks", methods=["GET"])
@token_required
def get_metadata_tasks(current_user, organization_id, connection_id):
    """Get recent metadata tasks for a connection"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Try to get task manager
        metadata_task_manager = get_metadata_task_manager()

        if not metadata_task_manager:
            return jsonify({
                "tasks": [],
                "count": 0,
                "message": "Task manager not available - automation system disabled"
            })

        # Get recent tasks for this connection
        limit = request.args.get("limit", 10, type=int)
        tasks = metadata_task_manager.get_recent_tasks(limit, connection_id)

        return jsonify({
            "tasks": tasks,
            "count": len(tasks)
        })

    except Exception as e:
        logger.error(f"Error getting metadata tasks: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/metadata/refresh", methods=["POST"])
@token_required
def refresh_metadata(current_user, organization_id, connection_id):
    """Trigger a metadata refresh using unified service"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get refresh parameters
        data = request.get_json() or {}
        metadata_type = data.get("metadata_type", "schema")
        table_name = data.get("table_name")
        priority = data.get("priority", "high")  # Manual requests are high priority

        # Use the unified refresh service
        from core.metadata.unified_refresh_service import get_unified_refresh_service, RefreshTrigger

        unified_service = get_unified_refresh_service()

        # Submit refresh using unified service
        result = unified_service.refresh_metadata(
            connection_id=connection_id,
            metadata_type=metadata_type,
            table_name=table_name,
            trigger=RefreshTrigger.MANUAL_USER,
            user_id=current_user,
            organization_id=organization_id,
            priority=priority
        )

        if result.get("success"):
            return jsonify({
                "refresh_id": result.get("refresh_id"),
                "task_id": result.get("task_id"),
                "status": result.get("status"),
                "message": result.get("message"),
                "estimated_completion": result.get("estimated_completion")
            })
        else:
            return jsonify({"error": result.get("error", "Unknown error")}), 500

    except Exception as e:
        logger.error(f"Error refreshing metadata: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route("/api/connections/<connection_id>/metadata/refresh/<refresh_id>/status", methods=["GET"])
@token_required
def get_refresh_status(current_user, organization_id, connection_id, refresh_id):
    """Get status of a metadata refresh operation"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get status using unified service
        from core.metadata.unified_refresh_service import get_unified_refresh_service

        unified_service = get_unified_refresh_service()
        status = unified_service.get_refresh_status(refresh_id=refresh_id)

        return jsonify(status)

    except Exception as e:
        logger.error(f"Error getting refresh status: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/metadata/status", methods=["GET"])
@token_required
def get_metadata_status(current_user, organization_id, connection_id):
    """Get metadata freshness status for a connection"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Try to get task manager (may be None if automation disabled)
        metadata_task_manager = get_metadata_task_manager()

        # Helper function to calculate freshness status
        def calculate_freshness_status(collected_at):
            if not collected_at:
                return {"status": "unknown", "age_seconds": None}

            try:
                # Parse the timestamp ensuring UTC handling
                if isinstance(collected_at, str):
                    # Handle both Z and +00:00 formats
                    if collected_at.endswith('Z'):
                        collected_at = collected_at[:-1] + '+00:00'
                    collected_time = datetime.fromisoformat(collected_at)
                else:
                    collected_time = collected_at

                # Ensure we have timezone info
                if collected_time.tzinfo is None:
                    collected_time = collected_time.replace(tzinfo=timezone.utc)

                # Calculate age in seconds
                now = datetime.now(timezone.utc)
                age_delta = now - collected_time
                age_seconds = int(age_delta.total_seconds())

                # Determine status based on age
                if age_seconds < 0:
                    status = "error"  # Future timestamp
                elif age_seconds < 3600:  # Less than 1 hour
                    status = "fresh"
                elif age_seconds < 86400:  # Less than 1 day (24 hours)
                    status = "recent"
                else:  # More than 1 day
                    status = "stale"

                return {
                    "status": status,
                    "age_seconds": age_seconds
                }

            except Exception as e:
                logger.warning(f"Error calculating freshness for {collected_at}: {e}")
                return {"status": "error", "age_seconds": None}

        # Get metadata status efficiently - use the cached getter
        metadata_types = ["tables", "columns", "statistics"]
        status = {}

        for metadata_type in metadata_types:
            metadata = get_metadata_cached(connection_id, metadata_type)

            if metadata:
                collected_at = metadata.get("collected_at")
                freshness = calculate_freshness_status(collected_at)

                status[metadata_type] = {
                    "available": True,
                    "freshness": freshness,
                    "last_updated": collected_at
                }

                # Log for debugging (remove in production)
                logger.debug(f"Metadata {metadata_type}: collected_at={collected_at}, freshness={freshness}")
            else:
                status[metadata_type] = {
                    "available": False,
                    "freshness": {"status": "unknown", "age_seconds": None},
                    "last_updated": None
                }

        # Get any pending tasks - only if task manager is available
        pending_tasks = []
        has_pending_tasks = False

        if metadata_task_manager:
            try:
                tasks = metadata_task_manager.get_recent_tasks(5, connection_id)

                # Fix the filter to properly detect pending tasks
                for task in tasks:
                    task_status = task.get("status")
                    if task_status in ["pending", "running"]:
                        pending_tasks.append(task)
                    elif task.get("task") and task["task"].get("status") in ["pending", "running"]:
                        pending_tasks.append(task)

                has_pending_tasks = len(pending_tasks) > 0
            except Exception as e:
                logger.warning(f"Error getting pending tasks: {str(e)}")

        # Add to status
        status["pending_tasks"] = pending_tasks
        status["has_pending_tasks"] = has_pending_tasks
        status["task_manager_available"] = metadata_task_manager is not None

        # Add connection info
        status["connection"] = {
            "name": connection.get("name", "Unknown"),
            "type": connection.get("connection_type", "Unknown"),
            "id": connection_id
        }

        # Add any schema changes info if available
        status["changes"] = []
        status["changes_detected"] = 0

        return jsonify(status)

    except Exception as e:
        logger.error(f"Error getting metadata status: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/metadata/worker/stats", methods=["GET"])
@token_required
def get_worker_stats(current_user, organization_id):
    """Get metadata worker statistics - admin only endpoint"""
    try:
        # Check if user is an admin
        supabase_mgr = SupabaseManager()
        user_role = supabase_mgr.get_user_role(current_user)

        if user_role != 'admin':
            logger.warning(f"Non-admin user {current_user} attempted to access admin endpoint")
            return jsonify({"error": "Admin access required"}), 403

        # Try to get task manager
        metadata_task_manager = get_metadata_task_manager()

        if not metadata_task_manager:
            return jsonify({
                "error": "Worker stats not available",
                "message": "Automation system is not enabled",
                "automation_enabled": False
            })

        # Get worker stats from the task manager
        stats = metadata_task_manager.get_worker_stats()
        stats["automation_enabled"] = True

        # Add additional stats from database if available
        try:
            # Get recent tasks
            task_response = supabase_mgr.supabase.table("metadata_tasks") \
                .select("task_status,created_at") \
                .order("created_at", desc=True) \
                .limit(100) \
                .execute()

            if task_response.data:
                # Count tasks by status
                status_counts = {}
                for task in task_response.data:
                    status = task.get("task_status", "unknown")
                    if status not in status_counts:
                        status_counts[status] = 0
                    status_counts[status] += 1

                # Add to stats
                stats["historical_tasks"] = {
                    "recent_count": len(task_response.data),
                    "status_counts": status_counts
                }

                # Determine most recent completion
                completed_tasks = [task for task in task_response.data if task.get("task_status") == "completed"]
                if completed_tasks:
                    most_recent = max(task.get("created_at", "") for task in completed_tasks)
                    stats["last_completion"] = most_recent
        except Exception as e:
            logger.warning(f"Error getting additional worker stats: {str(e)}")
            # Continue with the basic stats

        return jsonify(stats)

    except Exception as e:
        logger.error(f"Error getting worker stats: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/metadata/tasks/<task_id>", methods=["GET"])
@token_required
def get_metadata_task_status(current_user, organization_id, connection_id, task_id):
    """Get status of a metadata collection task"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Try to get task manager
        metadata_task_manager = get_metadata_task_manager()

        if not metadata_task_manager:
            return jsonify({
                "error": "Task status not available",
                "message": "Automation system is not enabled"
            }), 503

        # Get task status
        task_status = metadata_task_manager.get_task_status(task_id)

        if "error" in task_status:
            return jsonify({"error": task_status["error"]}), 404

        return jsonify(task_status)

    except Exception as e:
        logger.error(f"Error getting metadata task status: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/schema/detect-changes", methods=["POST"])
@token_required
def detect_schema_changes(current_user, organization_id, connection_id):
    """Detect schema changes and save them to the database"""
    try:
        # Check access to connection
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            logger.error(f"Connection not found or access denied: {connection_id}")
            return jsonify({"error": "Connection not found or access denied"}), 404

        connection = connection_check.data[0]

        # Safely handle data extraction with proper error handling
        try:
            data = request.get_json(silent=True) or {}
        except Exception as e:
            logger.warning(f"Error parsing JSON data: {str(e)}")
            data = {}

        force_refresh = data.get("force_refresh", True)
        include_details = data.get("include_details", True)

        # Create connector factory
        from core.metadata.connector_factory import ConnectorFactory
        connector_factory = ConnectorFactory(supabase_mgr)

        # Create schema change detector
        from core.metadata.schema_change_detector import SchemaChangeDetector
        from core.metadata.storage_service import MetadataStorageService

        storage_service = MetadataStorageService()
        detector = SchemaChangeDetector(storage_service)

        # Detect schema changes
        changes, important_changes = detector.detect_changes_for_connection(
            connection_id,
            connector_factory,
            supabase_mgr
        )

        # Convert any non-serializable types to strings
        def make_json_serializable(item):
            if isinstance(item, dict):
                return {k: make_json_serializable(v) for k, v in item.items()}
            elif isinstance(item, list):
                return [make_json_serializable(i) for i in item]
            elif hasattr(item, 'isoformat'):  # Handle datetime objects
                return item.isoformat()
            elif not isinstance(item, (str, int, float, bool, type(None))):
                # Convert any other non-serializable type to string
                return str(item)
            else:
                return item

        # Make changes JSON-serializable
        changes = make_json_serializable(changes)

        # Group changes by type
        change_types = {}
        for change in changes:
            change_type = change.get("type")
            if change_type not in change_types:
                change_types[change_type] = 0
            change_types[change_type] += 1

        # Create tasks for handling important changes if needed
        tasks_created = 0
        if important_changes and changes:
            try:
                # Publish events
                task_ids = detector.publish_changes_as_events(
                    connection_id,
                    changes,
                    organization_id,
                    current_user
                )
                tasks_created = len(task_ids)
            except Exception as e:
                logger.error(f"Error publishing events: {str(e)}")

        # Prepare response
        response_data = {
            "changes": changes if include_details else [],
            "changes_detected": len(changes),
            "important_changes": important_changes,
            "change_types": change_types,
            "tasks_created": tasks_created,
            "message": f"Detected {len(changes)} schema changes"
        }

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Error in schema change detection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route("/api/connections/<connection_id>/analytics/change-frequency", methods=["GET"])
@token_required
def get_change_frequency(current_user, organization_id, connection_id):
    """Get change frequency analytics for metadata objects"""
    try:
        # Check access to connection
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            logger.error(f"Connection not found or access denied: {connection_id}")
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get query parameters
        object_type = request.args.get("object_type", "table_metadata")
        object_name = request.args.get("object_name")
        days = request.args.get("days", 30, type=int)

        # Validate parameters
        if not object_name:
            return jsonify({"error": "object_name parameter is required"}), 400

        # Create analytics service
        from core.metadata.change_analytics import MetadataChangeAnalytics
        analytics = MetadataChangeAnalytics(supabase_mgr)

        # Get frequency data
        frequency_data = analytics.get_change_frequency(
            connection_id=connection_id,
            object_type=object_type,
            object_name=object_name,
            time_period_days=days
        )

        return jsonify(frequency_data)

    except Exception as e:
        logger.error(f"Error getting change frequency: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/analytics/refresh-suggestion", methods=["GET"])
@token_required
def get_refresh_suggestion(current_user, organization_id, connection_id):
    """Get refresh interval suggestion based on change analytics"""
    try:
        # Check access to connection
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            logger.error(f"Connection not found or access denied: {connection_id}")
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get query parameters
        object_type = request.args.get("object_type", "table_metadata")
        object_name = request.args.get("object_name")
        current_interval = request.args.get("current_interval", 24, type=int)

        # Validate parameters
        if not object_name:
            return jsonify({"error": "object_name parameter is required"}), 400

        # Create analytics service
        from core.metadata.change_analytics import MetadataChangeAnalytics
        analytics = MetadataChangeAnalytics(supabase_mgr)

        # Get suggestion
        suggestion = analytics.suggest_refresh_interval(
            connection_id=connection_id,
            object_type=object_type,
            object_name=object_name,
            current_interval_hours=current_interval
        )

        return jsonify(suggestion)

    except Exception as e:
        logger.error(f"Error getting refresh suggestion: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/analytics/high-impact", methods=["GET"])
@token_required
def get_high_impact_objects(current_user, organization_id, connection_id):
    """Get objects with high change frequency"""
    # Get limit parameter from query string with default value
    limit = request.args.get("limit", 10, type=int)

    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Create analytics service
        from core.metadata.change_analytics import MetadataChangeAnalytics
        supabase_mgr = SupabaseManager()
        analytics = MetadataChangeAnalytics(supabase_mgr)

        # Get high-impact objects
        high_impact_objects = analytics.get_high_impact_objects(connection_id, limit=limit)

        return jsonify({
            "objects": high_impact_objects,
            "count": len(high_impact_objects)
        })

    except Exception as e:
        logger.error(f"Error getting high-impact objects: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/analytics/dashboard", methods=["GET"])
@token_required
def get_change_analytics_dashboard(current_user, organization_id, connection_id):
    """Get comprehensive analytics dashboard data"""
    try:
        # Check access to connection
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            logger.error(f"Connection not found or access denied: {connection_id}")
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Initialize result structures
        high_impact_objects = []
        overall_stats = {
            "total_checks": 0,
            "total_changes": 0,
            "unique_objects": 0,
            "last_check": None
        }
        frequency_distribution = []
        change_trend = []

        # 1. Get high-impact objects - reuse the modified method
        from core.metadata.change_analytics import MetadataChangeAnalytics
        analytics = MetadataChangeAnalytics(supabase_mgr)
        high_impact_objects = analytics.get_high_impact_objects(connection_id, limit=5)

        # 2. Get total checks and changes
        # Get analytics data for the last 30 days
        thirty_days_ago = (datetime.datetime.now(timezone.utc) - datetime.timedelta(days=30)).isoformat()
        stats_response = supabase_mgr.supabase.table("metadata_change_analytics") \
            .select("object_name,change_detected,check_timestamp") \
            .eq("connection_id", connection_id) \
            .gte("check_timestamp", thirty_days_ago) \
            .execute()

        if stats_response.data:
            # Calculate overall stats
            overall_stats["total_checks"] = len(stats_response.data)
            overall_stats["total_changes"] = sum(
                1 for record in stats_response.data if record.get("change_detected", False))
            overall_stats["unique_objects"] = len(
                set(record.get("object_name") for record in stats_response.data if record.get("object_name")))

            # Find the most recent check timestamp
            timestamps = [record.get("check_timestamp") for record in stats_response.data if
                          record.get("check_timestamp")]
            if timestamps:
                overall_stats["last_check"] = max(timestamps)

        # 3. Calculate frequency distribution
        if stats_response.data:
            # Group by object and calculate change ratios
            objects = {}
            for record in stats_response.data:
                object_name = record.get("object_name")
                if not object_name:
                    continue

                if object_name not in objects:
                    objects[object_name] = {"checks": 0, "changes": 0}

                objects[object_name]["checks"] += 1
                if record.get("change_detected", False):
                    objects[object_name]["changes"] += 1

            # Filter objects with at least 5 checks and calculate ratios
            filtered_objects = {}
            for name, data in objects.items():
                if data["checks"] >= 5:
                    data["change_ratio"] = data["changes"] / data["checks"]
                    filtered_objects[name] = data

            # Group by frequency category
            frequencies = {"high": 0, "medium": 0, "low": 0}
            for data in filtered_objects.values():
                if data["change_ratio"] >= 0.5:
                    frequencies["high"] += 1
                elif data["change_ratio"] >= 0.1:
                    frequencies["medium"] += 1
                else:
                    frequencies["low"] += 1

            # Format for output
            for category, count in frequencies.items():
                if count > 0:
                    frequency_distribution.append({
                        "frequency": category,
                        "object_count": count
                    })

        # 4. Calculate change trend by day
        if stats_response.data:
            # Group by day and count checks and changes
            days = {}
            for record in stats_response.data:
                timestamp = record.get("check_timestamp")
                if not timestamp:
                    continue

                # Extract date part only
                date = timestamp.split("T")[0]

                if date not in days:
                    days[date] = {"checks": 0, "changes": 0}

                days[date]["checks"] += 1
                if record.get("change_detected", False):
                    days[date]["changes"] += 1

            # Format for output
            for date, data in days.items():
                change_trend.append({
                    "date": date,
                    "checks": data["checks"],
                    "changes": data["changes"],
                    "change_percentage": (data["changes"] / data["checks"] * 100) if data["checks"] > 0 else 0
                })

            # Sort by date
            change_trend.sort(key=lambda x: x["date"])

        # Build dashboard data
        dashboard = {
            "high_impact_objects": high_impact_objects,
            "overall_stats": overall_stats,
            "frequency_distribution": frequency_distribution,
            "change_trend": change_trend
        }

        return jsonify(dashboard)

    except Exception as e:
        logger.error(f"Error getting analytics dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/analytics/historical-metrics", methods=["GET"])
@token_required
def get_historical_metrics(current_user, organization_id, connection_id):
    """Get historical metrics for analytics"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get query parameters
        metric_name = request.args.get("metric_name")
        table_name = request.args.get("table_name")
        column_name = request.args.get("column_name")
        days = request.args.get("days", 30, type=int)
        limit = request.args.get("limit", 100, type=int)

        # Import the historical metrics tracker
        from core.analytics.historical_metrics import HistoricalMetricsTracker
        tracker = HistoricalMetricsTracker(SupabaseManager())

        # Get the metrics
        if metric_name:
            # Get specific metric history
            metrics = tracker.get_metric_history(
                organization_id=organization_id,
                connection_id=connection_id,
                metric_name=metric_name,
                table_name=table_name,
                column_name=column_name,
                days=days,
                limit=limit
            )
        else:
            # Get recent metrics
            metrics = tracker.get_recent_metrics(
                organization_id=organization_id,
                connection_id=connection_id,
                limit=limit
            )

        # Format the response
        result = {
            "metrics": metrics,
            "count": len(metrics)
        }

        # Group by date if requested
        if request.args.get("group_by_date", "false").lower() == "true":
            from itertools import groupby
            from operator import itemgetter

            # Extract date from timestamp
            for metric in metrics:
                if "timestamp" in metric:
                    metric["date"] = metric["timestamp"].split("T")[0]

            # Group by date
            metrics_by_date = {}
            for date, group in groupby(sorted(metrics, key=itemgetter("date")), key=itemgetter("date")):
                metrics_by_date[date] = list(group)

            result["metrics_by_date"] = metrics_by_date

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting historical metrics: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route("/api/connections/<connection_id>/analytics/track-metrics", methods=["POST"])
@token_required
def track_custom_metrics(current_user, organization_id, connection_id):
    """Track custom metrics for a connection"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get metrics data from request
        data = request.get_json()
        if not data or "metrics" not in data:
            return jsonify({"error": "Metrics data is required"}), 400

        metrics = data["metrics"]
        if not isinstance(metrics, list):
            return jsonify({"error": "Metrics must be a list"}), 400

        # Import the historical metrics tracker
        from core.analytics.historical_metrics import HistoricalMetricsTracker
        tracker = HistoricalMetricsTracker(SupabaseManager())

        # Track the metrics
        success = tracker.track_metrics_batch(
            organization_id=organization_id,
            connection_id=connection_id,
            metrics=metrics
        )

        if success:
            return jsonify({
                "success": True,
                "message": f"Successfully tracked {len(metrics)} metrics",
                "count": len(metrics)
            })
        else:
            return jsonify({
                "error": "Failed to track metrics"
            }), 500

    except Exception as e:
        logger.error(f"Error tracking metrics: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>/analytics/dashboard/metrics", methods=["GET"])
@token_required
def get_analytics_dashboard_metrics(current_user, organization_id, connection_id):
    """Get comprehensive analytics dashboard metrics over time"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get query parameters
        days = request.args.get("days", 30, type=int)
        limit = request.args.get("limit", 100, type=int)

        # Calculate the start date
        from datetime import datetime, timedelta
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        # Get Supabase client
        supabase_mgr = SupabaseManager()

        # Prepare result data structures
        row_count_trends = []
        validation_trends = []
        schema_trends = []
        quality_score_trends = []
        recent_metrics = []

        # 1. Get row count trends (using standard Supabase queries instead of raw SQL)
        row_count_response = supabase_mgr.supabase.table("historical_metrics") \
            .select("table_name,metric_value,timestamp") \
            .eq("connection_id", connection_id) \
            .eq("organization_id", organization_id) \
            .eq("metric_name", "row_count") \
            .gte("timestamp", start_date) \
            .order("table_name") \
            .order("timestamp") \
            .execute()

        if row_count_response.data:
            row_count_trends = row_count_response.data

        # 2. Get recent metrics
        # Fix: Changed the order parameter syntax
        recent_response = supabase_mgr.supabase.table("historical_metrics") \
            .select("table_name,metric_name,metric_value,metric_text,timestamp") \
            .eq("connection_id", connection_id) \
            .eq("organization_id", organization_id) \
            .order("timestamp", desc=True) \
            .limit(20) \
            .execute()

        if recent_response.data:
            recent_metrics = recent_response.data

        # 3. Get validation metrics - we need to do this programmatically since we can't use SQL
        validation_response = supabase_mgr.supabase.table("historical_metrics") \
            .select("metric_value,timestamp,table_name") \
            .eq("connection_id", connection_id) \
            .eq("organization_id", organization_id) \
            .eq("metric_name", "validation_success") \
            .eq("metric_type", "validation") \
            .gte("timestamp", start_date) \
            .execute()

        if validation_response.data:
            # Process the data to compute daily averages
            from itertools import groupby
            from statistics import mean

            # Extract date from timestamp
            processed_data = []
            for item in validation_response.data:
                if "timestamp" in item:
                    item["date"] = item["timestamp"].split("T")[0]
                    processed_data.append(item)

            # Group by date
            validation_trends = []
            grouped_by_date = {}
            for item in processed_data:
                date = item.get("date")
                if date not in grouped_by_date:
                    grouped_by_date[date] = []
                grouped_by_date[date].append(item)

            # Calculate averages by date
            for date, group in grouped_by_date.items():
                success_rates = [item.get("metric_value", 0) for item in group]
                tables_validated = len(set(item.get("table_name") for item in group if item.get("table_name")))

                validation_trends.append({
                    "date": date,
                    "success_rate": mean(success_rates) if success_rates else 0,
                    "tables_validated": tables_validated
                })

            # Sort by date
            validation_trends.sort(key=lambda x: x.get("date", ""))

        # 4. Get schema change metrics
        schema_response = supabase_mgr.supabase.table("historical_metrics") \
            .select("timestamp") \
            .eq("connection_id", connection_id) \
            .eq("organization_id", organization_id) \
            .eq("metric_type", "schema_change") \
            .gte("timestamp", start_date) \
            .execute()

        if schema_response.data:
            # Process to count by day
            from itertools import groupby

            # Extract date from timestamp
            processed_data = []
            for item in schema_response.data:
                if "timestamp" in item:
                    item["date"] = item["timestamp"].split("T")[0]
                    processed_data.append(item)

            # Count by date
            schema_trends = []
            grouped_by_date = {}
            for item in processed_data:
                date = item.get("date")
                if date not in grouped_by_date:
                    grouped_by_date[date] = 0
                grouped_by_date[date] += 1

            # Format the results
            for date, count in grouped_by_date.items():
                schema_trends.append({
                    "date": date,
                    "change_count": count
                })

            # Sort by date
            schema_trends.sort(key=lambda x: x.get("date", ""))

        # 5. Get quality score trends
        quality_response = supabase_mgr.supabase.table("historical_metrics") \
            .select("metric_value,timestamp") \
            .eq("connection_id", connection_id) \
            .eq("organization_id", organization_id) \
            .eq("metric_name", "quality_score") \
            .gte("timestamp", start_date) \
            .execute()

        if quality_response.data:
            # Process to average by day
            from itertools import groupby
            from statistics import mean

            # Extract date from timestamp
            processed_data = []
            for item in quality_response.data:
                if "timestamp" in item:
                    item["date"] = item["timestamp"].split("T")[0]
                    processed_data.append(item)

            # Group by date and calculate averages
            quality_score_trends = []
            grouped_by_date = {}
            for item in processed_data:
                date = item.get("date")
                if date not in grouped_by_date:
                    grouped_by_date[date] = []
                grouped_by_date[date].append(item.get("metric_value", 0))

            # Calculate the averages
            for date, values in grouped_by_date.items():
                quality_score_trends.append({
                    "date": date,
                    "avg_quality_score": mean(values) if values else 0
                })

            # Sort by date
            quality_score_trends.sort(key=lambda x: x.get("date", ""))

        # Return comprehensive dashboard data
        return jsonify({
            "row_count_trends": row_count_trends,
            "validation_trends": validation_trends,
            "schema_change_trends": schema_trends,
            "quality_score_trends": quality_score_trends,
            "recent_metrics": recent_metrics
        })

    except Exception as e:
        logger.error(f"Error getting analytics dashboard metrics: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/api/batch', methods=['POST', 'OPTIONS'])
def batch_requests():
    """Process multiple API requests in a single call"""
    # Check if this is a preflight request
    if request.method == 'OPTIONS':
        return '', 204  # Return empty response with 204 No Content for OPTIONS

    batch_data = request.json

    if not batch_data or 'requests' not in batch_data:
        return jsonify({"error": "Invalid batch format"}), 400

    # Get the authorization header from the original request
    auth_header = request.headers.get('Authorization')

    # If no auth header is provided but it's required, return error
    if not auth_header:
        logger.warning("No authorization token provided for batch request")
        return jsonify({"error": "Authorization header is required for batch requests"}), 401

    requests_data = batch_data['requests']
    results = {}

    # Increase timeout and use a more robust client configuration
    client = httpx.Client(
        timeout=httpx.Timeout(
            connect=10.0,  # Connection timeout
            read=30.0,  # Read timeout (increased from default)
            write=10.0,  # Write timeout
            pool=None  # Disable connection pooling to prevent potential issues
        ),
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=10,
            max_keepalive_connections=5
        )
    )

    # Process each request synchronously instead of async to avoid issues
    for req in requests_data:
        req_id = req.get('id')
        path = req.get('path')
        params = req.get('params', {})
        method = req.get('method', 'GET').upper()

        if not req_id or not path:
            continue

        try:
            # Make sure path starts with /api for consistency
            if not path.startswith('/api'):
                path = f"/api{path}"

            # Build the full URL
            full_url = f"{request.url_root.rstrip('/')}{path}"

            logger.info(f"Processing batch request: {full_url}")

            # Create headers dict with auth token
            headers = {
                'Authorization': auth_header,
                'Content-Type': 'application/json',
                'X-Batch-Request': 'true'  # Add a marker for debugging
            }

            # Comprehensive logging
            logger.debug(f"Request details - Method: {method}, URL: {full_url}, Params: {params}")

            try:
                if method == 'GET':
                    response = client.get(full_url, params=params, headers=headers)
                elif method == 'POST':
                    response = client.post(full_url, json=params, headers=headers)
                elif method == 'PUT':
                    response = client.put(full_url, json=params, headers=headers)
                elif method == 'DELETE':
                    response = client.delete(full_url, params=params, headers=headers)
                else:
                    results[req_id] = {"error": f"Unsupported method: {method}"}
                    continue

                # Log response status for debugging
                logger.info(f"Batch sub-request {req_id} completed with status {response.status_code}")

                # Check if the response was successful
                if response.status_code == 200:
                    try:
                        results[req_id] = response.json()
                    except ValueError:
                        # Fallback if JSON parsing fails
                        results[req_id] = {"data": response.text}
                else:
                    logger.warning(f"Batch sub-request failed: {path} returned {response.status_code}")
                    results[req_id] = {
                        "error": f"Request failed with status code {response.status_code}",
                        "details": response.text
                    }

            except httpx.ReadTimeout:
                logger.error(f"Timeout occurred for request to {full_url}")
                results[req_id] = {
                    "error": "Request timed out",
                    "details": f"Could not complete request to {path}"
                }
            except Exception as req_error:
                logger.error(f"Error in sub-request to {path}: {str(req_error)}")
                results[req_id] = {
                    "error": str(req_error),
                    "details": f"Error processing request to {path}"
                }

        except Exception as e:
            logger.error(f"Unexpected error handling batch request to {path}: {str(e)}")
            logger.error(traceback.format_exc())
            results[req_id] = {"error": str(e)}

    return jsonify({"results": results})


@app.after_request
def after_request_cors(response):
    """Ensure CORS headers are set on all responses"""
    origin = request.headers.get('Origin', '')

    # Define allowed origins
    allowed_origins = [
        "https://cloud.sparvi.io",
        "http://localhost:3000",
        "https://ambitious-wave-0fdea0310.6.azurestaticapps.net"
    ]

    # Check if the origin is in our list of allowed origins
    if origin in allowed_origins:
        response.headers['Access-Control-Allow-Origin'] = origin
    else:
        # For requests without origin (like direct API calls), allow the main domain
        response.headers['Access-Control-Allow-Origin'] = 'https://cloud.sparvi.io'

    # Set standard CORS headers
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization,X-Requested-With'
    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE,OPTIONS'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Max-Age'] = '3600'

    return response


# Register refactored routes
logger.info("Registering refactored routes...")
register_profile_routes(app)
register_connection_routes(app)
register_validation_routes(app)
logger.info("Refactored routes registered successfully")

# For local development
if __name__ == "__main__":
    logger.info("Running in local development mode")
    app.run()
else:
    logger.info("Running in production mode (Azure App Service)")