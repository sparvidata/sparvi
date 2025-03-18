import json
import urllib.parse
import psutil
import supabase
from flask import Flask, render_template, jsonify, request
import datetime
import os
import traceback
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
import threading
import queue
import uuid
import threading
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
from core.validations.supabase_validation_manager import SupabaseValidationManager
from core.history.supabase_profile_history import SupabaseProfileHistoryManager
from core.metadata.manager import MetadataTaskManager
from core.metadata.events import MetadataEventType, publish_metadata_event

import concurrent.futures
from sqlalchemy.pool import QueuePool
from functools import wraps
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import time


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


# Request-level caching decorator
def request_cache(func):
    """Decorator that caches function results within a single request"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if not hasattr(request, '_cache'):
            request._cache = {}

        # Create a cache key from function name and arguments
        key = f"{func.__name__}:{str(args)}:{str(kwargs)}"

        if key not in request._cache:
            request._cache[key] = func(*args, **kwargs)

        return request._cache[key]

    return wrapper


# Helper function for connection access checks
def connection_access_check(connection_id, organization_id):
    """Check if the organization has access to this connection"""
    supabase_mgr = SupabaseManager()
    connection_check = supabase_mgr.supabase.table("database_connections") \
        .select("*") \
        .eq("id", connection_id) \
        .eq("organization_id", organization_id) \
        .execute()

    if not connection_check.data or len(connection_check.data) == 0:
        logger.error(f"Connection not found or access denied: {connection_id}")
        return None

    return connection_check.data[0]


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

        if statistics_by_table and depth in ["medium", "deep"]:
            self.storage_service.store_statistics_metadata(connection_id, statistics_by_table)
            logger.info(f"Stored statistics for {len(statistics_by_table)} tables")

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
                "id": str(uuid.uuid4())  # Generate an ID for this table
            }

            # Basic statistics
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

            # If deep scan, add more statistics
            if depth == "deep":
                # Add additional statistics here if needed
                pass

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
        """Execute statistics refresh for a table"""
        connection_id = task.connection_id
        table_name = task.params.get("table_name")

        if not table_name:
            raise ValueError("Table name not provided for refresh_statistics task")

        # Get connection details
        connection = self.supabase_mgr.get_connection(connection_id)
        if not connection:
            raise ValueError(f"Connection not found: {connection_id}")

        # Create connector
        connector = get_connector_for_connection(connection)
        connector.connect()

        # Create metadata collector
        collector = MetadataCollector(connection_id, connector)

        # Get columns first
        columns = collector.collect_columns(table_name)

        # Get row count
        row_count = 0
        try:
            result = connector.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            if result and len(result) > 0:
                row_count = result[0][0]
        except Exception as e:
            logger.warning(f"Error getting row count for {table_name}: {str(e)}")

        # Build statistics object
        statistics = {
            "row_count": row_count,
            "column_count": len(columns),
            "columns": {
                col["name"]: {
                    "type": col["type"],
                    "nullable": col.get("nullable", False)
                } for col in columns
            }
        }

        # Store statistics
        statistics_by_table = {table_name: statistics}
        self.storage_service.store_statistics_metadata(connection_id, statistics_by_table)

        return {
            "status": "success",
            "table": table_name
        }

    def _execute_update_usage(self, task):
        """Execute usage statistics update"""
        connection_id = task.connection_id
        table_name = task.params.get("table_name")

        if not table_name:
            raise ValueError("Table name not provided for update_usage task")

        # Implementation for usage statistics would go here

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


# Initialize connection pool manager
connection_pool_manager = ConnectionPoolManager.get_instance()

# Initialize global task manager
metadata_task_manager = None

def init_metadata_task_manager():
    global metadata_task_manager
    try:
        if metadata_task_manager is None:
            from core.metadata.storage_service import MetadataStorageService

            logger.info("Initializing improved metadata task manager...")
            storage_service = MetadataStorageService()
            supabase_mgr = SupabaseManager()

            metadata_task_manager = ImprovedTaskManager.get_instance(storage_service, supabase_mgr)
            logger.info("Improved metadata task manager initialized successfully")

            # Connect to event system if available
            try:
                from core.metadata.events import event_publisher
                event_publisher.set_task_manager(metadata_task_manager)
                logger.info("Connected event publisher to improved metadata task manager")
            except ImportError:
                # Define a simple stub event_publisher
                class EventPublisher:
                    @staticmethod
                    def set_task_manager(task_manager):
                        logger.warning("Stub event publisher used - no actual event publishing")

                event_publisher = EventPublisher()
                logger.warning("Could not import event publisher, using stub implementation")
            except Exception as e:
                logger.error(f"Error connecting to event system: {str(e)}")
    except Exception as e:
        logger.error(f"Error initializing improved metadata task manager: {str(e)}")
        logger.error(traceback.format_exc())
        metadata_task_manager = None  # Ensure it's set to None if initialization fails



def setup_comprehensive_logging():
    """
    Set up a comprehensive logging configuration that:
    - Logs to console (stderr)
    - Logs to a file
    - Captures DEBUG level logs
    - Includes detailed log formatting
    - Handles uncaught exceptions
    """
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Set to lowest level to capture everything

    # Console Handler - writes to stderr
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.DEBUG)  # Capture all levels

    # File Handler - writes to a log file
    try:
        file_handler = logging.FileHandler('app.log', mode='a')
        file_handler.setLevel(logging.DEBUG)
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

app = Flask(__name__, template_folder="templates")
CORS(app,
     resources={r"/*": {"origins": ["https://cloud.sparvi.io", "http://localhost:3000"]}},
     supports_credentials=True,
     allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
     expose_headers=["Content-Type", "Authorization"],
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])

# Set the secret key from environment variables
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "default_secret_key")

# Initialize Supabase validation manager
validation_manager = SupabaseValidationManager()

run_validations_after_profile = True  # Set to True to automatically run validations after profiling

metadata_storage = MetadataStorage()

task_executor = ThreadPoolExecutor(max_workers=5)
metadata_task_queue = queue.Queue()
threading.Timer(5.0, init_metadata_task_manager).start()  # 5-second delay to ensure other services are ready

# Decorator to require a valid token for protected routes
import os

# Modify your token_required decorator to include a testing bypass
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Special case for testing
        if os.environ.get('TESTING') == 'True' and request.headers.get('Authorization') == 'Bearer test-token':
            logger.debug("Test token detected, bypassing authentication")
            return f("test-user-id", "test-org-id", *args, **kwargs)

        # Normal token check (your existing code)
        token = None
        auth_header = request.headers.get("Authorization", None)
        if auth_header:
            parts = auth_header.split()
            if len(parts) == 2 and parts[0] == "Bearer":
                token = parts[1]
        if not token:
            logger.warning("No token provided")
            return jsonify({"error": "Token is missing!"}), 401

        try:
            # Use Supabase to verify the token
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_SERVICE_KEY")

            # Log environment variables for debugging
            logger.debug(f"Supabase URL: {url}")
            logger.debug(f"Supabase Service Key: {bool(key)}")  # Don't log actual key

            if not url or not key:
                logger.error("Supabase URL or Service Key is missing")
                return jsonify({"error": "Server configuration error"}), 500

            supabase_client = create_client(url, key)

            # Verify the JWT token
            decoded = supabase_client.auth.get_user(token)
            current_user = decoded.user.id

            logger.info(f"Token valid for user: {current_user}")

            # Get the user's organization ID
            supabase_mgr = SupabaseManager()
            organization_id = supabase_mgr.get_user_organization(current_user)

            if not organization_id:
                logger.error(f"No organization found for user: {current_user}")
                return jsonify({"error": "User has no associated organization"}), 403

        except Exception as e:
            logger.error(f"Token verification error: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": "Token is invalid!", "message": str(e)}), 401

        # Pass both user_id and organization_id to the decorated function
        return f(current_user, organization_id, *args, **kwargs)

    return decorated


def build_connection_string(connection_data):
    """Build connection string from stored connection details"""
    conn_type = connection_data["connection_type"]
    details = connection_data["connection_details"]

    if conn_type == "snowflake":
        username = details.get("username", "")
        password = details.get("password", "")
        account = details.get("account", "")
        database = details.get("database", "")
        schema = details.get("schema", "PUBLIC")
        warehouse = details.get("warehouse", "")

        # URL encode password
        encoded_password = urllib.parse.quote_plus(password)

        return f"snowflake://{username}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"

    # Add other connection types as needed
    return None


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
    table_name = data["table"]
    profile_history_id = data.get("profile_history_id")
    connection_id = data.get("connection_id")

    try:
        force_gc()
        # Get all rules
        rules = validation_manager.get_rules(organization_id, table_name)

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

def get_connector_for_connection(connection_details):
    """Create the appropriate connector based on connection type"""
    connection_type = connection_details.get("connection_type")

    if connection_type == "snowflake":
        return SnowflakeConnector(connection_details.get("connection_details", {}))
    else:
        raise ValueError(f"Unsupported connection type: {connection_type}")

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
                                logger.info(f"Processing table {i+1}/{len(process_tables)}: {table}")

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
            "created_at": datetime.datetime.now().isoformat(),
            "updated_at": datetime.datetime.now().isoformat()
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
            "collected_at": datetime.datetime.now().isoformat(),
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
            "created_at": datetime.datetime.now().isoformat(),
            "updated_at": datetime.datetime.now().isoformat()
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
                "collected_at": datetime.datetime.now().isoformat(),
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
                    "created_at": datetime.datetime.now().isoformat(),
                    "updated_at": datetime.datetime.now().isoformat()
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
                            "collected_at": datetime.datetime.now().isoformat(),
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
                            "collected_at": datetime.datetime.now().isoformat(),
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
                            "collected_at": datetime.datetime.now().isoformat(),
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
            "collected_at": datetime.datetime.now().isoformat()
        }

        logger.info(f"Successfully collected metadata for table {table_name}")
        return table_metadata
    except Exception as e:
        logger.error(f"Error collecting table metadata for {table_name}: {str(e)}")
        return {
            "table_name": table_name,
            "error": str(e),
            "collected_at": datetime.datetime.now().isoformat()
        }


@request_cache
def get_connection_string(connection):
    """Build and cache a connection string from connection details"""
    try:
        connection_details = connection["connection_details"]
        connection_type = connection["connection_type"]

        if connection_type == "snowflake":
            username = connection_details.get("username", "")
            password = connection_details.get("password", "")
            account = connection_details.get("account", "")
            database = connection_details.get("database", "")
            schema = connection_details.get("schema", "PUBLIC")
            warehouse = connection_details.get("warehouse", "")

            # URL encode password to handle special characters
            import urllib.parse
            encoded_password = urllib.parse.quote_plus(password)

            return f"snowflake://{username}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"
        # Add other connection types here
    except Exception as e:
        logger.error(f"Error building connection string: {str(e)}")

    return None

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


@app.route("/api/validations", methods=["GET"])
@token_required
def get_validations(current_user, organization_id):
    """Get all validation rules for a table"""
    table_name = request.args.get("table")
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    try:
        logger.info(f"Getting validation rules for organization: {organization_id}, table: {table_name}")
        rules = validation_manager.get_rules(organization_id, table_name)
        logger.info(f"Retrieved {len(rules)} validation rules")
        logger.debug(f"Rules content: {rules}")
        return jsonify({"rules": rules})
    except Exception as e:
        logger.error(f"Error getting validation rules: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validations", methods=["POST"])
@token_required
def add_validation_rule(current_user, organization_id):
    """Add a new validation rule for a table"""
    table_name = request.args.get("table")
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    rule_data = request.get_json()
    if not rule_data:
        return jsonify({"error": "Rule data is required"}), 400

    required_fields = ["name", "description", "query", "operator", "expected_value"]
    for field in required_fields:
        if field not in rule_data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        logger.info(f"Adding validation rule for organization: {organization_id}, table: {table_name}")
        rule_id = validation_manager.add_rule(organization_id, table_name, rule_data)

        if rule_id:
            return jsonify({"success": True, "id": rule_id})
        else:
            return jsonify({"error": "Failed to add validation rule"}), 500
    except Exception as e:
        logger.error(f"Error adding validation rule: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validations", methods=["DELETE"])
@token_required
def delete_validation_rule(current_user, organization_id):
    """Delete a validation rule"""
    table_name = request.args.get("table")
    rule_name = request.args.get("rule_name")

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400
    if not rule_name:
        return jsonify({"error": "Rule name is required"}), 400

    try:
        logger.info(f"Deleting validation rule {rule_name} for organization: {organization_id}, table: {table_name}")
        success = validation_manager.delete_rule(organization_id, table_name, rule_name)

        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Rule not found or delete failed"}), 404
    except Exception as e:
        logger.error(f"Error deleting validation rule: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-validations", methods=["POST"])
@token_required
def run_validation_rules(current_user, organization_id):
    """Run all validation rules for a table"""
    data = request.get_json()
    logger.info(f"Run validations request: {data}")

    log_memory_usage()

    if not data or "table" not in data:
        logger.warning("Table name missing in request")
        return jsonify({"error": "Table name is required"}), 400

    connection_string = data.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    connection_id = data.get("connection_id")
    table_name = data["table"]
    profile_history_id = data.get("profile_history_id")

    logger.info(f"Running validations with profile_history_id: {profile_history_id}")

    try:
        logger.info(f"Running validations for org: {organization_id}, table: {table_name}")

        log_memory_usage()

        # Use the optimized internal function
        result = run_validation_rules_internal(current_user, organization_id, data)

        # If there was an error, return it
        if "error" in result:
            return jsonify({"error": result["error"]}), 500

        # Otherwise return the results
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error running validations: {str(e)}")
        traceback.print_exc()

        # If exception suggests schema issues and we have connection_id
        if connection_id and ("no such column" in str(e).lower() or
                              "table not found" in str(e).lower() or
                              "does not exist" in str(e).lower()):
            try:
                from core.metadata.events import MetadataEventType, publish_metadata_event
                # Publish schema mismatch event
                publish_metadata_event(
                    event_type=MetadataEventType.VALIDATION_FAILURE,
                    connection_id=connection_id,
                    details={
                        "table_name": table_name,
                        "reason": "schema_mismatch",
                        "error": str(e)
                    },
                    organization_id=organization_id,
                    user_id=current_user
                )
                logger.info(f"Published schema mismatch event due to exception for table {table_name}")
            except Exception as event_error:
                logger.error(f"Error publishing schema mismatch event: {str(event_error)}")

        return jsonify({"error": str(e)}), 500


@app.route("/api/generate-default-validations", methods=["POST"])
@token_required
def generate_default_validations(current_user, organization_id):
    data = request.get_json()

    logger.info(f"Received data: {data}")

    if not data or "table" not in data:
        return jsonify({"error": "Table name is required"}), 400

    # Extract values and provide fallbacks
    connection_string = data.get("connection_string")
    if not connection_string:
        connection_string = os.getenv("DEFAULT_CONNECTION_STRING")

    table_name = data["table"]

    try:
        # Get existing rules
        existing_rules = validation_manager.get_rules(organization_id, table_name)
        existing_rule_names = {rule['rule_name'] for rule in existing_rules}

        # Generate potential new validations using sparvi-core
        validations = get_default_validations(connection_string, table_name)

        count_added = 0
        count_skipped = 0

        for validation in validations:
            try:
                # Skip if rule with same name already exists
                if validation['name'] in existing_rule_names:
                    count_skipped += 1
                    continue

                validation_manager.add_rule(organization_id, table_name, validation)
                count_added += 1
            except Exception as e:
                logger.error(f"Failed to add validation rule {validation['name']}: {str(e)}")

        result = {
            "added": count_added,
            "skipped": count_skipped,
            "total": count_added + count_skipped
        }

        logger.info(f"Added {result['added']} default validation rules ({result['skipped']} skipped as duplicates)")
        return jsonify({
            "success": True,
            "message": f"Added {result['added']} default validation rules ({result['skipped']} skipped as duplicates)",
            "count": result['added'],
            "skipped": result['skipped'],
            "total": result['total']
        })
    except Exception as e:
        logger.error(f"Error generating default validations: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


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


@app.route("/api/profile", methods=["GET"])
@token_required
def get_profile(current_user, organization_id):
    connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
    table_name = request.args.get("table", "employees")
    connection_id = request.args.get("connection_id")

    # Explicitly set include_samples to false - no row data in profiles
    include_samples = False  # Override to always be false regardless of request

    try:
        logger.info(f"========== PROFILING STARTED ==========")
        logger.info(f"Profiling table {table_name}...")
        logger.info(f"User ID: {current_user}, Organization ID: {organization_id}")

        # Log memory usage at start
        log_memory_usage("Before profiling")
        force_gc()

        # If connection_id provided but no connection string, get connection string from ID
        if connection_id and not connection_string:
            connection = connection_access_check(connection_id, organization_id)
            if not connection:
                return jsonify({"error": "Connection not found or access denied"}), 404

            # Use cached connection string builder
            connection_string = get_connection_string(connection)
            if not connection_string:
                return jsonify({"error": "Failed to build connection string"}), 400

        # Check if connection string is properly formatted
        if "://" not in connection_string:
            logger.error(f"Invalid connection string format: {connection_string[:20]}...")
            return jsonify({"error": "Invalid connection string format"}), 400

        # Rest of the function stays the same
        # Resolve any environment variable references in connection string
        from core.utils.connection_utils import resolve_connection_string, detect_connection_type
        resolved_connection = resolve_connection_string(connection_string)
        db_type = detect_connection_type(resolved_connection)
        logger.info(f"Database type detected: {db_type}")

        # Create profile history manager
        profile_history = SupabaseProfileHistoryManager()
        logger.info("Created SupabaseProfileHistoryManager")

        # Try to get previous profile to detect changes
        previous_profile = profile_history.get_latest_profile(organization_id, table_name)
        logger.info(f"Previous profile found: {previous_profile is not None}")

        # Set reasonable timeouts for Snowflake queries
        if db_type == "snowflake":
            # Set query timeout options
            from urllib.parse import parse_qs, urlparse, urlencode, urlunparse

            # Parse the connection string
            parsed_url = urlparse(resolved_connection)

            # Get existing query parameters
            query_params = parse_qs(parsed_url.query)

            # Add timeout parameters if not already present
            if 'statement_timeout_in_seconds' not in query_params:
                query_params['statement_timeout_in_seconds'] = ['300']  # 5 minutes

            # Rebuild the query string
            new_query = urlencode(query_params, doseq=True)

            # Rebuild the connection string
            resolved_connection = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                new_query,
                parsed_url.fragment
            ))

            logger.info(f"Added timeout parameters to Snowflake connection")

        # Run the profiler using the sparvi-core package - with no samples
        logger.info(f"Starting profile_table call")
        result = profile_table(resolved_connection, table_name, previous_profile, include_samples=include_samples)
        result["timestamp"] = datetime.datetime.now().isoformat()
        logger.info(f"Profile completed with {len(result)} keys")

        # Log memory usage after profiling
        log_memory_usage("After profiling")
        force_gc()

        # Check for problematic fields
        problematic_fields = [k for k in result.keys() if not isinstance(k, str)]
        if problematic_fields:
            logger.warning(f"Found problematic field keys: {problematic_fields}")
            # Try to clean up problematic fields
            for field in problematic_fields:
                logger.warning(f"Removing problematic field: {field}")
                del result[field]

        # Save the profile to Supabase - use original connection string to avoid storing credentials
        from core.utils.connection_utils import sanitize_connection_string
        sanitized_connection = sanitize_connection_string(connection_string)
        logger.info("About to save profile to Supabase")

        # Force garbage collection before saving profile
        log_memory_usage("After garbage collection, before saving profile")

        profile_id = None
        try:
            profile_id = profile_history.save_profile(current_user, organization_id, result, sanitized_connection)
            logger.info(f"Profile save result: {profile_id}")
            force_gc()

            # After successfully saving the profile, publish a PROFILE_COMPLETION event if we have connection_id
            if profile_id and connection_id:
                try:
                    from core.metadata.events import MetadataEventType, publish_metadata_event
                    # Publish event for metadata update
                    publish_metadata_event(
                        event_type=MetadataEventType.PROFILE_COMPLETION,
                        connection_id=connection_id,
                        details={
                            "table_name": table_name,
                            "profile_id": profile_id
                        },
                        organization_id=organization_id,
                        user_id=current_user
                    )
                    logger.info(f"Published profile completion event for table {table_name}")
                except ImportError:
                    logger.warning("Could not import event system, skipping event publication")
                except Exception as e:
                    logger.error(f"Error publishing profile completion event: {str(e)}")
                    logger.error(traceback.format_exc())

            # Now that we have the profile_id, we can run validations with it
            if profile_id and run_validations_after_profile:
                logger.info(f"Automatically running validations for profile {profile_id}")
                try:
                    validation_data = {
                        "table": table_name,
                        "connection_string": connection_string,
                        "connection_id": connection_id,  # Pass connection_id for event publishing
                        "profile_history_id": profile_id  # Pass the profile ID
                    }
                    validation_results = run_validation_rules_internal(current_user, organization_id, validation_data)
                    if validation_results and "results" in validation_results:
                        result["validation_results"] = validation_results["results"]
                        logger.info(f"Added {len(validation_results['results'])} validation results to profile")
                except Exception as val_error:
                    logger.error(f"Error running automatic validations: {str(val_error)}")
                    # Continue even if validation fails

        except Exception as save_error:
            logger.error(f"Exception saving profile to Supabase: {str(save_error)}")
            logger.error(traceback.format_exc())
            # Continue execution even if save fails

        # Get trend data from history
        trends = profile_history.get_trends(organization_id, table_name)
        if not isinstance(trends, dict) or "error" not in trends:
            result["trends"] = trends
            logger.info(f"Added trends data with {len(trends.get('timestamps', []))} points")
        else:
            logger.warning(f"Could not get trends: {trends.get('error', 'Unknown error')}")

        # Final memory usage check
        log_memory_usage("End of profile endpoint")
        force_gc()

        return jsonify(result)
    except Exception as e:
        logger.error(f"Error profiling table: {str(e)}")
        traceback.print_exc()  # Print the full traceback to your console
        logger.info(f"About to return response for profile of table {table_name}")
        return jsonify(result)

@app.route("/api/validations/<rule_id>", methods=["PUT"])
@token_required
def update_validation(current_user, organization_id, rule_id):
    """Update an existing validation rule"""
    table_name = request.args.get("table")
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    rule_data = request.get_json()
    if not rule_data:
        return jsonify({"error": "Rule data is required"}), 400

    required_fields = ["name", "query", "operator", "expected_value"]
    for field in required_fields:
        if field not in rule_data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        logger.info(f"Updating validation rule {rule_id} for table {table_name}")
        success = validation_manager.update_rule(organization_id, rule_id, rule_data)

        if success:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Rule not found or update failed"}), 404
    except Exception as e:
        logger.error(f"Error updating validation rule: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


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


# These routes will be added to your backend/app.py file

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


# Add this new endpoint for data preview
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


# Connection management routes
@app.route("/api/connections", methods=["GET"])
@token_required
def get_connections(current_user, organization_id):
    """Get all connections for the organization"""
    try:
        # Query connections for this organization
        supabase_mgr = SupabaseManager()

        # Use the Supabase client to query the database_connections table
        connections_response = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("organization_id", organization_id) \
            .order("created_at") \
            .execute()

        connections = connections_response.data if connections_response.data else []

        # For security, remove passwords from the response
        for conn in connections:
            if 'connection_details' in conn and 'password' in conn['connection_details']:
                conn['connection_details'].pop('password', None)

        return jsonify({"connections": connections})
    except Exception as e:
        logger.error(f"Error fetching connections: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route("/api/connections/<connection_id>", methods=["GET"])
@token_required
def get_connection_by_id(current_user, organization_id, connection_id):
    """Get a specific connection by ID for the organization"""
    try:
        # Initialize Supabase manager
        supabase_mgr = SupabaseManager()

        # Query the specific connection
        connection_response = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("organization_id", organization_id) \
            .eq("id", connection_id) \
            .single() \
            .execute()

        connection = connection_response.data

        if not connection:
            return jsonify({"error": "Connection not found"}), 404

        # Remove password before returning
        if 'connection_details' in connection and 'password' in connection['connection_details']:
            connection['connection_details'].pop('password', None)

        return jsonify({"connection": connection})
    except Exception as e:
        logger.error(f"Error fetching connection {connection_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500



@app.route("/api/connections", methods=["POST"])
@token_required
def create_connection(current_user, organization_id):
    """Create a new database connection"""
    data = request.get_json()

    # Validate input
    required_fields = ["name", "connection_type", "connection_details"]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        # Check if this is the first connection - if so, make it the default
        is_default = False
        supabase_mgr = SupabaseManager()

        # Count existing connections
        count_response = supabase_mgr.supabase.table("database_connections") \
            .select("id", count="exact") \
            .eq("organization_id", organization_id) \
            .execute()

        # If no connections exist, make this one the default
        if count_response.count == 0:
            is_default = True

        # Insert the new connection
        connection_data = {
            "organization_id": organization_id,
            "created_by": current_user,
            "name": data["name"],
            "connection_type": data["connection_type"],
            "connection_details": data["connection_details"],
            "is_default": is_default
        }

        response = supabase_mgr.supabase.table("database_connections") \
            .insert(connection_data) \
            .execute()

        # If successful, return the new connection
        if response.data and len(response.data) > 0:
            new_connection = response.data[0]

            # For security, remove password from the response
            if 'connection_details' in new_connection and 'password' in new_connection['connection_details']:
                new_connection['connection_details'].pop('password', None)

            return jsonify({"connection": new_connection})
        else:
            return jsonify({"error": "Failed to create connection"}), 500

    except Exception as e:
        logger.error(f"Error creating connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>", methods=["PUT"])
@token_required
def update_connection(current_user, organization_id, connection_id):
    """Update an existing database connection"""
    data = request.get_json()

    # Validate input
    required_fields = ["name", "connection_type", "connection_details"]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

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

        # Handle password updates specially - don't update password if not provided
        existing_connection = get_response.data[0]
        update_data = {
            "name": data["name"],
            "connection_type": data["connection_type"],
            "connection_details": data["connection_details"],
            "updated_at": datetime.datetime.now().isoformat()
        }

        # If password is empty and we're updating, keep the existing password
        if not data["connection_details"].get("password") and \
                "password" in existing_connection["connection_details"]:
            update_data["connection_details"]["password"] = existing_connection["connection_details"]["password"]

        # Update the connection
        response = supabase_mgr.supabase.table("database_connections") \
            .update(update_data) \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        # If successful, return the updated connection
        if response.data and len(response.data) > 0:
            updated_connection = response.data[0]

            # For security, remove password from the response
            if 'connection_details' in updated_connection and 'password' in updated_connection['connection_details']:
                updated_connection['connection_details'].pop('password', None)

            return jsonify({"connection": updated_connection})
        else:
            return jsonify({"error": "Failed to update connection"}), 500

    except Exception as e:
        logger.error(f"Error updating connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/connections/<connection_id>", methods=["DELETE"])
@token_required
def delete_connection(current_user, organization_id, connection_id):
    """Delete a database connection"""
    try:
        supabase_mgr = SupabaseManager()

        # First check if the connection exists and belongs to this organization
        get_response = supabase_mgr.supabase.table("database_connections") \
            .select("is_default") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not get_response.data or len(get_response.data) == 0:
            return jsonify({"error": "Connection not found or you don't have permission to delete it"}), 404

        # Don't allow deleting the default connection
        if get_response.data[0].get("is_default", False):
            return jsonify(
                {"error": "Cannot delete the default connection. Make another connection the default first."}), 400

        # Delete the connection
        delete_response = supabase_mgr.supabase.table("database_connections") \
            .delete() \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        # Check if any rows were deleted
        if delete_response.data and len(delete_response.data) > 0:
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to delete connection"}), 500

    except Exception as e:
        logger.error(f"Error deleting connection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


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


@app.route("/api/connections/test", methods=["POST"])
@token_required
def test_connection(current_user, organization_id):
    """Test a database connection without saving it"""
    data = request.get_json()

    # Validate input
    required_fields = ["connection_type", "connection_details"]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    # Build connection string based on connection type
    connection_string = ""
    connection_type = data["connection_type"]
    details = data["connection_details"]

    try:
        if connection_type == "snowflake":
            if details.get("useEnvVars", False):
                # Use environment variables
                prefix = details.get("envVarPrefix", "SNOWFLAKE")
                from core.utils.connection_utils import get_snowflake_connection_from_env
                connection_string = get_snowflake_connection_from_env(prefix)

                if not connection_string:
                    return jsonify({
                        "error": f"Required environment variables for {prefix} connection not found"
                    }), 400
            else:
                # Validate required fields
                if not all([
                    details.get("username"),
                    details.get("password"),
                    details.get("account"),
                    details.get("database"),
                    details.get("warehouse")
                ]):
                    return jsonify({
                        "error": "Missing required Snowflake connection parameters"
                    }), 400

                # Use the cached connection string builder
                connection = {
                    "connection_type": "snowflake",
                    "connection_details": details
                }

                connection_string = get_connection_string(connection)
                if not connection_string:
                    return jsonify({
                        "error": "Failed to build connection string"
                    }), 400

        elif connection_type == "postgresql":
            # Validate required fields
            if not all([
                details.get("username"),
                details.get("password"),
                details.get("host"),
                details.get("database")
            ]):
                return jsonify({
                    "error": "Missing required PostgreSQL connection parameters"
                }), 400

            # Use the cached connection string builder
            connection = {
                "connection_type": "postgresql",
                "connection_details": details
            }

            connection_string = get_connection_string(connection)
            if not connection_string:
                return jsonify({
                    "error": "Failed to build connection string"
                }), 400

        else:
            return jsonify({"error": f"Unsupported connection type: {connection_type}"}), 400

        # OPTIMIZATION: Simplify connection test with minimal query
        try:
            # Use connection pool to test the connection
            engine = connection_pool_manager.get_engine(connection_string)

            # Simple connection test with minimal query based on database type
            with engine.connect() as conn:
                if connection_type == "snowflake":
                    # Minimal Snowflake test query
                    result = conn.execute(text("SELECT CURRENT_USER()"))
                    user = result.scalar()

                    return jsonify({
                        "message": "Connection successful!",
                        "details": {
                            "user": user,
                            "connection_type": "snowflake"
                        }
                    })

                elif connection_type == "postgresql":
                    # Minimal PostgreSQL test query
                    result = conn.execute(text("SELECT current_user"))
                    user = result.scalar()

                    return jsonify({
                        "message": "Connection successful!",
                        "details": {
                            "user": user,
                            "connection_type": "postgresql"
                        }
                    })

        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": f"Connection failed: {str(e)}"}), 400

    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
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


@app.route("/api/profile-history", methods=["GET"])
@token_required
def get_profile_history(current_user, organization_id):
    """Get history of profile runs for a table"""
    table_name = request.args.get("table")
    limit = request.args.get("limit", 10, type=int)

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    try:
        logger.info(f"Getting profile history for organization: {organization_id}, table: {table_name}, limit: {limit}")

        # Create profile history manager
        profile_history = SupabaseProfileHistoryManager()

        # Get history data
        history_data = profile_history.get_profile_history(organization_id, table_name, limit)

        logger.info(f"Retrieved {len(history_data)} profile history records")
        return jsonify({"history": history_data})
    except Exception as e:
        logger.error(f"Error getting profile history: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/validation-history/<profile_id>", methods=["GET"])
@token_required
def get_validation_history_by_profile(current_user, organization_id, profile_id):
    """Get validation results for a specific profile run"""
    try:
        logger.info(f"Fetching validation history for profile ID: {profile_id}")

        # Get direct Supabase client credentials
        import os
        from supabase import create_client

        # Get credentials from environment
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

        if not supabase_url or not supabase_key:
            logger.error("Missing Supabase configuration")
            return jsonify({"error": "Server configuration error"}), 500

        # Create direct client
        direct_client = create_client(supabase_url, supabase_key)

        # Query validation results linked to this profile
        response = direct_client.table("validation_results") \
            .select("*, validation_rules(*)") \
            .eq("organization_id", organization_id) \
            .eq("profile_history_id", profile_id) \
            .execute()

        if not response.data:
            logger.info(f"No validation results found for profile ID: {profile_id}")
            return jsonify({"results": []})

        logger.info(f"Found {len(response.data)} validation results for profile ID: {profile_id}")
        return jsonify({"results": response.data})
    except Exception as e:
        logger.error(f"Error getting validation history: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


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

        # Make sure task manager is initialized
        if metadata_task_manager is None:
            init_metadata_task_manager()

        # Determine collection type from request parameters
        collection_type = request.json.get("collection_type", "immediate")
        table_limit = request.json.get("table_limit", 50)
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
                collector = MetadataCollector(connection_id, connector)

                # Get table list (limited for immediate response)
                tables = collector.collect_table_list()
                # Fix: Apply the slice to tables AFTER we get them
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

        # Queue full collection as background task with the improved task manager
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
                "collected_at": datetime.datetime.now().isoformat(),
                "collection_duration_ms": 0
            },
            "column_statistics": {}
        }

        start_time = datetime.datetime.now()

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
        end_time = datetime.datetime.now()
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
                now = datetime.datetime.now().isoformat()

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


@app.route("/api/connections/<connection_id>/changes", methods=["GET"])
@token_required
def get_schema_changes(current_user, organization_id, connection_id):
    """Get schema changes since a specific timestamp"""
    try:
        # Parse query parameters
        since_timestamp = request.args.get("since")
        if not since_timestamp:
            # Default to 24 hours ago if not specified
            since_timestamp = (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat()

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

        # Query connection_metadata to find metadata updated since the timestamp
        # We want to query both table and column metadata types

        try:
            # Direct query to Supabase for metadata changes
            import os
            from supabase import create_client

            # Get credentials from environment
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

            # Create direct client for more complex query
            direct_client = create_client(supabase_url, supabase_key)

            # Query connection_metadata for changes since the timestamp
            response = direct_client.table("connection_metadata") \
                .select("metadata_type, collected_at, metadata") \
                .eq("connection_id", connection_id) \
                .gte("collected_at", since_timestamp) \
                .in_("metadata_type", ["tables", "columns"]) \
                .order("collected_at", desc=True) \
                .execute()

            if not response.data:
                return jsonify({
                    "changes": [],
                    "since": since_timestamp,
                    "message": "No schema changes detected"
                })

            # Process changes - need to compare with previous state
            changes = []

            # Get previous state (prior to since_timestamp)
            previous_state_response = direct_client.table("connection_metadata") \
                .select("metadata_type, collected_at, metadata") \
                .eq("connection_id", connection_id) \
                .lt("collected_at", since_timestamp) \
                .in_("metadata_type", ["tables", "columns"]) \
                .order("collected_at", desc=True)  \
                .execute()

            previous_tables = None
            previous_columns_by_table = None

            # Extract previous state
            if previous_state_response.data:
                for item in previous_state_response.data:
                    if item["metadata_type"] == "tables" and previous_tables is None:
                        previous_tables = item["metadata"].get("tables", [])
                    elif item["metadata_type"] == "columns" and previous_columns_by_table is None:
                        previous_columns_by_table = item["metadata"].get("columns_by_table", {})

            # If no previous state, we can't detect changes
            if previous_tables is None:
                return jsonify({
                    "changes": [],
                    "since": since_timestamp,
                    "message": "No baseline schema found for comparison"
                })

            # Process current state
            current_tables = None
            current_columns_by_table = None

            for item in response.data:
                if item["metadata_type"] == "tables" and current_tables is None:
                    current_tables = item["metadata"].get("tables", [])
                elif item["metadata_type"] == "columns" and current_columns_by_table is None:
                    current_columns_by_table = item["metadata"].get("columns_by_table", {})

            # Detect added/removed tables
            previous_table_names = {table["name"] for table in previous_tables}
            current_table_names = {table["name"] for table in current_tables} if current_tables else set()

            # Tables that were added
            for table_name in current_table_names - previous_table_names:
                changes.append({
                    "type": "table_added",
                    "table": table_name,
                    "timestamp": next(
                        (item["collected_at"] for item in response.data if item["metadata_type"] == "tables"), None)
                })

            # Tables that were removed
            for table_name in previous_table_names - current_table_names:
                changes.append({
                    "type": "table_removed",
                    "table": table_name,
                    "timestamp": next(
                        (item["collected_at"] for item in response.data if item["metadata_type"] == "tables"), None)
                })

            # For tables that exist in both, check for column changes
            for table_name in previous_table_names & current_table_names:
                # Skip if we don't have column information
                if (not current_columns_by_table or table_name not in current_columns_by_table or
                        not previous_columns_by_table or table_name not in previous_columns_by_table):
                    continue

                previous_columns = previous_columns_by_table[table_name]
                current_columns = current_columns_by_table[table_name]

                previous_column_names = {col["name"] for col in previous_columns}
                current_column_names = {col["name"] for col in current_columns}

                # Columns that were added
                for col_name in current_column_names - previous_column_names:
                    col_info = next((col for col in current_columns if col["name"] == col_name), {})
                    changes.append({
                        "type": "column_added",
                        "table": table_name,
                        "column": col_name,
                        "details": {
                            "type": col_info.get("type", "unknown"),
                            "nullable": col_info.get("nullable", True)
                        },
                        "timestamp": next(
                            (item["collected_at"] for item in response.data if item["metadata_type"] == "columns"),
                            None)
                    })

                # Columns that were removed
                for col_name in previous_column_names - current_column_names:
                    col_info = next((col for col in previous_columns if col["name"] == col_name), {})
                    changes.append({
                        "type": "column_removed",
                        "table": table_name,
                        "column": col_name,
                        "details": {
                            "type": col_info.get("type", "unknown")
                        },
                        "timestamp": next(
                            (item["collected_at"] for item in response.data if item["metadata_type"] == "columns"),
                            None)
                    })

                # For columns that exist in both, check for type changes
                for col_name in previous_column_names & current_column_names:
                    prev_col = next((col for col in previous_columns if col["name"] == col_name), {})
                    curr_col = next((col for col in current_columns if col["name"] == col_name), {})

                    # Check for type changes
                    if prev_col.get("type") != curr_col.get("type"):
                        changes.append({
                            "type": "column_type_changed",
                            "table": table_name,
                            "column": col_name,
                            "details": {
                                "previous_type": prev_col.get("type", "unknown"),
                                "new_type": curr_col.get("type", "unknown")
                            },
                            "timestamp": next(
                                (item["collected_at"] for item in response.data if item["metadata_type"] == "columns"),
                                None)
                        })

                    # Check for nullability changes
                    if prev_col.get("nullable") != curr_col.get("nullable"):
                        changes.append({
                            "type": "column_nullability_changed",
                            "table": table_name,
                            "column": col_name,
                            "details": {
                                "previous_nullable": prev_col.get("nullable", True),
                                "new_nullable": curr_col.get("nullable", True)
                            },
                            "timestamp": next(
                                (item["collected_at"] for item in response.data if item["metadata_type"] == "columns"),
                                None)
                        })

            # Sort changes by timestamp
            changes.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

            return jsonify({
                "changes": changes,
                "since": since_timestamp,
                "count": len(changes)
            })

        except Exception as e:
            logger.error(f"Error detecting schema changes: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": f"Error detecting schema changes: {str(e)}"}), 500

    except Exception as e:
        logger.error(f"Error in schema change detection: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


# In backend/app.py - add this new route

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
        schema["collected_at"] = tables_metadata.get("collected_at", datetime.datetime.now().isoformat())

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
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat()

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
        # Make sure task manager is initialized
        if metadata_task_manager is None:
            init_metadata_task_manager()

        # Get task parameters from request
        data = request.get_json() or {}
        task_type = data.get("task_type", "full_collection")
        priority = data.get("priority", "medium")
        table_name = data.get("table_name")

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

        # Make sure task manager is initialized
        if metadata_task_manager is None:
            init_metadata_task_manager()

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
    """Trigger a metadata refresh based on event or user request"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Get refresh parameters
        data = request.get_json() or {}
        metadata_type = data.get("metadata_type", "schema")
        table_name = data.get("table_name")

        # Make sure task manager is initialized
        if metadata_task_manager is None:
            init_metadata_task_manager()

        # Submit appropriate task based on metadata type
        task_id = None

        if metadata_type == "schema":
            if table_name:
                # Refresh a specific table's schema
                task_id = metadata_task_manager.submit_table_metadata_task(connection_id, table_name, "high")
                message = f"Scheduled schema refresh for table {table_name}"
            else:
                # Refresh all tables (limited)
                params = {"depth": "light", "table_limit": 100}
                task_id = metadata_task_manager.submit_collection_task(connection_id, params, "high")
                message = "Scheduled schema refresh for all tables"

        elif metadata_type == "statistics":
            if table_name:
                # Refresh statistics for a specific table
                task_id = metadata_task_manager.submit_statistics_refresh_task(connection_id, table_name, "high")
                message = f"Scheduled statistics refresh for table {table_name}"
            else:
                # Refresh statistics for recently accessed tables
                params = {"depth": "medium", "table_limit": 20, "focus": "statistics"}
                task_id = metadata_task_manager.submit_collection_task(connection_id, params, "high")
                message = "Scheduled statistics refresh for recently accessed tables"

        elif metadata_type == "full":
            # Comprehensive refresh
            params = {"depth": "deep", "table_limit": 50}
            task_id = metadata_task_manager.submit_collection_task(connection_id, params, "high")
            message = "Scheduled comprehensive metadata refresh"

        else:
            return jsonify({"error": f"Unknown metadata type: {metadata_type}"}), 400

        if task_id:
            # Also create a USER_REQUEST event for the event system
            try:
                publish_metadata_event(
                    event_type=MetadataEventType.USER_REQUEST,
                    connection_id=connection_id,
                    details={
                        "metadata_type": metadata_type,
                        "table_name": table_name
                    },
                    organization_id=organization_id,
                    user_id=current_user
                )
            except Exception as e:
                logger.warning(f"Failed to publish event, but task was still created: {str(e)}")

            return jsonify({
                "task_id": task_id,
                "status": "scheduled",
                "message": message
            })
        else:
            return jsonify({
                "status": "no_action",
                "message": "No refresh action needed at this time"
            })

    except Exception as e:
        logger.error(f"Error refreshing metadata: {str(e)}")
        logger.error(traceback.format_exc())
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

        # Make sure task manager is initialized
        if metadata_task_manager is None:
            init_metadata_task_manager()

        # Get metadata status efficiently - use the cached getter
        metadata_types = ["tables", "columns", "statistics"]
        status = {}

        for metadata_type in metadata_types:
            metadata = get_metadata_cached(connection_id, metadata_type)

            if metadata:
                status[metadata_type] = {
                    "available": True,
                    "freshness": metadata.get("freshness", {"status": "unknown"}),
                    "last_updated": metadata.get("collected_at")
                }
            else:
                status[metadata_type] = {
                    "available": False,
                    "freshness": {"status": "unknown"},
                    "last_updated": None
                }

        # Get any pending tasks - limit to this connection only
        tasks = metadata_task_manager.get_recent_tasks(5, connection_id)
        pending_tasks = [
            task for task in tasks
            if task.get("task", {}).get("status") == "pending"
        ]

        # Add to status
        status["pending_tasks"] = pending_tasks

        # Add connection info
        status["connection"] = {
            "name": connection.get("name", "Unknown"),
            "type": connection.get("connection_type", "Unknown"),
            "id": connection_id
        }

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

        # Make sure task manager is initialized
        if metadata_task_manager is None:
            init_metadata_task_manager()

        # Get worker stats
        stats = metadata_task_manager.get_worker_stats()

        return jsonify(stats)

    except Exception as e:
        logger.error(f"Error getting worker stats: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500
        supabase_mgr = SupabaseManager()
        connection_check = supabase_mgr.supabase.table("database_connections") \
            .select("*") \
            .eq("id", connection_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not connection_check.data or len(connection_check.data) == 0:
            logger.error(f"Connection not found or access denied: {connection_id}")
            return jsonify({"error": "Connection not found or access denied"}), 404

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


@app.route("/api/connections/<connection_id>/metadata/tasks/<task_id>", methods=["GET"])
@token_required
def get_metadata_task_status(current_user, organization_id, connection_id, task_id):
    """Get status of a metadata collection task"""
    try:
        # Check access to connection
        connection = connection_access_check(connection_id, organization_id)
        if not connection:
            return jsonify({"error": "Connection not found or access denied"}), 404

        # Make sure task manager is initialized
        if metadata_task_manager is None:
            init_metadata_task_manager()

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
    """Detect schema changes for a connection"""
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

        # Create connector for this connection
        from core.metadata.connector_factory import ConnectorFactory
        connector_factory = ConnectorFactory(supabase_mgr)

        # Create collector and storage service
        connector = connector_factory.create_connector(connection)
        connector.connect()

        from core.metadata.collector import MetadataCollector
        from core.metadata.storage_service import MetadataStorageService

        collector = MetadataCollector(connection_id, connector)
        storage_service = MetadataStorageService()

        # Collect current schema
        current_schema = {
            "tables": []
        }

        # Get table list (limited to 50 tables for performance)
        tables = collector.collect_table_list()[:50]

        # Collect column metadata for each table
        for table_name in tables:
            try:
                columns = collector.collect_columns(table_name)
                table_data = {
                    "name": table_name,
                    "columns": columns,
                    "column_count": len(columns)
                }
                current_schema["tables"].append(table_data)
            except Exception as e:
                logger.warning(f"Error collecting columns for {table_name}: {str(e)}")

        # Get previous schema from storage
        previous_schema = None
        stored_metadata = storage_service.get_metadata(connection_id, "tables")

        if stored_metadata and "metadata" in stored_metadata:
            previous_schema = stored_metadata["metadata"]

            # Compare schemas to find changes
            changes = []

            # Extract table names from both schemas
            current_tables = {table["name"]: table for table in current_schema["tables"]}
            previous_tables = {}

            if "tables" in previous_schema:
                previous_tables = {table["name"]: table for table in previous_schema["tables"]}

            # Find added tables
            for table_name in set(current_tables.keys()) - set(previous_tables.keys()):
                changes.append({
                    "type": "table_added",
                    "table": table_name,
                    "timestamp": datetime.datetime.now().isoformat()
                })

            # Find removed tables
            for table_name in set(previous_tables.keys()) - set(current_tables.keys()):
                changes.append({
                    "type": "table_removed",
                    "table": table_name,
                    "timestamp": datetime.datetime.now().isoformat()
                })

            # Check for column changes in common tables
            for table_name in set(current_tables.keys()) & set(previous_tables.keys()):
                # Get columns for this table in both schemas
                current_columns = {col["name"]: col for col in current_tables[table_name].get("columns", [])}
                previous_columns = {col["name"]: col for col in previous_tables[table_name].get("columns", [])}

                # Find added columns
                for col_name in set(current_columns.keys()) - set(previous_columns.keys()):
                    col_info = current_columns[col_name]
                    changes.append({
                        "type": "column_added",
                        "table": table_name,
                        "column": col_name,
                        "details": {
                            "type": col_info.get("type", "unknown"),
                            "nullable": col_info.get("nullable", None)
                        },
                        "timestamp": datetime.datetime.now().isoformat()
                    })

                # Find removed columns
                for col_name in set(previous_columns.keys()) - set(current_columns.keys()):
                    col_info = previous_columns[col_name]
                    changes.append({
                        "type": "column_removed",
                        "table": table_name,
                        "column": col_name,
                        "details": {
                            "type": col_info.get("type", "unknown")
                        },
                        "timestamp": datetime.datetime.now().isoformat()
                    })

                # Find column type or property changes
                for col_name in set(current_columns.keys()) & set(previous_columns.keys()):
                    current_col = current_columns[col_name]
                    previous_col = previous_columns[col_name]

                    # Check for type changes
                    if str(current_col.get("type", "")) != str(previous_col.get("type", "")):
                        changes.append({
                            "type": "column_type_changed",
                            "table": table_name,
                            "column": col_name,
                            "details": {
                                "previous_type": previous_col.get("type", "unknown"),
                                "new_type": current_col.get("type", "unknown")
                            },
                            "timestamp": datetime.datetime.now().isoformat()
                        })

            # If changes were detected, publish events and store new schema
            if changes:
                # Store the new schema
                storage_service.store_tables_metadata(connection_id, current_schema["tables"])

                # Publish events for each affected table
                task_ids = []
                affected_tables = set()

                for change in changes:
                    if "table" in change:
                        affected_tables.add(change["table"])

                for table_name in affected_tables:
                    # Get changes for this table
                    table_changes = [c for c in changes if c.get("table") == table_name]

                    # Publish a schema change event
                    task_id = publish_metadata_event(
                        event_type=MetadataEventType.SCHEMA_CHANGE,
                        connection_id=connection_id,
                        details={
                            "table_name": table_name,
                            "changes": [c["type"] for c in table_changes]
                        },
                        organization_id=organization_id,
                        user_id=current_user
                    )

                    if task_id:
                        task_ids.append(task_id)

                return jsonify({
                    "changes_detected": len(changes),
                    "changes": changes,
                    "tasks_created": len(task_ids)
                })
            else:
                return jsonify({
                    "changes_detected": 0,
                    "message": "No schema changes detected"
                })
        else:
            # No previous schema to compare, just store current schema
            storage_service.store_tables_metadata(connection_id, current_schema["tables"])

            return jsonify({
                "changes_detected": 0,
                "message": "Initial schema collected, no changes to detect"
            })

    except Exception as e:
        logger.error(f"Error detecting schema changes: {str(e)}")
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
        limit = request.args.get("limit", 10, type=int)

        # Create analytics service
        from core.metadata.change_analytics import MetadataChangeAnalytics
        analytics = MetadataChangeAnalytics(supabase_mgr)

        # Get high-impact objects
        objects = analytics.get_high_impact_objects(
            connection_id=connection_id,
            limit=limit
        )

        return jsonify({"objects": objects, "count": len(objects)})

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

        # Create analytics service
        from core.metadata.change_analytics import MetadataChangeAnalytics
        analytics = MetadataChangeAnalytics(supabase_mgr)

        # Get high-impact objects
        high_impact_objects = analytics.get_high_impact_objects(
            connection_id=connection_id,
            limit=5
        )

        # Get total checks and changes
        sql = f"""
        SELECT 
            COUNT(*) as total_checks,
            SUM(CASE WHEN change_detected = true THEN 1 ELSE 0 END) as total_changes,
            COUNT(DISTINCT object_name) as unique_objects,
            MAX(check_timestamp) as last_check
        FROM metadata_change_analytics
        WHERE connection_id = '{connection_id}'
        AND check_timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days';
        """

        stats_response = supabase_mgr.supabase.rpc("exec_sql", {"sql_query": sql}).execute()
        overall_stats = stats_response.data[0] if stats_response.data else {
            "total_checks": 0,
            "total_changes": 0,
            "unique_objects": 0,
            "last_check": None
        }

        # Get change frequency distribution
        sql = f"""
        WITH object_changes AS (
            SELECT 
                object_name,
                object_type,
                COUNT(*) as checks,
                SUM(CASE WHEN change_detected = true THEN 1 ELSE 0 END) as changes,
                (SUM(CASE WHEN change_detected = true THEN 1 ELSE 0 END)::float / 
                 NULLIF(COUNT(*), 0)::float) as change_ratio
            FROM metadata_change_analytics
            WHERE connection_id = '{connection_id}'
            AND check_timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days'
            GROUP BY object_name, object_type
            HAVING COUNT(*) >= 5
        )
        SELECT 
            CASE 
                WHEN change_ratio >= 0.5 THEN 'high'
                WHEN change_ratio >= 0.1 THEN 'medium'
                ELSE 'low'
            END as frequency,
            COUNT(*) as object_count
        FROM object_changes
        GROUP BY frequency
        ORDER BY frequency;
        """

        freq_response = supabase_mgr.supabase.rpc("exec_sql", {"sql_query": sql}).execute()
        frequency_distribution = freq_response.data if freq_response.data else []

        # Get historical trend of changes
        sql = f"""
        WITH daily_changes AS (
            SELECT 
                DATE_TRUNC('day', check_timestamp) as day,
                COUNT(*) as checks,
                SUM(CASE WHEN change_detected = true THEN 1 ELSE 0 END) as changes
            FROM metadata_change_analytics
            WHERE connection_id = '{connection_id}'
            AND check_timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days'
            GROUP BY day
            ORDER BY day
        )
        SELECT 
            TO_CHAR(day, 'YYYY-MM-DD') as date,
            checks,
            changes,
            (changes::float / NULLIF(checks, 0)::float * 100) as change_percentage
        FROM daily_changes
        ORDER BY day;
        """

        trend_response = supabase_mgr.supabase.rpc("exec_sql", {"sql_query": sql}).execute()
        change_trend = trend_response.data if trend_response.data else []

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


@app.after_request
def after_request(response):
    origin = request.headers.get('Origin', '')

    # Allow both your production and development environments
    if origin in ['https://cloud.sparvi.io', 'http://localhost:3000']:
        response.headers.set('Access-Control-Allow-Origin', origin)
    else:
        response.headers.set('Access-Control-Allow-Origin', 'https://cloud.sparvi.io')

    response.headers.set('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
    response.headers.set('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.set('Access-Control-Allow-Credentials', 'true')
    # Cache preflight requests to reduce OPTIONS calls
    response.headers.set('Access-Control-Max-Age', '3600')  # 1 hour
    return response


if __name__ == "__main__":
    # In production, ensure you run with HTTPS (via a reverse proxy or WSGI server with SSL configured)
    app.run(debug=True)