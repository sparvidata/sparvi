import logging
import time
import threading
import queue
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Configure logging
logger = logging.getLogger(__name__)


class MetadataTask:
    """Represents a metadata collection task"""

    def __init__(self, task_type, connection_id, params=None, priority="medium"):
        """
        Initialize a metadata task

        Args:
            task_type: Type of task (full_collection, table_metadata, refresh, etc.)
            connection_id: ID of the database connection
            params: Additional parameters for the task
            priority: Task priority (high, medium, low)
        """
        self.id = str(uuid.uuid4())
        self.task_type = task_type
        self.connection_id = connection_id
        self.params = params or {}
        self.priority = priority
        self.created_at = datetime.now()
        self.status = "pending"
        self.result = None
        self.error = None

    def to_dict(self):
        """Convert task to dictionary"""
        return {
            "id": self.id,
            "task_type": self.task_type,
            "connection_id": self.connection_id,
            "params": self.params,
            "priority": self.priority,
            "created_at": self.created_at.isoformat(),
            "status": self.status
        }

    @staticmethod
    def from_dict(data):
        """Create task from dictionary"""
        task = MetadataTask(
            data["task_type"],
            data["connection_id"],
            data.get("params", {}),
            data.get("priority", "medium")
        )
        task.id = data["id"]
        task.status = data["status"]
        if "created_at" in data:
            task.created_at = datetime.fromisoformat(data["created_at"])
        return task


class PriorityTaskQueue:
    """Queue for metadata tasks with priority handling"""

    def __init__(self):
        """Initialize the priority queue"""
        # Use three separate queues for different priorities
        self.high_priority = queue.Queue()
        self.medium_priority = queue.Queue()
        self.low_priority = queue.Queue()

        # Task counts
        self.counts = {
            "high": 0,
            "medium": 0,
            "low": 0
        }

        # Lock for thread safety
        self.lock = threading.RLock()

    def put(self, task):
        """
        Add a task to the queue based on priority

        Args:
            task: The MetadataTask to add
        """
        with self.lock:
            if task.priority == "high":
                self.high_priority.put(task)
                self.counts["high"] += 1
            elif task.priority == "low":
                self.low_priority.put(task)
                self.counts["low"] += 1
            else:  # Default to medium
                self.medium_priority.put(task)
                self.counts["medium"] += 1

    def get(self, block=True, timeout=None):
        """
        Get the next task from the queue based on priority

        Args:
            block: Whether to block if queue is empty
            timeout: How long to wait if blocking

        Returns:
            The next MetadataTask or None if timeout
        """
        try:
            with self.lock:
                # Try high priority first
                if not self.high_priority.empty():
                    task = self.high_priority.get(False)
                    self.counts["high"] -= 1
                    return task

                # Then medium priority
                if not self.medium_priority.empty():
                    task = self.medium_priority.get(False)
                    self.counts["medium"] -= 1
                    return task

                # Finally low priority
                if not self.low_priority.empty():
                    task = self.low_priority.get(False)
                    self.counts["low"] -= 1
                    return task

            # If all queues are empty and we should block
            if block:
                # Wait for any queue to have an item
                if timeout:
                    end_time = time.time() + timeout
                    while time.time() < end_time:
                        # Check each queue
                        with self.lock:
                            if not self.high_priority.empty():
                                task = self.high_priority.get(False)
                                self.counts["high"] -= 1
                                return task
                            if not self.medium_priority.empty():
                                task = self.medium_priority.get(False)
                                self.counts["medium"] -= 1
                                return task
                            if not self.low_priority.empty():
                                task = self.low_priority.get(False)
                                self.counts["low"] -= 1
                                return task
                        # Sleep a bit before checking again
                        time.sleep(0.1)
                    # Timeout reached
                    return None
                else:
                    # Wait indefinitely
                    while True:
                        with self.lock:
                            if not self.high_priority.empty():
                                task = self.high_priority.get(False)
                                self.counts["high"] -= 1
                                return task
                            if not self.medium_priority.empty():
                                task = self.medium_priority.get(False)
                                self.counts["medium"] -= 1
                                return task
                            if not self.low_priority.empty():
                                task = self.low_priority.get(False)
                                self.counts["low"] -= 1
                                return task
                        time.sleep(0.1)

            # If we shouldn't block, return None
            return None
        except Exception as e:
            logger.error(f"Error getting task from queue: {str(e)}")
            return None

    def task_done(self, priority="medium"):
        """Mark a task as done"""
        if priority == "high":
            self.high_priority.task_done()
        elif priority == "low":
            self.low_priority.task_done()
        else:
            self.medium_priority.task_done()

    def get_stats(self):
        """Get statistics about the queue"""
        with self.lock:
            return {
                "high": self.counts["high"],
                "medium": self.counts["medium"],
                "low": self.counts["low"],
                "total": sum(self.counts.values())
            }

    def empty(self):
        """Check if all queues are empty"""
        with self.lock:
            return (self.high_priority.empty() and
                    self.medium_priority.empty() and
                    self.low_priority.empty())


class MetadataWorker:
    """Worker thread that processes metadata collection tasks"""

    def __init__(self, task_queue, storage_service, connector_factory, max_workers=3):
        """
        Initialize the metadata worker

        Args:
            task_queue: Queue for metadata tasks
            storage_service: Service for storing metadata
            connector_factory: Factory for creating database connectors
            max_workers: Maximum number of worker threads
        """
        self.task_queue = task_queue
        self.storage_service = storage_service
        self.connector_factory = connector_factory
        self.max_workers = max_workers
        self.workers = []
        self.active = False
        self.task_history = {}  # Store recent task results
        self.max_history = 100  # Maximum number of tasks to keep in history

        # Stats
        self.stats = {
            "tasks_processed": 0,
            "tasks_succeeded": 0,
            "tasks_failed": 0,
            "start_time": None
        }

        # Lock for thread safety
        self.lock = threading.RLock()

    def start(self):
        """Start the worker threads"""
        with self.lock:
            if self.active:
                logger.warning("Worker threads already running")
                return

            self.active = True
            self.stats["start_time"] = datetime.now()

            # Start worker threads
            for i in range(self.max_workers):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"MetadataWorker-{i}",
                    daemon=True
                )
                worker.start()
                self.workers.append(worker)

            logger.info(f"Started {len(self.workers)} metadata worker threads")

    def stop(self):
        """Stop the worker threads"""
        with self.lock:
            if not self.active:
                logger.warning("Worker threads already stopped")
                return

            self.active = False

            # Wait for worker threads to finish (with timeout)
            for worker in self.workers:
                worker.join(timeout=5)

            self.workers = []
            logger.info("Stopped metadata worker threads")

    def _worker_loop(self):
        """Main worker thread loop"""
        logger.info(f"Starting metadata worker thread: {threading.current_thread().name}")

        while self.active:
            try:
                # Get a task from the queue
                task = self.task_queue.get(block=True, timeout=1)

                if task:
                    logger.info(f"Processing task {task.id}: {task.task_type} for connection {task.connection_id}")

                    # Update task status
                    task.status = "processing"

                    try:
                        # Process the task
                        result = self._process_task(task)

                        # Update task with result
                        task.status = "completed"
                        task.result = result

                        # Add to history
                        self._add_to_history(task)

                        # Update stats
                        with self.lock:
                            self.stats["tasks_processed"] += 1
                            self.stats["tasks_succeeded"] += 1

                        logger.info(f"Task {task.id} completed successfully")

                    except Exception as e:
                        # Update task with error
                        task.status = "failed"
                        task.error = str(e)

                        # Add to history
                        self._add_to_history(task)

                        # Update stats
                        with self.lock:
                            self.stats["tasks_processed"] += 1
                            self.stats["tasks_failed"] += 1

                        logger.error(f"Error processing task {task.id}: {str(e)}")

                    finally:
                        # Mark task as done
                        self.task_queue.task_done(task.priority)

            except queue.Empty:
                # No tasks in queue, just continue
                pass

            except Exception as e:
                logger.error(f"Error in worker loop: {str(e)}")
                time.sleep(1)  # Avoid tight loops in case of recurring errors

        logger.info(f"Stopping metadata worker thread: {threading.current_thread().name}")

    def _process_task(self, task):
        """
        Process a metadata task

        Args:
            task: The task to process

        Returns:
            The result of the task
        """
        # Get connection details
        connection = self._get_connection_details(task.connection_id)

        # Create connector
        connector = self.connector_factory.create_connector(connection)

        # Create collector
        from .collector import MetadataCollector
        collector = MetadataCollector(task.connection_id, connector)

        # Process based on task type
        if task.task_type == "full_collection":
            # Full metadata collection
            table_limit = task.params.get("table_limit", 50)
            depth = task.params.get("depth", "medium")

            # Run comprehensive collection
            metadata = collector.collect_comprehensive_metadata(table_limit, depth)

            # Store in database
            self._store_metadata(task.connection_id, metadata)

            return {
                "tables_processed": len(metadata.get("tables", [])),
                "collection_time": metadata.get("collection_metadata", {}).get("duration_seconds")
            }

        elif task.task_type == "table_metadata":
            # Collect metadata for specific table
            table_name = task.params.get("table_name")
            if not table_name:
                raise ValueError("table_name parameter is required")

            # Collect detailed table metadata
            metadata = collector.collect_table_metadata_sync(table_name)

            # Store in database
            self._store_table_metadata(task.connection_id, table_name, metadata)

            return {
                "table": table_name,
                "columns": len(metadata.get("columns", []))
            }

        elif task.task_type == "refresh_statistics":
            # Refresh statistics for specific table
            table_name = task.params.get("table_name")
            if not table_name:
                raise ValueError("table_name parameter is required")

            # Collect column statistics
            columns = collector.collect_columns(table_name)
            stats = {}

            for column in columns:
                col_name = column["name"]
                col_type = column["type"].lower() if isinstance(column["type"], str) else str(column["type"]).lower()

                # Collect statistics for this column
                col_stats = collector._collect_column_statistics(table_name, col_name, col_type)
                if col_stats:
                    stats[col_name] = col_stats

            # Store statistics
            self._store_column_statistics(task.connection_id, table_name, stats)

            return {
                "table": table_name,
                "columns_analyzed": len(stats)
            }

        elif task.task_type == "update_usage":
            # Update usage patterns
            table_name = task.params.get("table_name")
            if not table_name:
                raise ValueError("table_name parameter is required")

            # Collect usage patterns
            usage = collector.collect_usage_patterns(table_name)

            # Store usage patterns
            self._store_usage_patterns(task.connection_id, table_name, usage)

            return usage

        else:
            raise ValueError(f"Unknown task type: {task.task_type}")

    def _get_connection_details(self, connection_id):
        """Get connection details from database"""
        # To be implemented based on your storage system
        # This could query Supabase for the connection details
        raise NotImplementedError("_get_connection_details must be implemented")

    def _store_metadata(self, connection_id, metadata):
        """Store comprehensive metadata in database"""
        # Store tables metadata
        if "tables" in metadata and metadata["tables"]:
            self.storage_service.store_tables_metadata(connection_id, metadata["tables"])

        # Store columns metadata
        if "columns_by_table" in metadata and metadata["columns_by_table"]:
            self.storage_service.store_columns_metadata(connection_id, metadata["columns_by_table"])

        # Store statistics metadata
        if "statistics_by_table" in metadata and metadata["statistics_by_table"]:
            self.storage_service.store_statistics_metadata(connection_id, metadata["statistics_by_table"])

    def _store_table_metadata(self, connection_id, table_name, metadata):
        """Store table metadata in database"""
        # This is a simplified implementation
        # In a real implementation, you'd update specific records

        # Extract tables data
        table_data = [{
            "name": metadata["table_name"],
            "column_count": metadata["column_count"],
            "row_count": metadata.get("row_count"),
            "primary_key": metadata.get("primary_keys", []),
            "id": str(uuid.uuid4())
        }]

        # Store tables metadata
        self.storage_service.store_tables_metadata(connection_id, table_data)

        # Store columns metadata
        columns_by_table = {table_name: metadata.get("columns", [])}
        self.storage_service.store_columns_metadata(connection_id, columns_by_table)

    def _store_column_statistics(self, connection_id, table_name, statistics):
        """Store column statistics in database"""
        # This is a simplified implementation
        # In a real implementation, you'd update specific records

        # Get current statistics
        current_stats = self.storage_service.get_metadata(connection_id, "statistics")

        if current_stats and "metadata" in current_stats:
            # Update existing statistics
            stats_by_table = current_stats["metadata"].get("statistics_by_table", {})

            # Update or add statistics for this table
            if table_name in stats_by_table:
                # Update column statistics
                stats_by_table[table_name]["column_statistics"] = statistics
            else:
                # Add new table statistics
                stats_by_table[table_name] = {
                    "column_statistics": statistics,
                    "collected_at": datetime.now().isoformat()
                }

            # Store updated statistics
            self.storage_service.store_statistics_metadata(connection_id, stats_by_table)
        else:
            # Create new statistics
            stats_by_table = {
                table_name: {
                    "column_statistics": statistics,
                    "collected_at": datetime.now().isoformat()
                }
            }

            # Store new statistics
            self.storage_service.store_statistics_metadata(connection_id, stats_by_table)

    def _store_usage_patterns(self, connection_id, table_name, usage):
        """Store usage patterns in database"""
        # This would need to be implemented based on your storage system
        # It could store in a new table or update existing metadata
        pass

    def _add_to_history(self, task):
        """Add a task to history"""
        with self.lock:
            # Add to history
            self.task_history[task.id] = {
                "task": task.to_dict(),
                "result": task.result,
                "error": task.error,
                "completed_at": datetime.now().isoformat()
            }

            # Trim history if needed
            if len(self.task_history) > self.max_history:
                # Remove oldest entries
                oldest_keys = sorted(
                    self.task_history.keys(),
                    key=lambda k: self.task_history[k].get("completed_at", "")
                )[:len(self.task_history) - self.max_history]

                for key in oldest_keys:
                    del self.task_history[key]

    def get_task_history(self, limit=10):
        """Get recent task history"""
        with self.lock:
            # Sort by completion time (newest first)
            sorted_history = sorted(
                self.task_history.values(),
                key=lambda h: h.get("completed_at", ""),
                reverse=True
            )

            # Return limited history
            return sorted_history[:limit]

    def get_stats(self):
        """Get worker statistics"""
        with self.lock:
            stats = self.stats.copy()

            # Add queue stats
            stats["queue"] = self.task_queue.get_stats()

            # Calculate uptime
            if stats["start_time"]:
                uptime = datetime.now() - stats["start_time"]
                stats["uptime_seconds"] = uptime.total_seconds()

            # Add active workers
            stats["active_workers"] = len(self.workers)

            return stats

    def submit_task(self, task_type, connection_id, params=None, priority="medium"):
        """
        Submit a new task to the queue

        Args:
            task_type: Type of task
            connection_id: Connection ID
            params: Task parameters
            priority: Task priority

        Returns:
            The task ID
        """
        # Create task
        task = MetadataTask(task_type, connection_id, params, priority)

        # Add to queue
        self.task_queue.put(task)

        logger.info(f"Submitted task {task.id}: {task_type} for connection {connection_id}")

        return task.id

    def get_task_status(self, task_id):
        """Get status of a task"""
        with self.lock:
            # Check history first
            if task_id in self.task_history:
                return self.task_history[task_id]

            # Task not found
            return {"error": "Task not found"}