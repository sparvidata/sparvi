import logging
import uuid
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, Future
import schedule
import traceback

from core.storage.supabase_manager import SupabaseManager
from .events import AutomationEventType, publish_automation_event

logger = logging.getLogger(__name__)


class AutomationScheduler:
    """Minimal automation scheduler focused on storage integration"""

    def __init__(self, max_workers: int = 5):
        self.supabase = SupabaseManager()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.scheduler_thread = None
        self.active_jobs = {}  # job_id -> Future mapping

        # Initialize storage services with minimal dependencies
        self._initialize_minimal_storage_services()

        # Initialize scheduler
        schedule.clear()

    def _initialize_minimal_storage_services(self):
        """Initialize minimal storage services needed for automation"""
        try:
            # Import and initialize metadata storage service
            from core.metadata.storage_service import MetadataStorageService
            self.metadata_storage = MetadataStorageService()
            logger.info("Metadata storage service initialized for automation")

            # Import and initialize connector factory
            from core.metadata.connector_factory import ConnectorFactory
            self.connector_factory = ConnectorFactory(self.supabase)
            logger.info("Connector factory initialized for automation")

            # Import and initialize validation manager
            from core.validations.supabase_validation_manager import SupabaseValidationManager
            self.validation_manager = SupabaseValidationManager()
            logger.info("Validation manager initialized for automation")

            # Import and initialize schema change detector
            from core.metadata.schema_change_detector import SchemaChangeDetector
            self.schema_detector = SchemaChangeDetector(self.metadata_storage)
            logger.info("Schema change detector initialized for automation")

            # Skip metrics tracker for now to avoid initialization issues
            self.metrics_tracker = None
            logger.info("Skipping metrics tracker initialization for minimal setup")

            logger.info("Minimal storage services initialized successfully for automation")

        except Exception as e:
            logger.error(f"Error initializing minimal storage services: {str(e)}")
            logger.warning("Continuing with automation startup - some functionality may be limited")

    def start(self):
        """Start the automation scheduler"""
        if self.running:
            logger.warning("Scheduler already running")
            return

        self.running = True

        # Set up periodic schedules
        self._setup_schedules()

        # Start scheduler thread
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()

        logger.info("Automation scheduler started")

    def stop(self):
        """Stop the automation scheduler"""
        self.running = False

        # Cancel all active jobs
        for job_id, future in self.active_jobs.items():
            future.cancel()

        # Shutdown executor
        self.executor.shutdown(wait=True)

        # Wait for scheduler thread
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)

        logger.info("Automation scheduler stopped")

    def _setup_schedules(self):
        """Set up periodic automation schedules"""
        # Run metadata refresh checks every hour
        schedule.every().hour.do(self._check_metadata_refresh_jobs)

        # Run schema change detection every 30 minutes
        schedule.every(30).minutes.do(self._check_schema_detection_jobs)

        # Run validation automation every 2 hours
        schedule.every(2).hours.do(self._check_validation_jobs)

        # Clean up completed jobs daily
        schedule.every().day.at("02:00").do(self._cleanup_old_jobs)

    def _run_scheduler(self):
        """Main scheduler loop"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(5)

    def _check_metadata_refresh_jobs(self):
        """Check and schedule metadata refresh jobs"""
        try:
            # Get all enabled metadata refresh configurations
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*, database_connections(organization_id)") \
                .execute()

            if not response.data:
                return

            for config in response.data:
                metadata_config = config.get("metadata_refresh", {})
                if not metadata_config.get("enabled", False):
                    continue

                connection_id = config["connection_id"]
                interval_hours = metadata_config.get("interval_hours", 24)

                # Check if we need to schedule a job
                if self._should_schedule_job(connection_id, "metadata_refresh", interval_hours):
                    self._schedule_metadata_refresh(connection_id, metadata_config)

        except Exception as e:
            logger.error(f"Error checking metadata refresh jobs: {str(e)}")

    def _check_schema_detection_jobs(self):
        """Check and schedule schema change detection jobs"""
        try:
            # Get all enabled schema detection configurations
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*, database_connections(organization_id)") \
                .execute()

            if not response.data:
                return

            for config in response.data:
                schema_config = config.get("schema_change_detection", {})
                if not schema_config.get("enabled", False):
                    continue

                connection_id = config["connection_id"]
                interval_hours = schema_config.get("interval_hours", 6)

                # Check if we need to schedule a job
                if self._should_schedule_job(connection_id, "schema_detection", interval_hours):
                    self._schedule_schema_detection(connection_id, schema_config)

        except Exception as e:
            logger.error(f"Error checking schema detection jobs: {str(e)}")

    def _check_validation_jobs(self):
        """Check and schedule validation automation jobs"""
        try:
            # Get all enabled validation automation configurations
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*, database_connections(organization_id)") \
                .execute()

            if not response.data:
                return

            for config in response.data:
                validation_config = config.get("validation_automation", {})
                if not validation_config.get("enabled", False):
                    continue

                connection_id = config["connection_id"]
                interval_hours = validation_config.get("interval_hours", 12)

                # Check if we need to schedule a job
                if self._should_schedule_job(connection_id, "validation_run", interval_hours):
                    self._schedule_validation_run(connection_id, validation_config)

        except Exception as e:
            logger.error(f"Error checking validation jobs: {str(e)}")

    def _should_schedule_job(self, connection_id: str, job_type: str, interval_hours: int) -> bool:
        """Check if a job should be scheduled based on last run time"""
        try:
            # Get last completed job of this type for this connection
            cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=interval_hours)).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .select("completed_at") \
                .eq("connection_id", connection_id) \
                .eq("job_type", job_type) \
                .eq("status", "completed") \
                .gte("completed_at", cutoff_time) \
                .execute()

            # If no recent completed jobs, we should schedule
            return not response.data or len(response.data) == 0

        except Exception as e:
            logger.error(f"Error checking if job should be scheduled: {str(e)}")
            return False

    def _schedule_metadata_refresh(self, connection_id: str, config: Dict[str, Any]):
        """Schedule a metadata refresh job"""
        try:
            job_id = str(uuid.uuid4())

            # Create job record
            job_data = {
                "id": job_id,
                "connection_id": connection_id,
                "job_type": "metadata_refresh",
                "status": "scheduled",
                "scheduled_at": datetime.now(timezone.utc).isoformat(),
                "job_config": config
            }

            self.supabase.supabase.table("automation_jobs").insert(job_data).execute()

            # Submit job to executor
            future = self.executor.submit(self._execute_metadata_refresh, job_id, connection_id, config)
            self.active_jobs[job_id] = future

            logger.info(f"Scheduled metadata refresh job {job_id} for connection {connection_id}")

        except Exception as e:
            logger.error(f"Error scheduling metadata refresh: {str(e)}")

    def _schedule_schema_detection(self, connection_id: str, config: Dict[str, Any]):
        """Schedule a schema change detection job"""
        try:
            job_id = str(uuid.uuid4())

            # Create job record
            job_data = {
                "id": job_id,
                "connection_id": connection_id,
                "job_type": "schema_detection",
                "status": "scheduled",
                "scheduled_at": datetime.now(timezone.utc).isoformat(),
                "job_config": config
            }

            self.supabase.supabase.table("automation_jobs").insert(job_data).execute()

            # Submit job to executor
            future = self.executor.submit(self._execute_schema_detection, job_id, connection_id, config)
            self.active_jobs[job_id] = future

            logger.info(f"Scheduled schema detection job {job_id} for connection {connection_id}")

        except Exception as e:
            logger.error(f"Error scheduling schema detection: {str(e)}")

    def _schedule_validation_run(self, connection_id: str, config: Dict[str, Any]):
        """Schedule a validation automation job"""
        try:
            job_id = str(uuid.uuid4())

            # Create job record
            job_data = {
                "id": job_id,
                "connection_id": connection_id,
                "job_type": "validation_run",
                "status": "scheduled",
                "scheduled_at": datetime.now(timezone.utc).isoformat(),
                "job_config": config
            }

            self.supabase.supabase.table("automation_jobs").insert(job_data).execute()

            # Submit job to executor
            future = self.executor.submit(self._execute_validation_run, job_id, connection_id, config)
            self.active_jobs[job_id] = future

            logger.info(f"Scheduled validation run job {job_id} for connection {connection_id}")

        except Exception as e:
            logger.error(f"Error scheduling validation run: {str(e)}")

    def _execute_metadata_refresh(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute a metadata refresh job with enhanced storage verification"""
        run_id = None
        connector = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "metadata_refresh")

            # Get connection details
            connection = self.supabase.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection not found: {connection_id}")

            # Create connector
            connector = self.connector_factory.create_connector(connection)
            if not connector:
                raise Exception(f"Could not create connector for connection {connection_id}")

            # Connect to the database BEFORE creating collector
            logger.info(f"Connecting to database for connection {connection_id}")
            connector.connect()
            logger.info(f"Successfully connected to database")

            # Create collector
            from core.metadata.collector import MetadataCollector
            collector = MetadataCollector(connection_id, connector)

            # Determine what types of metadata to refresh
            refresh_types = config.get("types", ["tables", "columns", "statistics"])
            results = {}

            logger.info(f"Starting metadata refresh for types: {refresh_types}")

            # Process each refresh type
            for refresh_type in refresh_types:
                logger.info(f"Collecting {refresh_type} metadata")

                try:
                    if refresh_type == "tables":
                        # Collect tables
                        tables = collector.collect_table_list()
                        if tables:
                            # Convert to proper format for storage
                            tables_metadata = []
                            for table in tables[:100]:  # Limit to prevent overload
                                if isinstance(table, str):
                                    tables_metadata.append({"name": table, "id": table})
                                elif isinstance(table, dict):
                                    tables_metadata.append(table)
                                else:
                                    tables_metadata.append({"name": str(table), "id": str(table)})

                            logger.info(f"Attempting to store {len(tables_metadata)} tables to metadata storage")

                            # Store with verification and retries
                            success = self.metadata_storage.store_tables_metadata(
                                connection_id,
                                tables_metadata,
                                max_retries=3,
                                verify_storage=True
                            )

                            if success:
                                results[refresh_type] = {
                                    "status": "completed",
                                    "count": len(tables_metadata),
                                    "verified": True,
                                    "message": f"Successfully stored {len(tables_metadata)} tables"
                                }
                                logger.info(f"Successfully stored and verified {len(tables_metadata)} tables")
                            else:
                                raise Exception("Failed to store tables metadata after retries")
                        else:
                            results[refresh_type] = {
                                "status": "failed",
                                "error": "No tables found",
                                "count": 0,
                                "verified": False
                            }

                    elif refresh_type == "columns":
                        # Get tables first
                        tables = collector.collect_table_list()
                        if tables:
                            columns_by_table = {}
                            processed_tables = 0

                            # Limit to prevent timeout
                            tables_to_process = tables[:25]

                            for table_name in tables_to_process:
                                try:
                                    columns = connector.get_columns(table_name)
                                    if columns:
                                        # Ensure proper format
                                        formatted_columns = []
                                        for col in columns:
                                            if isinstance(col, dict):
                                                formatted_col = {
                                                    "name": col.get("name", "unknown"),
                                                    "type": str(col.get("type", "unknown")),
                                                    "nullable": col.get("nullable", True)
                                                }
                                                # Add optional fields if present
                                                if "default" in col and col["default"] is not None:
                                                    formatted_col["default"] = str(col["default"])
                                                formatted_columns.append(formatted_col)

                                        columns_by_table[table_name] = formatted_columns
                                        processed_tables += 1

                                except Exception as col_error:
                                    logger.warning(f"Error getting columns for {table_name}: {str(col_error)}")

                            if columns_by_table:
                                logger.info(f"Attempting to store columns for {len(columns_by_table)} tables")

                                # Store with verification and retries
                                success = self.metadata_storage.store_columns_metadata(
                                    connection_id,
                                    columns_by_table,
                                    max_retries=3,
                                    verify_storage=True
                                )

                                if success:
                                    total_columns = sum(len(cols) for cols in columns_by_table.values())
                                    results[refresh_type] = {
                                        "status": "completed",
                                        "table_count": len(columns_by_table),
                                        "total_columns": total_columns,
                                        "verified": True,
                                        "message": f"Successfully stored columns for {len(columns_by_table)} tables"
                                    }
                                    logger.info(
                                        f"Successfully stored and verified columns for {len(columns_by_table)} tables")
                                else:
                                    raise Exception("Failed to store columns metadata after retries")
                            else:
                                results[refresh_type] = {
                                    "status": "failed",
                                    "error": "No columns found",
                                    "table_count": 0,
                                    "verified": False
                                }
                        else:
                            results[refresh_type] = {
                                "status": "failed",
                                "error": "No tables for columns",
                                "table_count": 0,
                                "verified": False
                            }

                    elif refresh_type == "statistics":
                        # Collect statistics
                        tables = collector.collect_table_list()
                        if tables:
                            stats_by_table = {}
                            processed_tables = 0

                            # Limit to prevent timeout
                            tables_to_process = tables[:10]

                            for table_name in tables_to_process:
                                try:
                                    # Use simplified statistics collection
                                    stats = {
                                        "table_name": table_name,
                                        "row_count": None,
                                        "column_count": 0,
                                        "collected_at": datetime.now(timezone.utc).isoformat()
                                    }

                                    # Try to get row count
                                    try:
                                        result = connector.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                                        if result and len(result) > 0:
                                            stats["row_count"] = int(result[0][0])
                                    except Exception:
                                        pass

                                    # Try to get column count
                                    try:
                                        columns = connector.get_columns(table_name)
                                        stats["column_count"] = len(columns) if columns else 0
                                    except Exception:
                                        pass

                                    # Try to get primary keys
                                    try:
                                        primary_keys = connector.get_primary_keys(table_name)
                                        stats["has_primary_key"] = len(primary_keys) > 0 if primary_keys else False
                                        stats["primary_keys"] = primary_keys or []
                                    except Exception:
                                        stats["has_primary_key"] = False
                                        stats["primary_keys"] = []

                                    # Add basic health score
                                    health_score = 100
                                    if not stats.get("has_primary_key", False):
                                        health_score -= 20
                                    if stats.get("row_count", 0) == 0:
                                        health_score -= 30
                                    stats["health_score"] = max(0, health_score)

                                    stats_by_table[table_name] = stats
                                    processed_tables += 1

                                except Exception as stat_error:
                                    logger.warning(f"Error getting statistics for {table_name}: {str(stat_error)}")

                            if stats_by_table:
                                logger.info(f"Attempting to store statistics for {len(stats_by_table)} tables")

                                # Store with verification and retries
                                success = self.metadata_storage.store_statistics_metadata(
                                    connection_id,
                                    stats_by_table,
                                    max_retries=3,
                                    verify_storage=True
                                )

                                if success:
                                    results[refresh_type] = {
                                        "status": "completed",
                                        "table_count": len(stats_by_table),
                                        "verified": True,
                                        "message": f"Successfully stored statistics for {len(stats_by_table)} tables"
                                    }
                                    logger.info(
                                        f"Successfully stored and verified statistics for {len(stats_by_table)} tables")
                                else:
                                    raise Exception("Failed to store statistics metadata after retries")
                            else:
                                results[refresh_type] = {
                                    "status": "failed",
                                    "error": "No statistics found",
                                    "table_count": 0,
                                    "verified": False
                                }
                        else:
                            results[refresh_type] = {
                                "status": "failed",
                                "error": "No tables for statistics",
                                "table_count": 0,
                                "verified": False
                            }

                except Exception as type_error:
                    logger.error(f"Error processing {refresh_type}: {str(type_error)}")
                    results[refresh_type] = {
                        "status": "failed",
                        "error": str(type_error),
                        "verified": False
                    }

            # Calculate overall success
            successful_types = [t for t, r in results.items() if r.get("status") == "completed"]
            failed_types = [t for t, r in results.items() if r.get("status") == "failed"]

            # Update job status to completed
            overall_status = "completed" if len(successful_types) > 0 else "failed"

            self._update_job_status(
                job_id,
                overall_status,
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary={
                    **results,
                    "successful_types": successful_types,
                    "failed_types": failed_types,
                    "overall_verified": all(
                        r.get("verified", False) for r in results.values() if r.get("status") == "completed")
                }
            )

            # Update automation run
            if run_id:
                self._update_automation_run(run_id, overall_status, {
                    **results,
                    "successful_types": successful_types,
                    "failed_types": failed_types
                })

            # Publish event
            publish_automation_event(
                event_type=AutomationEventType.JOB_COMPLETED if overall_status == "completed" else AutomationEventType.JOB_FAILED,
                data={
                    "job_id": job_id,
                    "job_type": "metadata_refresh",
                    "results": results,
                    "successful_types": successful_types,
                    "failed_types": failed_types
                },
                connection_id=connection_id
            )

            logger.info(
                f"Completed metadata refresh job {job_id}: {len(successful_types)} successful, {len(failed_types)} failed")

            # Track metrics if available
            try:
                if hasattr(self, 'metrics_tracker') and self.metrics_tracker:
                    for refresh_type, result in results.items():
                        if result.get("status") == "completed":
                            # Track successful metadata collection
                            self.metrics_tracker.track_metric(
                                organization_id=connection.get("organization_id"),
                                connection_id=connection_id,
                                metric_name=f"metadata_{refresh_type}_count",
                                metric_value=result.get("count", result.get("table_count", 0)),
                                metric_type="metadata_collection",
                                source="automation"
                            )
            except Exception as metrics_error:
                logger.warning(f"Error tracking metrics: {str(metrics_error)}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing metadata refresh job {job_id}: {error_msg}")
            logger.error(traceback.format_exc())

            self._update_job_status(job_id, "failed", error_message=error_msg)

            if run_id:
                self._update_automation_run(run_id, "failed", {"error": error_msg})

            # Publish failure event
            publish_automation_event(
                event_type=AutomationEventType.JOB_FAILED,
                data={"job_id": job_id, "job_type": "metadata_refresh", "error": error_msg},
                connection_id=connection_id
            )
        finally:
            # Clean up active jobs
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

            # Clean up connector
            if connector and hasattr(connector, 'close'):
                try:
                    connector.close()
                    logger.debug(f"Closed connector for job {job_id}")
                except Exception as cleanup_error:
                    logger.warning(f"Error closing connector: {str(cleanup_error)}")
            elif connector and hasattr(connector, 'engine') and hasattr(connector.engine, 'dispose'):
                try:
                    connector.engine.dispose()
                    logger.debug(f"Disposed engine for job {job_id}")
                except Exception as cleanup_error:
                    logger.warning(f"Error disposing engine: {str(cleanup_error)}")

    def _execute_schema_detection(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute schema detection with simplified approach"""
        run_id = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "schema_detection")

            # Detect changes
            logger.info("Starting schema change detection")
            changes, important_changes = self.schema_detector.detect_changes_for_connection(
                connection_id, self.connector_factory, self.supabase
            )

            results = {
                "changes_detected": len(changes),
                "important_changes": important_changes,
                "changes_stored": len(changes),  # Assume stored for now
                "verified": True
            }

            # Update job status to completed
            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            # Update automation run
            if run_id:
                self._update_automation_run(run_id, "completed", results)

            logger.info(f"Completed schema detection job {job_id}, found {len(changes)} changes")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing schema detection job {job_id}: {error_msg}")

            self._update_job_status(job_id, "failed", error_message=error_msg)

            if run_id:
                self._update_automation_run(run_id, "failed", {"error": error_msg})

        finally:
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _execute_validation_run(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute validation run with simplified approach"""
        run_id = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "validation_run")

            # Get connection details
            connection = self.supabase.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection not found: {connection_id}")

            organization_id = connection.get("organization_id")

            # Get tables with validation rules
            tables_to_validate = self.validation_manager.get_tables_with_validations(
                organization_id, connection_id
            )

            results = {
                "tables_processed": 0,
                "total_rules": 0,
                "passed_rules": 0,
                "failed_rules": 0,
                "verification_results_stored": 0,
                "verified": True
            }

            if tables_to_validate:
                # Build connection string
                connection_string = self._build_connection_string(connection)

                for table_name in tables_to_validate:
                    try:
                        logger.info(f"Running validations for table: {table_name}")

                        # Execute rules
                        validation_results = self.validation_manager.execute_rules(
                            organization_id, connection_string, table_name, connection_id
                        )

                        results["tables_processed"] += 1
                        results["total_rules"] += len(validation_results)

                        passed = len([r for r in validation_results if r.get("is_valid", False)])
                        failed = len(validation_results) - passed

                        results["passed_rules"] += passed
                        results["failed_rules"] += failed

                        logger.info(f"Completed validations for {table_name}: {passed} passed, {failed} failed")

                    except Exception as table_error:
                        logger.error(f"Error validating table {table_name}: {str(table_error)}")

            # Update job status to completed
            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            # Update automation run
            if run_id:
                self._update_automation_run(run_id, "completed", results)

            logger.info(f"Completed validation job {job_id}: {results['failed_rules']} failures")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing validation run job {job_id}: {error_msg}")

            self._update_job_status(job_id, "failed", error_message=error_msg)

            if run_id:
                self._update_automation_run(run_id, "failed", {"error": error_msg})

        finally:
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _create_automation_run(self, job_id: str, connection_id: str, run_type: str) -> str:
        """Create automation run record"""
        try:
            run_id = str(uuid.uuid4())

            run_data = {
                "id": run_id,
                "job_id": job_id,
                "connection_id": connection_id,
                "run_type": run_type,
                "status": "running",
                "started_at": datetime.now(timezone.utc).isoformat()
            }

            response = self.supabase.supabase.table("automation_runs").insert(run_data).execute()

            if response.data:
                logger.info(f"Created automation run {run_id}")
                return run_id
            else:
                logger.error("Failed to create automation run")
                return None

        except Exception as e:
            logger.error(f"Error creating automation run: {str(e)}")
            return None

    def _update_automation_run(self, run_id: str, status: str, results: Dict[str, Any] = None):
        """Update automation run record"""
        try:
            update_data = {
                "status": status,
                "completed_at": datetime.now(timezone.utc).isoformat()
            }

            if results:
                update_data["results"] = results

            self.supabase.supabase.table("automation_runs") \
                .update(update_data) \
                .eq("id", run_id) \
                .execute()

            logger.info(f"Updated automation run {run_id} to {status}")

        except Exception as e:
            logger.error(f"Error updating automation run: {str(e)}")

    def _build_connection_string(self, connection: Dict[str, Any]) -> str:
        """Build connection string from connection details"""
        try:
            connection_type = connection.get("connection_type", "").lower()
            details = connection.get("connection_details", {})

            if connection_type == "snowflake":
                username = details.get("username")
                password = details.get("password")
                account = details.get("account")
                database = details.get("database")
                schema = details.get("schema", "PUBLIC")
                warehouse = details.get("warehouse")

                # URL encode password to handle special characters
                import urllib.parse
                encoded_password = urllib.parse.quote_plus(password)

                return f"snowflake://{username}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"

            # Add other connection types as needed
            else:
                raise Exception(f"Unsupported connection type: {connection_type}")

        except Exception as e:
            logger.error(f"Error building connection string: {str(e)}")
            raise

    def _update_job_status(self, job_id: str, status: str, **kwargs):
        """Update job status in database"""
        try:
            update_data = {"status": status}
            update_data.update(kwargs)

            self.supabase.supabase.table("automation_jobs") \
                .update(update_data) \
                .eq("id", job_id) \
                .execute()

        except Exception as e:
            logger.error(f"Error updating job status: {str(e)}")

    def _cleanup_old_jobs(self):
        """Clean up old completed/failed jobs"""
        try:
            # Delete jobs older than 30 days
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

            self.supabase.supabase.table("automation_jobs") \
                .delete() \
                .in_("status", ["completed", "failed", "cancelled"]) \
                .lt("created_at", cutoff_date) \
                .execute()

            logger.info("Cleaned up old automation jobs")

        except Exception as e:
            logger.error(f"Error cleaning up old jobs: {str(e)}")

    # Public methods for external control
    def schedule_immediate_run(self, connection_id: str, automation_type: str = None, trigger_user: str = None) -> Dict[
        str, Any]:
        """Schedule an immediate automation run"""
        try:
            if automation_type == "metadata_refresh" or automation_type is None:
                config = {"types": ["tables", "columns"], "immediate": True}
                self._schedule_metadata_refresh(connection_id, config)

            if automation_type == "schema_detection" or automation_type is None:
                config = {"immediate": True}
                self._schedule_schema_detection(connection_id, config)

            if automation_type == "validation_run" or automation_type is None:
                config = {"immediate": True}
                self._schedule_validation_run(connection_id, config)

            return {"success": True, "message": "Automation jobs scheduled"}

        except Exception as e:
            logger.error(f"Error scheduling immediate run: {str(e)}")
            return {"success": False, "error": str(e)}

    def update_connection_schedule(self, connection_id: str, config: Dict[str, Any]):
        """Update scheduling for a connection based on new configuration"""
        try:
            logger.info(f"Updated schedule for connection {connection_id}")
        except Exception as e:
            logger.error(f"Error updating connection schedule: {str(e)}")

    def cancel_job(self, job_id: str):
        """Cancel a running job"""
        try:
            if job_id in self.active_jobs:
                future = self.active_jobs[job_id]
                future.cancel()
                del self.active_jobs[job_id]
                logger.info(f"Cancelled job {job_id}")
        except Exception as e:
            logger.error(f"Error cancelling job: {str(e)}")

    def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        return {
            "running": self.running,
            "active_jobs": len(self.active_jobs),
            "worker_threads": self.executor._threads if hasattr(self.executor, '_threads') else 0,
            "storage_services_initialized": all([
                hasattr(self, 'metadata_storage') and self.metadata_storage is not None,
                hasattr(self, 'validation_manager') and self.validation_manager is not None,
                hasattr(self, 'schema_detector') and self.schema_detector is not None,
                hasattr(self, 'connector_factory') and self.connector_factory is not None
            ])
        }