# backend/core/automation/scheduler.py - REPLACE ENTIRE FILE

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
    """Manages scheduling and execution of automation jobs"""

    def __init__(self, max_workers: int = 5):
        self.supabase = SupabaseManager()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.scheduler_thread = None
        self.active_jobs = {}  # job_id -> Future mapping

        # Initialize scheduler
        schedule.clear()

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
        """Execute a metadata refresh job - FIXED VERSION with proper storage"""
        run_id = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "metadata_refresh")

            # Get connection details
            connection = self.supabase.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection not found: {connection_id}")

            # Initialize services properly
            from core.metadata.storage_service import MetadataStorageService
            from core.metadata.connector_factory import ConnectorFactory
            from core.metadata.collector import MetadataCollector

            storage_service = MetadataStorageService()
            connector_factory = ConnectorFactory(self.supabase)

            # Create connector and collector
            connector = connector_factory.create_connector(connection)
            collector = MetadataCollector(connection_id, connector)

            # Determine what types of metadata to refresh
            refresh_types = config.get("types", ["tables", "columns", "statistics"])
            results = {}

            logger.info(f"Starting metadata refresh for types: {refresh_types}")

            for refresh_type in refresh_types:
                logger.info(f"Collecting {refresh_type} metadata")

                try:
                    if refresh_type == "tables":
                        # Collect tables
                        tables = collector.collect_table_list()
                        if tables:
                            # Convert to proper format and store
                            tables_metadata = [{"name": table} for table in tables[:100]]
                            success = storage_service.store_tables_metadata(connection_id, tables_metadata)

                            # Verify storage
                            if success:
                                stored_tables = storage_service.get_tables_metadata(connection_id)
                                if stored_tables and len(stored_tables) > 0:
                                    results[refresh_type] = {
                                        "status": "completed",
                                        "count": len(tables_metadata),
                                        "stored_count": len(stored_tables),
                                        "method": "automation_collection"
                                    }
                                    logger.info(f"Successfully stored {len(tables_metadata)} tables")
                                else:
                                    raise Exception("Tables were not properly stored")
                            else:
                                raise Exception("Failed to store tables metadata")
                        else:
                            results[refresh_type] = {"status": "failed", "error": "No tables found"}

                    elif refresh_type == "columns":
                        # Get tables first
                        tables = collector.collect_table_list()
                        if tables:
                            columns_by_table = {}
                            for table_name in tables[:50]:  # Limit for automation
                                try:
                                    columns = connector.get_columns(table_name)
                                    if columns:
                                        columns_by_table[table_name] = columns
                                except Exception as col_error:
                                    logger.warning(f"Error getting columns for {table_name}: {str(col_error)}")

                            if columns_by_table:
                                success = storage_service.store_columns_metadata(connection_id, columns_by_table)

                                # Verify storage
                                if success:
                                    # Sample verification - check a few tables
                                    verification_count = 0
                                    for table_name in list(columns_by_table.keys())[:3]:
                                        stored_columns = storage_service.get_columns_metadata(connection_id, table_name)
                                        if stored_columns:
                                            verification_count += 1

                                    if verification_count > 0:
                                        results[refresh_type] = {
                                            "status": "completed",
                                            "count": len(columns_by_table),
                                            "verified_tables": verification_count,
                                            "method": "automation_collection"
                                        }
                                        logger.info(f"Successfully stored columns for {len(columns_by_table)} tables")
                                    else:
                                        raise Exception("Columns were not properly stored")
                                else:
                                    raise Exception("Failed to store columns metadata")
                            else:
                                results[refresh_type] = {"status": "failed", "error": "No columns found"}
                        else:
                            results[refresh_type] = {"status": "failed", "error": "No tables for columns"}

                    elif refresh_type == "statistics":
                        # Collect statistics
                        tables = collector.collect_table_list()
                        if tables:
                            stats_by_table = {}
                            for table_name in tables[:25]:  # Limit for automation
                                try:
                                    stats = collector.collect_table_statistics(table_name)
                                    if stats:
                                        stats_by_table[table_name] = stats
                                except Exception as stat_error:
                                    logger.warning(f"Error getting statistics for {table_name}: {str(stat_error)}")

                            if stats_by_table:
                                success = storage_service.store_statistics_metadata(connection_id, stats_by_table)

                                # Verify storage
                                if success:
                                    # Sample verification
                                    verification_count = 0
                                    for table_name in list(stats_by_table.keys())[:3]:
                                        stored_stats = storage_service.get_statistics_metadata(connection_id,
                                                                                               table_name)
                                        if stored_stats:
                                            verification_count += 1

                                    if verification_count > 0:
                                        results[refresh_type] = {
                                            "status": "completed",
                                            "count": len(stats_by_table),
                                            "verified_tables": verification_count,
                                            "method": "automation_collection"
                                        }
                                        logger.info(f"Successfully stored statistics for {len(stats_by_table)} tables")
                                    else:
                                        raise Exception("Statistics were not properly stored")
                                else:
                                    raise Exception("Failed to store statistics metadata")
                            else:
                                results[refresh_type] = {"status": "failed", "error": "No statistics found"}
                        else:
                            results[refresh_type] = {"status": "failed", "error": "No tables for statistics"}

                except Exception as type_error:
                    logger.error(f"Error processing {refresh_type}: {str(type_error)}")
                    results[refresh_type] = {"status": "failed", "error": str(type_error)}

            # Update job status to completed
            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            # Update automation run
            if run_id:
                self._update_automation_run(run_id, "completed", results)

            # Publish event
            publish_automation_event(
                event_type=AutomationEventType.JOB_COMPLETED,
                data={"job_id": job_id, "job_type": "metadata_refresh", "results": results},
                connection_id=connection_id
            )

            logger.info(f"Completed metadata refresh job {job_id}")

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

    def _execute_schema_detection(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute a schema change detection job - FIXED VERSION with proper storage"""
        run_id = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "schema_detection")

            # Use your existing schema change detector
            from core.metadata.schema_change_detector import SchemaChangeDetector
            from core.metadata.connector_factory import ConnectorFactory
            from core.metadata.storage_service import MetadataStorageService

            # Initialize properly
            storage_service = MetadataStorageService()
            detector = SchemaChangeDetector(storage_service)
            connector_factory = ConnectorFactory(self.supabase)

            # Detect changes - this should store changes in schema_changes table
            changes, important_changes = detector.detect_changes_for_connection(
                connection_id, connector_factory, self.supabase
            )

            # Verify changes were stored by checking the schema_changes table
            stored_changes_count = 0
            if changes:
                try:
                    # Check if changes were stored in the database
                    recent_changes_response = self.supabase.supabase.table("schema_changes") \
                        .select("id") \
                        .eq("connection_id", connection_id) \
                        .gte("detected_at", (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()) \
                        .execute()

                    stored_changes_count = len(recent_changes_response.data) if recent_changes_response.data else 0
                    logger.info(f"Verified {stored_changes_count} changes were stored in database")
                except Exception as verify_error:
                    logger.warning(f"Could not verify stored changes: {str(verify_error)}")

            results = {
                "changes_detected": len(changes),
                "important_changes": important_changes,
                "changes_stored": stored_changes_count,
                "auto_acknowledged": 0
            }

            # Auto-acknowledge safe changes if configured
            if config.get("auto_acknowledge_safe_changes", False) and changes:
                safe_change_types = ["column_added", "index_added", "foreign_key_added"]
                auto_ack_count = 0

                for change in changes:
                    if change.get("type") in safe_change_types:
                        # Auto-acknowledge this change
                        try:
                            self.supabase.supabase.table("schema_changes") \
                                .update({
                                "acknowledged": True,
                                "acknowledged_at": datetime.now(timezone.utc).isoformat(),
                                "acknowledged_by": "automation"
                            }) \
                                .eq("connection_id", connection_id) \
                                .eq("change_type", change.get("type")) \
                                .eq("table_name", change.get("table")) \
                                .execute()
                            auto_ack_count += 1
                        except Exception as ack_error:
                            logger.error(f"Error auto-acknowledging change: {str(ack_error)}")

                results["auto_acknowledged"] = auto_ack_count

            # Update job status to completed
            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            # Update automation run
            if run_id:
                self._update_automation_run(run_id, "completed", results)

            # Publish event if important changes found
            if important_changes:
                publish_automation_event(
                    event_type=AutomationEventType.SCHEMA_CHANGES_DETECTED,
                    data={"job_id": job_id, "changes": len(changes), "important": important_changes,
                          "stored": stored_changes_count},
                    connection_id=connection_id
                )

            logger.info(
                f"Completed schema detection job {job_id}, found {len(changes)} changes, stored {stored_changes_count}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing schema detection job {job_id}: {error_msg}")
            logger.error(traceback.format_exc())

            self._update_job_status(job_id, "failed", error_message=error_msg)

            if run_id:
                self._update_automation_run(run_id, "failed", {"error": error_msg})

        finally:
            # Clean up active jobs
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _execute_validation_run(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute a validation automation job - FIXED VERSION with proper storage"""
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

            # Use your existing validation manager
            from core.validations.supabase_validation_manager import SupabaseValidationManager
            validation_manager = SupabaseValidationManager()

            # Get all tables with automation enabled
            table_configs = self.supabase.supabase.table("automation_table_configs") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .eq("auto_run_validations", True) \
                .execute()

            results = {
                "tables_processed": 0,
                "total_rules": 0,
                "passed_rules": 0,
                "failed_rules": 0,
                "tables_with_failures": [],
                "validation_results_stored": 0
            }

            # If no table-specific configs, check connection-level config
            tables_to_validate = []
            if table_configs.data:
                tables_to_validate = [config["table_name"] for config in table_configs.data]
            else:
                # Get all tables that have validation rules
                tables_to_validate = validation_manager.get_tables_with_validations(
                    organization_id, connection_id
                )

            logger.info(f"Validating {len(tables_to_validate)} tables")

            if tables_to_validate:
                # Build connection string
                connection_string = self._build_connection_string(connection)

                # Track validation results before execution
                pre_execution_count = 0
                try:
                    pre_execution_response = self.supabase.supabase.table("validation_results") \
                        .select("id", count="exact") \
                        .eq("connection_id", connection_id) \
                        .execute()
                    pre_execution_count = pre_execution_response.count or 0
                except Exception:
                    pass

                for table_name in tables_to_validate:
                    # Execute validation rules for this table
                    try:
                        logger.info(f"Running validations for table: {table_name}")

                        # Execute rules - this should store results in validation_results table
                        validation_results = validation_manager.execute_rules(
                            organization_id, connection_string, table_name, connection_id
                        )

                        results["tables_processed"] += 1
                        results["total_rules"] += len(validation_results)

                        passed = len([r for r in validation_results if r.get("is_valid", False)])
                        failed = len(validation_results) - passed

                        results["passed_rules"] += passed
                        results["failed_rules"] += failed

                        if failed > 0:
                            results["tables_with_failures"].append({
                                "table_name": table_name,
                                "failed_rules": failed,
                                "total_rules": len(validation_results)
                            })

                        logger.info(f"Completed validations for {table_name}: {passed} passed, {failed} failed")

                    except Exception as table_error:
                        logger.error(f"Error validating table {table_name}: {str(table_error)}")
                        results["tables_with_failures"].append({
                            "table_name": table_name,
                            "error": str(table_error)
                        })

                # Verify validation results were stored
                try:
                    post_execution_response = self.supabase.supabase.table("validation_results") \
                        .select("id", count="exact") \
                        .eq("connection_id", connection_id) \
                        .gte("run_at", (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()) \
                        .execute()
                    post_execution_count = post_execution_response.count or 0
                    results["validation_results_stored"] = post_execution_count - pre_execution_count
                    logger.info(f"Stored {results['validation_results_stored']} new validation results")
                except Exception as verify_error:
                    logger.warning(f"Could not verify stored validation results: {str(verify_error)}")

            # Update job status to completed
            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            # Update automation run
            if run_id:
                self._update_automation_run(run_id, "completed", results)

            # Publish event if there were failures
            if results["failed_rules"] > 0:
                publish_automation_event(
                    event_type=AutomationEventType.VALIDATION_FAILURES_DETECTED,
                    data={"job_id": job_id, "failed_rules": results["failed_rules"],
                          "tables": results["tables_with_failures"],
                          "stored_results": results["validation_results_stored"]},
                    connection_id=connection_id
                )

            logger.info(
                f"Completed validation job {job_id}: {results['failed_rules']} failures, {results['validation_results_stored']} results stored")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing validation run job {job_id}: {error_msg}")
            logger.error(traceback.format_exc())

            self._update_job_status(job_id, "failed", error_message=error_msg)

            if run_id:
                self._update_automation_run(run_id, "failed", {"error": error_msg})

        finally:
            # Clean up active jobs
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
            # This method would be called when configuration changes
            # For now, the next scheduled check will pick up the new config
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
            "worker_threads": self.executor._threads if hasattr(self.executor, '_threads') else 0
        }