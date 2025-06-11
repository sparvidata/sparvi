import logging
import uuid
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, Future
import schedule

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
        """Execute a metadata refresh job"""
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Import metadata manager
            from core.metadata.manager import MetadataTaskManager
            task_manager = MetadataTaskManager.get_instance()

            # Determine what types of metadata to refresh
            refresh_types = config.get("types", ["tables", "columns", "statistics"])

            results = {}
            for refresh_type in refresh_types:
                if refresh_type == "tables":
                    task_id = task_manager.submit_collection_task(
                        connection_id, {"depth": "low", "table_limit": 100}, "high"
                    )
                elif refresh_type == "columns":
                    task_id = task_manager.submit_collection_task(
                        connection_id, {"depth": "medium", "table_limit": 50}, "medium"
                    )
                elif refresh_type == "statistics":
                    task_id = task_manager.submit_collection_task(
                        connection_id, {"depth": "high", "table_limit": 25}, "low"
                    )

                if task_id:
                    results[refresh_type] = {"task_id": task_id, "status": "submitted"}

            # Update job status to completed
            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            # Publish event
            publish_automation_event(
                event_type=AutomationEventType.JOB_COMPLETED,
                data={"job_id": job_id, "job_type": "metadata_refresh", "results": results},
                connection_id=connection_id
            )

        except Exception as e:
            logger.error(f"Error executing metadata refresh job {job_id}: {str(e)}")
            self._update_job_status(job_id, "failed", error_message=str(e))
        finally:
            # Clean up active jobs
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _execute_schema_detection(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute a schema change detection job"""
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Import schema change detector
            from core.metadata.schema_change_detector import SchemaChangeDetector
            from core.metadata.connector_factory import ConnectorFactory

            detector = SchemaChangeDetector()
            connector_factory = ConnectorFactory(self.supabase)

            # Detect changes
            changes, important_changes = detector.detect_changes_for_connection(
                connection_id, connector_factory, self.supabase
            )

            results = {
                "changes_detected": len(changes),
                "important_changes": important_changes,
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

            # Publish event if important changes found
            if important_changes:
                publish_automation_event(
                    event_type=AutomationEventType.SCHEMA_CHANGES_DETECTED,
                    data={"job_id": job_id, "changes": len(changes), "important": important_changes},
                    connection_id=connection_id
                )

        except Exception as e:
            logger.error(f"Error executing schema detection job {job_id}: {str(e)}")
            self._update_job_status(job_id, "failed", error_message=str(e))
        finally:
            # Clean up active jobs
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _execute_validation_run(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute a validation automation job"""
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Get connection details
            connection = self.supabase.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection not found: {connection_id}")

            organization_id = connection.get("organization_id")

            # Import validation manager
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
                "tables_with_failures": []
            }

            if table_configs.data:
                for table_config in table_configs.data:
                    table_name = table_config["table_name"]

                    # Execute validation rules for this table
                    try:
                        # Build connection string
                        connection_string = self._build_connection_string(connection)

                        # Execute rules
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

                    except Exception as table_error:
                        logger.error(f"Error validating table {table_name}: {str(table_error)}")
                        results["tables_with_failures"].append({
                            "table_name": table_name,
                            "error": str(table_error)
                        })

            # Update job status to completed
            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            # Publish event if there were failures
            if results["failed_rules"] > 0:
                publish_automation_event(
                    event_type=AutomationEventType.VALIDATION_FAILURES_DETECTED,
                    data={"job_id": job_id, "failed_rules": results["failed_rules"],
                          "tables": results["tables_with_failures"]},
                    connection_id=connection_id
                )

        except Exception as e:
            logger.error(f"Error executing validation run job {job_id}: {str(e)}")
            self._update_job_status(job_id, "failed", error_message=str(e))
        finally:
            # Clean up active jobs
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

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

                return f"snowflake://{username}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"

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