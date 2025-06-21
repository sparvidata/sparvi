import json
import logging
import uuid
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, Future
import schedule
import traceback

from core.storage.supabase_manager import SupabaseManager
from .events import AutomationEventType, publish_automation_event

logger = logging.getLogger(__name__)


class AutomationScheduler:
    """Automation scheduler that integrates with metadata task manager"""

    def __init__(self, max_workers: int = 3):
        self.supabase = SupabaseManager()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.scheduler_thread = None
        self.active_jobs = {}  # job_id -> Future mapping

        # Initialize metadata task manager integration
        self.metadata_task_manager = None
        self._initialize_metadata_integration()

        # Initialize scheduler
        schedule.clear()

    def _initialize_metadata_integration(self):
        """Initialize integration with metadata task manager"""
        try:
            from core.metadata.manager import MetadataTaskManager
            self.metadata_task_manager = MetadataTaskManager.get_instance(
                supabase_manager=self.supabase
            )
            logger.info("Automation scheduler integrated with metadata task manager")
        except Exception as e:
            logger.error(f"Failed to initialize metadata task manager integration: {str(e)}")
            self.metadata_task_manager = None

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
        """Set up periodic automation schedules - FIXED to run less frequently"""

        # FIXED: Since users can configure their own intervals (like 24 hours),
        # we only need to check periodically to see if any jobs are due.
        # No need to check every 30 minutes when user intervals are 24 hours!

        # Check for due jobs every 2 hours (was every 30 minutes - 2 hours)
        schedule.every(2).hours.do(self._check_metadata_refresh_jobs)
        schedule.every(2).hours.do(self._check_schema_detection_jobs)
        schedule.every(2).hours.do(self._check_validation_jobs)

        # Clean up completed jobs daily (this is fine)
        schedule.every().day.at("02:00").do(self._cleanup_old_jobs)

        logger.info("Automation scheduler configured with user-respecting intervals:")
        logger.info("- Checking for due jobs: every 2 hours")
        logger.info("- Cleanup: daily at 2:00 AM")
        logger.info("- Individual job intervals: determined by user configuration")

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
        """Check and schedule metadata refresh jobs - FIXED to respect user config"""
        try:
            # Get all enabled metadata refresh configurations
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*, database_connections(organization_id)") \
                .execute()

            if not response.data:
                return

            for config in response.data:
                try:
                    # Parse the JSON configuration
                    metadata_config = json.loads(config.get("metadata_refresh", "{}"))
                except (json.JSONDecodeError, TypeError):
                    logger.error(f"Invalid JSON in metadata_refresh config for connection {config['connection_id']}")
                    continue

                if not metadata_config.get("enabled", False):
                    continue

                connection_id = config["connection_id"]

                # FIXED: Use the user-configured interval, not a hardcoded default
                interval_hours = metadata_config.get("interval_hours", 24)

                logger.debug(f"Checking metadata refresh for connection {connection_id}, "
                             f"user configured interval: {interval_hours} hours")

                # Check if we need to schedule a job using the USER'S interval
                if self._should_schedule_job(connection_id, "metadata_refresh", interval_hours):
                    self._schedule_metadata_refresh(connection_id, metadata_config)

        except Exception as e:
            logger.error(f"Error checking metadata refresh jobs: {str(e)}")

    def _check_schema_detection_jobs(self):
        """Check and schedule schema change detection jobs - FIXED to respect user config"""
        try:
            # Get all enabled schema detection configurations
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*, database_connections(organization_id)") \
                .execute()

            if not response.data:
                return

            for config in response.data:
                try:
                    # Parse the JSON configuration
                    schema_config = json.loads(config.get("schema_change_detection", "{}"))
                except (json.JSONDecodeError, TypeError):
                    logger.error(
                        f"Invalid JSON in schema_change_detection config for connection {config['connection_id']}")
                    continue

                if not schema_config.get("enabled", False):
                    continue

                connection_id = config["connection_id"]

                # FIXED: Use the user-configured interval, not a hardcoded default
                interval_hours = schema_config.get("interval_hours", 6)

                logger.debug(f"Checking schema detection for connection {connection_id}, "
                             f"user configured interval: {interval_hours} hours")

                # Check if we need to schedule a job using the USER'S interval
                if self._should_schedule_job(connection_id, "schema_detection", interval_hours):
                    self._schedule_schema_detection(connection_id, schema_config)

        except Exception as e:
            logger.error(f"Error checking schema detection jobs: {str(e)}")

    def _check_validation_jobs(self):
        """Check and schedule validation automation jobs - FIXED to respect user config"""
        try:
            # Get all enabled validation automation configurations
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*, database_connections(organization_id)") \
                .execute()

            if not response.data:
                return

            for config in response.data:
                try:
                    # Parse the JSON configuration
                    validation_config = json.loads(config.get("validation_automation", "{}"))
                except (json.JSONDecodeError, TypeError):
                    logger.error(
                        f"Invalid JSON in validation_automation config for connection {config['connection_id']}")
                    continue

                if not validation_config.get("enabled", False):
                    continue

                connection_id = config["connection_id"]

                # FIXED: Use the user-configured interval, not a hardcoded default
                interval_hours = validation_config.get("interval_hours", 12)

                logger.debug(f"Checking validation automation for connection {connection_id}, "
                             f"user configured interval: {interval_hours} hours")

                # Check if we need to schedule a job using the USER'S interval
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
        """Schedule a metadata refresh job using task manager"""
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
            future = self.executor.submit(self._execute_metadata_refresh_integrated, job_id, connection_id, config)
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
            future = self.executor.submit(self._execute_schema_detection_integrated, job_id, connection_id, config)
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
            future = self.executor.submit(self._execute_validation_run_integrated, job_id, connection_id, config)
            self.active_jobs[job_id] = future

            logger.info(f"Scheduled validation run job {job_id} for connection {connection_id}")

        except Exception as e:
            logger.error(f"Error scheduling validation run: {str(e)}")

    def _execute_metadata_refresh_integrated(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute metadata refresh using integrated task manager"""
        run_id = None
        metadata_task_id = None

        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "metadata_refresh")

            if not self.metadata_task_manager:
                raise Exception("Metadata task manager not available")

            # Determine metadata collection parameters
            refresh_types = config.get("types", ["tables", "columns"])

            # Map refresh types to collection depth and limits
            if "statistics" in refresh_types:
                depth = "high"  # Deep collection for statistics
                table_limit = 25  # Limit for performance
            elif "columns" in refresh_types:
                depth = "medium"  # Medium collection for columns
                table_limit = 50
            else:
                depth = "low"  # Light collection for tables only
                table_limit = 100

            logger.info(f"Submitting metadata collection task: depth={depth}, table_limit={table_limit}")

            # Submit collection task to metadata task manager
            metadata_task_id = self.metadata_task_manager.submit_collection_task(
                connection_id=connection_id,
                params={
                    "depth": depth,
                    "table_limit": table_limit,
                    "automation_trigger": True,
                    "automation_job_id": job_id,
                    "refresh_types": refresh_types
                },
                priority="medium"  # Automation jobs are medium priority
            )

            logger.info(f"Submitted metadata collection task {metadata_task_id}")

            # Wait for task completion with timeout
            task_completed = self._wait_for_task_completion(metadata_task_id, timeout_minutes=45)

            if task_completed:
                # Get task results
                task_status = self.metadata_task_manager.get_task_status(metadata_task_id)
                task_result = task_status.get("result", {})

                results = {
                    "metadata_task_id": metadata_task_id,
                    "task_result": task_result,
                    "refresh_types": refresh_types,
                    "automation_triggered": True,
                    "integration_method": "task_manager"
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

                # Publish success event
                publish_automation_event(
                    event_type=AutomationEventType.JOB_COMPLETED,
                    data={
                        "job_id": job_id,
                        "job_type": "metadata_refresh",
                        "metadata_task_id": metadata_task_id,
                        "results": results
                    },
                    connection_id=connection_id
                )

                logger.info(f"Completed metadata refresh job {job_id} using task manager")

            else:
                # Task didn't complete in time
                raise Exception(f"Metadata collection task {metadata_task_id} did not complete within timeout")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing metadata refresh job {job_id}: {error_msg}")
            logger.error(traceback.format_exc())

            self._update_job_status(job_id, "failed", error_message=error_msg)

            if run_id:
                self._update_automation_run(run_id, "failed",
                                            {"error": error_msg, "metadata_task_id": metadata_task_id})

            # Publish failure event
            publish_automation_event(
                event_type=AutomationEventType.JOB_FAILED,
                data={
                    "job_id": job_id,
                    "job_type": "metadata_refresh",
                    "error": error_msg,
                    "metadata_task_id": metadata_task_id
                },
                connection_id=connection_id
            )
        finally:
            # Clean up active jobs
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _execute_schema_detection_integrated(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute schema detection using integrated approach"""
        run_id = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "schema_detection")

            # Use the schema change detector
            from core.metadata.schema_change_detector import SchemaChangeDetector
            from core.metadata.connector_factory import ConnectorFactory

            # Initialize components
            connector_factory = ConnectorFactory(self.supabase)
            schema_detector = SchemaChangeDetector()

            # If we have metadata task manager, ensure recent metadata exists
            if self.metadata_task_manager:
                # Check if we have recent metadata for schema comparison
                metadata_status = self.metadata_task_manager.get_metadata_collection_status(connection_id)

                # If metadata is stale, refresh it first
                if metadata_status.get("overall_status") in ["stale", "missing"]:
                    logger.info("Refreshing metadata before schema detection")

                    # Submit a quick metadata refresh
                    metadata_task_id = self.metadata_task_manager.submit_collection_task(
                        connection_id=connection_id,
                        params={"depth": "low", "table_limit": 100, "for_schema_detection": True},
                        priority="high"
                    )

                    # Wait for metadata refresh
                    self._wait_for_task_completion(metadata_task_id, timeout_minutes=10)

            # Detect changes
            logger.info("Starting schema change detection")
            changes, important_changes = schema_detector.detect_changes_for_connection(
                connection_id, connector_factory, self.supabase
            )

            results = {
                "changes_detected": len(changes),
                "important_changes": important_changes,
                "changes_stored": len(changes),
                "verified": True,
                "automation_triggered": True
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

    def _execute_validation_run_integrated(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute validation run using integrated validation system"""
        run_id = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "validation_run")

            # Use the validation automation integrator
            from core.utils.validation_automation_integration import create_validation_automation_integrator

            integrator = create_validation_automation_integrator()

            # Get connection details for organization ID
            connection = self.supabase.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection not found: {connection_id}")

            organization_id = connection.get("organization_id")
            if not organization_id:
                raise Exception(f"No organization ID found for connection {connection_id}")

            # Run automated validations
            results = integrator.run_automated_validations(connection_id, organization_id)

            # Add automation metadata
            results["automation_triggered"] = True
            results["integration_method"] = "validation_integrator"

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

    def _wait_for_task_completion(self, task_id: str, timeout_minutes: int = 45) -> bool:
        """Wait for a metadata task to complete"""
        try:
            if not self.metadata_task_manager:
                logger.warning("No metadata task manager available for task waiting")
                return False

            # Use the task manager's built-in sync waiting method
            completion_result = self.metadata_task_manager.wait_for_task_completion_sync(
                task_id, timeout_minutes
            )

            success = completion_result.get("completed", False) and completion_result.get("success", False)

            if not success:
                logger.error(f"Task {task_id} failed or timed out: {completion_result.get('error', 'Unknown error')}")

            return success

        except Exception as e:
            logger.error(f"Error waiting for task completion: {str(e)}")
            return False

    # Keep all the existing helper methods unchanged
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

    # Public methods for external control remain the same
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
            "metadata_task_manager_available": self.metadata_task_manager is not None,
            "storage_services_initialized": all([
                hasattr(self, 'supabase') and self.supabase is not None,
                self.metadata_task_manager is not None
            ])
        }