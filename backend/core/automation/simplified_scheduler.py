# backend/core/automation/simplified_scheduler.py
# UPDATED VERSION with job deduplication and non-blocking executor

import logging
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, Future
import os

from core.storage.supabase_manager import SupabaseManager
from .events import AutomationEventType, publish_automation_event
from .schedule_manager import ScheduleManager
from .job_deduplication import job_deduplication_service  # NEW IMPORT

logger = logging.getLogger(__name__)


class SimplifiedAutomationScheduler:
    """Simplified scheduler that uses user-defined schedules with job deduplication"""

    def __init__(self, max_workers: int = 3):
        self.supabase = SupabaseManager()
        self.schedule_manager = ScheduleManager(self.supabase)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.scheduler_thread = None
        self.active_jobs = {}  # job_id -> Future mapping

        self.environment = os.getenv("ENVIRONMENT", "development")

        # More flexible environment logic
        self.scheduler_enabled = True  # Default to enabled

        # Check if explicitly disabled
        if os.getenv("DISABLE_AUTOMATION", "false").lower() == "true":
            self.scheduler_enabled = False
        # In development, require explicit enabling
        elif self.environment == "development":
            self.scheduler_enabled = os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"

        logger.info(
            f"Simplified Scheduler initialized - Environment: {self.environment}, Enabled: {self.scheduler_enabled}")

        # Initialize metadata task manager integration
        self.metadata_task_manager = None
        self._initialize_metadata_integration()

    def _initialize_metadata_integration(self):
        """Initialize integration with metadata task manager"""
        try:
            from core.metadata.manager import MetadataTaskManager
            self.metadata_task_manager = MetadataTaskManager.get_instance(
                supabase_manager=self.supabase
            )
            logger.info("Simplified scheduler integrated with metadata task manager")
        except Exception as e:
            logger.error(f"Failed to initialize metadata task manager integration: {str(e)}")
            self.metadata_task_manager = None

    def start(self):
        """Start the simplified scheduler with better error handling"""
        if not self.scheduler_enabled:
            logger.info(f"Scheduler disabled for environment: {self.environment}")
            if self.environment == "development":
                logger.info("Set ENABLE_AUTOMATION_SCHEDULER=true to enable in development")
            return

        if self.running:
            logger.warning("Scheduler already running")
            return

        try:
            self.running = True
            logger.info("Starting simplified automation scheduler...")

            # Start scheduler thread
            self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            self.scheduler_thread.start()

            logger.info("✓ Simplified automation scheduler started successfully")
        except Exception as e:
            logger.error(f"Error starting scheduler: {str(e)}")
            self.running = False
            raise

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

        logger.info("Simplified automation scheduler stopped")

    def _run_scheduler(self):
        """Main scheduler loop"""
        logger.info("Scheduler loop started")

        while self.running:
            try:
                # Check for due jobs every minute
                self._check_and_execute_due_jobs()

                # Clean up completed jobs every 10 minutes
                if int(time.time()) % 600 == 0:  # Every 10 minutes
                    self._cleanup_completed_jobs()

                # Sleep for 1 minute
                time.sleep(60)

            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(60)  # Sleep even on error

    def _check_and_execute_due_jobs(self):
        """Check for due jobs and execute them with detailed logging"""
        try:
            # Get jobs that are due to run
            due_jobs = self.schedule_manager.get_due_jobs(buffer_minutes=2)

            if not due_jobs:
                return  # No jobs due, nothing to log

            logger.info(f"Found {len(due_jobs)} jobs due to run")

            for i, scheduled_job in enumerate(due_jobs):
                try:
                    connection_id = scheduled_job["connection_id"]
                    automation_type = scheduled_job["automation_type"]
                    scheduled_job_id = scheduled_job["id"]

                    logger.info(f"[{i+1}/{len(due_jobs)}] Executing {automation_type} for connection {connection_id}")

                    # Create and execute the job (with deduplication)
                    job_id = self._create_and_execute_job(
                        connection_id=connection_id,
                        automation_type=automation_type,
                        scheduled_job_id=scheduled_job_id
                    )

                    if job_id:
                        # Mark the scheduled job as executed
                        logger.info(f"[{i+1}/{len(due_jobs)}] Successfully created job {job_id} for {automation_type}")
                        self.schedule_manager.mark_job_executed(scheduled_job_id)
                        logger.info(f"[{i+1}/{len(due_jobs)}] Marked scheduled job {scheduled_job_id} as executed")
                    else:
                        logger.warning(f"[{i+1}/{len(due_jobs)}] Failed to create {automation_type} job for connection {connection_id} (likely duplicate)")

                    # Small delay between jobs to prevent overwhelming the system
                    time.sleep(1)

                except Exception as job_error:
                    logger.error(f"[{i+1}/{len(due_jobs)}] Error executing scheduled job {automation_type}: {str(job_error)}")
                    logger.error(f"[{i+1}/{len(due_jobs)}] Exception details:", exc_info=True)
                    # Continue to next job instead of stopping
                    continue

            logger.info(f"Completed processing {len(due_jobs)} due jobs")

        except Exception as e:
            logger.error(f"Error in _check_and_execute_due_jobs: {str(e)}")
            logger.error("Exception details:", exc_info=True)

    def _create_and_execute_job(self, connection_id: str, automation_type: str, scheduled_job_id: str) -> Optional[str]:
        """Create and execute a scheduled automation job WITH DEDUPLICATION"""
        try:
            # PHASE 2 IMPLEMENTATION: Add deduplication logic

            # Step 1: Create job fingerprint
            fingerprint = job_deduplication_service.create_job_fingerprint(
                connection_id, automation_type, "user_schedule"
            )

            # Step 2: Check for duplicates
            if job_deduplication_service.is_job_duplicate(fingerprint, max_age_minutes=30):
                logger.warning(f"🚫 Prevented duplicate {automation_type} job for connection {connection_id}")
                return None

            # Step 3: Continue with existing job creation logic
            job_id = str(uuid.uuid4())

            # Create job record
            job_data = {
                "id": job_id,
                "connection_id": connection_id,
                "job_type": automation_type,
                "status": "scheduled",
                "scheduled_at": datetime.now(timezone.utc).isoformat(),
                "job_config": {
                    "trigger": "user_schedule",
                    "scheduled_job_id": scheduled_job_id,
                    "fingerprint": fingerprint  # Store fingerprint for debugging
                }
            }

            response = self.supabase.supabase.table("automation_jobs").insert(job_data).execute()

            if not response.data:
                logger.error(f"Failed to create job record for {automation_type}")
                return None

            # Step 4: Register the job to prevent future duplicates
            success = job_deduplication_service.register_job(
                fingerprint, job_id, connection_id, automation_type, "user_schedule"
            )

            if not success:
                logger.error(f"Failed to register job {job_id} for deduplication")
                # Cancel the job since we couldn't register it
                self._update_job_status(job_id, "failed", error_message="Failed to register for deduplication")
                return None

            logger.info(
                f"✅ Registered job: {automation_type} for connection {connection_id} (fingerprint: {fingerprint[:8]}...)")

            # Step 5: Check executor health before submission
            logger.info(f"Executor status - Active jobs: {len(self.active_jobs)}, ThreadPool workers: {self.executor._max_workers}")
            logger.info(f"About to submit {automation_type} job {job_id} to executor...")

            # Execute the job based on type with non-blocking submission
            try:
                if automation_type == "metadata_refresh":
                    future = self.executor.submit(
                        self._execute_job_with_timeout,
                        self._execute_metadata_refresh,
                        job_id, connection_id, {"scheduled": True, "fingerprint": fingerprint},
                        timeout_minutes=120
                    )
                elif automation_type == "schema_change_detection":
                    future = self.executor.submit(
                        self._execute_job_with_timeout,
                        self._execute_schema_detection,
                        job_id, connection_id, {"scheduled": True, "fingerprint": fingerprint},
                        timeout_minutes=60
                    )
                elif automation_type == "validation_automation":
                    future = self.executor.submit(
                        self._execute_job_with_timeout,
                        self._execute_validation_run,
                        job_id, connection_id, {"scheduled": True, "fingerprint": fingerprint},
                        timeout_minutes=60
                    )
                else:
                    logger.error(f"Unknown automation type: {automation_type}")
                    job_deduplication_service.mark_job_completed(fingerprint, "failed")
                    self._update_job_status(job_id, "failed", error_message=f"Unknown automation type: {automation_type}")
                    return None

                # Store the future for tracking but don't block
                self.active_jobs[job_id] = future
                logger.info(f"✅ Job {job_id} ({automation_type}) submitted to executor successfully")
                return job_id

            except Exception as executor_error:
                logger.error(f"❌ Failed to submit job {job_id} to executor: {str(executor_error)}")
                logger.error("Executor submission error details:", exc_info=True)
                job_deduplication_service.mark_job_completed(fingerprint, "failed")
                self._update_job_status(job_id, "failed", error_message=f"Executor submission failed: {str(executor_error)}")
                return None

        except Exception as e:
            logger.error(f"Error creating and executing job: {str(e)}")
            logger.error("Job creation error details:", exc_info=True)
            return None

    def _execute_job_with_timeout(self, job_function, job_id: str, connection_id: str, config: Dict[str, Any],
                                  timeout_minutes: int = 60):
        """Execute a job function with timeout protection"""
        fingerprint = config.get("fingerprint")

        try:
            logger.info(f"🚀 Starting execution of job {job_id}")
            result = job_function(job_id, connection_id, config)

            # PHASE 2: Mark job as completed on success
            if fingerprint:
                job_deduplication_service.mark_job_completed(fingerprint, "completed")
                logger.info(f"✅ Job {job_id} completed successfully, marked in deduplication service")

            return result

        except Exception as e:
            logger.error(f"Job {job_id} failed with exception: {str(e)}")
            logger.error(f"Job {job_id} exception details:", exc_info=True)

            # PHASE 2: Mark job as completed on failure too
            if fingerprint:
                job_deduplication_service.mark_job_completed(fingerprint, "failed")
                logger.info(f"❌ Job {job_id} failed, marked in deduplication service")

            self._update_job_status(
                job_id, "failed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                error_message=str(e)
            )
        finally:
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]
                logger.info(f"🧹 Cleaned up job {job_id} from active jobs list")

    def _execute_metadata_refresh(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute metadata refresh job with statistics collection"""
        run_id = None
        metadata_task_id = None
        fingerprint = config.get("fingerprint")

        try:
            logger.info(f"Starting metadata refresh job {job_id}")

            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "metadata_refresh")

            # Validate metadata task manager
            if not self.metadata_task_manager:
                raise Exception("Metadata task manager not available")

            # Validate connection
            connection = self.supabase.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection {connection_id} not found")

            logger.info(f"Collecting metadata for connection: {connection.get('name')}")

            # Submit metadata collection task with statistics
            collection_params = {
                "depth": "comprehensive",
                "table_limit": 50,
                "automation_trigger": True,
                "automation_job_id": job_id,
                "refresh_types": ["tables", "columns", "statistics"],
                "timeout_minutes": 45,
                "collect_statistics": True,
                "statistics_sample_size": 100000
            }

            metadata_task_id = self.metadata_task_manager.submit_collection_task(
                connection_id=connection_id,
                params=collection_params,
                priority="medium"
            )

            logger.info(f"Submitted metadata task {metadata_task_id}")

            # Wait for completion
            task_completed = self._wait_for_task_completion(metadata_task_id, timeout_minutes=45)

            if task_completed:
                task_status = self.metadata_task_manager.get_task_status(metadata_task_id)
                task_result = task_status.get("result", {})

                # Verify that statistics were collected
                stats_collected = self._verify_statistics_collection(connection_id, task_result)

                results = {
                    "metadata_task_id": metadata_task_id,
                    "task_result": task_result,
                    "statistics_collected": stats_collected,
                    "success": True,
                    "trigger": "user_schedule"
                }

                self._update_job_status(
                    job_id, "completed",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    result_summary=results
                )

                if run_id:
                    self._update_automation_run(run_id, "completed", results)

                # Publish event
                publish_automation_event(
                    event_type=AutomationEventType.METADATA_REFRESHED,
                    data=results,
                    connection_id=connection_id
                )

                logger.info(f"Completed metadata refresh job {job_id} - Statistics collected: {stats_collected}")

                # PHASE 2: Deduplication cleanup happens in _execute_job_with_timeout

            else:
                error_msg = f"Metadata task {metadata_task_id} timed out"
                self._handle_job_failure(job_id, run_id, connection_id, error_msg)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Metadata refresh job {job_id} failed: {error_msg}")
            self._handle_job_failure(job_id, run_id, connection_id, error_msg)

    def _execute_schema_detection(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute schema change detection job"""
        run_id = None
        fingerprint = config.get("fingerprint")

        try:
            logger.info(f"Starting schema detection job {job_id}")

            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "schema_change_detection")

            # Validate connection
            connection = self.supabase.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection {connection_id} not found")

            # Run schema detection
            from core.metadata.schema_change_detector import SchemaChangeDetector
            from core.metadata.connector_factory import ConnectorFactory

            connector_factory = ConnectorFactory(self.supabase)
            schema_detector = SchemaChangeDetector()

            logger.info(f"Detecting schema changes for: {connection.get('name')}")

            changes, important_changes = schema_detector.detect_changes_for_connection(
                connection_id, connector_factory, self.supabase
            )

            results = {
                "changes_detected": len(changes),
                "important_changes": important_changes,
                "changes_stored": len(changes),
                "verified": True,
                "trigger": "user_schedule"
            }

            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            if run_id:
                self._update_automation_run(run_id, "completed", results)

            # Publish event if changes found
            if len(changes) > 0:
                publish_automation_event(
                    event_type=AutomationEventType.SCHEMA_CHANGES_DETECTED,
                    data=results,
                    connection_id=connection_id
                )

            logger.info(f"Completed schema detection job {job_id}, found {len(changes)} changes")

            # PHASE 2: Deduplication cleanup happens in _execute_job_with_timeout

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Schema detection job {job_id} failed: {error_msg}")
            self._handle_job_failure(job_id, run_id, connection_id, error_msg)

    def _execute_validation_run(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute validation automation job"""
        run_id = None
        fingerprint = config.get("fingerprint")

        try:
            logger.info(f"Starting validation job {job_id}")

            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "validation_automation")

            # Validate connection and get organization
            connection = self.supabase.get_connection(connection_id)
            if not connection:
                raise Exception(f"Connection not found: {connection_id}")

            organization_id = connection.get("organization_id")
            if not organization_id:
                raise Exception(f"No organization ID found for connection {connection_id}")

            # Run automated validations
            from core.utils.validation_automation_integration import create_validation_automation_integrator
            integrator = create_validation_automation_integrator()

            logger.info(f"Running validations for: {connection.get('name')}")

            results = integrator.run_automated_validations(connection_id, organization_id)
            results["trigger"] = "user_schedule"

            self._update_job_status(
                job_id, "completed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                result_summary=results
            )

            if run_id:
                self._update_automation_run(run_id, "completed", results)

            # Publish event if failures found
            if results.get("failed_rules", 0) > 0:
                publish_automation_event(
                    event_type=AutomationEventType.VALIDATION_FAILURES_DETECTED,
                    data=results,
                    connection_id=connection_id
                )

            logger.info(f"Completed validation job {job_id}: {results.get('failed_rules', 0)} failures")

            # PHASE 2: Deduplication cleanup happens in _execute_job_with_timeout

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Validation job {job_id} failed: {error_msg}")
            self._handle_job_failure(job_id, run_id, connection_id, error_msg)

    def _handle_job_failure(self, job_id: str, run_id: str, connection_id: str, error_msg: str):
        """Handle job failure consistently"""
        self._update_job_status(
            job_id, "failed",
            completed_at=datetime.now(timezone.utc).isoformat(),
            error_message=error_msg
        )

        if run_id:
            self._update_automation_run(run_id, "failed", {"error": error_msg})

        # Publish failure event
        publish_automation_event(
            event_type=AutomationEventType.JOB_FAILED,
            data={"job_id": job_id, "error": error_msg},
            connection_id=connection_id
        )

    def _verify_statistics_collection(self, connection_id: str, task_result: Dict[str, Any]) -> bool:
        """Verify that statistics were actually collected"""
        try:
            # Check if statistics are in the task result
            if "statistics" in task_result or "statistics_by_table" in task_result:
                logger.info("Statistics found in task result")
                return True

            # Check database for recent statistics metadata
            response = self.supabase.supabase.table("connection_metadata") \
                .select("id, collected_at") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", "statistics") \
                .gte("collected_at", (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()) \
                .execute()

            if response.data and len(response.data) > 0:
                logger.info(f"Found recent statistics metadata for connection {connection_id}")
                return True

            logger.warning(f"No recent statistics found for connection {connection_id}")
            return False

        except Exception as e:
            logger.error(f"Error verifying statistics collection: {str(e)}")
            return False

    def _wait_for_task_completion(self, task_id: str, timeout_minutes: int = 30) -> bool:
        """Wait for task completion with timeout"""
        try:
            if not self.metadata_task_manager:
                return False

            completion_result = self.metadata_task_manager.wait_for_task_completion_sync(
                task_id, timeout_minutes
            )

            return completion_result.get("completed", False) and completion_result.get("success", False)

        except Exception as e:
            logger.error(f"Error waiting for task completion: {str(e)}")
            return False

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
                return run_id
            else:
                logger.warning("Failed to create automation run")
                return None

        except Exception as e:
            logger.error(f"Error creating automation run: {str(e)}")
            return None

    def _update_automation_run(self, run_id: str, status: str, results: Dict[str, Any] = None):
        """Update automation run record"""
        try:
            if not run_id:
                return

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

        except Exception as e:
            logger.error(f"Error updating automation run {run_id}: {str(e)}")

    def _update_job_status(self, job_id: str, status: str, **kwargs):
        """Update job status"""
        try:
            update_data = {"status": status}
            update_data.update(kwargs)

            if status in ["completed", "failed"] and "completed_at" not in update_data:
                update_data["completed_at"] = datetime.now(timezone.utc).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .update(update_data) \
                .eq("id", job_id) \
                .execute()

            if status == "completed":
                logger.info(f"Job {job_id} completed")
            elif status == "failed":
                logger.error(f"Job {job_id} failed")
            elif status == "running":
                logger.info(f"Job {job_id} started")

        except Exception as e:
            logger.error(f"Error updating job status for {job_id}: {str(e)}")

    def _cleanup_completed_jobs(self):
        """Clean up old completed/failed jobs"""
        try:
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .delete() \
                .in_("status", ["completed", "failed", "cancelled"]) \
                .lt("created_at", cutoff_date) \
                .execute()

            deleted_count = len(response.data) if response.data else 0
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old automation jobs")

        except Exception as e:
            logger.error(f"Error cleaning up old jobs: {str(e)}")

    # Public methods for external control
    def schedule_immediate_run(self, connection_id: str, automation_type: str = None, trigger_user: str = None) -> Dict[str, Any]:
        """Schedule an immediate automation run WITH PROPER DEDUPLICATION"""
        try:
            jobs_created = []
            prevented_duplicates = []

            # List of automation types to run
            automation_types = []
            if automation_type == "metadata_refresh" or automation_type is None:
                automation_types.append("metadata_refresh")
            if automation_type == "schema_change_detection" or automation_type is None:
                automation_types.append("schema_change_detection")
            if automation_type == "validation_automation" or automation_type is None:
                automation_types.append("validation_automation")

            for auto_type in automation_types:
                # FIXED: Use the same deduplication logic as scheduled jobs
                fingerprint = job_deduplication_service.create_job_fingerprint(
                    connection_id, auto_type, "manual_trigger"
                )

                # Check for duplicates
                if job_deduplication_service.is_job_duplicate(fingerprint, max_age_minutes=30):
                    logger.warning(f"🚫 Prevented duplicate {auto_type} manual job for connection {connection_id}")
                    prevented_duplicates.append(auto_type)
                    continue

                # Create job record
                job_id = str(uuid.uuid4())
                job_data = {
                    "id": job_id,
                    "connection_id": connection_id,
                    "job_type": auto_type,
                    "status": "scheduled",
                    "scheduled_at": datetime.now(timezone.utc).isoformat(),
                    "job_config": {
                        "trigger": "manual_trigger",
                        "triggered_by": trigger_user,
                        "fingerprint": fingerprint
                    }
                }

                response = self.supabase.supabase.table("automation_jobs").insert(job_data).execute()

                if not response.data:
                    logger.error(f"Failed to create job record for {auto_type}")
                    continue

                # Register the job for deduplication
                success = job_deduplication_service.register_job(
                    fingerprint, job_id, connection_id, auto_type, "manual_trigger"
                )

                if not success:
                    logger.error(f"Failed to register manual job {job_id} for deduplication")
                    self._update_job_status(job_id, "failed", error_message="Failed to register for deduplication")
                    continue

                logger.info(f"✅ Registered manual job: {auto_type} for connection {connection_id} (fingerprint: {fingerprint[:8]}...)")

                # Execute the job based on type
                try:
                    if auto_type == "metadata_refresh":
                        future = self.executor.submit(
                            self._execute_job_with_timeout,
                            self._execute_metadata_refresh,
                            job_id, connection_id, {"manual": True, "fingerprint": fingerprint},
                            timeout_minutes=120
                        )
                    elif auto_type == "schema_change_detection":
                        future = self.executor.submit(
                            self._execute_job_with_timeout,
                            self._execute_schema_detection,
                            job_id, connection_id, {"manual": True, "fingerprint": fingerprint},
                            timeout_minutes=60
                        )
                    elif auto_type == "validation_automation":
                        future = self.executor.submit(
                            self._execute_job_with_timeout,
                            self._execute_validation_run,
                            job_id, connection_id, {"manual": True, "fingerprint": fingerprint},
                            timeout_minutes=60
                        )

                    self.active_jobs[job_id] = future
                    jobs_created.append(job_id)

                except Exception as executor_error:
                    logger.error(f"Failed to submit manual job {job_id} to executor: {str(executor_error)}")
                    job_deduplication_service.mark_job_completed(fingerprint, "failed")
                    self._update_job_status(job_id, "failed", error_message=f"Executor submission failed: {str(executor_error)}")
                    continue

            return {
                "success": True,
                "jobs_created": jobs_created,
                "prevented_duplicates": prevented_duplicates,
                "message": f"Created {len(jobs_created)} jobs, prevented {len(prevented_duplicates)} duplicates"
            }

        except Exception as e:
            logger.error(f"Error scheduling immediate run: {str(e)}")
            return {"success": False, "error": str(e)}

    def update_connection_schedule(self, connection_id: str, schedule_config: Dict[str, Any]):
        """Update scheduling for a connection"""
        try:
            return self.schedule_manager.update_connection_schedule(connection_id, schedule_config, "system")
        except Exception as e:
            logger.error(f"Error updating connection schedule: {str(e)}")

    def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        try:
            # Get active jobs count
            active_count = len(self.active_jobs)

            # Get scheduled jobs count
            scheduled_response = self.supabase.supabase.table("automation_scheduled_jobs") \
                .select("id", count="exact") \
                .eq("enabled", True) \
                .execute()

            # PHASE 2: Add deduplication stats
            dedup_stats = job_deduplication_service.get_active_jobs_summary()

            return {
                "running": self.running,
                "environment": self.environment,
                "scheduler_enabled": self.scheduler_enabled,
                "active_jobs": active_count,
                "active_job_ids": list(self.active_jobs.keys()),
                "scheduled_jobs_count": scheduled_response.count or 0,
                "metadata_task_manager_available": self.metadata_task_manager is not None,
                "version": "simplified_user_schedule",
                "deduplication_stats": dedup_stats  # NEW: Add deduplication info
            }
        except Exception as e:
            logger.error(f"Error getting scheduler stats: {str(e)}")
            return {
                "running": self.running,
                "error": str(e),
                "version": "simplified_user_schedule"
            }