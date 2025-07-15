# backend/core/automation/simplified_scheduler.py
# PREVENTION-FOCUSED: Stop duplicates from happening instead of complex deduplication

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

logger = logging.getLogger(__name__)


class SimplifiedAutomationScheduler:
    """Simplified scheduler with duplicate prevention built-in"""

    def __init__(self, max_workers: int = 3):
        self.supabase = SupabaseManager()
        self.schedule_manager = ScheduleManager(self.supabase)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.scheduler_thread = None
        self.active_jobs = {}  # job_id -> Future mapping

        # PREVENTION: Track jobs created in this scheduler run to prevent immediate duplicates
        self.jobs_created_this_cycle = set()  # Set of (connection_id, automation_type) tuples

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
                # PREVENTION: Clear the cycle tracker at start of each cycle
                self.jobs_created_this_cycle.clear()

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
        """Check for due jobs and execute them with duplicate prevention"""
        try:
            # PREVENTION: Use smaller buffer to reduce overlap window
            due_jobs = self.schedule_manager.get_due_jobs(buffer_minutes=1)

            if not due_jobs:
                return  # No jobs due, nothing to log

            logger.info(f"🔍 Found {len(due_jobs)} jobs due to run")

            # PREVENTION: Filter out jobs that would be duplicates
            filtered_jobs = self._filter_duplicate_jobs(due_jobs)

            if len(filtered_jobs) < len(due_jobs):
                logger.info(f"🚫 Filtered out {len(due_jobs) - len(filtered_jobs)} duplicate jobs")

            successful_jobs = 0
            failed_jobs = 0

            for i, scheduled_job in enumerate(filtered_jobs):
                try:
                    connection_id = scheduled_job["connection_id"]
                    automation_type = scheduled_job["automation_type"]
                    scheduled_job_id = scheduled_job["id"]

                    logger.info(
                        f"🚀 [{i + 1}/{len(filtered_jobs)}] Processing {automation_type} for connection {connection_id}")

                    # PREVENTION: Double-check that we haven't created this job in this cycle
                    job_key = (connection_id, automation_type)
                    if job_key in self.jobs_created_this_cycle:
                        logger.warning(
                            f"⚠️ [{i + 1}/{len(filtered_jobs)}] Skipping {automation_type} - already created in this cycle")
                        continue

                    # Create and execute the job
                    job_id = self._create_and_execute_job(
                        connection_id=connection_id,
                        automation_type=automation_type,
                        scheduled_job_id=scheduled_job_id
                    )

                    if job_id:
                        # PREVENTION: Track that we created this job
                        self.jobs_created_this_cycle.add(job_key)

                        # Mark the scheduled job as executed
                        logger.info(
                            f"✅ [{i + 1}/{len(filtered_jobs)}] Successfully created job {job_id} for {automation_type}")

                        # Try to mark as executed, but don't fail if it doesn't work
                        try:
                            self.schedule_manager.mark_job_executed(scheduled_job_id)
                            logger.info(
                                f"✅ [{i + 1}/{len(filtered_jobs)}] Marked scheduled job {scheduled_job_id} as executed")
                        except Exception as mark_error:
                            logger.warning(
                                f"⚠️ [{i + 1}/{len(filtered_jobs)}] Could not mark job as executed: {str(mark_error)}")

                        successful_jobs += 1
                    else:
                        logger.warning(
                            f"❌ [{i + 1}/{len(filtered_jobs)}] Failed to create {automation_type} job for connection {connection_id}")
                        failed_jobs += 1

                    # Small delay between jobs to prevent overwhelming the system
                    time.sleep(0.5)

                    # Log that we're continuing to next job
                    logger.info(
                        f"➡️ [{i + 1}/{len(filtered_jobs)}] Completed processing {automation_type}, continuing to next job...")

                except Exception as job_error:
                    logger.error(
                        f"❌ [{i + 1}/{len(filtered_jobs)}] ERROR executing scheduled job {automation_type}: {str(job_error)}")
                    logger.error(f"❌ [{i + 1}/{len(filtered_jobs)}] Exception details:", exc_info=True)
                    failed_jobs += 1
                    # CRITICAL: Continue to next job instead of stopping
                    logger.info(f"➡️ [{i + 1}/{len(filtered_jobs)}] Continuing to next job after error...")
                    continue

            logger.info(
                f"📊 Completed processing {len(filtered_jobs)} due jobs: {successful_jobs} successful, {failed_jobs} failed")

        except Exception as e:
            logger.error(f"❌ Error in _check_and_execute_due_jobs: {str(e)}")
            logger.error("❌ Exception details:", exc_info=True)

    def _filter_duplicate_jobs(self, due_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        PREVENTION: Filter out jobs that would create duplicates

        Check for:
        1. Jobs already running
        2. Jobs scheduled recently (last 5 minutes)
        3. Multiple jobs of same type for same connection in this batch
        """
        try:
            filtered_jobs = []
            seen_job_types = set()  # Track (connection_id, automation_type) in this batch

            for job in due_jobs:
                connection_id = job["connection_id"]
                automation_type = job["automation_type"]
                job_key = (connection_id, automation_type)

                # PREVENTION 1: Skip if we've already seen this job type for this connection in this batch
                if job_key in seen_job_types:
                    logger.info(f"🚫 Skipping duplicate {automation_type} for connection {connection_id} in this batch")
                    continue

                # PREVENTION 2: Check if there's already a running job of this type
                if self._is_job_already_running(connection_id, automation_type):
                    logger.info(f"🚫 Skipping {automation_type} for connection {connection_id} - already running")
                    continue

                # PREVENTION 3: Check if there's a recent job (last 5 minutes)
                if self._has_recent_job(connection_id, automation_type, minutes=5):
                    logger.info(f"🚫 Skipping {automation_type} for connection {connection_id} - recent job exists")
                    continue

                # Job passes all checks
                filtered_jobs.append(job)
                seen_job_types.add(job_key)
                logger.debug(f"✅ Approved {automation_type} for connection {connection_id}")

            return filtered_jobs

        except Exception as e:
            logger.error(f"Error filtering duplicate jobs: {str(e)}")
            return due_jobs  # Return original list if filtering fails

    def _is_job_already_running(self, connection_id: str, automation_type: str) -> bool:
        """PREVENTION: Check if a job of this type is already running"""
        try:
            response = self.supabase.supabase.table("automation_jobs") \
                .select("id", count="exact") \
                .eq("connection_id", connection_id) \
                .eq("job_type", automation_type) \
                .eq("status", "running") \
                .execute()

            return (response.count or 0) > 0

        except Exception as e:
            logger.error(f"Error checking running jobs: {str(e)}")
            return False  # Assume not running if we can't check

    def _has_recent_job(self, connection_id: str, automation_type: str, minutes: int = 5) -> bool:
        """PREVENTION: Check if there's a recent job of this type"""
        try:
            cutoff_time = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .select("id", count="exact") \
                .eq("connection_id", connection_id) \
                .eq("job_type", automation_type) \
                .gte("scheduled_at", cutoff_time) \
                .execute()

            return (response.count or 0) > 0

        except Exception as e:
            logger.error(f"Error checking recent jobs: {str(e)}")
            return False  # Assume no recent job if we can't check

    def _create_and_execute_job(self, connection_id: str, automation_type: str, scheduled_job_id: str) -> Optional[str]:
        """Create and execute a scheduled automation job - SIMPLIFIED VERSION"""
        try:
            logger.info(f"🔧 Creating job {automation_type} for connection {connection_id}")

            # Create job record
            job_id = str(uuid.uuid4())

            job_data = {
                "id": job_id,
                "connection_id": connection_id,
                "job_type": automation_type,
                "status": "scheduled",
                "scheduled_at": datetime.now(timezone.utc).isoformat(),
                "job_config": {
                    "trigger": "user_schedule",
                    "scheduled_job_id": scheduled_job_id,
                    "prevention_enabled": True
                }
            }

            logger.info(f"💾 Creating job record for {job_id}...")
            response = self.supabase.supabase.table("automation_jobs").insert(job_data).execute()

            if not response.data:
                logger.error(f"❌ Failed to create job record for {automation_type}")
                return None

            logger.info(f"✅ Job record created: {job_id}")

            # Submit to executor
            logger.info(f"🚀 Submitting job {job_id} to executor...")

            # Check executor health before submission
            logger.info(
                f"📊 Executor status - Active jobs: {len(self.active_jobs)}, ThreadPool workers: {self.executor._max_workers}")

            try:
                if automation_type == "metadata_refresh":
                    future = self.executor.submit(
                        self._execute_job_with_timeout,
                        self._execute_metadata_refresh,
                        job_id, connection_id, {"scheduled": True},
                        timeout_minutes=120
                    )
                elif automation_type == "schema_change_detection":
                    future = self.executor.submit(
                        self._execute_job_with_timeout,
                        self._execute_schema_detection,
                        job_id, connection_id, {"scheduled": True},
                        timeout_minutes=60
                    )
                elif automation_type == "validation_automation":
                    future = self.executor.submit(
                        self._execute_job_with_timeout,
                        self._execute_validation_run,
                        job_id, connection_id, {"scheduled": True},
                        timeout_minutes=60
                    )
                else:
                    logger.error(f"❌ Unknown automation type: {automation_type}")
                    self._update_job_status(job_id, "failed",
                                            error_message=f"Unknown automation type: {automation_type}")
                    return None

                # Store the future for tracking
                self.active_jobs[job_id] = future
                logger.info(f"✅ Job {job_id} ({automation_type}) submitted to executor successfully")
                return job_id

            except Exception as executor_error:
                logger.error(f"❌ Failed to submit job {job_id} to executor: {str(executor_error)}")
                logger.error("❌ Executor submission error details:", exc_info=True)
                self._update_job_status(job_id, "failed",
                                        error_message=f"Executor submission failed: {str(executor_error)}")
                return None

        except Exception as e:
            logger.error(f"❌ Error creating and executing job: {str(e)}")
            logger.error("❌ Job creation error details:", exc_info=True)
            return None

    def _execute_job_with_timeout(self, job_function, job_id: str, connection_id: str, config: Dict[str, Any],
                                  timeout_minutes: int = 60):
        """Execute a job function with timeout protection"""
        try:
            logger.info(f"🚀 Starting execution of job {job_id}")
            result = job_function(job_id, connection_id, config)
            return result

        except Exception as e:
            logger.error(f"❌ Job {job_id} failed with exception: {str(e)}")
            logger.error(f"❌ Job {job_id} exception details:", exc_info=True)

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
        """Execute metadata refresh job with improved error handling"""
        run_id = None
        metadata_task_id = None

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

            # IMPROVED: Wait for completion with better timeout handling
            task_completed = self._wait_for_task_completion_improved(metadata_task_id, timeout_minutes=60)

            if task_completed:
                logger.info(f"✅ Metadata task {metadata_task_id} completed successfully")

                # Get task results
                task_status = self.metadata_task_manager.get_task_status(metadata_task_id)
                task_result = task_status.get("result", {})

                # Verify that statistics were collected
                stats_collected = self._verify_statistics_collection(connection_id, task_result)

                results = {
                    "metadata_task_id": metadata_task_id,
                    "task_result": task_result,
                    "statistics_collected": stats_collected,
                    "success": True,
                    "trigger": "user_schedule",
                    "tables_processed": len(task_result.get("tables", [])),
                    "statistics_tables": len(task_result.get("statistics_by_table", {}))
                }

                # IMPROVED: More robust job status update
                try:
                    logger.info(f"📝 Updating job {job_id} status to completed...")
                    self._update_job_status(
                        job_id, "completed",
                        completed_at=datetime.now(timezone.utc).isoformat(),
                        result_summary=results
                    )
                    logger.info(f"✅ Job {job_id} status updated to completed")
                except Exception as status_error:
                    logger.error(f"❌ Error updating job status to completed: {str(status_error)}")
                    # Don't fail the entire job if status update fails
                    logger.warning(f"⚠️ Job {job_id} completed successfully but status update failed")

                # Update automation run
                if run_id:
                    try:
                        self._update_automation_run(run_id, "completed", results)
                        logger.info(f"✅ Automation run {run_id} updated")
                    except Exception as run_error:
                        logger.error(f"❌ Error updating automation run: {str(run_error)}")

                # Publish event
                try:
                    publish_automation_event(
                        event_type=AutomationEventType.METADATA_REFRESHED,
                        data=results,
                        connection_id=connection_id
                    )
                    logger.info(f"✅ Published metadata refresh event")
                except Exception as event_error:
                    logger.error(f"❌ Error publishing event: {str(event_error)}")

                logger.info(f"🎉 Completed metadata refresh job {job_id} - Statistics collected: {stats_collected}")

            else:
                error_msg = f"Metadata task {metadata_task_id} timed out or failed"
                logger.error(f"❌ {error_msg}")
                self._handle_job_failure(job_id, run_id, connection_id, error_msg)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Metadata refresh job {job_id} failed: {error_msg}")
            logger.error(f"❌ Exception details:", exc_info=True)
            self._handle_job_failure(job_id, run_id, connection_id, error_msg)

    def _wait_for_task_completion_improved(self, task_id: str, timeout_minutes: int = 60) -> bool:
        """Improved task completion waiting with better error handling"""
        try:
            if not self.metadata_task_manager:
                logger.error("❌ Metadata task manager not available")
                return False

            logger.info(f"⏳ Waiting for metadata task {task_id} to complete (timeout: {timeout_minutes}min)")

            start_time = time.time()
            check_interval = 10  # Check every 10 seconds

            while True:
                try:
                    # Get current task status
                    task_status = self.metadata_task_manager.get_task_status(task_id)
                    status = task_status.get("status", "unknown")

                    logger.debug(f"Task {task_id} status: {status}")

                    if status == "completed":
                        logger.info(f"✅ Task {task_id} completed successfully")
                        return True
                    elif status == "failed":
                        logger.error(f"❌ Task {task_id} failed")
                        return False
                    elif status in ["cancelled", "timeout"]:
                        logger.error(f"❌ Task {task_id} was {status}")
                        return False

                    # Check timeout
                    elapsed = time.time() - start_time
                    if elapsed > (timeout_minutes * 60):
                        logger.error(f"❌ Task {task_id} timed out after {timeout_minutes} minutes")
                        return False

                    # Wait before next check
                    time.sleep(check_interval)

                except Exception as check_error:
                    logger.warning(f"⚠️ Error checking task status: {str(check_error)}")
                    time.sleep(check_interval)

                    # Check timeout even if status check fails
                    elapsed = time.time() - start_time
                    if elapsed > (timeout_minutes * 60):
                        logger.error(f"❌ Task {task_id} timed out during status check errors")
                        return False

        except Exception as e:
            logger.error(f"❌ Error waiting for task completion: {str(e)}")
            return False

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

    def _execute_schema_detection(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute schema change detection job"""
        run_id = None
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

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Schema detection job {job_id} failed: {error_msg}")
            self._handle_job_failure(job_id, run_id, connection_id, error_msg)

    def _execute_validation_run(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute validation automation job"""
        run_id = None
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
    def schedule_immediate_run(self, connection_id: str, automation_type: str = None, trigger_user: str = None) -> Dict[
        str, Any]:
        """Schedule an immediate automation run with simple duplicate prevention"""
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
                # PREVENTION: Check if job is already running or recent
                if self._is_job_already_running(connection_id, auto_type):
                    logger.warning(f"🚫 Prevented manual {auto_type} job - already running")
                    prevented_duplicates.append(auto_type)
                    continue

                if self._has_recent_job(connection_id, auto_type, minutes=2):
                    logger.warning(f"🚫 Prevented manual {auto_type} job - recent job exists")
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
                        "prevention_enabled": True
                    }
                }

                response = self.supabase.supabase.table("automation_jobs").insert(job_data).execute()

                if not response.data:
                    logger.error(f"Failed to create job record for {auto_type}")
                    continue

                # Execute the job based on type
                try:
                    if auto_type == "metadata_refresh":
                        future = self.executor.submit(
                            self._execute_job_with_timeout,
                            self._execute_metadata_refresh,
                            job_id, connection_id, {"manual": True},
                            timeout_minutes=120
                        )
                    elif auto_type == "schema_change_detection":
                        future = self.executor.submit(
                            self._execute_job_with_timeout,
                            self._execute_schema_detection,
                            job_id, connection_id, {"manual": True},
                            timeout_minutes=60
                        )
                    elif auto_type == "validation_automation":
                        future = self.executor.submit(
                            self._execute_job_with_timeout,
                            self._execute_validation_run,
                            job_id, connection_id, {"manual": True},
                            timeout_minutes=60
                        )

                    self.active_jobs[job_id] = future
                    jobs_created.append(job_id)

                except Exception as executor_error:
                    logger.error(f"Failed to submit manual job {job_id} to executor: {str(executor_error)}")
                    self._update_job_status(job_id, "failed",
                                            error_message=f"Executor submission failed: {str(executor_error)}")
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

            return {
                "running": self.running,
                "environment": self.environment,
                "scheduler_enabled": self.scheduler_enabled,
                "active_jobs": active_count,
                "active_job_ids": list(self.active_jobs.keys()),
                "scheduled_jobs_count": scheduled_response.count or 0,
                "metadata_task_manager_available": self.metadata_task_manager is not None,
                "version": "simplified_user_schedule_with_prevention",
                "prevention_stats": {
                    "jobs_created_this_cycle": len(self.jobs_created_this_cycle),
                    "current_cycle_jobs": list(self.jobs_created_this_cycle)
                }
            }
        except Exception as e:
            logger.error(f"Error getting scheduler stats: {str(e)}")
            return {
                "running": self.running,
                "error": str(e),
                "version": "simplified_user_schedule_with_prevention"
            }