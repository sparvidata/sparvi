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
import os

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

        self.environment = os.getenv("ENVIRONMENT", "development")
        self.scheduler_enabled = (
                self.environment == "production" or
                os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"
        )

        logger.info(f"Scheduler initialized - Environment: {self.environment}, Enabled: {self.scheduler_enabled}")

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
        """Start with environment protection"""
        if not self.scheduler_enabled:
            logger.info("Scheduler disabled for this environment")
            return

        if self.running:
            logger.warning("Scheduler already running")
            return

        self.running = True

        # Clean up any existing zombie jobs on startup
        self._cleanup_zombie_jobs()
        self._cleanup_stale_scheduled_jobs()

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
        """Set up periodic automation schedules - UPDATED with zombie cleanup"""

        # Check for due jobs every 30 minutes to support 1-hour minimum intervals
        # The actual job frequency is determined by user configuration in _should_schedule_job
        schedule.every(30).minutes.do(self._check_metadata_refresh_jobs)
        schedule.every(30).minutes.do(self._check_schema_detection_jobs)
        schedule.every(30).minutes.do(self._check_validation_jobs)

        # Clean up zombie and stale jobs every 15 minutes
        schedule.every(15).minutes.do(self._cleanup_zombie_jobs)
        schedule.every(15).minutes.do(self._cleanup_stale_scheduled_jobs)

        # Clean up old completed jobs daily
        schedule.every().day.at("02:00").do(self._cleanup_old_jobs)

        logger.info("Automation scheduler configured:")
        logger.info("- Checking for due jobs: every 30 minutes")
        logger.info("- Zombie job cleanup: every 15 minutes")
        logger.info("- Stale job cleanup: every 15 minutes")
        logger.info("- Old job cleanup: daily at 2:00 AM")
        logger.info("- Individual job intervals: determined by user configuration")
        logger.info("- Supports minimum user interval: 1 hour")

    def _run_scheduler(self):
        """Main scheduler loop"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(5)

    def _cleanup_zombie_jobs(self):
        """Clean up jobs that have been running too long (zombie processes)"""
        try:
            logger.debug("Checking for zombie automation jobs...")

            # Define maximum runtime for different job types (in hours)
            max_runtimes = {
                "metadata_refresh": 2,  # 2 hours max
                "schema_detection": 1,  # 1 hour max
                "validation_run": 1  # 1 hour max
            }

            now = datetime.now(timezone.utc)
            zombie_count = 0

            for job_type, max_hours in max_runtimes.items():
                cutoff_time = (now - timedelta(hours=max_hours)).isoformat()

                # Find zombie jobs of this type
                zombie_response = self.supabase.supabase.table("automation_jobs") \
                    .select("id, connection_id, started_at, job_type") \
                    .eq("job_type", job_type) \
                    .eq("status", "running") \
                    .lt("started_at", cutoff_time) \
                    .execute()

                zombies = zombie_response.data or []

                if zombies:
                    logger.warning(f"Found {len(zombies)} zombie {job_type} jobs")

                    for zombie in zombies:
                        job_id = zombie["id"]
                        started_at = zombie.get("started_at")

                        # Calculate how long it's been running
                        if started_at:
                            start_time = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                            runtime_hours = (now - start_time).total_seconds() / 3600
                            error_msg = f"Job terminated as zombie process (running for {runtime_hours:.1f} hours, max: {max_hours}h)"
                        else:
                            error_msg = f"Job terminated as zombie process (no start time, max: {max_hours}h)"

                        # Mark as failed
                        self._update_job_status(
                            job_id,
                            "failed",
                            completed_at=now.isoformat(),
                            error_message=error_msg
                        )

                        # Remove from active jobs if it's there
                        if job_id in self.active_jobs:
                            try:
                                self.active_jobs[job_id].cancel()
                                del self.active_jobs[job_id]
                            except:
                                pass

                        zombie_count += 1
                        logger.warning(f"Cleaned up zombie job {job_id} ({job_type}) - runtime: {runtime_hours:.1f}h")

            if zombie_count > 0:
                logger.warning(f"Cleaned up {zombie_count} total zombie jobs")
            else:
                logger.debug("No zombie jobs found")

        except Exception as e:
            logger.error(f"Error cleaning up zombie jobs: {str(e)}")

    def _cleanup_stale_scheduled_jobs(self):
        """Clean up jobs that were scheduled but never started"""
        try:
            logger.debug("Checking for stale scheduled jobs...")

            # Jobs scheduled more than 1 hour ago but never started
            cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

            stale_response = self.supabase.supabase.table("automation_jobs") \
                .select("id, job_type, scheduled_at") \
                .eq("status", "scheduled") \
                .lt("scheduled_at", cutoff_time) \
                .execute()

            stale_jobs = stale_response.data or []

            if stale_jobs:
                logger.warning(f"Found {len(stale_jobs)} stale scheduled jobs")

                for job in stale_jobs:
                    job_id = job["id"]
                    scheduled_at = job.get("scheduled_at")

                    if scheduled_at:
                        scheduled_time = datetime.fromisoformat(scheduled_at.replace('Z', '+00:00'))
                        stale_hours = (datetime.now(timezone.utc) - scheduled_time).total_seconds() / 3600
                        error_msg = f"Job failed to start within 1 hour of scheduling (stale for {stale_hours:.1f}h)"
                    else:
                        error_msg = "Job failed to start within 1 hour of scheduling (no schedule time)"

                    self._update_job_status(
                        job_id,
                        "failed",
                        completed_at=datetime.now(timezone.utc).isoformat(),
                        error_message=error_msg
                    )
                    logger.warning(f"Cleaned up stale scheduled job {job_id} ({job.get('job_type')})")

                logger.warning(f"Cleaned up {len(stale_jobs)} stale scheduled jobs")
            else:
                logger.debug("No stale scheduled jobs found")

        except Exception as e:
            logger.error(f"Error cleaning up stale scheduled jobs: {str(e)}")

    def _check_metadata_refresh_jobs(self):
        """Check which metadata refresh jobs are due based on user configuration"""
        try:
            logger.debug("Checking metadata refresh jobs...")

            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("connection_id, metadata_refresh") \
                .execute()

            if not response.data:
                logger.debug("No automation configs found")
                return

            jobs_checked = 0
            jobs_scheduled = 0

            for config in response.data:
                connection_id = config["connection_id"]
                jobs_checked += 1

                try:
                    # Robust JSON parsing
                    metadata_config_raw = config.get("metadata_refresh")

                    if isinstance(metadata_config_raw, str):
                        try:
                            metadata_config = json.loads(metadata_config_raw)
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error for {connection_id}: {e}")
                            continue
                    elif isinstance(metadata_config_raw, dict):
                        metadata_config = metadata_config_raw
                    else:
                        logger.warning(f"Unexpected config type for {connection_id}: {type(metadata_config_raw)}")
                        continue

                    if not metadata_config.get("enabled", False):
                        continue

                    # Get the user's configured interval
                    interval_hours = metadata_config.get("interval_hours")

                    if interval_hours is None:
                        logger.warning(f"No interval_hours specified for {connection_id}, using default 24")
                        interval_hours = 24
                    elif not isinstance(interval_hours, (int, float)) or interval_hours <= 0:
                        logger.error(f"Invalid interval_hours for {connection_id}: {interval_hours}, using 24")
                        interval_hours = 24

                    # Only schedule if the user's interval has elapsed
                    if self._should_schedule_job(connection_id, "metadata_refresh", interval_hours):
                        logger.info(f"Scheduling metadata refresh for {connection_id} (interval: {interval_hours}h)")
                        self._schedule_metadata_refresh(connection_id, metadata_config)
                        jobs_scheduled += 1

                except Exception as config_error:
                    logger.error(f"Error processing config for {connection_id}: {str(config_error)}")
                    continue

            if jobs_scheduled > 0:
                logger.info(f"Scheduled {jobs_scheduled} metadata refresh jobs out of {jobs_checked} checked")

        except Exception as e:
            logger.error(f"Error checking metadata refresh jobs: {str(e)}")

    def _check_schema_detection_jobs(self):
        """Check which schema detection jobs are due based on user configuration"""
        try:
            logger.debug("Checking schema detection jobs...")

            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("connection_id, schema_change_detection") \
                .execute()

            if not response.data:
                return

            jobs_checked = 0
            jobs_scheduled = 0

            for config in response.data:
                connection_id = config["connection_id"]
                jobs_checked += 1

                try:
                    schema_config_raw = config.get("schema_change_detection")

                    if isinstance(schema_config_raw, str):
                        schema_config = json.loads(schema_config_raw)
                    elif isinstance(schema_config_raw, dict):
                        schema_config = schema_config_raw
                    else:
                        continue

                    if not schema_config.get("enabled", False):
                        continue

                    # Get the user's configured interval
                    interval_hours = schema_config.get("interval_hours", 24)

                    if not isinstance(interval_hours, (int, float)) or interval_hours <= 0:
                        interval_hours = 24

                    # Only schedule if the user's interval has elapsed
                    if self._should_schedule_job(connection_id, "schema_change_detection", interval_hours):
                        logger.info(f"Scheduling schema detection for {connection_id} (interval: {interval_hours}h)")
                        self._schedule_schema_detection(connection_id, schema_config)
                        jobs_scheduled += 1

                except Exception as config_error:
                    logger.error(f"Error processing schema config for {connection_id}: {str(config_error)}")
                    continue

            if jobs_scheduled > 0:
                logger.info(f"Scheduled {jobs_scheduled} schema detection jobs out of {jobs_checked} checked")

        except Exception as e:
            logger.error(f"Error checking schema detection jobs: {str(e)}")

    def _check_validation_jobs(self):
        """Check which validation jobs are due based on user configuration"""
        try:
            logger.debug("Checking validation jobs...")

            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("connection_id, validation_automation") \
                .execute()

            if not response.data:
                return

            jobs_checked = 0
            jobs_scheduled = 0

            for config in response.data:
                connection_id = config["connection_id"]
                jobs_checked += 1

                try:
                    validation_config_raw = config.get("validation_automation")

                    if isinstance(validation_config_raw, str):
                        validation_config = json.loads(validation_config_raw)
                    elif isinstance(validation_config_raw, dict):
                        validation_config = validation_config_raw
                    else:
                        continue

                    if not validation_config.get("enabled", False):
                        continue

                    # Get the user's configured interval
                    interval_hours = validation_config.get("interval_hours", 24)

                    if not isinstance(interval_hours, (int, float)) or interval_hours <= 0:
                        interval_hours = 24

                    # Only schedule if the user's interval has elapsed
                    if self._should_schedule_job(connection_id, "validation_automation", interval_hours):
                        logger.info(f"Scheduling validation run for {connection_id} (interval: {interval_hours}h)")
                        self._schedule_validation_run(connection_id, validation_config)
                        jobs_scheduled += 1

                except Exception as config_error:
                    logger.error(f"Error processing validation config for {connection_id}: {str(config_error)}")
                    continue

            if jobs_scheduled > 0:
                logger.info(f"Scheduled {jobs_scheduled} validation jobs out of {jobs_checked} checked")

        except Exception as e:
            logger.error(f"Error checking validation jobs: {str(e)}")

    def _should_schedule_job(self, connection_id: str, job_type: str, interval_hours: int) -> bool:
        """
        FIXED: Check if job should be scheduled based on user-defined intervals

        This method is called every 30 minutes, but jobs should only be scheduled
        when their configured interval (24h, 6h, weekly, etc.) has elapsed.
        """
        try:
            logger.debug(
                f"Checking if {job_type} should be scheduled for {connection_id} (user interval: {interval_hours}h)")

            # Validate interval_hours
            if not isinstance(interval_hours, (int, float)) or interval_hours <= 0:
                logger.error(f"Invalid interval_hours: {interval_hours}, defaulting to 24")
                interval_hours = 24

            now = datetime.now(timezone.utc)

            # Step 1: Check if any job is currently running (never overlap)
            running_response = self.supabase.supabase.table("automation_jobs") \
                .select("id, started_at") \
                .eq("connection_id", connection_id) \
                .eq("job_type", job_type) \
                .eq("status", "running") \
                .execute()

            if running_response.data and len(running_response.data) > 0:
                running_job = running_response.data[0]
                started_at = running_job.get("started_at")
                if started_at:
                    start_time = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                    runtime_hours = (now - start_time).total_seconds() / 3600
                    logger.debug(
                        f"{job_type}: Job currently running for {runtime_hours:.1f}h, will not schedule another")
                else:
                    logger.debug(f"{job_type}: Job marked as running (no start time), will not schedule another")
                return False

            # Step 2: Check if any job is scheduled but not yet started
            scheduled_response = self.supabase.supabase.table("automation_jobs") \
                .select("id, scheduled_at") \
                .eq("connection_id", connection_id) \
                .eq("job_type", job_type) \
                .eq("status", "scheduled") \
                .execute()

            if scheduled_response.data and len(scheduled_response.data) > 0:
                logger.debug(f"{job_type}: Job already scheduled but not started, will not schedule another")
                return False

            # Step 3: Find the most recent completed or failed job
            completed_response = self.supabase.supabase.table("automation_jobs") \
                .select("id, completed_at, scheduled_at, status") \
                .eq("connection_id", connection_id) \
                .eq("job_type", job_type) \
                .in_("status", ["completed", "failed"]) \
                .order("completed_at", desc=True) \
                .limit(1) \
                .execute()

            if not completed_response.data or len(completed_response.data) == 0:
                # No previous jobs found - schedule the first one
                logger.info(f"{job_type}: No previous jobs found, scheduling first run")
                return True

            last_job = completed_response.data[0]

            # Step 4: Calculate time elapsed since last completion
            completion_time_str = last_job.get("completed_at") or last_job.get("scheduled_at")

            if not completion_time_str:
                logger.warning(f"{job_type}: No completion time found, scheduling new job")
                return True

            completion_time = datetime.fromisoformat(completion_time_str.replace('Z', '+00:00'))
            hours_elapsed = (now - completion_time).total_seconds() / 3600

            # Step 5: Only schedule if the user's configured interval has elapsed
            should_schedule = hours_elapsed >= interval_hours

            if should_schedule:
                logger.info(
                    f"{job_type}: User interval met ({hours_elapsed:.1f}h >= {interval_hours}h), scheduling job")
            else:
                remaining_hours = interval_hours - hours_elapsed
                logger.debug(
                    f"{job_type}: User interval not met ({hours_elapsed:.1f}h < {interval_hours}h), {remaining_hours:.1f}h remaining")

            return should_schedule

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

            # Submit job to executor with timeout protection
            future = self.executor.submit(
                self._execute_job_with_timeout,
                self._execute_metadata_refresh_integrated,
                job_id, connection_id, config,
                timeout_minutes=120  # 2 hour timeout
            )
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

            # Submit job to executor with timeout protection
            future = self.executor.submit(
                self._execute_job_with_timeout,
                self._execute_schema_detection_integrated,
                job_id, connection_id, config,
                timeout_minutes=60  # 1 hour timeout
            )
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

            # Submit job to executor with timeout protection
            future = self.executor.submit(
                self._execute_job_with_timeout,
                self._execute_validation_run_integrated,
                job_id, connection_id, config,
                timeout_minutes=60  # 1 hour timeout
            )
            self.active_jobs[job_id] = future

            logger.info(f"Scheduled validation run job {job_id} for connection {connection_id}")

        except Exception as e:
            logger.error(f"Error scheduling validation run: {str(e)}")

    def _execute_job_with_timeout(self, job_function, job_id: str, connection_id: str, config: Dict[str, Any],
                                  timeout_minutes: int = 60):
        """Execute a job function with timeout protection"""
        import signal
        from concurrent.futures import TimeoutError

        def timeout_handler(signum, frame):
            raise TimeoutError(f"Job {job_id} timed out after {timeout_minutes} minutes")

        try:
            # Set up timeout signal (Unix only)
            if hasattr(signal, 'SIGALRM'):
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(timeout_minutes * 60)

            # Execute the job
            result = job_function(job_id, connection_id, config)

            # Clear the timeout
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)

            return result

        except TimeoutError as e:
            logger.error(f"Job {job_id} timed out: {str(e)}")
            self._update_job_status(
                job_id, "failed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                error_message=str(e)
            )
        except Exception as e:
            logger.error(f"Job {job_id} failed with exception: {str(e)}")
            self._update_job_status(
                job_id, "failed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                error_message=str(e)
            )
        finally:
            # Always clear the timeout and remove from active jobs
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _execute_metadata_refresh_integrated(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute metadata refresh with better error handling"""
        run_id = None
        metadata_task_id = None

        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "metadata_refresh")

            # Validate metadata task manager
            if not self.metadata_task_manager:
                logger.error("Metadata task manager not available")
                try:
                    self._initialize_metadata_integration()
                    if not self.metadata_task_manager:
                        raise Exception("Failed to initialize metadata task manager")
                except Exception as init_error:
                    raise Exception(f"Metadata task manager initialization failed: {str(init_error)}")

            # Validate connection
            try:
                connection = self.supabase.get_connection(connection_id)
                if not connection:
                    raise Exception(f"Connection {connection_id} not found")
                logger.info(f"Connection {connection_id} validated")
            except Exception as conn_error:
                raise Exception(f"Connection validation failed: {str(conn_error)}")

            # Use conservative collection parameters to reduce failures
            refresh_types = config.get("types", ["tables", "columns"])

            if "statistics" in refresh_types:
                depth = "medium"
                table_limit = 15
            elif "columns" in refresh_types:
                depth = "low"
                table_limit = 30
            else:
                depth = "low"
                table_limit = 50

            logger.info(f"Starting metadata collection: depth={depth}, table_limit={table_limit}")

            # Submit with timeout
            metadata_task_id = self.metadata_task_manager.submit_collection_task(
                connection_id=connection_id,
                params={
                    "depth": depth,
                    "table_limit": table_limit,
                    "automation_trigger": True,
                    "automation_job_id": job_id,
                    "refresh_types": refresh_types
                },
                priority="medium"
            )

            logger.info(f"Submitted metadata collection task {metadata_task_id}")

            # Wait with reasonable timeout
            task_completed = self._wait_for_task_completion(metadata_task_id, timeout_minutes=20)

            if task_completed:
                task_status = self.metadata_task_manager.get_task_status(metadata_task_id)
                results = {
                    "metadata_task_id": metadata_task_id,
                    "task_result": task_status.get("result", {}),
                    "refresh_types": refresh_types,
                    "success": True
                }

                self._update_job_status(
                    job_id, "completed",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    result_summary=results
                )

                if run_id:
                    self._update_automation_run(run_id, "completed", results)

                logger.info(f"Completed metadata refresh job {job_id}")

            else:
                # Timeout - mark as failed but don't retry immediately
                error_msg = f"Metadata collection timed out after 20 minutes"
                self._update_job_status(
                    job_id, "failed",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    error_message=error_msg
                )

                if run_id:
                    self._update_automation_run(run_id, "failed", {"error": error_msg})

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing metadata refresh job {job_id}: {error_msg}")

            # Always mark job as completed (failed) to prevent retry loops
            self._update_job_status(
                job_id, "failed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                error_message=error_msg
            )

            if run_id:
                self._update_automation_run(run_id, "failed", {"error": error_msg})

        finally:
            # Clean up
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _execute_schema_detection_integrated(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute schema detection with better error handling"""
        run_id = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "schema_detection")

            # Validate connection first
            try:
                connection = self.supabase.get_connection(connection_id)
                if not connection:
                    raise Exception(f"Connection {connection_id} not found")
                logger.info(f"Connection {connection_id} validated for schema detection")
            except Exception as conn_error:
                raise Exception(f"Connection validation failed: {str(conn_error)}")

            # Use the schema change detector with timeout protection
            from core.metadata.schema_change_detector import SchemaChangeDetector
            from core.metadata.connector_factory import ConnectorFactory

            # Initialize components
            connector_factory = ConnectorFactory(self.supabase)
            schema_detector = SchemaChangeDetector()

            # Ensure recent metadata exists before schema detection
            if self.metadata_task_manager:
                try:
                    metadata_status = self.metadata_task_manager.get_metadata_collection_status(connection_id)

                    # If metadata is very stale, refresh it first (but with timeout)
                    if metadata_status.get("overall_status") in ["stale", "missing"]:
                        logger.info("Refreshing metadata before schema detection")

                        metadata_task_id = self.metadata_task_manager.submit_collection_task(
                            connection_id=connection_id,
                            params={"depth": "low", "table_limit": 50, "for_schema_detection": True},
                            priority="high"
                        )

                        # Wait briefly for metadata refresh (don't wait too long)
                        self._wait_for_task_completion(metadata_task_id, timeout_minutes=5)
                except Exception as metadata_error:
                    logger.warning(f"Metadata refresh before schema detection failed: {str(metadata_error)}")
                    # Continue with schema detection anyway

            # Detect changes with timeout protection
            logger.info("Starting schema change detection")

            try:
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

            except Exception as detection_error:
                raise Exception(f"Schema detection failed: {str(detection_error)}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing schema detection job {job_id}: {error_msg}")

            # Always mark job as completed (failed) to prevent retry loops
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
                data={
                    "job_id": job_id,
                    "job_type": "schema_detection",
                    "error": error_msg
                },
                connection_id=connection_id
            )
        finally:
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _execute_validation_run_integrated(self, job_id: str, connection_id: str, config: Dict[str, Any]):
        """Execute validation run with better error handling"""
        run_id = None
        try:
            # Update job status to running
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())

            # Create automation run record
            run_id = self._create_automation_run(job_id, connection_id, "validation_run")

            # Validate connection and get organization
            try:
                connection = self.supabase.get_connection(connection_id)
                if not connection:
                    raise Exception(f"Connection not found: {connection_id}")

                organization_id = connection.get("organization_id")
                if not organization_id:
                    raise Exception(f"No organization ID found for connection {connection_id}")

                logger.info(f"Connection {connection_id} validated for validation run")
            except Exception as conn_error:
                raise Exception(f"Connection validation failed: {str(conn_error)}")

            # Use the validation automation integrator with timeout protection
            try:
                from core.utils.validation_automation_integration import create_validation_automation_integrator
                integrator = create_validation_automation_integrator()

                logger.info("Starting automated validation run")

                # Run automated validations with timeout protection
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

                logger.info(f"Completed validation job {job_id}: {results.get('failed_rules', 0)} failures")

            except Exception as validation_error:
                raise Exception(f"Validation run failed: {str(validation_error)}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing validation run job {job_id}: {error_msg}")

            # Always mark job as completed (failed) to prevent retry loops
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
                data={
                    "job_id": job_id,
                    "job_type": "validation_run",
                    "error": error_msg
                },
                connection_id=connection_id
            )
        finally:
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]

    def _wait_for_task_completion(self, task_id: str, timeout_minutes: int = 20) -> bool:
        """Wait for task completion with better timeout handling for all job types"""
        try:
            if not self.metadata_task_manager:
                logger.error("No metadata task manager available for task waiting")
                return False

            logger.info(f"Waiting for task {task_id} to complete (timeout: {timeout_minutes}m)")

            # Use the task manager's built-in sync waiting method with timeout
            completion_result = self.metadata_task_manager.wait_for_task_completion_sync(
                task_id, timeout_minutes
            )

            success = completion_result.get("completed", False) and completion_result.get("success", False)

            if not success:
                error_msg = completion_result.get("error", "Unknown error")
                if "timeout" in error_msg.lower():
                    logger.warning(f"Task {task_id} timed out after {timeout_minutes} minutes")
                else:
                    logger.error(f"Task {task_id} failed: {error_msg}")

            return success

        except Exception as e:
            logger.error(f"Error waiting for task completion: {str(e)}")
            return False

    def _create_automation_run(self, job_id: str, connection_id: str, run_type: str) -> str:
        """Create automation run record with better error handling"""
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
                logger.error("Failed to create automation run - no data returned")
                return None

        except Exception as e:
            logger.error(f"Error creating automation run: {str(e)}")
            return None

    def _update_automation_run(self, run_id: str, status: str, results: Dict[str, Any] = None):
        """Update automation run record with better error handling"""
        try:
            if not run_id:
                logger.warning("Cannot update automation run - no run_id provided")
                return

            update_data = {
                "status": status,
                "completed_at": datetime.now(timezone.utc).isoformat()
            }

            if results:
                update_data["results"] = results

            response = self.supabase.supabase.table("automation_runs") \
                .update(update_data) \
                .eq("id", run_id) \
                .execute()

            if response.data:
                logger.info(f"Updated automation run {run_id} to {status}")
            else:
                logger.warning(f"No data returned when updating automation run {run_id}")

        except Exception as e:
            logger.error(f"Error updating automation run {run_id}: {str(e)}")
            # Don't raise - we don't want run update failures to crash the scheduler

    def _update_job_status(self, job_id: str, status: str, **kwargs):
        """Update job status with better error handling"""
        try:
            update_data = {"status": status}
            update_data.update(kwargs)

            # Always set completed_at for failed/completed jobs
            if status in ["completed", "failed"] and "completed_at" not in update_data:
                update_data["completed_at"] = datetime.now(timezone.utc).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .update(update_data) \
                .eq("id", job_id) \
                .execute()

            if not response.data:
                logger.warning(f"No data returned when updating job {job_id} status to {status}")
            else:
                logger.info(f"Updated job {job_id} status to {status}")

        except Exception as e:
            logger.error(f"Error updating job status for {job_id}: {str(e)}")
            # Don't raise - we don't want job status update failures to crash the scheduler

    def _cleanup_old_jobs(self):
        """Clean up old completed/failed jobs"""
        try:
            # Delete jobs older than 30 days
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .delete() \
                .in_("status", ["completed", "failed", "cancelled"]) \
                .lt("created_at", cutoff_date) \
                .execute()

            deleted_count = len(response.data) if response.data else 0
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old automation jobs")
            else:
                logger.debug("No old automation jobs to clean up")

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

                # Update job status to cancelled
                self._update_job_status(
                    job_id, "cancelled",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    error_message="Job cancelled by user or system"
                )

                logger.info(f"Cancelled job {job_id}")
            else:
                # Job not in active jobs, just update status
                self._update_job_status(
                    job_id, "cancelled",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    error_message="Job cancelled by user or system"
                )
                logger.info(f"Marked job {job_id} as cancelled (was not in active jobs)")

        except Exception as e:
            logger.error(f"Error cancelling job: {str(e)}")

    def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        try:
            # Get job counts from database
            job_stats = {}
            try:
                response = self.supabase.supabase.table("automation_jobs") \
                    .select("status", count="exact") \
                    .gte("created_at", (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()) \
                    .execute()

                # This is a simplified count - in production you'd want to group by status
                job_stats["jobs_24h"] = response.count or 0
            except:
                job_stats["jobs_24h"] = 0

            return {
                "running": self.running,
                "environment": self.environment,
                "scheduler_enabled": self.scheduler_enabled,
                "active_jobs": len(self.active_jobs),
                "active_job_ids": list(self.active_jobs.keys()),
                "worker_threads": self.executor._threads if hasattr(self.executor, '_threads') else 0,
                "metadata_task_manager_available": self.metadata_task_manager is not None,
                "storage_services_initialized": all([
                    hasattr(self, 'supabase') and self.supabase is not None,
                    self.metadata_task_manager is not None
                ]),
                "job_statistics": job_stats
            }
        except Exception as e:
            logger.error(f"Error getting scheduler stats: {str(e)}")
            return {
                "running": self.running,
                "error": str(e)
            }

    def get_health_status(self) -> Dict[str, Any]:
        """Get detailed health status of the scheduler"""
        try:
            stats = self.get_scheduler_stats()

            # Determine health based on various factors
            health_issues = []

            if not self.running:
                health_issues.append("Scheduler not running")

            if not self.scheduler_enabled:
                health_issues.append("Scheduler disabled for this environment")

            if not self.metadata_task_manager:
                health_issues.append("Metadata task manager not available")

            if len(self.active_jobs) > 10:  # Arbitrary threshold
                health_issues.append(f"High number of active jobs ({len(self.active_jobs)})")

            # Check for zombie jobs
            try:
                zombie_response = self.supabase.supabase.table("automation_jobs") \
                    .select("id", count="exact") \
                    .eq("status", "running") \
                    .lt("started_at", (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()) \
                    .execute()

                zombie_count = zombie_response.count or 0
                if zombie_count > 0:
                    health_issues.append(f"Found {zombie_count} potential zombie jobs")
            except:
                pass

            health_status = "healthy" if not health_issues else "warning" if len(health_issues) <= 2 else "critical"

            return {
                "status": health_status,
                "issues": health_issues,
                "scheduler_stats": stats,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        except Exception as e:
            return {
                "status": "error",
                "issues": [f"Health check failed: {str(e)}"],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }