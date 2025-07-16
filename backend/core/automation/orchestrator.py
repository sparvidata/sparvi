"""
Clean, maintainable automation orchestrator that replaces the complex SimplifiedAutomationScheduler.
Follows SOLID principles with clear separation of concerns.
"""

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import os

from core.storage.supabase_manager import SupabaseManager
from .events import AutomationEventType, publish_automation_event
from .schedule_manager import ScheduleManager
from .task_executor import TaskExecutor
from .task_status_tracker import TaskStatusTracker

logger = logging.getLogger(__name__)


class AutomationOrchestrator:
    """
    Clean automation orchestrator with proper separation of concerns.
    Replaces SimplifiedAutomationScheduler with maintainable architecture.
    """

    def __init__(self, max_workers: int = 3):
        """Initialize the orchestrator"""
        self.supabase = SupabaseManager()
        self.schedule_manager = ScheduleManager(self.supabase)
        self.task_executor = TaskExecutor(self.supabase)
        self.status_tracker = TaskStatusTracker(self.supabase)

        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.scheduler_thread = None

        # Environment configuration
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.scheduler_enabled = self._should_enable_scheduler()

        logger.info(
            f"AutomationOrchestrator initialized - Environment: {self.environment}, Enabled: {self.scheduler_enabled}")

    def _should_enable_scheduler(self) -> bool:
        """Determine if scheduler should be enabled based on environment"""
        if os.getenv("DISABLE_AUTOMATION", "false").lower() == "true":
            return False

        if self.environment == "development":
            return os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"

        return True  # Enable in production by default

    def start(self):
        """Start the automation orchestrator"""
        if not self.scheduler_enabled:
            logger.info(f"Automation orchestrator disabled for environment: {self.environment}")
            return

        if self.running:
            logger.warning("Automation orchestrator already running")
            return

        try:
            self.running = True
            logger.info("Starting automation orchestrator...")

            # Start scheduler thread
            self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self.scheduler_thread.start()

            logger.info("✓ Automation orchestrator started successfully")
        except Exception as e:
            logger.error(f"Error starting orchestrator: {str(e)}")
            self.running = False
            raise

    def stop(self):
        """Stop the automation orchestrator"""
        self.running = False

        # Shutdown executor
        self.executor.shutdown(wait=True)

        # Wait for scheduler thread
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)

        logger.info("Automation orchestrator stopped")

    def _scheduler_loop(self):
        """Main scheduler loop - clean and simple"""
        logger.info("Automation scheduler loop started")

        while self.running:
            try:
                # Check for due jobs every minute
                self._process_due_jobs()

                # Clean up old jobs every 10 minutes
                if int(time.time()) % 600 == 0:
                    self._cleanup_old_jobs()

                # Sleep for 1 minute
                time.sleep(60)

            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(60)

    def _process_due_jobs(self):
        """Process jobs that are due to run"""
        try:
            # Get due jobs from schedule manager
            due_jobs = self.schedule_manager.get_due_jobs(buffer_minutes=2)

            if not due_jobs:
                return

            logger.info(f"🔍 Processing {len(due_jobs)} due jobs")

            # Filter out duplicates and recent jobs
            executable_jobs = self._filter_executable_jobs(due_jobs)

            if len(executable_jobs) < len(due_jobs):
                logger.info(f"🚫 Filtered out {len(due_jobs) - len(executable_jobs)} duplicate/recent jobs")

            # Execute each job
            for i, scheduled_job in enumerate(executable_jobs):
                try:
                    self._execute_scheduled_job(scheduled_job, i + 1, len(executable_jobs))
                except Exception as job_error:
                    logger.error(f"Error executing job {i + 1}: {str(job_error)}")

            logger.info(f"📊 Completed processing {len(executable_jobs)} jobs")

        except Exception as e:
            logger.error(f"Error processing due jobs: {str(e)}")

    def _filter_executable_jobs(self, due_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out jobs that shouldn't run (duplicates, recent jobs, etc.)"""
        executable_jobs = []
        seen_job_types = set()

        for job in due_jobs:
            connection_id = job["connection_id"]
            automation_type = job["automation_type"]
            job_key = (connection_id, automation_type)

            # Skip if we've already seen this job type for this connection
            if job_key in seen_job_types:
                logger.debug(f"Skipping duplicate {automation_type} for connection {connection_id}")
                continue

            # Skip if there's already a running job
            if self.status_tracker.is_job_running(connection_id, automation_type):
                logger.debug(f"Skipping {automation_type} - already running for connection {connection_id}")
                continue

            # Skip if there's a recent job (last 5 minutes)
            if self.status_tracker.has_recent_job(connection_id, automation_type, minutes=5):
                logger.debug(f"Skipping {automation_type} - recent job exists for connection {connection_id}")
                continue

            executable_jobs.append(job)
            seen_job_types.add(job_key)

        return executable_jobs

    def _execute_scheduled_job(self, scheduled_job: Dict[str, Any], job_num: int, total_jobs: int):
        """Execute a single scheduled job"""
        connection_id = scheduled_job["connection_id"]
        automation_type = scheduled_job["automation_type"]
        scheduled_job_id = scheduled_job["id"]

        logger.info(f"🚀 [{job_num}/{total_jobs}] Executing {automation_type} for connection {connection_id}")

        # Create automation job record
        job_id = self._create_job_record(connection_id, automation_type, scheduled_job_id)
        if not job_id:
            logger.error(f"❌ [{job_num}/{total_jobs}] Failed to create job record")
            return

        # Submit to executor
        try:
            future = self.executor.submit(self._run_automation_task, job_id, connection_id, automation_type)
            logger.info(f"✅ [{job_num}/{total_jobs}] Job {job_id} submitted successfully")

            # Mark scheduled job as executed
            self.schedule_manager.mark_job_executed(scheduled_job_id)

        except Exception as e:
            logger.error(f"❌ [{job_num}/{total_jobs}] Failed to submit job: {str(e)}")
            self.status_tracker.mark_job_failed(job_id, str(e))

    def _create_job_record(self, connection_id: str, automation_type: str, scheduled_job_id: str) -> Optional[str]:
        """Create automation job record in database"""
        try:
            job_id = str(uuid.uuid4())

            job_data = {
                "id": job_id,
                "connection_id": connection_id,
                "job_type": automation_type,
                "status": "scheduled",
                "scheduled_at": datetime.now(timezone.utc).isoformat(),
                "job_config": {
                    "trigger": "user_schedule",
                    "scheduled_job_id": scheduled_job_id
                }
            }

            response = self.supabase.supabase.table("automation_jobs").insert(job_data).execute()

            if response.data:
                return job_id
            else:
                logger.error("Failed to create job record - no data returned")
                return None

        except Exception as e:
            logger.error(f"Error creating job record: {str(e)}")
            return None

    def _run_automation_task(self, job_id: str, connection_id: str, automation_type: str):
        """Run an automation task with proper error handling"""
        try:
            logger.info(f"🏃 Starting {automation_type} job {job_id}")

            # Update job status to running
            self.status_tracker.mark_job_running(job_id)

            # Execute the task based on type
            if automation_type == "metadata_refresh":
                success = self.task_executor.execute_metadata_refresh(job_id, connection_id)
            elif automation_type == "schema_change_detection":
                success = self.task_executor.execute_schema_detection(job_id, connection_id)
            elif automation_type == "validation_automation":
                success = self.task_executor.execute_validation_run(job_id, connection_id)
            else:
                raise Exception(f"Unknown automation type: {automation_type}")

            # Mark job as completed or failed
            if success:
                self.status_tracker.mark_job_completed(job_id)
                logger.info(f"✅ Job {job_id} ({automation_type}) completed successfully")
            else:
                self.status_tracker.mark_job_failed(job_id, "Task execution returned failure")
                logger.error(f"❌ Job {job_id} ({automation_type}) failed")

        except Exception as e:
            logger.error(f"❌ Job {job_id} ({automation_type}) failed with exception: {str(e)}")
            self.status_tracker.mark_job_failed(job_id, str(e))

    def _cleanup_old_jobs(self):
        """Clean up old completed/failed jobs"""
        try:
            from datetime import timedelta
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .delete() \
                .in_("status", ["completed", "failed", "cancelled"]) \
                .lt("created_at", cutoff_date) \
                .execute()

            deleted_count = len(response.data) if response.data else 0
            if deleted_count > 0:
                logger.info(f"🧹 Cleaned up {deleted_count} old automation jobs")

        except Exception as e:
            logger.error(f"Error cleaning up old jobs: {str(e)}")

    # Public methods for external control

    def schedule_immediate_run(self, connection_id: str, automation_type: str = None, trigger_user: str = None) -> Dict[
        str, Any]:
        """Schedule an immediate automation run"""
        try:
            jobs_created = []
            prevented_duplicates = []

            # Determine automation types to run
            automation_types = []
            if automation_type == "metadata_refresh" or automation_type is None:
                automation_types.append("metadata_refresh")
            if automation_type == "schema_change_detection" or automation_type is None:
                automation_types.append("schema_change_detection")
            if automation_type == "validation_automation" or automation_type is None:
                automation_types.append("validation_automation")

            for auto_type in automation_types:
                # Check for duplicates
                if self.status_tracker.is_job_running(connection_id, auto_type):
                    prevented_duplicates.append(auto_type)
                    continue

                if self.status_tracker.has_recent_job(connection_id, auto_type, minutes=2):
                    prevented_duplicates.append(auto_type)
                    continue

                # Create and execute job
                job_id = self._create_job_record(connection_id, auto_type, None)
                if job_id:
                    try:
                        self.executor.submit(self._run_automation_task, job_id, connection_id, auto_type)
                        jobs_created.append(job_id)
                    except Exception as e:
                        logger.error(f"Failed to submit immediate job: {str(e)}")
                        self.status_tracker.mark_job_failed(job_id, str(e))

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
            # Get scheduled jobs count
            scheduled_response = self.supabase.supabase.table("automation_scheduled_jobs") \
                .select("id", count="exact") \
                .eq("enabled", True) \
                .execute()

            return {
                "running": self.running,
                "environment": self.environment,
                "scheduler_enabled": self.scheduler_enabled,
                "scheduled_jobs_count": scheduled_response.count or 0,
                "version": "clean_orchestrator_v1"
            }
        except Exception as e:
            logger.error(f"Error getting scheduler stats: {str(e)}")
            return {
                "running": self.running,
                "error": str(e),
                "version": "clean_orchestrator_v1"
            }