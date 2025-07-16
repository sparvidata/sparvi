"""
Clean task status tracker that handles job status management.
Single responsibility: track job status and prevent duplicates.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


class TaskStatusTracker:
    """
    Clean task status tracker with single responsibility: manage job status.
    Provides simple, reliable methods for status tracking.
    """

    def __init__(self, supabase_manager: SupabaseManager):
        """Initialize status tracker"""
        self.supabase = supabase_manager

    def is_job_running(self, connection_id: str, automation_type: str) -> bool:
        """
        Check if a job of this type is currently running.

        Args:
            connection_id: Database connection ID
            automation_type: Type of automation job

        Returns:
            True if job is running, False otherwise
        """
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

    def has_recent_job(self, connection_id: str, automation_type: str, minutes: int = 5) -> bool:
        """
        Check if there's a recent job of this type.

        Args:
            connection_id: Database connection ID
            automation_type: Type of automation job
            minutes: How many minutes back to check

        Returns:
            True if recent job exists, False otherwise
        """
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

    def mark_job_running(self, job_id: str):
        """
        Mark a job as running.

        Args:
            job_id: Automation job ID
        """
        try:
            self._update_job_status(job_id, "running", started_at=datetime.now(timezone.utc).isoformat())
            logger.debug(f"Marked job {job_id} as running")

        except Exception as e:
            logger.error(f"Error marking job {job_id} as running: {str(e)}")

    def mark_job_completed(self, job_id: str, result_summary: dict = None):
        """
        Mark a job as completed.

        Args:
            job_id: Automation job ID
            result_summary: Optional result summary
        """
        try:
            update_data = {
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat()
            }

            if result_summary:
                update_data["result_summary"] = result_summary

            self._update_job_status(job_id, **update_data)
            logger.info(f"Marked job {job_id} as completed")

        except Exception as e:
            logger.error(f"Error marking job {job_id} as completed: {str(e)}")

    def mark_job_failed(self, job_id: str, error_message: str):
        """
        Mark a job as failed.

        Args:
            job_id: Automation job ID
            error_message: Error message
        """
        try:
            self._update_job_status(
                job_id,
                status="failed",
                completed_at=datetime.now(timezone.utc).isoformat(),
                error_message=error_message
            )
            logger.error(f"Marked job {job_id} as failed: {error_message}")

        except Exception as e:
            logger.error(f"Error marking job {job_id} as failed: {str(e)}")

    def get_job_status(self, job_id: str) -> Optional[dict]:
        """
        Get current status of a job.

        Args:
            job_id: Automation job ID

        Returns:
            Job status dictionary or None if not found
        """
        try:
            response = self.supabase.supabase.table("automation_jobs") \
                .select("id, status, job_type, scheduled_at, started_at, completed_at, error_message, result_summary") \
                .eq("id", job_id) \
                .execute()

            if response.data and len(response.data) > 0:
                return response.data[0]
            else:
                return None

        except Exception as e:
            logger.error(f"Error getting job status for {job_id}: {str(e)}")
            return None

    def _update_job_status(self, job_id: str, **kwargs):
        """
        Update job status in database.

        Args:
            job_id: Automation job ID
            **kwargs: Fields to update
        """
        try:
            response = self.supabase.supabase.table("automation_jobs") \
                .update(kwargs) \
                .eq("id", job_id) \
                .execute()

            if not response.data:
                logger.warning(f"No data returned when updating job {job_id}")

        except Exception as e:
            logger.error(f"Error updating job status for {job_id}: {str(e)}")

    def get_connection_job_summary(self, connection_id: str, hours: int = 24) -> dict:
        """
        Get job summary for a connection over a time period.

        Args:
            connection_id: Database connection ID
            hours: Hours back to analyze

        Returns:
            Summary dictionary with job statistics
        """
        try:
            cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .select("status, job_type, scheduled_at") \
                .eq("connection_id", connection_id) \
                .gte("scheduled_at", cutoff_time) \
                .execute()

            jobs = response.data if response.data else []

            summary = {
                "connection_id": connection_id,
                "time_period_hours": hours,
                "total_jobs": len(jobs),
                "by_status": {},
                "by_type": {},
                "last_job_time": None
            }

            # Count by status and type
            for job in jobs:
                status = job.get("status", "unknown")
                job_type = job.get("job_type", "unknown")

                summary["by_status"][status] = summary["by_status"].get(status, 0) + 1
                summary["by_type"][job_type] = summary["by_type"].get(job_type, 0) + 1

            # Find last job time
            if jobs:
                summary["last_job_time"] = max(job.get("scheduled_at", "") for job in jobs)

            return summary

        except Exception as e:
            logger.error(f"Error getting connection job summary: {str(e)}")
            return {
                "connection_id": connection_id,
                "error": str(e),
                "total_jobs": 0
            }