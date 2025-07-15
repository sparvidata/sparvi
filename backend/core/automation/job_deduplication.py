# backend/core/automation/job_deduplication.py

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, Optional
import hashlib

logger = logging.getLogger(__name__)


class JobDeduplicationService:
    """
    Service to prevent duplicate automation jobs from being created.
    Tracks active jobs and prevents duplicates based on job fingerprints.
    """

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        if JobDeduplicationService._instance is not None:
            raise Exception("JobDeduplicationService is a singleton!")

        # Track active jobs by fingerprint
        self.active_jobs: Dict[str, Dict] = {}
        self.lock = threading.Lock()

        # Cleanup old entries periodically
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

        logger.info("JobDeduplicationService initialized")

    def create_job_fingerprint(self, connection_id: str, job_type: str,
                               trigger: str = "unknown", **kwargs) -> str:
        """
        Create a unique fingerprint for a job to detect duplicates.

        Args:
            connection_id: Database connection ID
            job_type: Type of automation job
            trigger: What triggered the job
            **kwargs: Additional parameters that affect job uniqueness

        Returns:
            Unique job fingerprint string
        """
        # Create a string that uniquely identifies this job
        fingerprint_data = f"{connection_id}:{job_type}:{trigger}"

        # Add relevant kwargs
        for key in sorted(kwargs.keys()):
            fingerprint_data += f":{key}={kwargs[key]}"

        # Hash the fingerprint for consistent length
        return hashlib.md5(fingerprint_data.encode()).hexdigest()

    def is_job_duplicate(self, fingerprint: str, max_age_minutes: int = 30) -> bool:
        """
        Check if a job with this fingerprint is already active.

        Args:
            fingerprint: Job fingerprint to check
            max_age_minutes: Maximum age for considering a job "active"

        Returns:
            True if this is a duplicate job, False otherwise
        """
        with self.lock:
            if fingerprint not in self.active_jobs:
                return False

            job_info = self.active_jobs[fingerprint]

            # Check if the job is too old
            job_age = time.time() - job_info["created_at"]
            if job_age > (max_age_minutes * 60):
                # Remove old job and allow new one
                del self.active_jobs[fingerprint]
                return False

            # Check if the job is still actually running
            if self._is_job_still_active(job_info):
                logger.info(f"Duplicate job detected: {fingerprint} (age: {job_age:.1f}s)")
                return True
            else:
                # Job is no longer active, remove and allow new one
                del self.active_jobs[fingerprint]
                return False

    def register_job(self, fingerprint: str, job_id: str, connection_id: str,
                     job_type: str, trigger: str = "unknown") -> bool:
        """
        Register a new job to prevent duplicates.

        Args:
            fingerprint: Job fingerprint
            job_id: Unique job ID
            connection_id: Database connection ID
            job_type: Type of automation job
            trigger: What triggered the job

        Returns:
            True if job was registered, False if duplicate detected
        """
        with self.lock:
            # Double-check for duplicates
            if self.is_job_duplicate(fingerprint):
                return False

            # Register the job
            self.active_jobs[fingerprint] = {
                "job_id": job_id,
                "connection_id": connection_id,
                "job_type": job_type,
                "trigger": trigger,
                "created_at": time.time(),
                "status": "active"
            }

            logger.info(
                f"Registered job: {job_type} for connection {connection_id} (fingerprint: {fingerprint[:8]}...)")
            return True

    def mark_job_completed(self, fingerprint: str, status: str = "completed"):
        """
        Mark a job as completed and remove from active tracking.

        Args:
            fingerprint: Job fingerprint
            status: Final job status
        """
        with self.lock:
            if fingerprint in self.active_jobs:
                job_info = self.active_jobs[fingerprint]
                logger.info(f"Marking job completed: {job_info['job_type']} ({status})")
                del self.active_jobs[fingerprint]

    def _is_job_still_active(self, job_info: Dict) -> bool:
        """
        Check if a job is still actually running in the database.

        Args:
            job_info: Job information dictionary

        Returns:
            True if job is still active, False otherwise
        """
        try:
            from core.storage.supabase_manager import SupabaseManager

            supabase = SupabaseManager()

            # Check if job still exists and is running
            response = supabase.supabase.table("automation_jobs") \
                .select("status") \
                .eq("id", job_info["job_id"]) \
                .execute()

            if response.data and len(response.data) > 0:
                status = response.data[0]["status"]
                return status in ["scheduled", "running"]

            return False

        except Exception as e:
            logger.error(f"Error checking job status: {str(e)}")
            # Assume job is still active if we can't check
            return True

    def _cleanup_loop(self):
        """Periodic cleanup of old job entries"""
        while True:
            try:
                time.sleep(300)  # Run every 5 minutes
                self._cleanup_old_jobs()
            except Exception as e:
                logger.error(f"Error in cleanup loop: {str(e)}")

    def _cleanup_old_jobs(self):
        """Remove old job entries from tracking"""
        with self.lock:
            current_time = time.time()
            max_age = 1800  # 30 minutes

            fingerprints_to_remove = []

            for fingerprint, job_info in self.active_jobs.items():
                job_age = current_time - job_info["created_at"]

                if job_age > max_age:
                    fingerprints_to_remove.append(fingerprint)
                elif not self._is_job_still_active(job_info):
                    fingerprints_to_remove.append(fingerprint)

            # Remove old/completed jobs
            for fingerprint in fingerprints_to_remove:
                del self.active_jobs[fingerprint]

            if fingerprints_to_remove:
                logger.info(f"Cleaned up {len(fingerprints_to_remove)} old job entries")

    def get_active_jobs_summary(self) -> Dict:
        """Get summary of currently active jobs"""
        with self.lock:
            summary = {
                "total_active": len(self.active_jobs),
                "by_type": {},
                "by_connection": {},
                "oldest_job_age": 0
            }

            current_time = time.time()

            for job_info in self.active_jobs.values():
                # Count by type
                job_type = job_info["job_type"]
                summary["by_type"][job_type] = summary["by_type"].get(job_type, 0) + 1

                # Count by connection
                connection_id = job_info["connection_id"]
                summary["by_connection"][connection_id] = summary["by_connection"].get(connection_id, 0) + 1

                # Track oldest job
                job_age = current_time - job_info["created_at"]
                summary["oldest_job_age"] = max(summary["oldest_job_age"], job_age)

            return summary


# Decorator for automatic job deduplication
def deduplicate_job(max_age_minutes: int = 30):
    """
    Decorator to automatically prevent duplicate jobs.

    Args:
        max_age_minutes: Maximum age for considering a job "active"
    """

    def decorator(func):
        def wrapper(connection_id: str, job_type: str, trigger: str = "unknown", **kwargs):
            dedup_service = JobDeduplicationService.get_instance()

            # Create job fingerprint
            fingerprint = dedup_service.create_job_fingerprint(
                connection_id, job_type, trigger, **kwargs
            )

            # Check for duplicates
            if dedup_service.is_job_duplicate(fingerprint, max_age_minutes):
                logger.warning(f"Prevented duplicate job: {job_type} for connection {connection_id}")
                return {
                    "success": False,
                    "reason": "duplicate_job",
                    "message": f"Job {job_type} is already running for this connection"
                }

            # Execute the original function
            try:
                result = func(connection_id, job_type, trigger, **kwargs)

                # If job was successfully created, register it
                if result.get("success") and result.get("job_id"):
                    dedup_service.register_job(
                        fingerprint, result["job_id"], connection_id, job_type, trigger
                    )

                return result

            except Exception as e:
                logger.error(f"Error in job execution: {str(e)}")
                return {
                    "success": False,
                    "reason": "execution_error",
                    "error": str(e)
                }

        return wrapper

    return decorator


# Global instance
job_deduplication_service = JobDeduplicationService.get_instance()