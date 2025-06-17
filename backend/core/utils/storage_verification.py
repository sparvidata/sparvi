# backend/core/utils/storage_verification.py - NEW FILE

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class StorageVerificationUtility:
    """Utility for verifying that automation results are properly stored in Supabase"""

    def __init__(self, supabase_manager):
        """
        Initialize with Supabase manager

        Args:
            supabase_manager: SupabaseManager instance
        """
        self.supabase = supabase_manager

    def verify_metadata_storage(self, connection_id: str, metadata_type: str, expected_count: int = None,
                                max_attempts: int = 3, wait_seconds: int = 2) -> Dict[str, Any]:
        """
        Verify that metadata was properly stored

        Args:
            connection_id: Connection ID
            metadata_type: Type of metadata (tables, columns, statistics)
            expected_count: Expected number of items (optional)
            max_attempts: Maximum verification attempts
            wait_seconds: Seconds to wait between attempts

        Returns:
            Dictionary with verification results
        """
        try:
            logger.info(f"Verifying {metadata_type} metadata storage for connection {connection_id}")

            for attempt in range(max_attempts):
                # Wait before checking (except first attempt)
                if attempt > 0:
                    time.sleep(wait_seconds)

                # Check for recent metadata records
                cutoff_time = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

                response = self.supabase.supabase.table("connection_metadata") \
                    .select("id, metadata, collected_at") \
                    .eq("connection_id", connection_id) \
                    .eq("metadata_type", metadata_type) \
                    .gte("collected_at", cutoff_time) \
                    .order("collected_at", desc=True) \
                    .limit(1) \
                    .execute()

                if response.data and len(response.data) > 0:
                    metadata_record = response.data[0]
                    metadata_content = metadata_record.get("metadata", {})

                    # Count items based on metadata type
                    actual_count = 0
                    if metadata_type == "tables":
                        actual_count = len(metadata_content.get("tables", []))
                    elif metadata_type == "columns":
                        actual_count = len(metadata_content.get("columns_by_table", {}))
                    elif metadata_type == "statistics":
                        actual_count = len(metadata_content.get("statistics_by_table", {}))

                    logger.info(f"Verification attempt {attempt + 1}: Found {actual_count} {metadata_type} items")

                    # Check if count matches expectation
                    count_ok = True
                    if expected_count is not None:
                        count_ok = actual_count >= expected_count

                    if actual_count > 0 and count_ok:
                        return {
                            "verified": True,
                            "attempt": attempt + 1,
                            "actual_count": actual_count,
                            "expected_count": expected_count,
                            "collected_at": metadata_record.get("collected_at"),
                            "message": f"Successfully verified {metadata_type} metadata storage"
                        }

                logger.warning(f"Verification attempt {attempt + 1} failed for {metadata_type} metadata")

            # All attempts failed
            return {
                "verified": False,
                "attempts": max_attempts,
                "actual_count": 0,
                "expected_count": expected_count,
                "message": f"Failed to verify {metadata_type} metadata storage after {max_attempts} attempts"
            }

        except Exception as e:
            logger.error(f"Error verifying metadata storage: {str(e)}")
            return {
                "verified": False,
                "error": str(e),
                "message": f"Error during {metadata_type} metadata verification"
            }

    def verify_validation_results_storage(self, connection_id: str, expected_count: int = None,
                                          max_attempts: int = 3, wait_seconds: int = 2) -> Dict[str, Any]:
        """
        Verify that validation results were properly stored

        Args:
            connection_id: Connection ID
            expected_count: Expected number of validation results
            max_attempts: Maximum verification attempts
            wait_seconds: Seconds to wait between attempts

        Returns:
            Dictionary with verification results
        """
        try:
            logger.info(f"Verifying validation results storage for connection {connection_id}")

            for attempt in range(max_attempts):
                # Wait before checking (except first attempt)
                if attempt > 0:
                    time.sleep(wait_seconds)

                # Check for recent validation results
                cutoff_time = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

                response = self.supabase.supabase.table("validation_results") \
                    .select("id, rule_id, is_valid, run_at") \
                    .eq("connection_id", connection_id) \
                    .gte("run_at", cutoff_time) \
                    .execute()

                actual_count = len(response.data) if response.data else 0

                logger.info(f"Verification attempt {attempt + 1}: Found {actual_count} validation results")

                # Check if count matches expectation
                count_ok = True
                if expected_count is not None:
                    count_ok = actual_count >= expected_count

                if actual_count > 0 and count_ok:
                    # Calculate success/failure counts
                    passed_count = sum(1 for result in response.data if result.get("is_valid", False))
                    failed_count = actual_count - passed_count

                    return {
                        "verified": True,
                        "attempt": attempt + 1,
                        "total_results": actual_count,
                        "passed_results": passed_count,
                        "failed_results": failed_count,
                        "expected_count": expected_count,
                        "message": f"Successfully verified validation results storage"
                    }

                logger.warning(f"Verification attempt {attempt + 1} failed for validation results")

            # All attempts failed
            return {
                "verified": False,
                "attempts": max_attempts,
                "total_results": 0,
                "expected_count": expected_count,
                "message": f"Failed to verify validation results storage after {max_attempts} attempts"
            }

        except Exception as e:
            logger.error(f"Error verifying validation results storage: {str(e)}")
            return {
                "verified": False,
                "error": str(e),
                "message": f"Error during validation results verification"
            }

    def verify_schema_changes_storage(self, connection_id: str, expected_count: int = None,
                                      max_attempts: int = 3, wait_seconds: int = 2) -> Dict[str, Any]:
        """
        Verify that schema changes were properly stored

        Args:
            connection_id: Connection ID
            expected_count: Expected number of schema changes
            max_attempts: Maximum verification attempts
            wait_seconds: Seconds to wait between attempts

        Returns:
            Dictionary with verification results
        """
        try:
            logger.info(f"Verifying schema changes storage for connection {connection_id}")

            for attempt in range(max_attempts):
                # Wait before checking (except first attempt)
                if attempt > 0:
                    time.sleep(wait_seconds)

                # Check for recent schema changes
                cutoff_time = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

                response = self.supabase.supabase.table("schema_changes") \
                    .select("id, change_type, table_name, important, detected_at") \
                    .eq("connection_id", connection_id) \
                    .gte("detected_at", cutoff_time) \
                    .execute()

                actual_count = len(response.data) if response.data else 0

                logger.info(f"Verification attempt {attempt + 1}: Found {actual_count} schema changes")

                # Check if count matches expectation
                count_ok = True
                if expected_count is not None:
                    count_ok = actual_count >= expected_count

                if actual_count > 0 and count_ok:
                    # Calculate important vs non-important changes
                    important_count = sum(1 for change in response.data if change.get("important", False))

                    # Group by change type
                    change_types = {}
                    for change in response.data:
                        change_type = change.get("change_type", "unknown")
                        if change_type not in change_types:
                            change_types[change_type] = 0
                        change_types[change_type] += 1

                    return {
                        "verified": True,
                        "attempt": attempt + 1,
                        "total_changes": actual_count,
                        "important_changes": important_count,
                        "change_types": change_types,
                        "expected_count": expected_count,
                        "message": f"Successfully verified schema changes storage"
                    }

                logger.warning(f"Verification attempt {attempt + 1} failed for schema changes")

            # All attempts failed
            return {
                "verified": False,
                "attempts": max_attempts,
                "total_changes": 0,
                "expected_count": expected_count,
                "message": f"Failed to verify schema changes storage after {max_attempts} attempts"
            }

        except Exception as e:
            logger.error(f"Error verifying schema changes storage: {str(e)}")
            return {
                "verified": False,
                "error": str(e),
                "message": f"Error during schema changes verification"
            }

    def verify_automation_run_tracking(self, job_id: str, expected_status: str = "completed",
                                       max_attempts: int = 3, wait_seconds: int = 2) -> Dict[str, Any]:
        """
        Verify that automation run was properly tracked

        Args:
            job_id: Automation job ID
            expected_status: Expected status (completed, failed, etc.)
            max_attempts: Maximum verification attempts
            wait_seconds: Seconds to wait between attempts

        Returns:
            Dictionary with verification results
        """
        try:
            logger.info(f"Verifying automation run tracking for job {job_id}")

            for attempt in range(max_attempts):
                # Wait before checking (except first attempt)
                if attempt > 0:
                    time.sleep(wait_seconds)

                # Check automation job record
                job_response = self.supabase.supabase.table("automation_jobs") \
                    .select("id, status, job_type, result_summary, error_message") \
                    .eq("id", job_id) \
                    .execute()

                if job_response.data and len(job_response.data) > 0:
                    job_record = job_response.data[0]
                    actual_status = job_record.get("status")

                    # Check automation run record
                    run_response = self.supabase.supabase.table("automation_runs") \
                        .select("id, status, results, run_type") \
                        .eq("job_id", job_id) \
                        .execute()

                    run_record = run_response.data[0] if run_response.data else None

                    logger.info(
                        f"Verification attempt {attempt + 1}: Job status {actual_status}, Run record exists: {run_record is not None}")

                    status_ok = actual_status == expected_status

                    if status_ok or actual_status in ["completed", "failed"]:  # Accept final states
                        return {
                            "verified": True,
                            "attempt": attempt + 1,
                            "job_status": actual_status,
                            "job_type": job_record.get("job_type"),
                            "has_run_record": run_record is not None,
                            "run_status": run_record.get("status") if run_record else None,
                            "result_summary": job_record.get("result_summary"),
                            "error_message": job_record.get("error_message"),
                            "message": f"Successfully verified automation run tracking"
                        }

                logger.warning(f"Verification attempt {attempt + 1} failed for automation run tracking")

            # All attempts failed
            return {
                "verified": False,
                "attempts": max_attempts,
                "job_status": None,
                "expected_status": expected_status,
                "message": f"Failed to verify automation run tracking after {max_attempts} attempts"
            }

        except Exception as e:
            logger.error(f"Error verifying automation run tracking: {str(e)}")
            return {
                "verified": False,
                "error": str(e),
                "message": f"Error during automation run verification"
            }

    def comprehensive_storage_health_check(self, connection_id: str) -> Dict[str, Any]:
        """
        Perform a comprehensive health check of all storage systems

        Args:
            connection_id: Connection ID to check

        Returns:
            Dictionary with comprehensive health check results
        """
        try:
            logger.info(f"Performing comprehensive storage health check for connection {connection_id}")

            health_check = {
                "connection_id": connection_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "overall_health": "unknown",
                "components": {}
            }

            # Check metadata storage
            try:
                # Check if we have any recent metadata
                recent_metadata = self.supabase.supabase.table("connection_metadata") \
                    .select("metadata_type, collected_at") \
                    .eq("connection_id", connection_id) \
                    .gte("collected_at", (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()) \
                    .execute()

                metadata_count = len(recent_metadata.data) if recent_metadata.data else 0

                health_check["components"]["metadata_storage"] = {
                    "status": "healthy" if metadata_count > 0 else "warning",
                    "recent_records": metadata_count,
                    "message": f"Found {metadata_count} recent metadata records"
                }

            except Exception as e:
                health_check["components"]["metadata_storage"] = {
                    "status": "error",
                    "error": str(e),
                    "message": "Error checking metadata storage"
                }

            # Check validation results storage
            try:
                recent_validations = self.supabase.supabase.table("validation_results") \
                    .select("id, is_valid, run_at") \
                    .eq("connection_id", connection_id) \
                    .gte("run_at", (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()) \
                    .execute()

                validation_count = len(recent_validations.data) if recent_validations.data else 0

                health_check["components"]["validation_storage"] = {
                    "status": "healthy" if validation_count > 0 else "warning",
                    "recent_records": validation_count,
                    "message": f"Found {validation_count} recent validation results"
                }

            except Exception as e:
                health_check["components"]["validation_storage"] = {
                    "status": "error",
                    "error": str(e),
                    "message": "Error checking validation storage"
                }

            # Check schema changes storage
            try:
                recent_changes = self.supabase.supabase.table("schema_changes") \
                    .select("id, change_type, detected_at") \
                    .eq("connection_id", connection_id) \
                    .gte("detected_at", (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()) \
                    .execute()

                changes_count = len(recent_changes.data) if recent_changes.data else 0

                health_check["components"]["schema_changes_storage"] = {
                    "status": "healthy",  # Schema changes can be zero and still healthy
                    "recent_records": changes_count,
                    "message": f"Found {changes_count} recent schema changes"
                }

            except Exception as e:
                health_check["components"]["schema_changes_storage"] = {
                    "status": "error",
                    "error": str(e),
                    "message": "Error checking schema changes storage"
                }

            # Check automation job tracking
            try:
                recent_jobs = self.supabase.supabase.table("automation_jobs") \
                    .select("id, status, job_type") \
                    .eq("connection_id", connection_id) \
                    .gte("scheduled_at", (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()) \
                    .execute()

                jobs_count = len(recent_jobs.data) if recent_jobs.data else 0

                health_check["components"]["automation_tracking"] = {
                    "status": "healthy" if jobs_count > 0 else "warning",
                    "recent_records": jobs_count,
                    "message": f"Found {jobs_count} recent automation jobs"
                }

            except Exception as e:
                health_check["components"]["automation_tracking"] = {
                    "status": "error",
                    "error": str(e),
                    "message": "Error checking automation tracking"
                }

            # Determine overall health
            component_statuses = [comp["status"] for comp in health_check["components"].values()]

            if "error" in component_statuses:
                health_check["overall_health"] = "error"
                health_check["message"] = "One or more storage components have errors"
            elif "warning" in component_statuses:
                health_check["overall_health"] = "warning"
                health_check["message"] = "Some storage components have warnings"
            else:
                health_check["overall_health"] = "healthy"
                health_check["message"] = "All storage components are healthy"

            return health_check

        except Exception as e:
            logger.error(f"Error in comprehensive storage health check: {str(e)}")
            return {
                "connection_id": connection_id,
                "overall_health": "error",
                "error": str(e),
                "message": "Failed to perform health check"
            }

    def get_storage_statistics(self, connection_id: str, days: int = 7) -> Dict[str, Any]:
        """
        Get storage statistics for a connection over a time period

        Args:
            connection_id: Connection ID
            days: Number of days to analyze

        Returns:
            Dictionary with storage statistics
        """
        try:
            logger.info(f"Getting storage statistics for connection {connection_id} over {days} days")

            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            stats = {
                "connection_id": connection_id,
                "analysis_period_days": days,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Metadata storage stats
            try:
                metadata_response = self.supabase.supabase.table("connection_metadata") \
                    .select("metadata_type, collected_at") \
                    .eq("connection_id", connection_id) \
                    .gte("collected_at", cutoff_date) \
                    .execute()

                metadata_records = metadata_response.data or []
                metadata_by_type = {}

                for record in metadata_records:
                    metadata_type = record.get("metadata_type", "unknown")
                    if metadata_type not in metadata_by_type:
                        metadata_by_type[metadata_type] = 0
                    metadata_by_type[metadata_type] += 1

                stats["metadata_storage"] = {
                    "total_records": len(metadata_records),
                    "by_type": metadata_by_type,
                    "status": "active" if len(metadata_records) > 0 else "inactive"
                }

            except Exception as e:
                stats["metadata_storage"] = {"error": str(e)}

            # Validation results stats
            try:
                validation_response = self.supabase.supabase.table("validation_results") \
                    .select("is_valid, run_at") \
                    .eq("connection_id", connection_id) \
                    .gte("run_at", cutoff_date) \
                    .execute()

                validation_records = validation_response.data or []
                passed_count = sum(1 for r in validation_records if r.get("is_valid", False))
                failed_count = len(validation_records) - passed_count

                stats["validation_storage"] = {
                    "total_results": len(validation_records),
                    "passed_results": passed_count,
                    "failed_results": failed_count,
                    "success_rate": (passed_count / len(validation_records) * 100) if validation_records else 0,
                    "status": "active" if len(validation_records) > 0 else "inactive"
                }

            except Exception as e:
                stats["validation_storage"] = {"error": str(e)}

            # Schema changes stats
            try:
                changes_response = self.supabase.supabase.table("schema_changes") \
                    .select("change_type, important, detected_at") \
                    .eq("connection_id", connection_id) \
                    .gte("detected_at", cutoff_date) \
                    .execute()

                change_records = changes_response.data or []
                important_count = sum(1 for r in change_records if r.get("important", False))

                change_types = {}
                for record in change_records:
                    change_type = record.get("change_type", "unknown")
                    if change_type not in change_types:
                        change_types[change_type] = 0
                    change_types[change_type] += 1

                stats["schema_changes_storage"] = {
                    "total_changes": len(change_records),
                    "important_changes": important_count,
                    "by_type": change_types,
                    "status": "changes_detected" if len(change_records) > 0 else "stable"
                }

            except Exception as e:
                stats["schema_changes_storage"] = {"error": str(e)}

            # Automation jobs stats
            try:
                jobs_response = self.supabase.supabase.table("automation_jobs") \
                    .select("status, job_type, scheduled_at") \
                    .eq("connection_id", connection_id) \
                    .gte("scheduled_at", cutoff_date) \
                    .execute()

                job_records = jobs_response.data or []

                jobs_by_status = {}
                jobs_by_type = {}

                for record in job_records:
                    status = record.get("status", "unknown")
                    job_type = record.get("job_type", "unknown")

                    if status not in jobs_by_status:
                        jobs_by_status[status] = 0
                    jobs_by_status[status] += 1

                    if job_type not in jobs_by_type:
                        jobs_by_type[job_type] = 0
                    jobs_by_type[job_type] += 1

                completed_jobs = jobs_by_status.get("completed", 0)
                failed_jobs = jobs_by_status.get("failed", 0)
                total_finished = completed_jobs + failed_jobs

                stats["automation_jobs"] = {
                    "total_jobs": len(job_records),
                    "by_status": jobs_by_status,
                    "by_type": jobs_by_type,
                    "success_rate": (completed_jobs / total_finished * 100) if total_finished > 0 else 0,
                    "status": "active" if len(job_records) > 0 else "inactive"
                }

            except Exception as e:
                stats["automation_jobs"] = {"error": str(e)}

            return stats

        except Exception as e:
            logger.error(f"Error getting storage statistics: {str(e)}")
            return {
                "connection_id": connection_id,
                "error": str(e),
                "message": "Failed to get storage statistics"
            }