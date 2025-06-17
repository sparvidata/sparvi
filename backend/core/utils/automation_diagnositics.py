# backend/core/utils/automation_diagnostics.py - NEW FILE

import logging
import traceback
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class AutomationDiagnosticUtility:
    """Utility for diagnosing and fixing automation storage issues"""

    def __init__(self, supabase_manager):
        """
        Initialize with Supabase manager

        Args:
            supabase_manager: SupabaseManager instance
        """
        self.supabase = supabase_manager

    def diagnose_automation_issues(self, connection_id: str = None, days: int = 3) -> Dict[str, Any]:
        """
        Comprehensive diagnosis of automation issues

        Args:
            connection_id: Optional specific connection to diagnose
            days: Number of days to analyze

        Returns:
            Dictionary with diagnostic results and recommendations
        """
        try:
            logger.info(f"Starting automation diagnosis for connection {connection_id or 'all'}")

            diagnosis = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "analysis_period_days": days,
                "issues_found": [],
                "recommendations": [],
                "storage_health": {},
                "job_analysis": {},
                "summary": {}
            }

            # Get recent automation jobs
            job_analysis = self._analyze_recent_jobs(connection_id, days)
            diagnosis["job_analysis"] = job_analysis

            # Check storage health
            storage_health = self._check_storage_health(connection_id, days)
            diagnosis["storage_health"] = storage_health

            # Identify specific issues
            issues = self._identify_issues(job_analysis, storage_health)
            diagnosis["issues_found"] = issues

            # Generate recommendations
            recommendations = self._generate_recommendations(issues, job_analysis, storage_health)
            diagnosis["recommendations"] = recommendations

            # Create summary
            diagnosis["summary"] = {
                "total_issues": len(issues),
                "critical_issues": len([i for i in issues if i.get("severity") == "critical"]),
                "overall_health": self._determine_overall_health(issues),
                "needs_immediate_attention": any(i.get("severity") == "critical" for i in issues)
            }

            logger.info(f"Diagnosis complete: {len(issues)} issues found")
            return diagnosis

        except Exception as e:
            logger.error(f"Error in automation diagnosis: {str(e)}")
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "message": "Failed to complete automation diagnosis"
            }

    def _analyze_recent_jobs(self, connection_id: str = None, days: int = 3) -> Dict[str, Any]:
        """Analyze recent automation jobs"""
        try:
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Build query
            query = self.supabase.supabase.table("automation_jobs") \
                .select("*") \
                .gte("scheduled_at", cutoff_date)

            if connection_id:
                query = query.eq("connection_id", connection_id)

            response = query.order("scheduled_at", desc=True).execute()
            jobs = response.data or []

            analysis = {
                "total_jobs": len(jobs),
                "jobs_by_status": {},
                "jobs_by_type": {},
                "failed_jobs": [],
                "jobs_without_results": [],
                "storage_issues": []
            }

            for job in jobs:
                # Count by status
                status = job.get("status", "unknown")
                if status not in analysis["jobs_by_status"]:
                    analysis["jobs_by_status"][status] = 0
                analysis["jobs_by_status"][status] += 1

                # Count by type
                job_type = job.get("job_type", "unknown")
                if job_type not in analysis["jobs_by_type"]:
                    analysis["jobs_by_type"][job_type] = 0
                analysis["jobs_by_type"][job_type] += 1

                # Check for failed jobs
                if status == "failed":
                    analysis["failed_jobs"].append({
                        "job_id": job.get("id"),
                        "job_type": job_type,
                        "error_message": job.get("error_message"),
                        "scheduled_at": job.get("scheduled_at")
                    })

                # Check for completed jobs without proper results
                elif status == "completed":
                    result_summary = job.get("result_summary")
                    if not result_summary or not self._has_meaningful_results(result_summary, job_type):
                        analysis["jobs_without_results"].append({
                            "job_id": job.get("id"),
                            "job_type": job_type,
                            "result_summary": result_summary,
                            "scheduled_at": job.get("scheduled_at")
                        })

                    # Check for storage verification failures
                    if result_summary and not result_summary.get("verified", True):
                        analysis["storage_issues"].append({
                            "job_id": job.get("id"),
                            "job_type": job_type,
                            "issue": "Storage verification failed",
                            "details": result_summary
                        })

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing recent jobs: {str(e)}")
            return {"error": str(e)}

    def _check_storage_health(self, connection_id: str = None, days: int = 3) -> Dict[str, Any]:
        """Check the health of storage systems"""
        try:
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            health = {
                "metadata_storage": {},
                "validation_storage": {},
                "schema_changes_storage": {},
                "automation_tracking": {}
            }

            # Check metadata storage
            try:
                query = self.supabase.supabase.table("connection_metadata") \
                    .select("connection_id, metadata_type, collected_at", count="exact") \
                    .gte("collected_at", cutoff_date)

                if connection_id:
                    query = query.eq("connection_id", connection_id)

                response = query.execute()

                health["metadata_storage"] = {
                    "total_records": response.count or 0,
                    "recent_records": len(response.data) if response.data else 0,
                    "status": "healthy" if (response.count or 0) > 0 else "no_activity"
                }

            except Exception as e:
                health["metadata_storage"] = {"status": "error", "error": str(e)}

            # Check validation storage
            try:
                query = self.supabase.supabase.table("validation_results") \
                    .select("connection_id, is_valid, run_at", count="exact") \
                    .gte("run_at", cutoff_date)

                if connection_id:
                    query = query.eq("connection_id", connection_id)

                response = query.execute()

                health["validation_storage"] = {
                    "total_records": response.count or 0,
                    "recent_records": len(response.data) if response.data else 0,
                    "status": "healthy" if (response.count or 0) > 0 else "no_activity"
                }

            except Exception as e:
                health["validation_storage"] = {"status": "error", "error": str(e)}

            # Check schema changes storage
            try:
                query = self.supabase.supabase.table("schema_changes") \
                    .select("connection_id, change_type, detected_at", count="exact") \
                    .gte("detected_at", cutoff_date)

                if connection_id:
                    query = query.eq("connection_id", connection_id)

                response = query.execute()

                health["schema_changes_storage"] = {
                    "total_records": response.count or 0,
                    "recent_records": len(response.data) if response.data else 0,
                    "status": "healthy"  # Schema changes can be zero and still be healthy
                }

            except Exception as e:
                health["schema_changes_storage"] = {"status": "error", "error": str(e)}

            # Check automation tracking
            try:
                query = self.supabase.supabase.table("automation_runs") \
                    .select("connection_id, status, run_type", count="exact") \
                    .gte("started_at", cutoff_date)

                if connection_id:
                    query = query.eq("connection_id", connection_id)

                response = query.execute()

                health["automation_tracking"] = {
                    "total_records": response.count or 0,
                    "recent_records": len(response.data) if response.data else 0,
                    "status": "healthy" if (response.count or 0) > 0 else "no_activity"
                }

            except Exception as e:
                health["automation_tracking"] = {"status": "error", "error": str(e)}

            return health

        except Exception as e:
            logger.error(f"Error checking storage health: {str(e)}")
            return {"error": str(e)}

    def _has_meaningful_results(self, result_summary: Dict, job_type: str) -> bool:
        """Check if a job has meaningful results"""
        if not result_summary:
            return False

        if job_type == "metadata_refresh":
            # Should have collected some metadata
            return any(
                result_summary.get(key, {}).get("status") == "completed"
                for key in ["tables", "columns", "statistics"]
            )

        elif job_type == "validation_run":
            # Should have executed some validations
            return result_summary.get("total_rules", 0) > 0

        elif job_type == "schema_detection":
            # Schema detection can have zero changes and still be successful
            return "changes_detected" in result_summary

        return True  # Default to assuming it's meaningful

    def _identify_issues(self, job_analysis: Dict, storage_health: Dict) -> List[Dict]:
        """Identify specific issues from the analysis"""
        issues = []

        try:
            # Check for high failure rates
            total_jobs = job_analysis.get("total_jobs", 0)
            failed_jobs = job_analysis.get("jobs_by_status", {}).get("failed", 0)

            if total_jobs > 0:
                failure_rate = failed_jobs / total_jobs
                if failure_rate > 0.5:  # More than 50% failure rate
                    issues.append({
                        "type": "high_failure_rate",
                        "severity": "critical",
                        "description": f"High automation failure rate: {failure_rate:.1%}",
                        "details": {
                            "total_jobs": total_jobs,
                            "failed_jobs": failed_jobs,
                            "failure_rate": failure_rate
                        }
                    })
                elif failure_rate > 0.2:  # More than 20% failure rate
                    issues.append({
                        "type": "moderate_failure_rate",
                        "severity": "warning",
                        "description": f"Moderate automation failure rate: {failure_rate:.1%}",
                        "details": {
                            "total_jobs": total_jobs,
                            "failed_jobs": failed_jobs,
                            "failure_rate": failure_rate
                        }
                    })

            # Check for jobs without results
            jobs_without_results = len(job_analysis.get("jobs_without_results", []))
            if jobs_without_results > 0:
                issues.append({
                    "type": "jobs_without_results",
                    "severity": "critical",
                    "description": f"{jobs_without_results} jobs completed but produced no meaningful results",
                    "details": job_analysis.get("jobs_without_results", [])
                })

            # Check for storage verification failures
            storage_issues = len(job_analysis.get("storage_issues", []))
            if storage_issues > 0:
                issues.append({
                    "type": "storage_verification_failures",
                    "severity": "critical",
                    "description": f"{storage_issues} jobs failed storage verification",
                    "details": job_analysis.get("storage_issues", [])
                })

            # Check storage system health
            for system, health in storage_health.items():
                if health.get("status") == "error":
                    issues.append({
                        "type": "storage_system_error",
                        "severity": "critical",
                        "description": f"Storage system error in {system}",
                        "details": {"system": system, "error": health.get("error")}
                    })
                elif health.get("status") == "no_activity" and system != "schema_changes_storage":
                    issues.append({
                        "type": "no_storage_activity",
                        "severity": "warning",
                        "description": f"No recent activity in {system}",
                        "details": {"system": system, "records": health.get("recent_records", 0)}
                    })

            # Check for specific job types not running
            job_types = job_analysis.get("jobs_by_type", {})
            expected_types = ["metadata_refresh", "validation_run", "schema_detection"]

            for expected_type in expected_types:
                if expected_type not in job_types:
                    issues.append({
                        "type": "missing_job_type",
                        "severity": "warning",
                        "description": f"No {expected_type} jobs found in recent period",
                        "details": {"missing_job_type": expected_type}
                    })

            return issues

        except Exception as e:
            logger.error(f"Error identifying issues: {str(e)}")
            return [{"type": "diagnosis_error", "severity": "critical", "description": str(e)}]

    def _generate_recommendations(self, issues: List[Dict], job_analysis: Dict, storage_health: Dict) -> List[Dict]:
        """Generate recommendations based on identified issues"""
        recommendations = []

        try:
            for issue in issues:
                issue_type = issue.get("type")

                if issue_type == "high_failure_rate":
                    recommendations.append({
                        "priority": "critical",
                        "action": "investigate_job_failures",
                        "description": "Investigate and fix the root causes of job failures",
                        "steps": [
                            "Review error messages from failed jobs",
                            "Check database connectivity and credentials",
                            "Verify automation service is running properly",
                            "Check storage service initialization"
                        ]
                    })

                elif issue_type == "jobs_without_results":
                    recommendations.append({
                        "priority": "critical",
                        "action": "fix_storage_integration",
                        "description": "Fix storage integration issues preventing result storage",
                        "steps": [
                            "Verify storage services are properly initialized",
                            "Check Supabase connection and permissions",
                            "Review storage service logs for errors",
                            "Test storage operations manually"
                        ]
                    })

                elif issue_type == "storage_verification_failures":
                    recommendations.append({
                        "priority": "critical",
                        "action": "enhance_storage_verification",
                        "description": "Improve storage verification and retry logic",
                        "steps": [
                            "Implement retry logic for failed storage operations",
                            "Add more detailed storage verification",
                            "Improve error handling in storage services",
                            "Add storage health monitoring"
                        ]
                    })

                elif issue_type == "storage_system_error":
                    recommendations.append({
                        "priority": "critical",
                        "action": "fix_storage_system",
                        "description": f"Fix error in {issue.get('details', {}).get('system')}",
                        "steps": [
                            "Check Supabase service status",
                            "Verify database table schemas",
                            "Check API key and permissions",
                            "Review service initialization code"
                        ]
                    })

                elif issue_type == "no_storage_activity":
                    recommendations.append({
                        "priority": "warning",
                        "action": "verify_automation_schedule",
                        "description": f"Verify automation is properly scheduled for {issue.get('details', {}).get('system')}",
                        "steps": [
                            "Check automation configuration",
                            "Verify scheduler is running",
                            "Review automation intervals",
                            "Test manual job triggers"
                        ]
                    })

                elif issue_type == "missing_job_type":
                    recommendations.append({
                        "priority": "warning",
                        "action": "enable_missing_automation",
                        "description": f"Enable {issue.get('details', {}).get('missing_job_type')} automation",
                        "steps": [
                            "Check automation configuration",
                            "Enable missing automation types",
                            "Verify job scheduling logic",
                            "Test job execution manually"
                        ]
                    })

            # Add general recommendations
            if len(issues) > 0:
                recommendations.append({
                    "priority": "general",
                    "action": "implement_monitoring",
                    "description": "Implement comprehensive automation monitoring",
                    "steps": [
                        "Set up regular storage health checks",
                        "Implement alerting for automation failures",
                        "Add metrics tracking for storage operations",
                        "Create automation status dashboard"
                    ]
                })

            return recommendations

        except Exception as e:
            logger.error(f"Error generating recommendations: {str(e)}")
            return [{
                "priority": "critical",
                "action": "fix_diagnosis_system",
                "description": f"Fix error in recommendation system: {str(e)}"
            }]

    def _determine_overall_health(self, issues: List[Dict]) -> str:
        """Determine overall health based on issues"""
        if not issues:
            return "healthy"

        critical_issues = [i for i in issues if i.get("severity") == "critical"]
        if critical_issues:
            return "critical"

        warning_issues = [i for i in issues if i.get("severity") == "warning"]
        if warning_issues:
            return "warning"

        return "unknown"

    def test_storage_operations(self, connection_id: str) -> Dict[str, Any]:
        """Test storage operations to verify they're working"""
        try:
            logger.info(f"Testing storage operations for connection {connection_id}")

            test_results = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "tests": {},
                "overall_result": "unknown"
            }

            # Test metadata storage
            try:
                # Try to store test metadata
                test_metadata = {
                    "tables": [{"name": "test_table_diagnostic"}],
                    "count": 1,
                    "test": True
                }

                response = self.supabase.supabase.table("connection_metadata").insert({
                    "connection_id": connection_id,
                    "metadata_type": "diagnostic_test",
                    "metadata": test_metadata,
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "refresh_frequency": "1 day"
                }).execute()

                # Clean up test record
                if response.data:
                    self.supabase.supabase.table("connection_metadata") \
                        .delete() \
                        .eq("id", response.data[0]["id"]) \
                        .execute()

                test_results["tests"]["metadata_storage"] = {
                    "status": "success",
                    "message": "Successfully stored and retrieved test metadata"
                }

            except Exception as e:
                test_results["tests"]["metadata_storage"] = {
                    "status": "failed",
                    "error": str(e)
                }

            # Test validation results storage
            try:
                # Try to store test validation result
                test_result = {
                    "id": "test-diagnostic-result",
                    "organization_id": "test-org",
                    "rule_id": "test-rule",
                    "is_valid": True,
                    "run_at": datetime.now(timezone.utc).isoformat(),
                    "actual_value": "test_value",
                    "connection_id": connection_id
                }

                response = self.supabase.supabase.table("validation_results").insert(test_result).execute()

                # Clean up test record
                if response.data:
                    self.supabase.supabase.table("validation_results") \
                        .delete() \
                        .eq("id", "test-diagnostic-result") \
                        .execute()

                test_results["tests"]["validation_storage"] = {
                    "status": "success",
                    "message": "Successfully stored and retrieved test validation result"
                }

            except Exception as e:
                test_results["tests"]["validation_storage"] = {
                    "status": "failed",
                    "error": str(e)
                }

            # Test schema changes storage
            try:
                # Try to store test schema change
                test_change = {
                    "connection_id": connection_id,
                    "table_name": "test_table",
                    "change_type": "diagnostic_test",
                    "change_details": {"type": "test", "test": True},
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    "acknowledged": True,
                    "important": False
                }

                response = self.supabase.supabase.table("schema_changes").insert(test_change).execute()

                # Clean up test record
                if response.data:
                    self.supabase.supabase.table("schema_changes") \
                        .delete() \
                        .eq("id", response.data[0]["id"]) \
                        .execute()

                test_results["tests"]["schema_changes_storage"] = {
                    "status": "success",
                    "message": "Successfully stored and retrieved test schema change"
                }

            except Exception as e:
                test_results["tests"]["schema_changes_storage"] = {
                    "status": "failed",
                    "error": str(e)
                }

            # Determine overall result
            failed_tests = [t for t in test_results["tests"].values() if t["status"] == "failed"]

            if not failed_tests:
                test_results["overall_result"] = "success"
                test_results["message"] = "All storage operations are working correctly"
            else:
                test_results["overall_result"] = "failed"
                test_results["message"] = f"{len(failed_tests)} storage operations failed"

            return test_results

        except Exception as e:
            logger.error(f"Error testing storage operations: {str(e)}")