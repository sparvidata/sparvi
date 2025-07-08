import logging
import traceback
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class AutomationDiagnosticUtility:
    """Utility for diagnosing automation issues"""

    def __init__(self, supabase_manager):
        """
        Initialize with Supabase manager

        Args:
            supabase_manager: SupabaseManager instance
        """
        self.supabase = supabase_manager

    def diagnose_automation_issues(self, connection_id: str, days: int = 3) -> Dict[str, Any]:
        """
        Comprehensive diagnosis of automation issues

        Args:
            connection_id: Connection ID to diagnose
            days: Number of days to analyze

        Returns:
            Dictionary with diagnostic results and recommendations
        """
        try:
            logger.info(f"Starting automation diagnosis for connection {connection_id}")

            diagnosis = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "analysis_period_days": days,
                "issues_found": [],
                "recommendations": [],
                "job_analysis": {},
                "config_analysis": {},
                "scheduler_analysis": {},
                "summary": {}
            }

            # Analyze recent jobs
            job_analysis = self._analyze_recent_jobs(connection_id, days)
            diagnosis["job_analysis"] = job_analysis

            # Analyze configuration
            config_analysis = self._analyze_automation_config(connection_id)
            diagnosis["config_analysis"] = config_analysis

            # Analyze scheduler behavior
            scheduler_analysis = self._analyze_scheduler_behavior(connection_id, days)
            diagnosis["scheduler_analysis"] = scheduler_analysis

            # Identify specific issues
            issues = self._identify_issues(job_analysis, config_analysis, scheduler_analysis)
            diagnosis["issues_found"] = issues

            # Generate recommendations
            recommendations = self._generate_recommendations(issues, job_analysis, config_analysis)
            diagnosis["recommendations"] = recommendations

            # Create summary
            diagnosis["summary"] = {
                "total_issues": len(issues),
                "critical_issues": len([i for i in issues if i.get("severity") == "critical"]),
                "job_failure_rate": job_analysis.get("failure_rate", 0),
                "config_health": config_analysis.get("health", "unknown"),
                "needs_immediate_attention": any(i.get("severity") == "critical" for i in issues)
            }

            logger.info(f"Automation diagnosis complete: {len(issues)} issues found")
            return diagnosis

        except Exception as e:
            logger.error(f"Error in automation diagnosis: {str(e)}")
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "message": "Failed to complete automation diagnosis"
            }

    def _analyze_recent_jobs(self, connection_id: str, days: int) -> Dict[str, Any]:
        """Analyze recent automation jobs"""
        try:
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            response = self.supabase.supabase.table("automation_jobs") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .gte("scheduled_at", cutoff_date) \
                .order("scheduled_at", desc=True) \
                .execute()

            jobs = response.data or []

            analysis = {
                "total_jobs": len(jobs),
                "jobs_by_status": {},
                "jobs_by_type": {},
                "failure_rate": 0,
                "avg_duration_minutes": 0,
                "recent_failures": [],
                "frequency_analysis": {}
            }

            # Count by status and type
            for job in jobs:
                status = job.get("status", "unknown")
                job_type = job.get("job_type", "unknown")

                if status not in analysis["jobs_by_status"]:
                    analysis["jobs_by_status"][status] = 0
                analysis["jobs_by_status"][status] += 1

                if job_type not in analysis["jobs_by_type"]:
                    analysis["jobs_by_type"][job_type] = 0
                analysis["jobs_by_type"][job_type] += 1

                # Collect recent failures with details
                if status == "failed":
                    analysis["recent_failures"].append({
                        "job_id": job.get("id"),
                        "job_type": job_type,
                        "scheduled_at": job.get("scheduled_at"),
                        "error_message": job.get("error_message"),
                        "result_summary": job.get("result_summary")
                    })

            # Calculate failure rate
            total_finished = analysis["jobs_by_status"].get("completed", 0) + analysis["jobs_by_status"].get("failed",
                                                                                                             0)
            if total_finished > 0:
                analysis["failure_rate"] = analysis["jobs_by_status"].get("failed", 0) / total_finished

            # Analyze job frequency
            analysis["frequency_analysis"] = self._analyze_job_frequency(jobs)

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing recent jobs: {str(e)}")
            return {"error": str(e)}

    def _analyze_job_frequency(self, jobs: List[Dict]) -> Dict[str, Any]:
        """Analyze how frequently jobs are being created"""
        if len(jobs) < 2:
            return {"status": "insufficient_data"}

        try:
            # Group jobs by type and analyze intervals
            jobs_by_type = {}
            for job in jobs:
                job_type = job.get("job_type", "unknown")
                if job_type not in jobs_by_type:
                    jobs_by_type[job_type] = []
                jobs_by_type[job_type].append(job)

            frequency_analysis = {}

            for job_type, type_jobs in jobs_by_type.items():
                if len(type_jobs) < 2:
                    continue

                # Sort by scheduled_at
                type_jobs.sort(key=lambda x: x.get("scheduled_at", ""))

                intervals = []
                for i in range(len(type_jobs) - 1):
                    try:
                        current_time = datetime.fromisoformat(type_jobs[i]["scheduled_at"].replace('Z', '+00:00'))
                        prev_time = datetime.fromisoformat(type_jobs[i + 1]["scheduled_at"].replace('Z', '+00:00'))
                        interval_hours = abs((current_time - prev_time).total_seconds() / 3600)
                        intervals.append(interval_hours)
                    except:
                        continue

                if intervals:
                    avg_interval = sum(intervals) / len(intervals)
                    min_interval = min(intervals)

                    frequency_analysis[job_type] = {
                        "average_interval_hours": avg_interval,
                        "minimum_interval_hours": min_interval,
                        "job_count": len(type_jobs),
                        "is_too_frequent": min_interval < 1,  # Less than 1 hour between jobs
                        "intervals": intervals[:5]  # Show first 5 intervals
                    }

            return frequency_analysis

        except Exception as e:
            logger.warning(f"Error analyzing job frequency: {str(e)}")
            return {"error": str(e)}

    def _analyze_automation_config(self, connection_id: str) -> Dict[str, Any]:
        """Analyze automation configuration"""
        try:
            # Get connection config
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .execute()

            config_data = response.data[0] if response.data else None

            if not config_data:
                return {
                    "health": "missing",
                    "message": "No automation configuration found",
                    "issues": ["No automation configuration exists for this connection"]
                }

            analysis = {
                "health": "unknown",
                "config_exists": True,
                "enabled_automations": [],
                "intervals": {},
                "issues": []
            }

            # Check each automation type
            for automation_type in ["metadata_refresh", "schema_change_detection", "validation_automation"]:
                config_value = config_data.get(automation_type)

                if isinstance(config_value, str):
                    try:
                        import json
                        config_value = json.loads(config_value)
                    except:
                        analysis["issues"].append(f"Invalid JSON for {automation_type}")
                        continue

                if isinstance(config_value, dict):
                    enabled = config_value.get("enabled", False)
                    interval_hours = config_value.get("interval_hours", 24)

                    if enabled:
                        analysis["enabled_automations"].append(automation_type)
                        analysis["intervals"][automation_type] = interval_hours

                        # Check for problematic intervals
                        if interval_hours < 1:
                            analysis["issues"].append(f"{automation_type} interval too short: {interval_hours}h")
                        elif interval_hours > 168:  # More than a week
                            analysis["issues"].append(f"{automation_type} interval very long: {interval_hours}h")

            # Determine overall health
            if len(analysis["issues"]) == 0:
                analysis["health"] = "healthy"
            elif len(analysis["issues"]) <= 2:
                analysis["health"] = "warning"
            else:
                analysis["health"] = "critical"

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing automation config: {str(e)}")
            return {"health": "error", "error": str(e)}

    def _analyze_scheduler_behavior(self, connection_id: str, days: int) -> Dict[str, Any]:
        """Analyze scheduler behavior patterns"""
        try:
            # This would require scheduler logs, but we can infer from job patterns
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Get job creation patterns
            response = self.supabase.supabase.table("automation_jobs") \
                .select("scheduled_at, job_type, status") \
                .eq("connection_id", connection_id) \
                .gte("scheduled_at", cutoff_date) \
                .order("scheduled_at", desc=True) \
                .execute()

            jobs = response.data or []

            analysis = {
                "job_creation_pattern": "unknown",
                "potential_issues": []
            }

            if len(jobs) >= 5:
                # Check if jobs are being created too frequently
                recent_jobs = jobs[:10]  # Last 10 jobs
                timestamps = []

                for job in recent_jobs:
                    try:
                        timestamp = datetime.fromisoformat(job["scheduled_at"].replace('Z', '+00:00'))
                        timestamps.append(timestamp)
                    except:
                        continue

                if len(timestamps) >= 3:
                    intervals = []
                    for i in range(len(timestamps) - 1):
                        interval = abs((timestamps[i] - timestamps[i + 1]).total_seconds() / 60)  # minutes
                        intervals.append(interval)

                    avg_interval_minutes = sum(intervals) / len(intervals)

                    if avg_interval_minutes < 45:  # Less than 45 minutes between jobs
                        analysis["potential_issues"].append("Jobs being created too frequently")
                        analysis["job_creation_pattern"] = "too_frequent"
                    elif avg_interval_minutes > 1440:  # More than 24 hours
                        analysis["job_creation_pattern"] = "infrequent"
                    else:
                        analysis["job_creation_pattern"] = "normal"

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing scheduler behavior: {str(e)}")
            return {"error": str(e)}

    def _identify_issues(self, job_analysis: Dict, config_analysis: Dict, scheduler_analysis: Dict) -> List[Dict]:
        """Identify specific issues based on analysis"""
        issues = []

        try:
            # High failure rate
            failure_rate = job_analysis.get("failure_rate", 0)
            if failure_rate > 0.8:  # More than 80% failure rate
                issues.append({
                    "type": "high_failure_rate",
                    "severity": "critical",
                    "description": f"Very high job failure rate: {failure_rate:.1%}",
                    "details": {
                        "failure_rate": failure_rate,
                        "total_jobs": job_analysis.get("total_jobs", 0),
                        "failed_jobs": job_analysis.get("jobs_by_status", {}).get("failed", 0)
                    }
                })

            # Jobs running too frequently
            frequency_analysis = job_analysis.get("frequency_analysis", {})
            for job_type, freq_data in frequency_analysis.items():
                if freq_data.get("is_too_frequent", False):
                    issues.append({
                        "type": "jobs_too_frequent",
                        "severity": "critical",
                        "description": f"{job_type} jobs running too frequently",
                        "details": {
                            "job_type": job_type,
                            "minimum_interval_hours": freq_data.get("minimum_interval_hours"),
                            "average_interval_hours": freq_data.get("average_interval_hours")
                        }
                    })

            # Configuration issues
            config_issues = config_analysis.get("issues", [])
            for issue in config_issues:
                issues.append({
                    "type": "configuration_issue",
                    "severity": "warning",
                    "description": issue,
                    "details": {"config_analysis": config_analysis}
                })

            # Scheduler issues
            scheduler_issues = scheduler_analysis.get("potential_issues", [])
            for issue in scheduler_issues:
                issues.append({
                    "type": "scheduler_issue",
                    "severity": "critical",
                    "description": issue,
                    "details": {"scheduler_analysis": scheduler_analysis}
                })

            # All jobs failing (100% failure rate)
            if job_analysis.get("total_jobs", 0) > 0 and job_analysis.get("jobs_by_status", {}).get("completed",
                                                                                                    0) == 0:
                issues.append({
                    "type": "all_jobs_failing",
                    "severity": "critical",
                    "description": "All automation jobs are failing",
                    "details": {
                        "recent_failures": job_analysis.get("recent_failures", [])[:3]  # Show first 3 failures
                    }
                })

            return issues

        except Exception as e:
            logger.error(f"Error identifying issues: {str(e)}")
            return [{"type": "diagnosis_error", "severity": "critical", "description": str(e)}]

    def _generate_recommendations(self, issues: List[Dict], job_analysis: Dict, config_analysis: Dict) -> List[Dict]:
        """Generate recommendations for fixing issues"""
        recommendations = []

        try:
            for issue in issues:
                issue_type = issue.get("type")

                if issue_type == "high_failure_rate" or issue_type == "all_jobs_failing":
                    recommendations.append({
                        "priority": "critical",
                        "action": "investigate_job_failures",
                        "description": "Investigate and fix job failures",
                        "steps": [
                            "Check error messages from recent failed jobs",
                            "Verify automation system initialization",
                            "Check database connectivity and permissions",
                            "Test metadata task manager integration",
                            "Review automation service startup logs"
                        ]
                    })

                elif issue_type == "jobs_too_frequent":
                    recommendations.append({
                        "priority": "critical",
                        "action": "fix_scheduler_frequency",
                        "description": "Fix scheduler creating jobs too frequently",
                        "steps": [
                            "Review _should_schedule_job logic in scheduler",
                            "Verify user interval configuration is being respected",
                            "Check for scheduler bugs causing overlapping jobs",
                            "Add better interval validation",
                            "Test with longer intervals temporarily"
                        ]
                    })

                elif issue_type == "configuration_issue":
                    recommendations.append({
                        "priority": "medium",
                        "action": "fix_configuration",
                        "description": "Fix automation configuration issues",
                        "steps": [
                            "Review automation configuration format",
                            "Fix JSON parsing issues",
                            "Validate interval values",
                            "Reset configuration if corrupted"
                        ]
                    })

                elif issue_type == "scheduler_issue":
                    recommendations.append({
                        "priority": "high",
                        "action": "fix_scheduler_behavior",
                        "description": "Fix scheduler behavior issues",
                        "steps": [
                            "Review scheduler timing logic",
                            "Check for infinite loops or rapid scheduling",
                            "Verify cleanup processes aren't interfering",
                            "Add rate limiting to job creation"
                        ]
                    })

            # Add general recommendations if there are issues
            if len(issues) > 0:
                recommendations.append({
                    "priority": "general",
                    "action": "enable_debug_logging",
                    "description": "Enable detailed logging for debugging",
                    "steps": [
                        "Set logging level to DEBUG for automation components",
                        "Monitor scheduler behavior in real-time",
                        "Track job creation and execution patterns",
                        "Review metadata task manager logs"
                    ]
                })

            return recommendations

        except Exception as e:
            logger.error(f"Error generating recommendations: {str(e)}")
            return [{
                "priority": "critical",
                "action": "fix_recommendation_system",
                "description": f"Fix error in recommendation system: {str(e)}"
            }]

    def test_storage_operations(self, connection_id: str) -> Dict[str, Any]:
        """Test that storage operations are working correctly"""
        try:
            logger.info(f"Testing storage operations for connection {connection_id}")

            test_results = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "tests": {},
                "overall_result": "unknown"
            }

            # Test automation job creation
            try:
                test_job = {
                    "id": f"test_job_{int(datetime.now().timestamp())}",
                    "connection_id": connection_id,
                    "job_type": "test",
                    "status": "completed",
                    "scheduled_at": datetime.now(timezone.utc).isoformat(),
                    "job_config": {"test": True}
                }

                response = self.supabase.supabase.table("automation_jobs").insert(test_job).execute()

                if response.data:
                    # Clean up test job
                    self.supabase.supabase.table("automation_jobs").delete().eq("id", test_job["id"]).execute()

                    test_results["tests"]["job_creation"] = {
                        "status": "success",
                        "message": "Successfully created and deleted test job"
                    }
                else:
                    test_results["tests"]["job_creation"] = {
                        "status": "failed",
                        "message": "Failed to create test job"
                    }

            except Exception as e:
                test_results["tests"]["job_creation"] = {
                    "status": "failed",
                    "error": str(e)
                }

            # Test automation run creation
            try:
                test_run = {
                    "id": f"test_run_{int(datetime.now().timestamp())}",
                    "job_id": "test_job_id",
                    "connection_id": connection_id,
                    "run_type": "test",
                    "status": "completed",
                    "started_at": datetime.now(timezone.utc).isoformat()
                }

                response = self.supabase.supabase.table("automation_runs").insert(test_run).execute()

                if response.data:
                    # Clean up test run
                    self.supabase.supabase.table("automation_runs").delete().eq("id", test_run["id"]).execute()

                    test_results["tests"]["run_creation"] = {
                        "status": "success",
                        "message": "Successfully created and deleted test run"
                    }
                else:
                    test_results["tests"]["run_creation"] = {
                        "status": "failed",
                        "message": "Failed to create test run"
                    }

            except Exception as e:
                test_results["tests"]["run_creation"] = {
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
                test_results["message"] = f"{len(failed_tests)} out of {len(test_results['tests'])} tests failed"

            return test_results

        except Exception as e:
            logger.error(f"Error testing storage operations: {str(e)}")
            return {
                "connection_id": connection_id,
                "overall_result": "error",
                "error": str(e),
                "message": "Failed to test storage operations"
            }

    def fix_common_issues(self, connection_id: str) -> Dict[str, Any]:
        """Attempt to fix common automation issues"""
        try:
            logger.info(f"Attempting to fix automation issues for connection {connection_id}")

            fix_results = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "fixes_attempted": [],
                "fixes_successful": [],
                "fixes_failed": [],
                "overall_result": "unknown"
            }

            # Fix 1: Cancel any stuck running jobs
            try:
                fix_results["fixes_attempted"].append("cancel_stuck_jobs")

                # Find jobs that have been running for more than 2 hours
                cutoff_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()

                stuck_jobs = self.supabase.supabase.table("automation_jobs") \
                    .select("id") \
                    .eq("connection_id", connection_id) \
                    .eq("status", "running") \
                    .lt("started_at", cutoff_time) \
                    .execute()

                stuck_count = len(stuck_jobs.data) if stuck_jobs.data else 0

                if stuck_count > 0:
                    # Mark stuck jobs as failed
                    for job in stuck_jobs.data:
                        self.supabase.supabase.table("automation_jobs") \
                            .update({
                            "status": "failed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "error_message": "Job cancelled due to excessive runtime (stuck job cleanup)"
                        }) \
                            .eq("id", job["id"]) \
                            .execute()

                fix_results["fixes_successful"].append({
                    "fix": "cancel_stuck_jobs",
                    "message": f"Cancelled {stuck_count} stuck jobs"
                })

            except Exception as e:
                fix_results["fixes_failed"].append({
                    "fix": "cancel_stuck_jobs",
                    "error": str(e)
                })

            # Fix 2: Reset automation configuration to safe defaults
            try:
                fix_results["fixes_attempted"].append("reset_to_safe_config")

                safe_config = {
                    "metadata_refresh": {
                        "enabled": True,
                        "interval_hours": 24,  # Safe 24-hour interval
                        "types": ["tables", "columns"]
                    },
                    "schema_change_detection": {
                        "enabled": False,  # Disable temporarily
                        "interval_hours": 24
                    },
                    "validation_automation": {
                        "enabled": False,  # Disable temporarily
                        "interval_hours": 24
                    },
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }

                response = self.supabase.supabase.table("automation_connection_configs") \
                    .update(safe_config) \
                    .eq("connection_id", connection_id) \
                    .execute()

                if response.data:
                    fix_results["fixes_successful"].append({
                        "fix": "reset_to_safe_config",
                        "message": "Reset to safe configuration (24-hour intervals, minimal automation)"
                    })
                else:
                    # Try to create config if it doesn't exist
                    safe_config["connection_id"] = connection_id
                    safe_config["created_at"] = datetime.now(timezone.utc).isoformat()

                    create_response = self.supabase.supabase.table("automation_connection_configs") \
                        .insert(safe_config) \
                        .execute()

                    if create_response.data:
                        fix_results["fixes_successful"].append({
                            "fix": "reset_to_safe_config",
                            "message": "Created safe configuration (24-hour intervals, minimal automation)"
                        })
                    else:
                        raise Exception("Failed to create or update configuration")

            except Exception as e:
                fix_results["fixes_failed"].append({
                    "fix": "reset_to_safe_config",
                    "error": str(e)
                })

            # Fix 3: Clean up recent failed jobs (keep only last 5)
            try:
                fix_results["fixes_attempted"].append("cleanup_failed_jobs")

                # Get all failed jobs for this connection
                failed_jobs = self.supabase.supabase.table("automation_jobs") \
                    .select("id, scheduled_at") \
                    .eq("connection_id", connection_id) \
                    .eq("status", "failed") \
                    .order("scheduled_at", desc=True) \
                    .execute()

                if failed_jobs.data and len(failed_jobs.data) > 5:
                    # Keep only the 5 most recent, delete the rest
                    jobs_to_delete = failed_jobs.data[5:]
                    job_ids = [job["id"] for job in jobs_to_delete]

                    self.supabase.supabase.table("automation_jobs") \
                        .delete() \
                        .in_("id", job_ids) \
                        .execute()

                    fix_results["fixes_successful"].append({
                        "fix": "cleanup_failed_jobs",
                        "message": f"Cleaned up {len(job_ids)} old failed jobs (kept 5 most recent)"
                    })
                else:
                    fix_results["fixes_successful"].append({
                        "fix": "cleanup_failed_jobs",
                        "message": "No cleanup needed - fewer than 5 failed jobs"
                    })

            except Exception as e:
                fix_results["fixes_failed"].append({
                    "fix": "cleanup_failed_jobs",
                    "error": str(e)
                })

            # Determine overall result
            if len(fix_results["fixes_failed"]) == 0:
                fix_results["overall_result"] = "success"
                fix_results["message"] = "All attempted fixes were successful"
            elif len(fix_results["fixes_successful"]) > len(fix_results["fixes_failed"]):
                fix_results["overall_result"] = "partial_success"
                fix_results[
                    "message"] = f"{len(fix_results['fixes_successful'])} fixes successful, {len(fix_results['fixes_failed'])} failed"
            else:
                fix_results["overall_result"] = "failed"
                fix_results[
                    "message"] = f"Most fixes failed ({len(fix_results['fixes_failed'])} failed, {len(fix_results['fixes_successful'])} successful)"

            return fix_results

        except Exception as e:
            logger.error(f"Error fixing automation issues: {str(e)}")
            return {
                "connection_id": connection_id,
                "overall_result": "error",
                "error": str(e),
                "message": "Failed to attempt fixes"
            }

    def create_comprehensive_report(self, connection_id: str) -> Dict[str, Any]:
        """Create a comprehensive diagnostic report"""
        try:
            logger.info(f"Creating comprehensive automation report for connection {connection_id}")

            report = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "executive_summary": {},
                "detailed_analysis": {},
                "test_results": {},
                "fix_attempts": {},
                "recommendations": []
            }

            # Run diagnosis
            diagnosis = self.diagnose_automation_issues(connection_id, days=3)
            report["detailed_analysis"] = diagnosis

            # Run tests
            test_results = self.test_storage_operations(connection_id)
            report["test_results"] = test_results

            # Attempt fixes if there are issues
            if len(diagnosis.get("issues_found", [])) > 0:
                fix_results = self.fix_common_issues(connection_id)
                report["fix_attempts"] = fix_results

            # Create executive summary
            issues_count = len(diagnosis.get("issues_found", []))
            critical_issues = len([i for i in diagnosis.get("issues_found", []) if i.get("severity") == "critical"])

            report["executive_summary"] = {
                "overall_health": "critical" if critical_issues > 0 else "warning" if issues_count > 0 else "healthy",
                "total_issues_found": issues_count,
                "critical_issues": critical_issues,
                "storage_tests_passed": test_results.get("overall_result") == "success",
                "fixes_attempted": len(report.get("fix_attempts", {}).get("fixes_attempted", [])),
                "fixes_successful": len(report.get("fix_attempts", {}).get("fixes_successful", [])),
                "needs_immediate_attention": critical_issues > 0,
                "recommendation": self._generate_executive_recommendation(diagnosis, test_results)
            }

            # Compile top recommendations
            report["recommendations"] = diagnosis.get("recommendations", [])[:5]  # Top 5 recommendations

            return report

        except Exception as e:
            logger.error(f"Error creating comprehensive report: {str(e)}")
            return {
                "connection_id": connection_id,
                "error": str(e),
                "message": "Failed to create comprehensive report"
            }

    def _generate_executive_recommendation(self, diagnosis: Dict, test_results: Dict) -> str:
        """Generate executive-level recommendation"""
        try:
            issues_count = len(diagnosis.get("issues_found", []))
            critical_issues = len([i for i in diagnosis.get("issues_found", []) if i.get("severity") == "critical"])
            tests_passed = test_results.get("overall_result") == "success"

            if critical_issues > 0:
                return "IMMEDIATE ACTION REQUIRED: Critical automation issues detected. Jobs are failing consistently."
            elif issues_count > 0 and not tests_passed:
                return "ACTION REQUIRED: Multiple automation issues detected. Storage and job execution problems."
            elif issues_count > 0:
                return "MONITOR: Some automation issues detected but basic functionality is working."
            elif tests_passed:
                return "HEALTHY: Automation system is working correctly."
            else:
                return "REVIEW NEEDED: Unable to determine automation health conclusively."

        except Exception:
            return "REVIEW NEEDED: Error generating recommendation."