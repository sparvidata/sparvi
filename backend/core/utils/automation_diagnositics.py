# backend/core/utils/automation_diagnostics.py

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

    def diagnose_metadata_storage_issues(self, connection_id: str, days: int = 3) -> Dict[str, Any]:
        """
        Diagnose metadata storage issues for a specific connection

        Args:
            connection_id: Connection ID to diagnose
            days: Number of days to analyze

        Returns:
            Dictionary with diagnostic results and recommendations
        """
        try:
            logger.info(f"Starting metadata storage diagnosis for connection {connection_id}")

            diagnosis = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "analysis_period_days": days,
                "issues_found": [],
                "recommendations": [],
                "storage_analysis": {},
                "job_analysis": {},
                "summary": {}
            }

            # Analyze recent automation jobs
            job_analysis = self._analyze_metadata_jobs(connection_id, days)
            diagnosis["job_analysis"] = job_analysis

            # Analyze storage patterns
            storage_analysis = self._analyze_storage_patterns(connection_id, days)
            diagnosis["storage_analysis"] = storage_analysis

            # Identify specific issues
            issues = self._identify_metadata_storage_issues(job_analysis, storage_analysis)
            diagnosis["issues_found"] = issues

            # Generate recommendations
            recommendations = self._generate_storage_recommendations(issues, job_analysis, storage_analysis)
            diagnosis["recommendations"] = recommendations

            # Create summary
            diagnosis["summary"] = {
                "total_issues": len(issues),
                "critical_issues": len([i for i in issues if i.get("severity") == "critical"]),
                "storage_health": self._determine_storage_health(storage_analysis),
                "needs_immediate_attention": any(i.get("severity") == "critical" for i in issues)
            }

            logger.info(f"Metadata storage diagnosis complete: {len(issues)} issues found")
            return diagnosis

        except Exception as e:
            logger.error(f"Error in metadata storage diagnosis: {str(e)}")
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "message": "Failed to complete metadata storage diagnosis"
            }

    def _analyze_metadata_jobs(self, connection_id: str, days: int) -> Dict[str, Any]:
        """Analyze recent metadata refresh jobs"""
        try:
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Get metadata refresh jobs
            response = self.supabase.supabase.table("automation_jobs") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .eq("job_type", "metadata_refresh") \
                .gte("scheduled_at", cutoff_date) \
                .order("scheduled_at", desc=True) \
                .execute()

            jobs = response.data or []

            analysis = {
                "total_jobs": len(jobs),
                "completed_jobs": 0,
                "failed_jobs": 0,
                "jobs_with_storage_issues": 0,
                "jobs_without_verification": 0,
                "storage_verification_failures": [],
                "metadata_types_processed": {}
            }

            for job in jobs:
                status = job.get("status")
                result_summary = job.get("result_summary", {})

                if status == "completed":
                    analysis["completed_jobs"] += 1

                    # Check for storage verification issues
                    storage_verified = True
                    for metadata_type, result in result_summary.items():
                        if isinstance(result, dict):
                            if metadata_type not in analysis["metadata_types_processed"]:
                                analysis["metadata_types_processed"][metadata_type] = {
                                    "attempts": 0,
                                    "successful": 0,
                                    "verified": 0
                                }

                            analysis["metadata_types_processed"][metadata_type]["attempts"] += 1

                            if result.get("status") == "completed":
                                analysis["metadata_types_processed"][metadata_type]["successful"] += 1

                                if result.get("verified", False):
                                    analysis["metadata_types_processed"][metadata_type]["verified"] += 1
                                else:
                                    storage_verified = False
                                    analysis["storage_verification_failures"].append({
                                        "job_id": job.get("id"),
                                        "metadata_type": metadata_type,
                                        "result": result,
                                        "scheduled_at": job.get("scheduled_at")
                                    })

                    if not storage_verified:
                        analysis["jobs_with_storage_issues"] += 1

                elif status == "failed":
                    analysis["failed_jobs"] += 1

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing metadata jobs: {str(e)}")
            return {"error": str(e)}

    def _analyze_storage_patterns(self, connection_id: str, days: int) -> Dict[str, Any]:
        """Analyze storage patterns for metadata"""
        try:
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            analysis = {
                "metadata_records": {},
                "storage_gaps": [],
                "data_quality_issues": []
            }

            # Check each metadata type
            for metadata_type in ["tables", "columns", "statistics"]:
                try:
                    response = self.supabase.supabase.table("connection_metadata") \
                        .select("id, metadata, collected_at") \
                        .eq("connection_id", connection_id) \
                        .eq("metadata_type", metadata_type) \
                        .gte("collected_at", cutoff_date) \
                        .order("collected_at", desc=True) \
                        .execute()

                    records = response.data or []

                    analysis["metadata_records"][metadata_type] = {
                        "total_records": len(records),
                        "latest_record": records[0] if records else None,
                        "record_frequency": self._calculate_record_frequency(records),
                        "data_quality": self._assess_data_quality(records, metadata_type)
                    }

                    # Check for storage gaps (expected records but missing)
                    if len(records) == 0:
                        analysis["storage_gaps"].append({
                            "metadata_type": metadata_type,
                            "issue": "No records found",
                            "severity": "critical"
                        })

                except Exception as e:
                    analysis["metadata_records"][metadata_type] = {
                        "error": str(e),
                        "total_records": 0
                    }

            return analysis

        except Exception as e:
            logger.error(f"Error analyzing storage patterns: {str(e)}")
            return {"error": str(e)}

    def _calculate_record_frequency(self, records: List[Dict]) -> Dict[str, Any]:
        """Calculate how frequently records are being stored"""
        if len(records) < 2:
            return {"frequency": "insufficient_data", "average_interval_hours": None}

        try:
            timestamps = []
            for record in records:
                timestamp_str = record.get("collected_at")
                if timestamp_str:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    timestamps.append(timestamp)

            if len(timestamps) < 2:
                return {"frequency": "insufficient_data", "average_interval_hours": None}

            # Calculate intervals between records
            intervals = []
            for i in range(len(timestamps) - 1):
                interval = abs((timestamps[i] - timestamps[i + 1]).total_seconds() / 3600)
                intervals.append(interval)

            average_interval = sum(intervals) / len(intervals)

            if average_interval < 6:
                frequency = "very_frequent"
            elif average_interval < 24:
                frequency = "frequent"
            elif average_interval < 72:
                frequency = "normal"
            else:
                frequency = "infrequent"

            return {
                "frequency": frequency,
                "average_interval_hours": average_interval,
                "min_interval_hours": min(intervals),
                "max_interval_hours": max(intervals)
            }

        except Exception as e:
            logger.warning(f"Error calculating record frequency: {str(e)}")
            return {"frequency": "error", "error": str(e)}

    def _assess_data_quality(self, records: List[Dict], metadata_type: str) -> Dict[str, Any]:
        """Assess the quality of stored metadata"""
        if not records:
            return {"quality": "no_data", "issues": ["No records to assess"]}

        issues = []
        latest_record = records[0]
        metadata = latest_record.get("metadata", {})

        try:
            if metadata_type == "tables":
                tables = metadata.get("tables", [])
                if len(tables) == 0:
                    issues.append("No tables found in latest record")
                elif len(tables) < 5:
                    issues.append(f"Very few tables found ({len(tables)})")

                # Check table data structure
                for table in tables[:5]:  # Check first 5 tables
                    if not isinstance(table, dict) or "name" not in table:
                        issues.append("Invalid table data structure")
                        break

            elif metadata_type == "columns":
                columns_by_table = metadata.get("columns_by_table", {})
                if len(columns_by_table) == 0:
                    issues.append("No columns found in latest record")

                # Check column data structure
                for table_name, columns in list(columns_by_table.items())[:3]:
                    if not isinstance(columns, list):
                        issues.append(f"Invalid columns data for table {table_name}")
                        continue

                    for column in columns[:3]:  # Check first 3 columns
                        if not isinstance(column, dict) or "name" not in column:
                            issues.append(f"Invalid column data structure in table {table_name}")
                            break

            elif metadata_type == "statistics":
                stats_by_table = metadata.get("statistics_by_table", {})
                if len(stats_by_table) == 0:
                    issues.append("No statistics found in latest record")

                # Check statistics data structure
                for table_name, stats in list(stats_by_table.items())[:3]:
                    if not isinstance(stats, dict):
                        issues.append(f"Invalid statistics data for table {table_name}")
                        continue

                    if "row_count" not in stats and "column_count" not in stats:
                        issues.append(f"Missing basic statistics for table {table_name}")

            # Determine overall quality
            if len(issues) == 0:
                quality = "good"
            elif len(issues) <= 2:
                quality = "acceptable"
            else:
                quality = "poor"

            return {
                "quality": quality,
                "issues": issues,
                "record_count": len(records)
            }

        except Exception as e:
            return {
                "quality": "error",
                "issues": [f"Error assessing data quality: {str(e)}"]
            }

    def _identify_metadata_storage_issues(self, job_analysis: Dict, storage_analysis: Dict) -> List[Dict]:
        """Identify specific metadata storage issues"""
        issues = []

        try:
            # Check for high job failure rates
            total_jobs = job_analysis.get("total_jobs", 0)
            failed_jobs = job_analysis.get("failed_jobs", 0)

            if total_jobs > 0:
                failure_rate = failed_jobs / total_jobs
                if failure_rate > 0.3:  # More than 30% failure rate
                    issues.append({
                        "type": "high_metadata_job_failure_rate",
                        "severity": "critical",
                        "description": f"High metadata job failure rate: {failure_rate:.1%}",
                        "details": {
                            "total_jobs": total_jobs,
                            "failed_jobs": failed_jobs,
                            "failure_rate": failure_rate
                        }
                    })

            # Check for storage verification failures
            storage_failures = len(job_analysis.get("storage_verification_failures", []))
            if storage_failures > 0:
                issues.append({
                    "type": "storage_verification_failures",
                    "severity": "critical",
                    "description": f"{storage_failures} metadata storage verification failures",
                    "details": job_analysis.get("storage_verification_failures", [])
                })

            # Check for missing metadata types
            metadata_types = job_analysis.get("metadata_types_processed", {})
            expected_types = ["tables", "columns", "statistics"]

            for expected_type in expected_types:
                if expected_type not in metadata_types:
                    issues.append({
                        "type": "missing_metadata_type",
                        "severity": "warning",
                        "description": f"No {expected_type} metadata processing found",
                        "details": {"missing_type": expected_type}
                    })
                else:
                    type_stats = metadata_types[expected_type]
                    success_rate = type_stats["successful"] / type_stats["attempts"] if type_stats[
                                                                                            "attempts"] > 0 else 0
                    verification_rate = type_stats["verified"] / type_stats["successful"] if type_stats[
                                                                                                 "successful"] > 0 else 0

                    if success_rate < 0.8:  # Less than 80% success rate
                        issues.append({
                            "type": "low_metadata_success_rate",
                            "severity": "critical",
                            "description": f"Low success rate for {expected_type} metadata: {success_rate:.1%}",
                            "details": {
                                "metadata_type": expected_type,
                                "success_rate": success_rate,
                                "stats": type_stats
                            }
                        })

                    if verification_rate < 0.9:  # Less than 90% verification rate
                        issues.append({
                            "type": "low_storage_verification_rate",
                            "severity": "critical",
                            "description": f"Low verification rate for {expected_type} metadata: {verification_rate:.1%}",
                            "details": {
                                "metadata_type": expected_type,
                                "verification_rate": verification_rate,
                                "stats": type_stats
                            }
                        })

            # Check storage patterns
            storage_records = storage_analysis.get("metadata_records", {})
            for metadata_type, record_info in storage_records.items():
                if record_info.get("total_records", 0) == 0:
                    issues.append({
                        "type": "no_stored_metadata",
                        "severity": "critical",
                        "description": f"No {metadata_type} metadata found in storage",
                        "details": {"metadata_type": metadata_type}
                    })

                data_quality = record_info.get("data_quality", {})
                if data_quality.get("quality") == "poor":
                    issues.append({
                        "type": "poor_data_quality",
                        "severity": "warning",
                        "description": f"Poor data quality for {metadata_type} metadata",
                        "details": {
                            "metadata_type": metadata_type,
                            "issues": data_quality.get("issues", [])
                        }
                    })

            return issues

        except Exception as e:
            logger.error(f"Error identifying metadata storage issues: {str(e)}")
            return [{"type": "diagnosis_error", "severity": "critical", "description": str(e)}]

    def _generate_storage_recommendations(self, issues: List[Dict], job_analysis: Dict, storage_analysis: Dict) -> List[
        Dict]:
        """Generate recommendations for fixing metadata storage issues"""
        recommendations = []

        try:
            for issue in issues:
                issue_type = issue.get("type")

                if issue_type == "high_metadata_job_failure_rate":
                    recommendations.append({
                        "priority": "critical",
                        "action": "investigate_metadata_job_failures",
                        "description": "Investigate and fix metadata job failures",
                        "steps": [
                            "Review error messages from failed metadata jobs",
                            "Check database connection stability",
                            "Verify metadata storage service initialization",
                            "Test manual metadata collection",
                            "Check Supabase connection and permissions"
                        ]
                    })

                elif issue_type == "storage_verification_failures":
                    recommendations.append({
                        "priority": "critical",
                        "action": "fix_metadata_storage_verification",
                        "description": "Fix metadata storage verification issues",
                        "steps": [
                            "Enable storage verification in metadata refresh jobs",
                            "Add retry logic for failed storage operations",
                            "Implement immediate verification after storage",
                            "Check Supabase table schema and permissions",
                            "Add detailed logging for storage operations"
                        ]
                    })

                elif issue_type == "no_stored_metadata":
                    recommendations.append({
                        "priority": "critical",
                        "action": "fix_metadata_storage_pipeline",
                        "description": f"Fix storage pipeline for {issue.get('details', {}).get('metadata_type')} metadata",
                        "steps": [
                            "Verify MetadataStorageService is properly initialized",
                            "Check connection_metadata table exists and is accessible",
                            "Test manual metadata storage operations",
                            "Verify data format before storage",
                            "Add enhanced error handling and logging"
                        ]
                    })

                elif issue_type in ["low_metadata_success_rate", "low_storage_verification_rate"]:
                    recommendations.append({
                        "priority": "high",
                        "action": "improve_metadata_reliability",
                        "description": f"Improve reliability for {issue.get('details', {}).get('metadata_type')} metadata",
                        "steps": [
                            "Add retry logic with exponential backoff",
                            "Implement data validation before storage",
                            "Add storage verification with immediate feedback",
                            "Improve error handling and recovery",
                            "Monitor storage operations with metrics"
                        ]
                    })

                elif issue_type == "poor_data_quality":
                    recommendations.append({
                        "priority": "medium",
                        "action": "improve_data_quality",
                        "description": f"Improve data quality for {issue.get('details', {}).get('metadata_type')} metadata",
                        "steps": [
                            "Add data validation before storage",
                            "Improve data cleaning and formatting",
                            "Handle edge cases in data collection",
                            "Add data quality checks and alerts",
                            "Review and update collection logic"
                        ]
                    })

            # Add general recommendations if there are issues
            if len(issues) > 0:
                recommendations.append({
                    "priority": "general",
                    "action": "implement_metadata_monitoring",
                    "description": "Implement comprehensive metadata storage monitoring",
                    "steps": [
                        "Add real-time storage verification",
                        "Implement metadata quality metrics",
                        "Set up alerting for storage failures",
                        "Create metadata storage dashboard",
                        "Add automated recovery procedures"
                    ]
                })

            return recommendations

        except Exception as e:
            logger.error(f"Error generating storage recommendations: {str(e)}")
            return [{
                "priority": "critical",
                "action": "fix_recommendation_system",
                "description": f"Fix error in recommendation system: {str(e)}"
            }]

    def _determine_storage_health(self, storage_analysis: Dict) -> str:
        """Determine overall storage health"""
        try:
            metadata_records = storage_analysis.get("metadata_records", {})

            if not metadata_records:
                return "unknown"

            health_scores = []
            for metadata_type, record_info in metadata_records.items():
                if record_info.get("error"):
                    health_scores.append(0)  # Error = 0 health
                elif record_info.get("total_records", 0) == 0:
                    health_scores.append(0)  # No records = 0 health
                else:
                    data_quality = record_info.get("data_quality", {})
                    quality = data_quality.get("quality", "unknown")

                    if quality == "good":
                        health_scores.append(100)
                    elif quality == "acceptable":
                        health_scores.append(70)
                    elif quality == "poor":
                        health_scores.append(30)
                    else:
                        health_scores.append(50)

            if not health_scores:
                return "unknown"

            average_health = sum(health_scores) / len(health_scores)

            if average_health >= 80:
                return "healthy"
            elif average_health >= 50:
                return "warning"
            else:
                return "critical"

        except Exception as e:
            logger.error(f"Error determining storage health: {str(e)}")
            return "unknown"

    def test_metadata_storage_operations(self, connection_id: str) -> Dict[str, Any]:
        """Test metadata storage operations to verify they work"""
        try:
            logger.info(f"Testing metadata storage operations for connection {connection_id}")

            test_results = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "tests": {},
                "overall_result": "unknown"
            }

            # Import the storage service
            from core.metadata.storage_service import MetadataStorageService
            storage_service = MetadataStorageService()

            # Test tables metadata storage
            try:
                test_tables = [
                    {"name": "test_table_1", "id": "test_table_1"},
                    {"name": "test_table_2", "id": "test_table_2", "column_count": 5}
                ]

                success = storage_service.store_tables_metadata(
                    connection_id,
                    test_tables,
                    max_retries=2,
                    verify_storage=True
                )

                test_results["tests"]["tables_storage"] = {
                    "status": "success" if success else "failed",
                    "message": "Successfully stored and verified test tables" if success else "Failed to store test tables"
                }

            except Exception as e:
                test_results["tests"]["tables_storage"] = {
                    "status": "failed",
                    "error": str(e)
                }

            # Test columns metadata storage
            try:
                test_columns = {
                    "test_table": [
                        {"name": "id", "type": "INTEGER", "nullable": False},
                        {"name": "name", "type": "VARCHAR(255)", "nullable": True}
                    ]
                }

                success = storage_service.store_columns_metadata(
                    connection_id,
                    test_columns,
                    max_retries=2,
                    verify_storage=True
                )

                test_results["tests"]["columns_storage"] = {
                    "status": "success" if success else "failed",
                    "message": "Successfully stored and verified test columns" if success else "Failed to store test columns"
                }

            except Exception as e:
                test_results["tests"]["columns_storage"] = {
                    "status": "failed",
                    "error": str(e)
                }

            # Test statistics metadata storage
            try:
                test_stats = {
                    "test_table": {
                        "table_name": "test_table",
                        "row_count": 1000,
                        "column_count": 5,
                        "has_primary_key": True,
                        "health_score": 85,
                        "collected_at": datetime.now(timezone.utc).isoformat()
                    }
                }

                success = storage_service.store_statistics_metadata(
                    connection_id,
                    test_stats,
                    max_retries=2,
                    verify_storage=True
                )

                test_results["tests"]["statistics_storage"] = {
                    "status": "success" if success else "failed",
                    "message": "Successfully stored and verified test statistics" if success else "Failed to store test statistics"
                }

            except Exception as e:
                test_results["tests"]["statistics_storage"] = {
                    "status": "failed",
                    "error": str(e)
                }

            # Determine overall result
            failed_tests = [t for t in test_results["tests"].values() if t["status"] == "failed"]

            if not failed_tests:
                test_results["overall_result"] = "success"
                test_results["message"] = "All metadata storage operations are working correctly"
            else:
                test_results["overall_result"] = "failed"
                test_results[
                    "message"] = f"{len(failed_tests)} out of {len(test_results['tests'])} storage operations failed"

            return test_results

        except Exception as e:
            logger.error(f"Error testing metadata storage operations: {str(e)}")
            return {
                "connection_id": connection_id,
                "overall_result": "error",
                "error": str(e),
                "message": "Failed to test metadata storage operations"
            }

    def fix_common_metadata_storage_issues(self, connection_id: str) -> Dict[str, Any]:
        """Attempt to fix common metadata storage issues"""
        try:
            logger.info(f"Attempting to fix metadata storage issues for connection {connection_id}")

            fix_results = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "connection_id": connection_id,
                "fixes_attempted": [],
                "fixes_successful": [],
                "fixes_failed": [],
                "overall_result": "unknown"
            }

            # Fix 1: Verify and recreate storage service
            try:
                fix_results["fixes_attempted"].append("recreate_storage_service")

                from core.metadata.storage_service import MetadataStorageService
                storage_service = MetadataStorageService()

                # Test basic connectivity
                response = storage_service.supabase.table("connection_metadata").select("id").limit(1).execute()

                fix_results["fixes_successful"].append({
                    "fix": "recreate_storage_service",
                    "message": "Successfully recreated and tested storage service"
                })

            except Exception as e:
                fix_results["fixes_failed"].append({
                    "fix": "recreate_storage_service",
                    "error": str(e)
                })

            # Fix 2: Clean up any corrupted metadata records
            try:
                fix_results["fixes_attempted"].append("cleanup_corrupted_records")

                # Find records with invalid metadata
                response = storage_service.supabase.table("connection_metadata") \
                    .select("id, metadata") \
                    .eq("connection_id", connection_id) \
                    .execute()

                corrupted_records = []
                for record in response.data or []:
                    metadata = record.get("metadata")
                    if not metadata or not isinstance(metadata, dict):
                        corrupted_records.append(record["id"])

                if corrupted_records:
                    # Delete corrupted records
                    storage_service.supabase.table("connection_metadata") \
                        .delete() \
                        .in_("id", corrupted_records) \
                        .execute()

                fix_results["fixes_successful"].append({
                    "fix": "cleanup_corrupted_records",
                    "message": f"Cleaned up {len(corrupted_records)} corrupted records"
                })

            except Exception as e:
                fix_results["fixes_failed"].append({
                    "fix": "cleanup_corrupted_records",
                    "error": str(e)
                })

            # Fix 3: Test storage operations
            try:
                fix_results["fixes_attempted"].append("test_storage_operations")

                test_results = self.test_metadata_storage_operations(connection_id)

                if test_results.get("overall_result") == "success":
                    fix_results["fixes_successful"].append({
                        "fix": "test_storage_operations",
                        "message": "Storage operations are working correctly"
                    })
                else:
                    fix_results["fixes_failed"].append({
                        "fix": "test_storage_operations",
                        "error": test_results.get("message", "Storage test failed")
                    })

            except Exception as e:
                fix_results["fixes_failed"].append({
                    "fix": "test_storage_operations",
                    "error": str(e)
                })

            # Fix 4: Reset automation configuration if needed
            try:
                fix_results["fixes_attempted"].append("reset_automation_config")

                # Check if automation is configured
                config_response = storage_service.supabase.table("automation_connection_configs") \
                    .select("*") \
                    .eq("connection_id", connection_id) \
                    .execute()

                if not config_response.data:
                    # Create basic automation config
                    basic_config = {
                        "connection_id": connection_id,
                        "metadata_refresh": {
                            "enabled": True,
                            "interval_hours": 24,
                            "types": ["tables", "columns"]
                        },
                        "schema_change_detection": {
                            "enabled": False,
                            "interval_hours": 12
                        },
                        "validation_automation": {
                            "enabled": False,
                            "interval_hours": 24
                        },
                        "created_at": datetime.now(timezone.utc).isoformat()
                    }

                    storage_service.supabase.table("automation_connection_configs") \
                        .insert(basic_config) \
                        .execute()

                fix_results["fixes_successful"].append({
                    "fix": "reset_automation_config",
                    "message": "Created basic automation configuration"
                })

            except Exception as e:
                fix_results["fixes_failed"].append({
                    "fix": "reset_automation_config",
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
            logger.error(f"Error fixing metadata storage issues: {str(e)}")
            return {
                "connection_id": connection_id,
                "overall_result": "error",
                "error": str(e),
                "message": "Failed to attempt fixes"
            }

    def create_comprehensive_report(self, connection_id: str) -> Dict[str, Any]:
        """Create a comprehensive diagnostic report for metadata storage"""
        try:
            logger.info(f"Creating comprehensive metadata storage report for connection {connection_id}")

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
            diagnosis = self.diagnose_metadata_storage_issues(connection_id, days=7)
            report["detailed_analysis"] = diagnosis

            # Run tests
            test_results = self.test_metadata_storage_operations(connection_id)
            report["test_results"] = test_results

            # Attempt fixes if there are issues
            if len(diagnosis.get("issues_found", [])) > 0:
                fix_results = self.fix_common_metadata_storage_issues(connection_id)
                report["fix_attempts"] = fix_results

            # Create executive summary
            issues_count = len(diagnosis.get("issues_found", []))
            critical_issues = len([i for i in diagnosis.get("issues_found", []) if i.get("severity") == "critical"])

            report["executive_summary"] = {
                "overall_health": diagnosis.get("summary", {}).get("storage_health", "unknown"),
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
                return "IMMEDIATE ACTION REQUIRED: Critical metadata storage issues detected. Automation may not be storing results properly."
            elif issues_count > 0 and not tests_passed:
                return "ACTION REQUIRED: Multiple metadata storage issues detected. Review and fix recommended."
            elif issues_count > 0:
                return "MONITOR: Some metadata storage issues detected but basic functionality is working."
            elif tests_passed:
                return "HEALTHY: Metadata storage is working correctly."
            else:
                return "REVIEW NEEDED: Unable to determine storage health conclusively."

        except Exception:
            return "REVIEW NEEDED: Error generating recommendation."