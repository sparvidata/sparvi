import logging
import traceback
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone

from core.storage.supabase_manager import SupabaseManager
from .scheduler import AutomationScheduler
from .events import AutomationEventType, publish_automation_event

logger = logging.getLogger(__name__)


class AutomationAPI:
    """API for managing automation configurations and jobs"""

    def __init__(self):
        self.supabase = SupabaseManager()
        self.scheduler = AutomationScheduler()

    # Global Configuration Management
    def get_global_config(self) -> Dict[str, Any]:
        """Get global automation configuration"""
        try:
            response = self.supabase.supabase.table("automation_global_config") \
                .select("*") \
                .order("created_at", desc=True) \
                .limit(1) \
                .execute()

            if response.data and len(response.data) > 0:
                return response.data[0]

            # Return default config if none exists
            return {
                "automation_enabled": True,
                "max_concurrent_jobs": 3,
                "default_retry_attempts": 2,
                "notification_settings": {}
            }

        except Exception as e:
            logger.error(f"Error getting global config: {str(e)}")
            return {"error": str(e)}

    def update_global_config(self, config_data: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        """Update global automation configuration"""
        try:
            # Clean the config_data to only include valid table columns
            clean_config_data = {
                "automation_enabled": config_data.get("automation_enabled", True),
                "max_concurrent_jobs": config_data.get("max_concurrent_jobs", 3),
                "default_retry_attempts": config_data.get("default_retry_attempts", 2),
                "notification_settings": config_data.get("notification_settings", {}),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            # Check if config exists
            existing = self.supabase.supabase.table("automation_global_config") \
                .select("id") \
                .execute()

            if existing.data and len(existing.data) > 0:
                # Update existing
                response = self.supabase.supabase.table("automation_global_config") \
                    .update(clean_config_data) \
                    .eq("id", existing.data[0]["id"]) \
                    .execute()
            else:
                # Create new
                clean_config_data["created_at"] = datetime.now(timezone.utc).isoformat()
                response = self.supabase.supabase.table("automation_global_config") \
                    .insert(clean_config_data) \
                    .execute()

            if response.data:
                return response.data[0]
            else:
                raise Exception("Failed to update global config")

        except Exception as e:
            logger.error(f"Error updating global config: {str(e)}")
            return {"error": str(e)}

    def toggle_global_automation(self, enabled: bool, user_id: str) -> Dict[str, Any]:
        """Toggle global automation on/off"""
        try:
            return self.update_global_config({"automation_enabled": enabled}, user_id)
        except Exception as e:
            logger.error(f"Error toggling automation: {str(e)}")
            return {"error": str(e)}

    # Connection-Level Configuration
    def get_connection_configs(self, organization_id: str) -> List[Dict[str, Any]]:
        """Get all connection configurations for an organization"""
        try:
            # Get all connections for the organization first
            connections_response = self.supabase.supabase.table("database_connections") \
                .select("id, name, connection_type") \
                .eq("organization_id", organization_id) \
                .execute()

            if not connections_response.data:
                return []

            connection_ids = [conn["id"] for conn in connections_response.data]

            # Create a lookup dict for connection info
            connections_lookup = {conn["id"]: conn for conn in connections_response.data}

            # Get automation configs for these connections (without join)
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*") \
                .in_("connection_id", connection_ids) \
                .execute()

            # Manually add connection info to avoid join issues
            configs = response.data if response.data else []
            for config in configs:
                connection_info = connections_lookup.get(config["connection_id"])
                if connection_info:
                    config["connection_name"] = connection_info["name"]
                    config["connection_type"] = connection_info["connection_type"]

            return configs

        except Exception as e:
            logger.error(f"Error getting connection configs: {str(e)}")
            return []

    def get_connection_config(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get automation configuration for a specific connection"""
        try:
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .execute()

            if response.data and len(response.data) > 0:
                return response.data[0]

            # Return default config if none exists
            return {
                "connection_id": connection_id,
                "metadata_refresh": {
                    "enabled": False,
                    "interval_hours": 24,
                    "types": ["tables", "columns", "statistics"]
                },
                "schema_change_detection": {
                    "enabled": False,
                    "interval_hours": 6,
                    "auto_acknowledge_safe_changes": False
                },
                "validation_automation": {
                    "enabled": False,
                    "interval_hours": 12,
                    "auto_generate_for_new_tables": True
                }
            }

        except Exception as e:
            logger.error(f"Error getting connection config: {str(e)}")
            return None

    def update_connection_config(self, connection_id: str, config_data: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        """Update automation configuration for a connection"""
        try:
            # Clean the config_data to only include valid table columns
            clean_config_data = {
                "metadata_refresh": config_data.get("metadata_refresh", {}),
                "schema_change_detection": config_data.get("schema_change_detection", {}),
                "validation_automation": config_data.get("validation_automation", {}),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            # Check if config exists
            existing = self.supabase.supabase.table("automation_connection_configs") \
                .select("id") \
                .eq("connection_id", connection_id) \
                .execute()

            if existing.data and len(existing.data) > 0:
                # Update existing
                response = self.supabase.supabase.table("automation_connection_configs") \
                    .update(clean_config_data) \
                    .eq("connection_id", connection_id) \
                    .execute()
            else:
                # Create new
                clean_config_data["connection_id"] = connection_id
                clean_config_data["created_at"] = datetime.now(timezone.utc).isoformat()
                response = self.supabase.supabase.table("automation_connection_configs") \
                    .insert(clean_config_data) \
                    .execute()

            if response.data:
                # Update scheduler with new configuration
                self.scheduler.update_connection_schedule(connection_id, response.data[0])

                # Publish event
                publish_automation_event(
                    event_type=AutomationEventType.CONFIG_UPDATED,
                    data=response.data[0],
                    connection_id=connection_id,
                    user_id=user_id
                )

                return response.data[0]
            else:
                raise Exception("Failed to update connection config")

        except Exception as e:
            logger.error(f"Error updating connection config: {str(e)}")
            return {"error": str(e)}

    # Table-Level Configuration
    def get_table_config(self, connection_id: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Get automation configuration for a specific table"""
        try:
            response = self.supabase.supabase.table("automation_table_configs") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .eq("table_name", table_name) \
                .execute()

            if response.data and len(response.data) > 0:
                return response.data[0]

            # Return default config if none exists
            return {
                "connection_id": connection_id,
                "table_name": table_name,
                "auto_run_validations": False,
                "auto_run_interval_hours": 24,
                "validation_notification_threshold": "failures_only",
                "custom_schedule": {}
            }

        except Exception as e:
            logger.error(f"Error getting table config: {str(e)}")
            return None

    def update_table_config(self, connection_id: str, table_name: str, config_data: Dict[str, Any], user_id: str) -> \
    Dict[str, Any]:
        """Update automation configuration for a table"""
        try:
            # Clean the config_data to only include valid table columns
            clean_config_data = {
                "auto_run_validations": config_data.get("auto_run_validations", False),
                "auto_run_interval_hours": config_data.get("auto_run_interval_hours", 24),
                "validation_notification_threshold": config_data.get("validation_notification_threshold",
                                                                     "failures_only"),
                "custom_schedule": config_data.get("custom_schedule", {}),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            # Check if config exists
            existing = self.supabase.supabase.table("automation_table_configs") \
                .select("id") \
                .eq("connection_id", connection_id) \
                .eq("table_name", table_name) \
                .execute()

            if existing.data and len(existing.data) > 0:
                # Update existing
                response = self.supabase.supabase.table("automation_table_configs") \
                    .update(clean_config_data) \
                    .eq("connection_id", connection_id) \
                    .eq("table_name", table_name) \
                    .execute()
            else:
                # Create new
                clean_config_data.update({
                    "connection_id": connection_id,
                    "table_name": table_name,
                    "created_at": datetime.now(timezone.utc).isoformat()
                })
                response = self.supabase.supabase.table("automation_table_configs") \
                    .insert(clean_config_data) \
                    .execute()

            if response.data:
                return response.data[0]
            else:
                raise Exception("Failed to update table config")

        except Exception as e:
            logger.error(f"Error updating table config: {str(e)}")
            return {"error": str(e)}

    # Status and Monitoring
    def get_automation_status(self, organization_id: str = None, connection_id: str = None) -> Dict[str, Any]:
        """Get automation system status"""
        try:
            status = {
                "global_enabled": True,
                "active_jobs": 0,
                "pending_jobs": 0,
                "failed_jobs_24h": 0,
                "last_run": None,
                "connections": []
            }

            # Get global config
            global_config = self.get_global_config()
            status["global_enabled"] = global_config.get("automation_enabled", True)

            # Build job query
            job_query = self.supabase.supabase.table("automation_jobs").select("*")

            if connection_id:
                job_query = job_query.eq("connection_id", connection_id)
            elif organization_id:
                # Filter by organization's connections
                connections_response = self.supabase.supabase.table("database_connections") \
                    .select("id") \
                    .eq("organization_id", organization_id) \
                    .execute()

                if connections_response.data:
                    connection_ids = [conn["id"] for conn in connections_response.data]
                    job_query = job_query.in_("connection_id", connection_ids)

            # Get job statistics
            jobs_response = job_query.execute()

            if jobs_response.data:
                jobs = jobs_response.data
                status["active_jobs"] = len([j for j in jobs if j["status"] == "running"])
                status["pending_jobs"] = len([j for j in jobs if j["status"] == "scheduled"])

                # Failed jobs in last 24 hours
                yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
                status["failed_jobs_24h"] = len([
                    j for j in jobs
                    if j["status"] == "failed" and j.get("created_at", "") >= yesterday
                ])

                # Last run time
                completed_jobs = [j for j in jobs if j["status"] == "completed"]
                if completed_jobs:
                    status["last_run"] = max(completed_jobs, key=lambda x: x.get("completed_at", ""))["completed_at"]

            return status

        except Exception as e:
            logger.error(f"Error getting automation status: {str(e)}")
            return {"error": str(e)}

    def get_jobs(self, connection_id: str = None, status: str = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Get automation jobs"""
        try:
            query = self.supabase.supabase.table("automation_jobs").select("*")

            if connection_id:
                query = query.eq("connection_id", connection_id)

            if status:
                query = query.eq("status", status)

            response = query.order("created_at", desc=True).limit(limit).execute()
            return response.data if response.data else []

        except Exception as e:
            logger.error(f"Error getting jobs: {str(e)}")
            return []

    # Control Operations
    def toggle_connection_automation(self, connection_id: str, enabled: bool, user_id: str) -> Dict[str, Any]:
        """Toggle automation for a specific connection"""
        try:
            config = self.get_connection_config(connection_id)
            if not config:
                return {"error": "Connection config not found"}

            # Update all automation types
            config["metadata_refresh"]["enabled"] = enabled
            config["schema_change_detection"]["enabled"] = enabled
            config["validation_automation"]["enabled"] = enabled

            return self.update_connection_config(connection_id, config, user_id)

        except Exception as e:
            logger.error(f"Error toggling connection automation: {str(e)}")
            return {"error": str(e)}

    def trigger_automation(self, connection_id: str, automation_type: str = None, user_id: str = None) -> Dict[
        str, Any]:
        """Manually trigger automation for a connection"""
        try:
            # Schedule immediate automation run
            result = self.scheduler.schedule_immediate_run(
                connection_id=connection_id,
                automation_type=automation_type,
                trigger_user=user_id
            )

            if result.get("success"):
                # Publish event
                publish_automation_event(
                    event_type=AutomationEventType.MANUAL_TRIGGER,
                    data={
                        "automation_type": automation_type,
                        "job_id": result.get("job_id")
                    },
                    connection_id=connection_id,
                    user_id=user_id
                )

            return result

        except Exception as e:
            logger.error(f"Error triggering automation: {str(e)}")
            return {"error": str(e)}

    def cancel_job(self, job_id: str, user_id: str) -> Dict[str, Any]:
        """Cancel a scheduled or running automation job"""
        try:
            # Update job status to cancelled
            response = self.supabase.supabase.table("automation_jobs") \
                .update({"status": "cancelled", "completed_at": datetime.now(timezone.utc).isoformat()}) \
                .eq("id", job_id) \
                .execute()

            if response.data:
                # Notify scheduler to cancel the job
                self.scheduler.cancel_job(job_id)
                return {"success": True, "message": "Job cancelled successfully"}
            else:
                return {"error": "Job not found"}

        except Exception as e:
            logger.error(f"Error cancelling job: {str(e)}")
            return {"error": str(e)}

    def get_next_run_times(self, organization_id: str, connection_id: str) -> Dict[str, Any]:
        """
        Get next estimated run times for all automation types for a connection
        Enhanced with better error handling and logging
        """
        try:
            logger.info(f"Getting next run times for connection {connection_id} in organization {organization_id}")

            # First verify the connection exists and belongs to the organization
            connection_response = self.supabase.supabase.table("database_connections") \
                .select("id, name") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                logger.warning(f"Connection {connection_id} not found for organization {organization_id}")
                return {
                    "connection_id": connection_id,
                    "next_runs": {},
                    "error": "Connection not found"
                }

            connection_name = connection_response.data[0]["name"]
            logger.info(f"Found connection: {connection_name}")

            # Get connection config
            config_response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .execute()

            if not config_response.data:
                logger.info(f"No automation configuration found for connection {connection_id}")
                return {
                    "connection_id": connection_id,
                    "connection_name": connection_name,
                    "next_runs": {},
                    "message": "No automation configuration found"
                }

            config = config_response.data[0]
            logger.info(f"Found automation config for connection {connection_id}")

            # Get recent jobs to determine last run times
            try:
                recent_jobs_response = self.supabase.supabase.table("automation_jobs") \
                    .select("job_type, status, created_at, completed_at, started_at") \
                    .eq("connection_id", connection_id) \
                    .in_("status", ["completed", "failed", "running"]) \
                    .gte("created_at", (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()) \
                    .order("created_at", desc=True) \
                    .execute()

                jobs_by_type = {}
                if recent_jobs_response.data:
                    for job in recent_jobs_response.data:
                        job_type = job.get("job_type")
                        if job_type:
                            if job_type not in jobs_by_type:
                                jobs_by_type[job_type] = []
                            jobs_by_type[job_type].append(job)

                logger.info(f"Found {len(recent_jobs_response.data or [])} recent jobs for connection {connection_id}")

            except Exception as jobs_error:
                logger.warning(f"Error fetching recent jobs for connection {connection_id}: {str(jobs_error)}")
                jobs_by_type = {}

            # Calculate next run times for each automation type
            next_runs = {}
            automation_types = [
                ("metadata_refresh", "metadata_refresh"),
                ("schema_change_detection", "schema_change_detection"),
                ("validation_automation", "validation_automation")
            ]

            for config_key, job_type in automation_types:
                try:
                    automation_config = config.get(config_key, {})

                    if automation_config and automation_config.get("enabled", False):
                        logger.info(f"Calculating next run for {job_type}")

                        next_run_info = self._calculate_next_run_simple(
                            automation_config,
                            jobs_by_type.get(job_type, []),
                            job_type
                        )

                        if next_run_info:
                            next_runs[job_type] = next_run_info
                            logger.info(f"Next run for {job_type}: {next_run_info.get('time_until_next', 'unknown')}")
                    else:
                        logger.info(f"Automation {job_type} is disabled for connection {connection_id}")

                except Exception as calc_error:
                    logger.error(f"Error calculating next run for {job_type}: {str(calc_error)}")
                    # Continue with other automation types
                    continue

            result = {
                "connection_id": connection_id,
                "connection_name": connection_name,
                "next_runs": next_runs,
                "generated_at": datetime.now(timezone.utc).isoformat()
            }

            logger.info(
                f"Successfully calculated next runs for connection {connection_id}: {len(next_runs)} automation types")
            return result

        except Exception as e:
            logger.error(f"Error getting next run times for connection {connection_id}: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return {
                "connection_id": connection_id,
                "next_runs": {},
                "error": str(e)
            }

    def _calculate_next_run_simple(self, config: Dict[str, Any], recent_jobs: List[Dict[str, Any]],
                                   job_type: str) -> Dict[str, Any]:
        """
        Simplified calculation for next run times with better error handling
        """
        try:
            if not config or not isinstance(config, dict):
                logger.warning(f"Invalid config for job type {job_type}")
                return None

            interval_hours = config.get("interval_hours", 24)
            if not isinstance(interval_hours, (int, float)) or interval_hours <= 0:
                logger.warning(f"Invalid interval_hours for job type {job_type}: {interval_hours}")
                interval_hours = 24

            interval_seconds = interval_hours * 3600
            now = datetime.now(timezone.utc)

            next_run_info = {
                "enabled": True,
                "interval_hours": interval_hours,
                "next_run_timestamp": None,
                "next_run_iso": None,
                "time_until_next": None,
                "is_overdue": False,
                "last_run": None,
                "last_run_status": None,
                "currently_running": False,
                "calculation_method": None
            }

            # Find running jobs
            running_jobs = [job for job in recent_jobs if job.get("status") == "running"]
            if running_jobs:
                next_run_info["currently_running"] = True
                # Estimate next run after current job completes
                next_run_time = now + timedelta(minutes=10) + timedelta(seconds=interval_seconds)
                next_run_info.update({
                    "next_run_timestamp": next_run_time.timestamp(),
                    "next_run_iso": next_run_time.isoformat(),
                    "time_until_next": self._format_time_until(next_run_time, now),
                    "calculation_method": "from_running_job"
                })
                return next_run_info

            # Find completed jobs
            completed_jobs = [job for job in recent_jobs if job.get("status") in ["completed", "failed"]]
            if completed_jobs:
                # Use most recent completed job
                last_job = completed_jobs[0]
                last_run_time_str = last_job.get("completed_at") or last_job.get("created_at")

                if last_run_time_str:
                    try:
                        last_run_time = datetime.fromisoformat(last_run_time_str.replace('Z', '+00:00'))
                        next_run_time = last_run_time + timedelta(seconds=interval_seconds)

                        is_overdue = next_run_time < now

                        next_run_info.update({
                            "next_run_timestamp": next_run_time.timestamp(),
                            "next_run_iso": next_run_time.isoformat(),
                            "time_until_next": self._format_time_until(next_run_time, now),
                            "is_overdue": is_overdue,
                            "last_run": last_run_time_str,
                            "last_run_status": last_job.get("status"),
                            "calculation_method": "from_last_completion"
                        })
                        return next_run_info

                    except Exception as time_error:
                        logger.warning(f"Error parsing job completion time for {job_type}: {str(time_error)}")

            # No job history - estimate based on current time
            next_run_time = now + timedelta(seconds=interval_seconds)
            next_run_info.update({
                "next_run_timestamp": next_run_time.timestamp(),
                "next_run_iso": next_run_time.isoformat(),
                "time_until_next": self._format_time_until(next_run_time, now),
                "calculation_method": "current_time_estimate"
            })

            return next_run_info

        except Exception as e:
            logger.error(f"Error calculating next run for {job_type}: {str(e)}")
            return None

    def _get_automation_enable_history(self, connection_id: str) -> Dict[str, Any]:
        """
        Get history of when each automation type was enabled
        This could come from audit logs, or we can approximate from config updates
        """
        try:
            # For now, we'll use a simple approach - look at recent config updates
            # In a more sophisticated system, you'd have an audit log of enable/disable events

            # If you want to track enable times more precisely, you could:
            # 1. Add individual timestamps for when each automation was enabled
            # 2. Create an automation_events table to track enable/disable history
            # 3. Use the updated_at field as a proxy for "last configuration change"

            # For this implementation, we'll return empty dict and rely on config timestamps
            return {}

        except Exception as e:
            logger.warning(f"Error getting automation enable history: {str(e)}")
            return {}

    def _calculate_next_run_enhanced(self, config: Dict[str, Any], recent_jobs: List[Dict[str, Any]],
                                     job_type: str, enable_time: str = None) -> Dict[str, Any]:
        """
        Enhanced calculation that considers automation enable time

        Args:
            config: Automation configuration
            recent_jobs: List of recent jobs for this automation type
            job_type: Type of automation
            enable_time: When this automation was enabled (ISO string)

        Returns:
            Next run information or None if calculation fails
        """
        try:
            if not config or not isinstance(config, dict):
                logger.warning(f"Invalid config for job type {job_type}")
                return None

            interval_hours = config.get("interval_hours", 24)
            if not isinstance(interval_hours, (int, float)) or interval_hours <= 0:
                logger.warning(f"Invalid interval_hours for job type {job_type}: {interval_hours}")
                interval_hours = 24

            interval_seconds = interval_hours * 3600
            now = datetime.now(timezone.utc)

            # Find the most recent completed or running job
            completed_jobs = []
            running_jobs = []

            if recent_jobs and isinstance(recent_jobs, list):
                for job in recent_jobs:
                    if not isinstance(job, dict):
                        continue

                    job_status = job.get("status")
                    if job_status in ["completed", "failed"]:
                        completed_jobs.append(job)
                    elif job_status == "running":
                        running_jobs.append(job)

            next_run_info = {
                "enabled": True,
                "interval_hours": interval_hours,
                "next_run_timestamp": None,
                "next_run_iso": None,
                "time_until_next": None,
                "is_overdue": False,
                "last_run": None,
                "last_run_status": None,
                "currently_running": len(running_jobs) > 0,
                "calculation_method": None  # Track how we calculated this
            }

            # Priority 1: If there's a currently running job
            if running_jobs:
                running_job = running_jobs[0]
                if running_job.get("started_at"):
                    try:
                        started_time = datetime.fromisoformat(running_job["started_at"].replace('Z', '+00:00'))
                        # Estimate completion in 10 minutes, then add interval
                        estimated_completion = started_time + timedelta(minutes=10)
                        next_run_time = estimated_completion + timedelta(seconds=interval_seconds)
                    except Exception:
                        next_run_time = now + timedelta(minutes=10) + timedelta(seconds=interval_seconds)
                else:
                    next_run_time = now + timedelta(minutes=10) + timedelta(seconds=interval_seconds)

                next_run_info.update({
                    "next_run_timestamp": next_run_time.timestamp(),
                    "next_run_iso": next_run_time.isoformat(),
                    "time_until_next": self._format_time_until(next_run_time, now),
                    "currently_running": True,
                    "last_run_status": "running",
                    "calculation_method": "from_running_job"
                })

            # Priority 2: If we have completed jobs, use the last completion time
            elif completed_jobs:
                last_job = completed_jobs[0]
                last_run_time_str = last_job.get("completed_at") or last_job.get("created_at")

                if last_run_time_str:
                    try:
                        last_run_time = datetime.fromisoformat(last_run_time_str.replace('Z', '+00:00'))
                        next_run_time = last_run_time + timedelta(seconds=interval_seconds)

                        is_overdue = next_run_time < now

                        next_run_info.update({
                            "next_run_timestamp": next_run_time.timestamp(),
                            "next_run_iso": next_run_time.isoformat(),
                            "time_until_next": self._format_time_until(next_run_time, now),
                            "is_overdue": is_overdue,
                            "last_run": last_run_time_str,
                            "last_run_status": last_job.get("status"),
                            "calculation_method": "from_last_completion"
                        })
                    except Exception as time_error:
                        logger.warning(f"Error parsing job completion time for {job_type}: {str(time_error)}")
                        # Fall through to enable time calculation
                        completed_jobs = []
                else:
                    # No valid completion time, fall through to enable time calculation
                    completed_jobs = []

            # Priority 3: No job history - calculate from when automation was enabled
            if not running_jobs and not completed_jobs:
                enable_timestamp = None
                calculation_method = "from_enable_time"

                # Try to get enable time from various sources
                if enable_time:
                    try:
                        enable_timestamp = datetime.fromisoformat(enable_time.replace('Z', '+00:00'))
                        calculation_method = "from_explicit_enable_time"
                    except Exception:
                        pass

                # If no explicit enable time, try to infer from first job creation
                # (This helps if jobs were created but failed/cancelled)
                if not enable_timestamp and recent_jobs:
                    try:
                        # Use the oldest job creation time as a proxy for enable time
                        oldest_job = min(recent_jobs, key=lambda x: x.get("created_at", ""))
                        oldest_job_time = oldest_job.get("created_at")
                        if oldest_job_time:
                            enable_timestamp = datetime.fromisoformat(oldest_job_time.replace('Z', '+00:00'))
                            calculation_method = "from_first_job_creation"
                    except Exception:
                        pass

                # Calculate next run from enable time
                if enable_timestamp:
                    # Calculate what the next run should be based on enable time + intervals
                    intervals_since_enable = int((now - enable_timestamp).total_seconds() / interval_seconds)
                    next_run_time = enable_timestamp + timedelta(
                        seconds=(intervals_since_enable + 1) * interval_seconds)

                    # Check if we're overdue (next run time has passed)
                    is_overdue = next_run_time < now

                    next_run_info.update({
                        "next_run_timestamp": next_run_time.timestamp(),
                        "next_run_iso": next_run_time.isoformat(),
                        "time_until_next": self._format_time_until(next_run_time, now),
                        "is_overdue": is_overdue,
                        "last_run": None,
                        "last_run_status": None,
                        "calculation_method": calculation_method,
                        "enabled_at": enable_time
                    })
                else:
                    # Absolute fallback - current time + interval
                    next_run_time = now + timedelta(seconds=interval_seconds)
                    next_run_info.update({
                        "next_run_timestamp": next_run_time.timestamp(),
                        "next_run_iso": next_run_time.isoformat(),
                        "time_until_next": self._format_time_until(next_run_time, now),
                        "last_run": None,
                        "last_run_status": None,
                        "calculation_method": "current_time_fallback"
                    })

            return next_run_info

        except Exception as e:
            logger.error(f"Error calculating enhanced next run for {job_type}: {str(e)}")
            return None

    def _calculate_next_run(self, config: Dict[str, Any], recent_jobs: List[Dict[str, Any]], job_type: str) -> Dict[
        str, Any]:
        """
        Calculate next run time for a specific automation type

        Args:
            config: Automation configuration
            recent_jobs: List of recent jobs for this automation type
            job_type: Type of automation

        Returns:
            Next run information
        """
        interval_hours = config.get("interval_hours", 24)
        interval_seconds = interval_hours * 3600
        now = datetime.now(timezone.utc)

        # Find the most recent completed or running job
        completed_jobs = [job for job in recent_jobs if job["status"] in ["completed", "failed"]]
        running_jobs = [job for job in recent_jobs if job["status"] == "running"]

        next_run_info = {
            "enabled": True,
            "interval_hours": interval_hours,
            "next_run_timestamp": None,
            "next_run_iso": None,
            "time_until_next": None,
            "is_overdue": False,
            "last_run": None,
            "last_run_status": None,
            "currently_running": len(running_jobs) > 0
        }

        # If there's a currently running job, next run is interval from when it completes
        if running_jobs:
            running_job = running_jobs[0]
            # Estimate completion time (assume current job will complete soon)
            # For now, use started time + 10 minutes as estimated completion
            if running_job.get("started_at"):
                started_time = datetime.fromisoformat(running_job["started_at"].replace('Z', '+00:00'))
                estimated_completion = started_time + timedelta(minutes=10)
                next_run_time = estimated_completion + timedelta(seconds=interval_seconds)
            else:
                # Job is running but no start time, assume it will complete soon
                next_run_time = now + timedelta(minutes=10) + timedelta(seconds=interval_seconds)

            next_run_info.update({
                "next_run_timestamp": next_run_time.timestamp(),
                "next_run_iso": next_run_time.isoformat(),
                "time_until_next": self._format_time_until(next_run_time, now),
                "currently_running": True,
                "last_run_status": "running"
            })

        elif completed_jobs:
            # Base next run on last completed job
            last_job = completed_jobs[0]
            last_run_time_str = last_job.get("completed_at") or last_job.get("created_at")

            if last_run_time_str:
                last_run_time = datetime.fromisoformat(last_run_time_str.replace('Z', '+00:00'))
                next_run_time = last_run_time + timedelta(seconds=interval_seconds)

                # Check if overdue
                is_overdue = next_run_time < now

                next_run_info.update({
                    "next_run_timestamp": next_run_time.timestamp(),
                    "next_run_iso": next_run_time.isoformat(),
                    "time_until_next": self._format_time_until(next_run_time, now),
                    "is_overdue": is_overdue,
                    "last_run": last_run_time_str,
                    "last_run_status": last_job["status"]
                })
            else:
                # No valid timestamp, estimate based on current time
                next_run_time = now + timedelta(seconds=interval_seconds)
                next_run_info.update({
                    "next_run_timestamp": next_run_time.timestamp(),
                    "next_run_iso": next_run_time.isoformat(),
                    "time_until_next": self._format_time_until(next_run_time, now),
                    "last_run_status": last_job["status"]
                })
        else:
            # No previous jobs - estimate based on current time + interval
            next_run_time = now + timedelta(seconds=interval_seconds)
            next_run_info.update({
                "next_run_timestamp": next_run_time.timestamp(),
                "next_run_iso": next_run_time.isoformat(),
                "time_until_next": self._format_time_until(next_run_time, now),
                "last_run": None,
                "last_run_status": None
            })

        return next_run_info

    def _format_time_until(self, target_time: datetime, current_time: datetime) -> str:
        """Format time until target as human readable string"""
        diff = target_time - current_time

        if diff.total_seconds() <= 0:
            return "Overdue"

        total_seconds = int(diff.total_seconds())

        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60

        if days > 0:
            return f"in {days}d {hours}h"
        elif hours > 0:
            return f"in {hours}h {minutes}m"
        elif minutes > 0:
            return f"in {minutes}m"
        else:
            return "in <1m"

    def get_automation_status_with_next_runs(self, organization_id: str, connection_id: str = None) -> Dict[str, Any]:
        """
        Enhanced status method that includes next run times

        Args:
            organization_id: Organization ID
            connection_id: Optional connection ID to filter

        Returns:
            Status with next run information
        """
        try:
            # Get base status
            base_status = self.get_automation_status(organization_id, connection_id)

            # If connection_id provided, get next run times
            if connection_id:
                next_runs_data = self.get_next_run_times(organization_id, connection_id)
                base_status["next_runs"] = next_runs_data.get("next_runs", {})
            else:
                # Get next runs for all connections
                connections_response = self.supabase.supabase.table("database_connections") \
                    .select("id") \
                    .eq("organization_id", organization_id) \
                    .execute()

                if connections_response.data:
                    all_next_runs = {}
                    for conn in connections_response.data:
                        conn_id = conn["id"]
                        next_runs_data = self.get_next_run_times(organization_id, conn_id)
                        if next_runs_data.get("next_runs"):
                            all_next_runs[conn_id] = next_runs_data["next_runs"]

                    base_status["next_runs_by_connection"] = all_next_runs

            return base_status

        except Exception as e:
            logger.error(f"Error getting automation status with next runs: {str(e)}")
            return {"error": str(e)}