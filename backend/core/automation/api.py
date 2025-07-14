import json
import logging
import traceback
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone

from core.storage.supabase_manager import SupabaseManager
from .simplified_scheduler import SimplifiedAutomationScheduler
from .schedule_manager import ScheduleManager
from .events import AutomationEventType, publish_automation_event

logger = logging.getLogger(__name__)


class AutomationAPI:
    """API for managing automation configurations and jobs - Updated for simplified scheduler"""

    def __init__(self):
        self.supabase = SupabaseManager()
        self.scheduler = SimplifiedAutomationScheduler()
        self.schedule_manager = ScheduleManager(self.supabase)

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
            connections_lookup = {conn["id"]: conn for conn in connections_response.data}

            # Get automation configs for these connections
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*") \
                .in_("connection_id", connection_ids) \
                .execute()

            # Add connection info to configs
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
                config_row = response.data[0]

                # Check if this has schedule_config (new format)
                if config_row.get("schedule_config"):
                    return {
                        "connection_id": connection_id,
                        "schedule_config": config_row["schedule_config"],
                        "legacy_format": False
                    }

                # Handle legacy format - convert to schedule format for API compatibility
                parsed_config = {"connection_id": connection_id}

                for field in ["metadata_refresh", "schema_change_detection", "validation_automation"]:
                    try:
                        field_value = config_row.get(field)

                        if field_value is None:
                            parsed_config[field] = {}
                        elif isinstance(field_value, str):
                            try:
                                parsed_config[field] = json.loads(field_value)
                            except json.JSONDecodeError as e:
                                logger.error(f"Error parsing {field} JSON for connection {connection_id}: {str(e)}")
                                parsed_config[field] = {}
                        elif isinstance(field_value, dict):
                            parsed_config[field] = field_value
                        else:
                            logger.warning(f"Unexpected type for {field}: {type(field_value)}")
                            parsed_config[field] = {}

                    except Exception as e:
                        logger.error(f"Error processing {field} for connection {connection_id}: {str(e)}")
                        parsed_config[field] = {}

                parsed_config["legacy_format"] = True
                return parsed_config

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
                    "interval_hours": 24,
                    "auto_acknowledge_safe_changes": False
                },
                "validation_automation": {
                    "enabled": False,
                    "interval_hours": 24,
                    "auto_generate_for_new_tables": True
                },
                "legacy_format": True
            }

        except Exception as e:
            logger.error(f"Error getting connection config: {str(e)}")
            return None

    def update_connection_config(self, connection_id: str, config_data: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        """Update automation configuration for a connection"""
        try:
            # Check if this is a schedule_config update (new format)
            if "schedule_config" in config_data:
                result = self.schedule_manager.update_connection_schedule(
                    connection_id, config_data["schedule_config"], user_id
                )

                if "error" in result:
                    return result

                # Update the scheduler with new configuration
                if self.scheduler:
                    self.scheduler.update_connection_schedule(connection_id, config_data["schedule_config"])

                # Publish event
                publish_automation_event(
                    event_type=AutomationEventType.CONFIG_UPDATED,
                    data=result,
                    connection_id=connection_id,
                    user_id=user_id
                )

                return result

            # Handle legacy format update
            clean_config_data = {
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            # Store legacy fields as JSON
            for field in ["metadata_refresh", "schema_change_detection", "validation_automation"]:
                if field in config_data:
                    field_data = config_data[field]

                    if field_data is None:
                        clean_config_data[field] = None
                    else:
                        if isinstance(field_data, str):
                            try:
                                clean_config_data[field] = json.loads(field_data)
                            except json.JSONDecodeError:
                                logger.error(f"Invalid JSON string for {field}: {field_data}")
                                return {"error": f"Invalid JSON for {field}"}
                        else:
                            clean_config_data[field] = field_data

            logger.info(f"Storing legacy config data: {clean_config_data}")

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

    # Table-Level Configuration (unchanged)
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

            # Handle both legacy and new formats
            if config.get("schedule_config"):
                # New schedule format
                schedule_config = config["schedule_config"]
                for automation_type in ["metadata_refresh", "schema_change_detection", "validation_automation"]:
                    if automation_type in schedule_config:
                        schedule_config[automation_type]["enabled"] = enabled

                return self.update_connection_config(connection_id, {"schedule_config": schedule_config}, user_id)
            else:
                # Legacy format
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
            # Use simplified scheduler for immediate runs
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
                        "jobs_created": result.get("jobs_created", [])
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
                # Notify scheduler to cancel the job (simplified scheduler doesn't need complex cancellation)
                return {"success": True, "message": "Job cancelled successfully"}
            else:
                return {"error": "Job not found"}

        except Exception as e:
            logger.error(f"Error cancelling job: {str(e)}")
            return {"error": str(e)}

    def get_next_run_times(self, organization_id: str, connection_id: str) -> Dict[str, Any]:
        """
        Get next estimated run times for all automation types for a connection
        SIMPLIFIED: Uses schedule manager instead of complex interval calculations
        """
        try:
            logger.info(f"Getting next run times for connection {connection_id} in organization {organization_id}")

            # Verify the connection exists and belongs to the organization
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

            # Get schedule configuration for this connection
            schedule_data = self.schedule_manager.get_connection_schedule(connection_id)

            if "error" in schedule_data:
                logger.warning(f"Error getting schedule for connection {connection_id}: {schedule_data['error']}")
                return {
                    "connection_id": connection_id,
                    "connection_name": connection_name,
                    "next_runs": {},
                    "error": schedule_data["error"]
                }

            # Extract next runs from schedule data
            next_runs = schedule_data.get("next_runs", {})

            # If no schedule configured, check for legacy interval-based config
            if not next_runs:
                logger.info(f"No schedule found for connection {connection_id}, checking legacy config")
                legacy_config = self.get_connection_config(connection_id)

                if legacy_config and legacy_config.get("legacy_format"):
                    # Convert legacy config to simple next run estimates
                    next_runs = self._convert_legacy_to_next_runs(legacy_config)

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

    def _convert_legacy_to_next_runs(self, legacy_config: Dict[str, Any]) -> Dict[str, Any]:
        """Convert legacy interval-based config to next run estimates for API compatibility"""
        next_runs = {}

        try:
            now = datetime.now(timezone.utc)

            automation_types = ["metadata_refresh", "schema_change_detection", "validation_automation"]

            for automation_type in automation_types:
                config = legacy_config.get(automation_type, {})

                if config.get("enabled", False):
                    interval_hours = config.get("interval_hours", 24)

                    # Simple estimate: current time + interval
                    next_run_time = now + timedelta(hours=interval_hours)

                    next_runs[automation_type] = {
                        "enabled": True,
                        "interval_hours": interval_hours,
                        "next_run_timestamp": next_run_time.timestamp(),
                        "next_run_iso": next_run_time.isoformat(),
                        "time_until_next": self._format_time_until(next_run_time, now),
                        "is_overdue": False,
                        "last_run": None,
                        "last_run_status": None,
                        "currently_running": False,
                        "calculation_method": "legacy_interval_estimate"
                    }

            return next_runs

        except Exception as e:
            logger.error(f"Error converting legacy config: {str(e)}")
            return {}

    def _format_time_until(self, target_time: datetime, current_time: datetime) -> str:
        """Format time until target as human readable string"""
        try:
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

        except Exception:
            return "unknown"

    def get_automation_status_with_next_runs(self, organization_id: str, connection_id: str = None) -> Dict[str, Any]:
        """Enhanced status method that includes next run times"""
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