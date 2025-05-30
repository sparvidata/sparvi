import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

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
            # Check if config exists
            existing = self.supabase.supabase.table("automation_global_config") \
                .select("id") \
                .execute()

            config_data["updated_at"] = datetime.now().isoformat()

            if existing.data and len(existing.data) > 0:
                # Update existing
                response = self.supabase.supabase.table("automation_global_config") \
                    .update(config_data) \
                    .eq("id", existing.data[0]["id"]) \
                    .execute()
            else:
                # Create new
                config_data["created_at"] = datetime.now().isoformat()
                response = self.supabase.supabase.table("automation_global_config") \
                    .insert(config_data) \
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
                .select("id") \
                .eq("organization_id", organization_id) \
                .execute()

            if not connections_response.data:
                return []

            connection_ids = [conn["id"] for conn in connections_response.data]

            # Get automation configs for these connections
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("*, database_connections(name, connection_type)") \
                .in_("connection_id", connection_ids) \
                .execute()

            return response.data if response.data else []

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
            # Check if config exists
            existing = self.supabase.supabase.table("automation_connection_configs") \
                .select("id") \
                .eq("connection_id", connection_id) \
                .execute()

            config_data["updated_at"] = datetime.now().isoformat()

            if existing.data and len(existing.data) > 0:
                # Update existing
                response = self.supabase.supabase.table("automation_connection_configs") \
                    .update(config_data) \
                    .eq("connection_id", connection_id) \
                    .execute()
            else:
                # Create new
                config_data["connection_id"] = connection_id
                config_data["created_at"] = datetime.now().isoformat()
                response = self.supabase.supabase.table("automation_connection_configs") \
                    .insert(config_data) \
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
            # Check if config exists
            existing = self.supabase.supabase.table("automation_table_configs") \
                .select("id") \
                .eq("connection_id", connection_id) \
                .eq("table_name", table_name) \
                .execute()

            config_data["updated_at"] = datetime.now().isoformat()

            if existing.data and len(existing.data) > 0:
                # Update existing
                response = self.supabase.supabase.table("automation_table_configs") \
                    .update(config_data) \
                    .eq("connection_id", connection_id) \
                    .eq("table_name", table_name) \
                    .execute()
            else:
                # Create new
                config_data.update({
                    "connection_id": connection_id,
                    "table_name": table_name,
                    "created_at": datetime.now().isoformat()
                })
                response = self.supabase.supabase.table("automation_table_configs") \
                    .insert(config_data) \
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
                yesterday = (datetime.now() - timedelta(days=1)).isoformat()
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
                .update({"status": "cancelled", "completed_at": datetime.now().isoformat()}) \
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