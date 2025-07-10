import json
import logging
import pytz
from datetime import datetime, timezone, timedelta, time
from typing import Dict, List, Any, Optional
import uuid

logger = logging.getLogger(__name__)


class ScheduleManager:
    """Manages user-defined automation schedules"""

    def __init__(self, supabase_manager):
        self.supabase = supabase_manager

    def update_connection_schedule(self, connection_id: str, schedule_config: Dict[str, Any], user_id: str) -> Dict[
        str, Any]:
        """
        Update schedule configuration for a connection

        Args:
            connection_id: Connection ID
            schedule_config: New schedule configuration
            user_id: User making the change

        Returns:
            Updated configuration or error
        """
        try:
            logger.info(f"Updating schedule for connection {connection_id}")

            # Validate schedule configuration
            validation_result = self._validate_schedule_config(schedule_config)
            if not validation_result["valid"]:
                return {"error": validation_result["error"]}

            # Update the automation_connection_configs table
            response = self.supabase.supabase.table("automation_connection_configs") \
                .update({
                "schedule_config": schedule_config,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }) \
                .eq("connection_id", connection_id) \
                .execute()

            if not response.data:
                # Create new config if it doesn't exist
                new_config = {
                    "connection_id": connection_id,
                    "schedule_config": schedule_config,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }

                response = self.supabase.supabase.table("automation_connection_configs") \
                    .insert(new_config) \
                    .execute()

            if response.data:
                # Update the scheduled jobs table
                self._update_scheduled_jobs(connection_id, schedule_config)

                logger.info(f"Successfully updated schedule for connection {connection_id}")

                return {
                    "success": True,
                    "schedule_config": schedule_config,
                    "next_runs": self._calculate_next_runs(connection_id, schedule_config)
                }
            else:
                return {"error": "Failed to update schedule configuration"}

        except Exception as e:
            logger.error(f"Error updating connection schedule: {str(e)}")
            return {"error": str(e)}

    def get_connection_schedule(self, connection_id: str) -> Dict[str, Any]:
        """Get schedule configuration for a connection"""
        try:
            response = self.supabase.supabase.table("automation_connection_configs") \
                .select("schedule_config") \
                .eq("connection_id", connection_id) \
                .execute()

            if response.data and len(response.data) > 0:
                schedule_config = response.data[0].get("schedule_config", {})

                # Add next run times
                next_runs = self._calculate_next_runs(connection_id, schedule_config)

                return {
                    "connection_id": connection_id,
                    "schedule_config": schedule_config,
                    "next_runs": next_runs
                }
            else:
                # Return default configuration
                default_config = self._get_default_schedule_config()
                return {
                    "connection_id": connection_id,
                    "schedule_config": default_config,
                    "next_runs": {}
                }

        except Exception as e:
            logger.error(f"Error getting connection schedule: {str(e)}")
            return {"error": str(e)}

    def get_due_jobs(self, buffer_minutes: int = 5) -> List[Dict[str, Any]]:
        """
        Get jobs that are due to run (within buffer_minutes of scheduled time)

        Args:
            buffer_minutes: How many minutes before/after scheduled time to consider "due"

        Returns:
            List of jobs that should be executed
        """
        try:
            now = datetime.now(timezone.utc)
            buffer_start = now - timedelta(minutes=buffer_minutes)
            buffer_end = now + timedelta(minutes=buffer_minutes)

            # Get all enabled scheduled jobs that are due
            response = self.supabase.supabase.table("automation_scheduled_jobs") \
                .select("*") \
                .eq("enabled", True) \
                .gte("next_run_at", buffer_start.isoformat()) \
                .lte("next_run_at", buffer_end.isoformat()) \
                .execute()

            due_jobs = response.data or []

            # Check if each job is actually ready to run (not already running)
            ready_jobs = []
            for job in due_jobs:
                if self._is_job_ready_to_run(job):
                    ready_jobs.append(job)

            logger.info(f"Found {len(ready_jobs)} jobs due to run")
            return ready_jobs

        except Exception as e:
            logger.error(f"Error getting due jobs: {str(e)}")
            return []

    def mark_job_executed(self, scheduled_job_id: str) -> bool:
        """Mark a scheduled job as executed and calculate next run time"""
        try:
            # Get the job details
            response = self.supabase.supabase.table("automation_scheduled_jobs") \
                .select("*") \
                .eq("id", scheduled_job_id) \
                .execute()

            if not response.data:
                return False

            job = response.data[0]
            now = datetime.now(timezone.utc)

            # Calculate next run time
            next_run = self._calculate_single_next_run(
                schedule_type=job["schedule_type"],
                scheduled_time=job["scheduled_time"],
                timezone_name=job["timezone"],
                days_of_week=job.get("days_of_week"),
                from_time=now
            )

            # Update the job
            self.supabase.supabase.table("automation_scheduled_jobs") \
                .update({
                "last_run_at": now.isoformat(),
                "next_run_at": next_run.isoformat() if next_run else None,
                "updated_at": now.isoformat()
            }) \
                .eq("id", scheduled_job_id) \
                .execute()

            return True

        except Exception as e:
            logger.error(f"Error marking job as executed: {str(e)}")
            return False

    def _validate_schedule_config(self, schedule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate schedule configuration format"""
        try:
            valid_automation_types = ["metadata_refresh", "schema_change_detection", "validation_automation"]
            valid_schedule_types = ["daily", "weekly"]
            valid_days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

            for automation_type, config in schedule_config.items():
                if automation_type not in valid_automation_types:
                    return {"valid": False, "error": f"Invalid automation type: {automation_type}"}

                if not isinstance(config, dict):
                    return {"valid": False, "error": f"Config for {automation_type} must be an object"}

                # Check enabled field
                if "enabled" not in config or not isinstance(config["enabled"], bool):
                    return {"valid": False, "error": f"Missing or invalid 'enabled' field for {automation_type}"}

                if config["enabled"]:
                    # Validate required fields for enabled automations
                    if "schedule_type" not in config or config["schedule_type"] not in valid_schedule_types:
                        return {"valid": False, "error": f"Invalid schedule_type for {automation_type}"}

                    if "time" not in config:
                        return {"valid": False, "error": f"Missing time field for {automation_type}"}

                    # Validate time format (HH:MM)
                    try:
                        time_parts = config["time"].split(":")
                        if len(time_parts) != 2:
                            raise ValueError("Invalid time format")
                        hour, minute = int(time_parts[0]), int(time_parts[1])
                        if not (0 <= hour <= 23 and 0 <= minute <= 59):
                            raise ValueError("Invalid time values")
                    except:
                        return {"valid": False, "error": f"Invalid time format for {automation_type}. Use HH:MM format"}

                    # Validate timezone
                    if "timezone" not in config:
                        return {"valid": False, "error": f"Missing timezone for {automation_type}"}

                    try:
                        pytz.timezone(config["timezone"])
                    except:
                        return {"valid": False, "error": f"Invalid timezone for {automation_type}"}

                    # Validate days for weekly schedules
                    if config["schedule_type"] == "weekly":
                        if "days" not in config or not isinstance(config["days"], list) or not config["days"]:
                            return {"valid": False,
                                    "error": f"Weekly schedule requires at least one day for {automation_type}"}

                        for day in config["days"]:
                            if day.lower() not in valid_days:
                                return {"valid": False, "error": f"Invalid day '{day}' for {automation_type}"}

            return {"valid": True}

        except Exception as e:
            return {"valid": False, "error": f"Validation error: {str(e)}"}

    def _get_default_schedule_config(self) -> Dict[str, Any]:
        """Get default schedule configuration"""
        return {
            "metadata_refresh": {
                "enabled": True,
                "schedule_type": "daily",
                "time": "02:00",
                "timezone": "UTC"
            },
            "schema_change_detection": {
                "enabled": True,
                "schedule_type": "daily",
                "time": "03:00",
                "timezone": "UTC"
            },
            "validation_automation": {
                "enabled": False,
                "schedule_type": "weekly",
                "time": "01:00",
                "timezone": "UTC",
                "days": ["sunday"]
            }
        }

    def _update_scheduled_jobs(self, connection_id: str, schedule_config: Dict[str, Any]):
        """Update the automation_scheduled_jobs table"""
        try:
            # Delete existing scheduled jobs for this connection
            self.supabase.supabase.table("automation_scheduled_jobs") \
                .delete() \
                .eq("connection_id", connection_id) \
                .execute()

            # Create new scheduled jobs
            for automation_type, config in schedule_config.items():
                if config.get("enabled", False):
                    scheduled_job = {
                        "id": str(uuid.uuid4()),
                        "connection_id": connection_id,
                        "automation_type": automation_type,
                        "schedule_type": config["schedule_type"],
                        "scheduled_time": config["time"],
                        "timezone": config["timezone"],
                        "days_of_week": config.get("days"),
                        "enabled": True,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }

                    # Calculate next run time
                    next_run = self._calculate_single_next_run(
                        schedule_type=config["schedule_type"],
                        scheduled_time=config["time"],
                        timezone_name=config["timezone"],
                        days_of_week=config.get("days")
                    )

                    if next_run:
                        scheduled_job["next_run_at"] = next_run.isoformat()

                    self.supabase.supabase.table("automation_scheduled_jobs") \
                        .insert(scheduled_job) \
                        .execute()

            logger.info(f"Updated scheduled jobs for connection {connection_id}")

        except Exception as e:
            logger.error(f"Error updating scheduled jobs: {str(e)}")

    def _calculate_next_runs(self, connection_id: str, schedule_config: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate next run times for all automation types"""
        next_runs = {}

        try:
            for automation_type, config in schedule_config.items():
                if config.get("enabled", False):
                    next_run = self._calculate_single_next_run(
                        schedule_type=config["schedule_type"],
                        scheduled_time=config["time"],
                        timezone_name=config["timezone"],
                        days_of_week=config.get("days")
                    )

                    if next_run:
                        next_runs[automation_type] = {
                            "next_run_iso": next_run.isoformat(),
                            "next_run_timestamp": next_run.timestamp(),
                            "time_until_next": self._format_time_until(next_run),
                            "schedule_type": config["schedule_type"],
                            "scheduled_time": config["time"],
                            "timezone": config["timezone"],
                            "days": config.get("days")
                        }

        except Exception as e:
            logger.error(f"Error calculating next runs: {str(e)}")

        return next_runs

    def _calculate_single_next_run(self, schedule_type: str, scheduled_time: str,
                                   timezone_name: str, days_of_week: List[str] = None,
                                   from_time: datetime = None) -> Optional[datetime]:
        """Calculate next run time for a single automation"""
        try:
            if from_time is None:
                from_time = datetime.now(timezone.utc)

            user_tz = pytz.timezone(timezone_name)

            # Parse scheduled time
            time_parts = scheduled_time.split(":")
            hour, minute = int(time_parts[0]), int(time_parts[1])
            scheduled_time_obj = time(hour, minute)

            # Convert current time to user timezone
            current_local = from_time.astimezone(user_tz)

            if schedule_type == "daily":
                # Calculate next daily run
                next_run_local = current_local.replace(
                    hour=hour, minute=minute, second=0, microsecond=0
                )

                # If the time has passed today, schedule for tomorrow
                if next_run_local <= current_local:
                    next_run_local += timedelta(days=1)

                return next_run_local.astimezone(timezone.utc)

            elif schedule_type == "weekly" and days_of_week:
                # Find next occurrence of any specified day
                weekday_map = {
                    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
                    "friday": 4, "saturday": 5, "sunday": 6
                }

                target_weekdays = [weekday_map[day.lower()] for day in days_of_week if day.lower() in weekday_map]

                if not target_weekdays:
                    return None

                # Find the next target day
                current_weekday = current_local.weekday()
                days_ahead = None

                # Check each target weekday to find the closest one
                for target_weekday in sorted(target_weekdays):
                    if target_weekday > current_weekday:
                        days_ahead = target_weekday - current_weekday
                        break
                    elif target_weekday == current_weekday:
                        # Check if time has passed today
                        target_time_today = current_local.replace(
                            hour=hour, minute=minute, second=0, microsecond=0
                        )
                        if target_time_today > current_local:
                            days_ahead = 0
                            break

                # If no day found this week, use the first target day next week
                if days_ahead is None:
                    days_ahead = 7 + min(target_weekdays) - current_weekday

                next_run_local = current_local.replace(
                    hour=hour, minute=minute, second=0, microsecond=0
                ) + timedelta(days=days_ahead)

                return next_run_local.astimezone(timezone.utc)

            return None

        except Exception as e:
            logger.error(f"Error calculating single next run: {str(e)}")
            return None

    def _format_time_until(self, target_time: datetime) -> str:
        """Format time until target as human readable string"""
        try:
            now = datetime.now(timezone.utc)
            diff = target_time - now

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

    def _is_job_ready_to_run(self, scheduled_job: Dict[str, Any]) -> bool:
        """Check if a scheduled job is ready to run (not already running)"""
        try:
            connection_id = scheduled_job["connection_id"]
            automation_type = scheduled_job["automation_type"]

            # Check if there's already a running job
            response = self.supabase.supabase.table("automation_jobs") \
                .select("id") \
                .eq("connection_id", connection_id) \
                .eq("job_type", automation_type) \
                .eq("status", "running") \
                .execute()

            return len(response.data or []) == 0

        except Exception as e:
            logger.error(f"Error checking if job is ready to run: {str(e)}")
            return False


# Migration function to convert existing interval configs
def migrate_interval_to_schedule_configs(supabase_manager):
    """Convert existing interval-based configs to schedule configs"""
    try:
        logger.info("Starting migration from interval to schedule configs")

        response = supabase_manager.supabase.table("automation_connection_configs") \
            .select("*") \
            .execute()

        migrated_count = 0

        for config in response.data or []:
            connection_id = config["connection_id"]

            # Skip if already has schedule_config
            if config.get("schedule_config"):
                continue

            schedule_config = {}

            # Convert each automation type
            for automation_type in ["metadata_refresh", "schema_change_detection", "validation_automation"]:
                old_config = config.get(automation_type, {})

                if isinstance(old_config, str):
                    try:
                        old_config = json.loads(old_config)
                    except:
                        continue

                if old_config.get("enabled", False):
                    interval_hours = old_config.get("interval_hours", 24)

                    # Convert interval to schedule
                    if interval_hours <= 24:
                        schedule_type = "daily"
                        time_str = "02:00"  # Default to 2 AM
                    else:
                        schedule_type = "weekly"
                        time_str = "02:00"
                        days = ["sunday"]

                    schedule_config[automation_type] = {
                        "enabled": True,
                        "schedule_type": schedule_type,
                        "time": time_str,
                        "timezone": "UTC"
                    }

                    if schedule_type == "weekly":
                        schedule_config[automation_type]["days"] = days

            # Update with schedule config
            if schedule_config:
                supabase_manager.supabase.table("automation_connection_configs") \
                    .update({"schedule_config": schedule_config}) \
                    .eq("connection_id", connection_id) \
                    .execute()

                migrated_count += 1
                logger.info(f"Migrated schedule config for connection {connection_id}")

        logger.info(f"Migration complete: {migrated_count} connections migrated")
        return {"migrated": migrated_count}

    except Exception as e:
        logger.error(f"Error migrating to schedule configs: {str(e)}")
        return {"error": str(e)}