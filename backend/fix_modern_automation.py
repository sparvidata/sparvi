import logging
import sys
import os
from datetime import datetime, timezone, timedelta

# Add the backend directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_current_automation_system():
    """Check what automation system is currently in use"""
    try:
        logger.info("🔍 Checking current automation system...")

        from core.storage.supabase_manager import SupabaseManager
        supabase = SupabaseManager()

        connection_id = "a84eba26-da4a-4946-af2a-c91fc90680b4"

        # Check for scheduled jobs (new system)
        scheduled_jobs_response = supabase.supabase.table("automation_scheduled_jobs") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .execute()

        scheduled_jobs = scheduled_jobs_response.data or []

        # Check for connection configs (old system)
        connection_configs_response = supabase.supabase.table("automation_connection_configs") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .execute()

        connection_configs = connection_configs_response.data or []

        logger.info(f"   📊 Scheduled jobs found: {len(scheduled_jobs)}")
        logger.info(f"   📊 Connection configs found: {len(connection_configs)}")

        # Show scheduled jobs details
        if scheduled_jobs:
            logger.info("   📋 Current scheduled jobs:")
            for job in scheduled_jobs:
                automation_type = job.get("automation_type")
                enabled = job.get("enabled", False)
                schedule_type = job.get("schedule_type")
                scheduled_time = job.get("scheduled_time")
                next_run = job.get("next_run_at")

                status_icon = "✅" if enabled else "❌"
                logger.info(
                    f"      {status_icon} {automation_type}: {schedule_type} at {scheduled_time}, next: {next_run}")
        else:
            logger.warning("   ⚠️  No scheduled jobs found")

        # Show connection configs if they exist
        if connection_configs:
            logger.info("   📋 Legacy connection config also exists")
            config = connection_configs[0]

            # Check if it has schedule_config (newer format)
            if "schedule_config" in config:
                logger.info("      📊 Uses schedule-based config")
            else:
                logger.info("      📊 Uses legacy interval-based config")

        return {
            "scheduled_jobs": scheduled_jobs,
            "connection_configs": connection_configs,
            "system_type": "schedule_based" if scheduled_jobs else "legacy" if connection_configs else "none"
        }

    except Exception as e:
        logger.error(f"❌ Error checking automation system: {str(e)}")
        return {"error": str(e)}


def fix_schedule_based_automation(connection_id, supabase):
    """Fix the modern schedule-based automation system"""
    try:
        logger.info("🔧 Fixing schedule-based automation...")

        from core.automation.schedule_manager import ScheduleManager

        # Initialize schedule manager
        schedule_manager = ScheduleManager(supabase)

        # Create a proper schedule configuration
        schedule_config = {
            "metadata_refresh": {
                "enabled": True,
                "schedule_type": "daily",
                "time": "02:00",  # 2 AM
                "timezone": "UTC"
            },
            "schema_change_detection": {
                "enabled": True,
                "schedule_type": "daily",
                "time": "03:00",  # 3 AM
                "timezone": "UTC"
            },
            "validation_automation": {
                "enabled": False,  # Start with this disabled
                "schedule_type": "weekly",
                "time": "01:00",  # 1 AM on Sunday
                "timezone": "UTC",
                "days": ["sunday"]
            }
        }

        logger.info(f"   📊 Applying schedule config: {schedule_config}")

        # Update the connection schedule
        result = schedule_manager.update_connection_schedule(
            connection_id,
            schedule_config,
            "system_fix"
        )

        if "error" in result:
            logger.error(f"   ❌ Failed to update schedule: {result['error']}")
            return False
        else:
            logger.info("   ✅ Successfully updated schedule configuration")

            # Show next run times
            next_runs = result.get("next_runs", {})
            if next_runs:
                logger.info("   📅 Next scheduled runs:")
                for automation_type, run_info in next_runs.items():
                    time_until = run_info.get("time_until_next", "unknown")
                    logger.info(f"      - {automation_type}: {time_until}")

            return True

    except Exception as e:
        logger.error(f"❌ Error fixing schedule-based automation: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def check_automation_service_environment():
    """Check if automation service can run in current environment"""
    try:
        logger.info("🤖 Checking automation service environment...")

        import os

        environment = os.getenv("ENVIRONMENT", "development")
        scheduler_enabled = os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"
        disabled = os.getenv("DISABLE_AUTOMATION", "false").lower() == "true"

        logger.info(f"   📊 Environment: {environment}")
        logger.info(f"   📊 ENABLE_AUTOMATION_SCHEDULER: {scheduler_enabled}")
        logger.info(f"   📊 DISABLE_AUTOMATION: {disabled}")

        can_run = False

        if disabled:
            logger.error("   🔴 Automation is explicitly disabled")
        elif environment == "production":
            can_run = True
            logger.info("   ✅ Production environment - automation enabled by default")
        elif environment == "development" and scheduler_enabled:
            can_run = True
            logger.info("   ✅ Development environment with scheduler explicitly enabled")
        elif environment == "development":
            logger.warning("   🟡 Development environment requires ENABLE_AUTOMATION_SCHEDULER=true")
            logger.info("   💡 Set environment variable: ENABLE_AUTOMATION_SCHEDULER=true")
        else:
            logger.warning(f"   🟡 Unknown environment: {environment}")

        return can_run

    except Exception as e:
        logger.error(f"❌ Error checking environment: {str(e)}")
        return False


def manually_trigger_metadata_collection(connection_id, supabase):
    """Manually trigger metadata collection to get fresh statistics"""
    try:
        logger.info("🚀 Manually triggering metadata collection...")

        from core.metadata.manager import MetadataTaskManager

        # Get metadata task manager
        metadata_manager = MetadataTaskManager.get_instance(supabase_manager=supabase)

        if not metadata_manager:
            logger.error("❌ Could not get metadata task manager")
            return False

        # Submit a statistics-focused task
        task_params = {
            "depth": "standard",
            "table_limit": 9,  # All 9 tables
            "automation_trigger": False,
            "manual_trigger": True,
            "refresh_types": ["statistics"],  # Focus on statistics only
            "timeout_minutes": 10,
            "force_refresh": True
        }

        logger.info(f"   📊 Submitting task: {task_params}")

        task_id = metadata_manager.submit_collection_task(
            connection_id=connection_id,
            params=task_params,
            priority="high"
        )

        logger.info(f"   ✅ Task submitted: {task_id}")
        logger.info("   ⏳ Waiting for completion...")

        # Wait for completion with timeout
        completion_result = metadata_manager.wait_for_task_completion_sync(task_id, timeout_minutes=10)

        if completion_result.get("completed", False) and completion_result.get("success", False):
            logger.info("   🎉 Metadata collection completed successfully!")

            # Get task results
            task_status = metadata_manager.get_task_status(task_id)
            result = task_status.get("result", {})

            logger.info(f"   📊 Results: {result}")

            # Verify statistics were stored
            recent_cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()

            recent_stats = supabase.supabase.table("connection_metadata") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", "statistics") \
                .gte("collected_at", recent_cutoff) \
                .execute()

            if recent_stats.data:
                latest = recent_stats.data[0]
                collected_at = latest.get("collected_at")
                metadata_content = latest.get("metadata", {})
                stats_tables = metadata_content.get("statistics_by_table", {})

                logger.info(f"   ✅ Fresh statistics stored!")
                logger.info(f"      📅 Collected at: {collected_at}")
                logger.info(f"      📊 Tables with stats: {len(stats_tables)}")

                return True
            else:
                logger.warning("   ⚠️  Task completed but no fresh statistics found")
                return False
        else:
            error_msg = completion_result.get("error", "Unknown error")
            logger.error(f"   ❌ Task failed: {error_msg}")

            # Get detailed task status
            task_status = metadata_manager.get_task_status(task_id)
            logger.error(f"   🔍 Task details: {task_status}")

            return False

    except Exception as e:
        logger.error(f"❌ Error triggering metadata collection: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Main function to fix modern automation system"""
    try:
        logger.info("=== Modern Automation System Fix ===")
        logger.info(f"Started at: {datetime.now(timezone.utc).isoformat()}")

        from core.storage.supabase_manager import SupabaseManager
        supabase = SupabaseManager()

        connection_id = "a84eba26-da4a-4946-af2a-c91fc90680b4"

        # Step 1: Check current automation system
        logger.info("Step 1: Checking current automation system...")
        system_info = check_current_automation_system()

        if "error" in system_info:
            logger.error(f"❌ Could not check automation system: {system_info['error']}")
            return False

        system_type = system_info.get("system_type", "none")
        logger.info(f"   📊 Detected system type: {system_type}")

        # Step 2: Check environment
        logger.info("Step 2: Checking automation environment...")
        can_run_automation = check_automation_service_environment()

        # Step 3: Fix the automation configuration
        logger.info("Step 3: Fixing automation configuration...")

        if system_type in ["schedule_based", "legacy"]:
            # Use the modern schedule-based system
            config_fixed = fix_schedule_based_automation(connection_id, supabase)
        else:
            logger.warning("   ⚠️  No existing automation config - creating new schedule-based config")
            config_fixed = fix_schedule_based_automation(connection_id, supabase)

        if not config_fixed:
            logger.error("❌ Failed to fix automation configuration")
            return False

        # Step 4: Manually trigger collection to get immediate results
        logger.info("Step 4: Triggering immediate metadata collection...")
        collection_success = manually_trigger_metadata_collection(connection_id, supabase)

        # Step 5: Check final status
        logger.info("Step 5: Final status check...")

        # Check scheduled jobs again
        final_check = check_current_automation_system()
        scheduled_jobs = final_check.get("scheduled_jobs", [])

        metadata_refresh_jobs = [job for job in scheduled_jobs if job.get("automation_type") == "metadata_refresh"]

        if metadata_refresh_jobs:
            metadata_job = metadata_refresh_jobs[0]
            enabled = metadata_job.get("enabled", False)
            next_run = metadata_job.get("next_run_at")

            if enabled:
                logger.info(f"✅ Metadata refresh automation is ENABLED")
                logger.info(f"   📅 Next run: {next_run}")
            else:
                logger.warning("⚠️  Metadata refresh job exists but is disabled")
        else:
            logger.warning("⚠️  No metadata refresh job found")

        # Final summary
        logger.info("\n" + "=" * 50)
        logger.info("📋 FINAL SUMMARY:")

        if config_fixed and collection_success:
            logger.info("🎉 SUCCESS! Automation system has been fixed:")
            logger.info("   ✅ Schedule-based automation configured")
            logger.info("   ✅ Fresh statistics collected manually")

            if can_run_automation:
                logger.info("   ✅ Environment supports automation")
                logger.info("   📅 Statistics will be collected automatically daily at 2:00 AM UTC")
            else:
                logger.warning("   ⚠️  Environment may not support automatic scheduling")
                logger.info("   💡 For automatic collection, set ENABLE_AUTOMATION_SCHEDULER=true")

        elif config_fixed:
            logger.info("🟡 PARTIAL SUCCESS:")
            logger.info("   ✅ Automation configuration fixed")
            logger.warning("   ⚠️  Manual collection had issues")
            logger.info("   💡 Automation should still work on schedule")

        else:
            logger.error("❌ FAILED to fix automation system")
            logger.info("💡 You may need to manually configure automation through the UI")

        logger.info("\n💡 Next steps:")
        logger.info("   1. Check your frontend automation settings")
        logger.info("   2. Verify statistics appear in your dashboard")
        logger.info("   3. Monitor for fresh statistics over the next 24 hours")

        return config_fixed

    except Exception as e:
        logger.error(f"❌ Main function failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)