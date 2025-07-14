import logging
import sys
import os
from datetime import datetime, timezone, timedelta

# Add the backend directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_automation_config(connection_id, supabase):
    """Check and fix automation configuration"""
    try:
        logger.info("🔧 Checking automation configuration...")

        # Get current automation config
        config_response = supabase.supabase.table("automation_connection_configs") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .execute()

        if not config_response.data:
            logger.error("❌ No automation configuration found")
            logger.info("💡 Creating default automation configuration...")

            # Create default config
            default_config = {
                "connection_id": connection_id,
                "metadata_refresh": {
                    "enabled": True,
                    "interval_hours": 24,
                    "types": ["tables", "columns", "statistics"]
                },
                "schema_change_detection": {
                    "enabled": True,
                    "interval_hours": 24
                },
                "validation_automation": {
                    "enabled": False,
                    "interval_hours": 24
                },
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            create_response = supabase.supabase.table("automation_connection_configs") \
                .insert(default_config) \
                .execute()

            if create_response.data:
                logger.info("✅ Created default automation configuration")
                return True
            else:
                logger.error("❌ Failed to create automation configuration")
                return False

        config = config_response.data[0]
        metadata_config = config.get("metadata_refresh", {})

        # Handle JSON string format
        if isinstance(metadata_config, str):
            import json
            try:
                metadata_config = json.loads(metadata_config)
            except:
                metadata_config = {}

        logger.info(f"   📊 Current metadata config: {metadata_config}")

        # Check if statistics are included and enabled
        enabled = metadata_config.get("enabled", False)
        types = metadata_config.get("types", [])
        interval_hours = metadata_config.get("interval_hours", 24)

        issues_found = []

        if not enabled:
            issues_found.append("metadata_refresh_disabled")
            logger.warning("   ⚠️  Metadata refresh is disabled")

        if "statistics" not in types:
            issues_found.append("statistics_not_included")
            logger.warning("   ⚠️  Statistics not included in metadata types")

        if interval_hours > 48:
            issues_found.append("interval_too_long")
            logger.warning(f"   ⚠️  Interval too long: {interval_hours} hours")

        # Fix issues
        if issues_found:
            logger.info("   🔧 Fixing automation configuration issues...")

            fixed_config = metadata_config.copy()
            fixed_config["enabled"] = True
            fixed_config["types"] = ["tables", "columns", "statistics"]
            fixed_config["interval_hours"] = 24  # Reset to 24 hours

            update_data = {
                "metadata_refresh": fixed_config,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            update_response = supabase.supabase.table("automation_connection_configs") \
                .update(update_data) \
                .eq("connection_id", connection_id) \
                .execute()

            if update_response.data:
                logger.info("✅ Fixed automation configuration")
                return True
            else:
                logger.error("❌ Failed to update automation configuration")
                return False
        else:
            logger.info("✅ Automation configuration looks good")
            return True

    except Exception as e:
        logger.error(f"❌ Error checking automation config: {str(e)}")
        return False


def trigger_manual_metadata_collection(connection_id, supabase):
    """Trigger manual metadata collection to update statistics"""
    try:
        logger.info("🚀 Triggering manual metadata collection...")

        # Import metadata task manager
        from core.metadata.manager import MetadataTaskManager
        metadata_manager = MetadataTaskManager.get_instance(supabase_manager=supabase)

        if not metadata_manager:
            logger.error("❌ Could not get metadata task manager")
            return False

        # Submit a statistics-focused collection task
        task_params = {
            "depth": "standard",
            "table_limit": 10,  # Reasonable limit
            "automation_trigger": False,
            "manual_trigger": True,
            "refresh_types": ["statistics"],  # Focus on statistics only
            "timeout_minutes": 15,
            "force_refresh": True
        }

        logger.info(f"   📊 Submitting statistics collection task: {task_params}")

        task_id = metadata_manager.submit_collection_task(
            connection_id=connection_id,
            params=task_params,
            priority="high"
        )

        logger.info(f"   ✅ Task submitted with ID: {task_id}")

        # Wait for completion
        logger.info("   ⏳ Waiting for task completion (max 15 minutes)...")

        completion_result = metadata_manager.wait_for_task_completion_sync(task_id, timeout_minutes=15)

        if completion_result.get("completed", False):
            if completion_result.get("success", False):
                logger.info("   🎉 Statistics collection completed successfully!")

                # Get task results
                task_status = metadata_manager.get_task_status(task_id)
                result = task_status.get("result", {})

                logger.info(f"   📊 Task results:")
                logger.info(f"      - Statistics collected: {result.get('statistics_collected', 0)}")
                logger.info(f"      - Tables processed: {result.get('tables_processed', 0)}")
                logger.info(f"      - Execution time: {result.get('execution_time_seconds', 0):.2f}s")

                return True
            else:
                error_msg = completion_result.get("error", "Unknown error")
                logger.error(f"   ❌ Task failed: {error_msg}")

                # Get detailed task status
                task_status = metadata_manager.get_task_status(task_id)
                logger.error(f"   🔍 Task details: {task_status}")
                return False
        else:
            logger.error("   ❌ Task did not complete within timeout")

            # Get task status for debugging
            task_status = metadata_manager.get_task_status(task_id)
            logger.error(f"   🔍 Task status: {task_status}")
            return False

    except Exception as e:
        logger.error(f"❌ Error triggering metadata collection: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def verify_statistics_update(connection_id, supabase):
    """Verify that statistics were actually updated"""
    try:
        logger.info("🔍 Verifying statistics update...")

        # Check for recent statistics metadata
        recent_cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()

        recent_stats = supabase.supabase.table("connection_metadata") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .eq("metadata_type", "statistics") \
            .gte("collected_at", recent_cutoff) \
            .order("collected_at", desc=True) \
            .execute()

        if recent_stats.data:
            latest_stats = recent_stats.data[0]
            collected_at = latest_stats.get("collected_at")
            metadata_content = latest_stats.get("metadata", {})

            stats_tables = metadata_content.get("statistics_by_table", {})

            logger.info(f"✅ Found recent statistics update!")
            logger.info(f"   📅 Collected at: {collected_at}")
            logger.info(f"   📊 Tables with statistics: {len(stats_tables)}")

            # Show sample statistics
            for table_name, table_stats in list(stats_tables.items())[:2]:
                logger.info(f"   📊 {table_name}: {len(table_stats)} statistics")
                for stat_name, stat_value in list(table_stats.items())[:3]:
                    logger.info(f"      - {stat_name}: {stat_value}")

            return True
        else:
            logger.warning("⚠️  No recent statistics found")

            # Check the most recent statistics regardless of age
            all_stats = supabase.supabase.table("connection_metadata") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", "statistics") \
                .order("collected_at", desc=True) \
                .limit(1) \
                .execute()

            if all_stats.data:
                latest_stats = all_stats.data[0]
                collected_at = latest_stats.get("collected_at")

                try:
                    collected_time = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                    age_hours = (datetime.now(timezone.utc) - collected_time).total_seconds() / 3600
                    logger.warning(f"   📅 Most recent statistics are {age_hours:.1f} hours old")
                except:
                    logger.warning(f"   📅 Most recent statistics collected at: {collected_at}")
            else:
                logger.error("   ❌ No statistics metadata found at all")

            return False

    except Exception as e:
        logger.error(f"❌ Error verifying statistics update: {str(e)}")
        return False


def check_automation_service_status():
    """Check if automation service is running and can handle statistics"""
    try:
        logger.info("🤖 Checking automation service status...")

        # Try to import and check automation service
        try:
            from core.automation.service import automation_service

            status = automation_service.get_status()
            logger.info(f"   📊 Automation service status:")
            logger.info(f"      - Running: {status.get('running', False)}")
            logger.info(f"      - Environment: {status.get('environment', 'unknown')}")
            logger.info(f"      - Scheduler active: {status.get('scheduler_active', False)}")
            logger.info(f"      - Explicitly enabled: {status.get('explicitly_enabled', False)}")

            if status.get('error'):
                logger.warning(f"      - Error: {status['error']}")

            # Check if automation is properly enabled
            if not status.get('running', False):
                logger.warning("   ⚠️  Automation service is not running")

                # Try to start it
                logger.info("   🔧 Attempting to start automation service...")
                try:
                    success = automation_service.start()
                    if success:
                        logger.info("   ✅ Automation service started successfully")
                        return True
                    else:
                        logger.error("   ❌ Failed to start automation service")
                        return False
                except Exception as start_error:
                    logger.error(f"   ❌ Error starting automation service: {str(start_error)}")
                    return False
            else:
                logger.info("   ✅ Automation service is running")
                return True

        except ImportError:
            logger.warning("   ⚠️  Automation service not available")
            return False

    except Exception as e:
        logger.error(f"❌ Error checking automation service: {str(e)}")
        return False


def main():
    """Main function to fix metadata statistics collection"""
    try:
        logger.info("=== Sparvi Metadata Statistics Fix Script ===")
        logger.info(f"Started at: {datetime.now(timezone.utc).isoformat()}")

        # Initialize Supabase
        from core.storage.supabase_manager import SupabaseManager
        supabase = SupabaseManager()

        if not supabase.health_check():
            logger.error("❌ Supabase connection failed")
            return False

        connection_id = "a84eba26-da4a-4946-af2a-c91fc90680b4"

        # Step 1: Check current statistics status
        logger.info("Step 1: Checking current statistics status...")

        stats_response = supabase.supabase.table("connection_metadata") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .eq("metadata_type", "statistics") \
            .order("collected_at", desc=True) \
            .limit(1) \
            .execute()

        if stats_response.data:
            latest_stats = stats_response.data[0]
            collected_at = latest_stats.get("collected_at")

            try:
                collected_time = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                age_hours = (datetime.now(timezone.utc) - collected_time).total_seconds() / 3600
                logger.info(f"   📊 Latest statistics: {age_hours:.1f} hours old")

                if age_hours < 2:
                    logger.info("✅ Statistics are fresh - no action needed")
                    return True
                elif age_hours < 48:
                    logger.info("🟡 Statistics are somewhat stale - will refresh")
                else:
                    logger.warning("🔴 Statistics are very stale - need to fix automation")

            except:
                logger.warning("⚠️  Could not determine statistics age")
        else:
            logger.error("❌ No statistics metadata found")

        # Step 2: Check automation service
        logger.info("Step 2: Checking automation service...")
        automation_ok = check_automation_service_status()

        # Step 3: Check and fix automation configuration
        logger.info("Step 3: Checking automation configuration...")
        config_ok = check_automation_config(connection_id, supabase)

        if not config_ok:
            logger.error("❌ Could not fix automation configuration")
            return False

        # Step 4: Trigger manual metadata collection
        logger.info("Step 4: Triggering manual statistics collection...")
        collection_ok = trigger_manual_metadata_collection(connection_id, supabase)

        if not collection_ok:
            logger.error("❌ Manual metadata collection failed")
            return False

        # Step 5: Verify statistics were updated
        logger.info("Step 5: Verifying statistics update...")
        verification_ok = verify_statistics_update(connection_id, supabase)

        if verification_ok:
            logger.info("🎉 SUCCESS: Statistics have been updated!")

            # Final recommendation
            if automation_ok:
                logger.info("💡 Automation is working - statistics should stay fresh automatically")
            else:
                logger.warning("💡 Manual collection worked, but automation may need attention")
                logger.info("   Consider enabling automation for automatic statistics updates")

            return True
        else:
            logger.error("❌ Statistics update verification failed")

            logger.info("💡 Troubleshooting recommendations:")
            logger.info("   1. Check metadata collection worker logs for errors")
            logger.info("   2. Verify database permissions for statistics queries")
            logger.info("   3. Check if metadata storage is working properly")
            logger.info("   4. Try reducing table limit for statistics collection")

            return False

    except Exception as e:
        logger.error(f"❌ Fix script failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)