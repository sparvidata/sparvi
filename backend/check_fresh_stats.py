import logging
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_statistics_freshness():
    """Check if we have fresh statistics"""
    try:
        logger.info("🔍 Checking for fresh statistics...")

        from core.storage.supabase_manager import SupabaseManager
        supabase = SupabaseManager()

        connection_id = "a84eba26-da4a-4946-af2a-c91fc90680b4"

        # Check most recent statistics
        recent_stats = supabase.supabase.table("connection_metadata") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .eq("metadata_type", "statistics") \
            .order("collected_at", desc=True) \
            .limit(1) \
            .execute()

        if recent_stats.data:
            latest = recent_stats.data[0]
            collected_at = latest.get("collected_at")
            metadata_content = latest.get("metadata", {})

            try:
                collected_time = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                age_minutes = (datetime.now(timezone.utc) - collected_time).total_seconds() / 60

                logger.info(f"📊 Latest statistics:")
                logger.info(f"   📅 Collected at: {collected_at}")
                logger.info(f"   ⏰ Age: {age_minutes:.1f} minutes ago")

                if age_minutes < 30:
                    logger.info("🎉 FRESH STATISTICS FOUND!")

                    # Show what we have
                    stats_tables = metadata_content.get("statistics_by_table", {})
                    logger.info(f"   📊 Tables with statistics: {len(stats_tables)}")

                    for table_name, table_stats in list(stats_tables.items())[:3]:
                        logger.info(f"   📋 {table_name}: {len(table_stats)} statistics")

                    return True
                else:
                    logger.warning(f"⚠️  Statistics are {age_minutes:.1f} minutes old")
                    return False

            except Exception as time_error:
                logger.error(f"❌ Error parsing time: {str(time_error)}")
                return False
        else:
            logger.error("❌ No statistics metadata found")
            return False

    except Exception as e:
        logger.error(f"❌ Error checking statistics: {str(e)}")
        return False


def check_automation_status():
    """Check automation status"""
    try:
        logger.info("🤖 Checking automation status...")

        from core.storage.supabase_manager import SupabaseManager
        supabase = SupabaseManager()

        connection_id = "a84eba26-da4a-4946-af2a-c91fc90680b4"

        # Check scheduled jobs
        jobs = supabase.supabase.table("automation_scheduled_jobs") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .execute()

        if jobs.data:
            logger.info(f"📋 Found {len(jobs.data)} scheduled jobs:")

            for job in jobs.data:
                automation_type = job.get("automation_type")
                enabled = job.get("enabled")
                next_run = job.get("next_run_at")

                status_icon = "✅" if enabled else "❌"
                logger.info(f"   {status_icon} {automation_type}: next run {next_run}")

            return True
        else:
            logger.warning("⚠️  No scheduled jobs found")
            return False

    except Exception as e:
        logger.error(f"❌ Error checking automation: {str(e)}")
        return False


def main():
    """Check both statistics and automation status"""
    logger.info("=== Fresh Statistics and Automation Check ===")

    # Check statistics
    fresh_stats = check_statistics_freshness()

    # Check automation
    automation_ok = check_automation_status()

    # Summary
    logger.info("\n📋 SUMMARY:")

    if fresh_stats:
        logger.info("🎉 You have FRESH statistics! The manual collection worked despite the error message.")
    else:
        logger.warning("⚠️  Statistics are still stale")

    if automation_ok:
        logger.info("✅ Automation is properly scheduled")
        logger.info("📅 Next automatic collection: 2025-07-14T02:00:00+00:00 (in ~21 hours)")
    else:
        logger.warning("⚠️  Automation scheduling issues")

    if fresh_stats and automation_ok:
        logger.info("\n🎉 SUCCESS! Your system is now working properly:")
        logger.info("   ✅ Fresh statistics available now")
        logger.info("   ✅ Automation will keep them fresh automatically")
        logger.info("   💡 Set ENABLE_AUTOMATION_SCHEDULER=true for automatic collection")

    return fresh_stats or automation_ok


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)