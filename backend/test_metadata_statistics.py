# backend/test_metadata_statistics.py - METADATA STATISTICS TEST SCRIPT
# Run this script to diagnose and fix metadata statistics collection issues

import logging
import sys
import os
from datetime import datetime, timezone, timedelta

# Add the backend directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function to test and fix metadata statistics collection"""
    try:
        logger.info("=== Sparvi Metadata Statistics Test and Fix Script ===")
        logger.info(f"Started at: {datetime.now(timezone.utc).isoformat()}")

        # Step 1: Initialize Supabase connection
        logger.info("Step 1: Initializing Supabase connection...")
        from core.storage.supabase_manager import SupabaseManager
        supabase = SupabaseManager()

        if not supabase.health_check():
            logger.error("❌ Supabase connection failed")
            return False

        logger.info("✅ Supabase connection successful")

        # Step 2: Get connection details
        logger.info("Step 2: Finding database connections...")

        connection_id = "a84eba26-da4a-4946-af2a-c91fc90680b4"  # Your Sparvi Sandbox connection

        connection = supabase.get_connection(connection_id)
        if not connection:
            logger.error(f"❌ Connection {connection_id} not found")
            return False

        logger.info(f"✅ Found connection: {connection.get('name')}")
        organization_id = connection.get("organization_id")

        # Step 3: Check current metadata statistics state
        logger.info("Step 3: Checking current metadata statistics...")

        # Check what metadata we have
        metadata_response = supabase.supabase.table("connection_metadata") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .order("collected_at", desc=True) \
            .execute()

        metadata_records = metadata_response.data or []
        logger.info(f"Found {len(metadata_records)} metadata records")

        # Group by metadata type
        metadata_by_type = {}
        for record in metadata_records:
            metadata_type = record.get("metadata_type", "unknown")
            if metadata_type not in metadata_by_type:
                metadata_by_type[metadata_type] = []
            metadata_by_type[metadata_type].append(record)

        for metadata_type, records in metadata_by_type.items():
            latest_record = records[0]  # Already sorted by collected_at desc
            collected_at = latest_record.get("collected_at", "unknown")

            # Calculate age
            try:
                collected_time = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                age_hours = (datetime.now(timezone.utc) - collected_time).total_seconds() / 3600
                age_status = "🟢 Fresh" if age_hours < 24 else "🟡 Stale" if age_hours < 72 else "🔴 Very Stale"
            except:
                age_hours = float('inf')
                age_status = "🔴 Unknown"

            logger.info(f"   📊 {metadata_type}: {len(records)} records, latest: {age_status} ({age_hours:.1f}h ago)")

        # Step 4: Check metadata task manager
        logger.info("Step 4: Testing metadata task manager...")

        try:
            from core.metadata.manager import MetadataTaskManager

            # Get metadata task manager instance
            metadata_manager = MetadataTaskManager.get_instance(supabase_manager=supabase)

            if not metadata_manager:
                logger.error("❌ Could not get metadata task manager instance")
                return False

            logger.info("✅ Metadata task manager available")

            # Check if workers are available
            try:
                worker_stats = metadata_manager.get_worker_stats()
                logger.info(f"   📈 Worker stats: {worker_stats}")
            except Exception as worker_error:
                logger.warning(f"   ⚠️  Could not get worker stats: {str(worker_error)}")

        except ImportError as import_error:
            logger.error(f"❌ Could not import metadata task manager: {str(import_error)}")
            return False

        # Step 5: Test basic metadata collection
        logger.info("Step 5: Testing basic metadata collection...")

        try:
            # Test connector creation
            from core.metadata.connector_factory import ConnectorFactory
            connector_factory = ConnectorFactory(supabase)
            connector = connector_factory.create_connector(connection_id)

            if not connector:
                logger.error("❌ Could not create database connector")
                return False

            logger.info("✅ Database connector created successfully")

            # Test basic operations
            logger.info("   🔍 Testing basic database operations...")

            # Get tables
            tables = connector.get_tables()
            logger.info(f"   ✅ Found {len(tables)} tables: {tables[:5]}...")  # Show first 5

            if not tables:
                logger.warning("   ⚠️  No tables found - this may be why statistics collection is failing")
                return False

            # Test getting columns for first table
            first_table = tables[0]
            columns = connector.get_columns(first_table)
            logger.info(f"   ✅ Found {len(columns)} columns in {first_table}")

            # Test basic statistics gathering
            logger.info(f"   📊 Testing statistics collection for {first_table}...")

            try:
                # Try to get basic stats manually
                from core.metadata.collector import MetadataCollector
                collector = MetadataCollector(connection_id, connector)

                # Test table statistics collection
                table_stats = collector._collect_table_statistics(first_table)
                logger.info(f"   ✅ Collected basic statistics for {first_table}: {table_stats}")

            except Exception as stats_error:
                logger.warning(f"   ⚠️  Manual statistics collection failed: {str(stats_error)}")

        except Exception as connector_error:
            logger.error(f"❌ Connector test failed: {str(connector_error)}")
            return False

        # Step 6: Test full metadata collection task
        logger.info("Step 6: Testing full metadata collection task...")

        try:
            # Submit a comprehensive metadata collection task
            task_params = {
                "depth": "standard",
                "table_limit": 5,  # Limit to 5 tables for testing
                "automation_trigger": False,  # Mark as manual test
                "test_run": True,
                "refresh_types": ["tables", "columns", "statistics"],
                "timeout_minutes": 10
            }

            logger.info(f"   🚀 Submitting metadata collection task with params: {task_params}")

            task_id = metadata_manager.submit_collection_task(
                connection_id=connection_id,
                params=task_params,
                priority="high"
            )

            logger.info(f"   ✅ Task submitted with ID: {task_id}")

            # Wait for task completion
            logger.info("   ⏳ Waiting for task completion (max 10 minutes)...")

            completion_result = metadata_manager.wait_for_task_completion_sync(task_id, timeout_minutes=10)

            if completion_result.get("completed", False):
                if completion_result.get("success", False):
                    logger.info("   🎉 Metadata collection task completed successfully!")

                    # Get task result details
                    task_status = metadata_manager.get_task_status(task_id)
                    result = task_status.get("result", {})

                    logger.info(f"   📊 Task results:")
                    logger.info(f"      - Tables processed: {result.get('tables_processed', 0)}")
                    logger.info(f"      - Columns collected: {result.get('columns_collected', 0)}")
                    logger.info(f"      - Statistics collected: {result.get('statistics_collected', 0)}")
                    logger.info(f"      - Execution time: {result.get('execution_time_seconds', 0):.2f}s")

                else:
                    error_msg = completion_result.get("error", "Unknown error")
                    logger.error(f"   ❌ Task completed but failed: {error_msg}")

                    # Get detailed task status for debugging
                    task_status = metadata_manager.get_task_status(task_id)
                    logger.error(f"   🔍 Task status details: {task_status}")
            else:
                logger.error("   ❌ Task did not complete within timeout")

                # Get task status for debugging
                task_status = metadata_manager.get_task_status(task_id)
                logger.error(f"   🔍 Task status: {task_status}")

        except Exception as task_error:
            logger.error(f"❌ Metadata collection task failed: {str(task_error)}")
            import traceback
            logger.error(traceback.format_exc())

        # Step 7: Check for updated metadata
        logger.info("Step 7: Checking for updated metadata...")

        # Check if new metadata was stored
        recent_cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()

        recent_metadata = supabase.supabase.table("connection_metadata") \
            .select("*") \
            .eq("connection_id", connection_id) \
            .gte("collected_at", recent_cutoff) \
            .order("collected_at", desc=True) \
            .execute()

        recent_records = recent_metadata.data or []

        if recent_records:
            logger.info(f"✅ Found {len(recent_records)} recent metadata records (last 15 minutes)")

            for record in recent_records:
                metadata_type = record.get("metadata_type", "unknown")
                collected_at = record.get("collected_at", "unknown")
                metadata_content = record.get("metadata", {})

                if metadata_type == "statistics":
                    stats_tables = metadata_content.get("statistics_by_table", {})
                    logger.info(f"   📊 Statistics record: {len(stats_tables)} tables, collected at {collected_at}")

                    # Show sample statistics
                    for table_name, table_stats in list(stats_tables.items())[:2]:  # Show first 2 tables
                        logger.info(f"      - {table_name}: {len(table_stats)} statistics")
                        for stat_name, stat_value in list(table_stats.items())[:3]:  # Show first 3 stats
                            logger.info(f"        * {stat_name}: {stat_value}")
                else:
                    logger.info(f"   📁 {metadata_type} record collected at {collected_at}")
        else:
            logger.warning("⚠️  No recent metadata records found - collection may have failed")

        # Step 8: Test automation integration
        logger.info("Step 8: Testing automation integration...")

        try:
            from core.automation.simplified_scheduler import SimplifiedAutomationScheduler

            # Test creating a metadata refresh job manually
            scheduler = SimplifiedAutomationScheduler(max_workers=1)

            logger.info("   🤖 Testing manual metadata refresh job creation...")

            # This would normally be called by the scheduler
            job_id = scheduler._create_and_execute_job(
                connection_id=connection_id,
                automation_type="metadata_refresh",
                scheduled_job_id="manual_test"
            )

            if job_id:
                logger.info(f"   ✅ Created metadata refresh job: {job_id}")

                # Wait a bit and check job status
                import time
                time.sleep(5)

                job_response = supabase.supabase.table("automation_jobs") \
                    .select("*") \
                    .eq("id", job_id) \
                    .execute()

                if job_response.data:
                    job = job_response.data[0]
                    status = job.get("status", "unknown")
                    logger.info(f"   📊 Job status after 5s: {status}")

                    if status == "completed":
                        result_summary = job.get("result_summary", {})
                        logger.info(f"   🎉 Job completed! Results: {result_summary}")
                    elif status == "failed":
                        error_msg = job.get("error_message", "Unknown error")
                        logger.error(f"   ❌ Job failed: {error_msg}")
                    elif status == "running":
                        logger.info("   ⏳ Job still running...")
                else:
                    logger.warning("   ⚠️  Could not find job record")
            else:
                logger.error("   ❌ Failed to create metadata refresh job")

        except Exception as automation_error:
            logger.warning(f"⚠️  Automation integration test failed: {str(automation_error)}")
            logger.info("   This may be expected if automation is disabled")

        # Step 9: Diagnostic recommendations
        logger.info("Step 9: Generating diagnostic recommendations...")

        recommendations = []

        # Check metadata freshness
        statistics_records = metadata_by_type.get("statistics", [])
        if not statistics_records:
            recommendations.append("🔴 CRITICAL: No statistics metadata found - statistics collection is not working")
        else:
            latest_stats = statistics_records[0]
            try:
                collected_time = datetime.fromisoformat(latest_stats["collected_at"].replace('Z', '+00:00'))
                age_hours = (datetime.now(timezone.utc) - collected_time).total_seconds() / 3600

                if age_hours > 48:
                    recommendations.append(
                        f"🟡 WARNING: Statistics metadata is {age_hours:.1f} hours old - consider more frequent collection")
                elif age_hours > 168:  # 1 week
                    recommendations.append(
                        f"🔴 CRITICAL: Statistics metadata is {age_hours:.1f} hours old - collection may be broken")
            except:
                recommendations.append("🔴 CRITICAL: Cannot determine statistics metadata age")

        # Check automation configuration
        try:
            config_response = supabase.supabase.table("automation_connection_configs") \
                .select("metadata_refresh") \
                .eq("connection_id", connection_id) \
                .execute()

            if config_response.data:
                metadata_config = config_response.data[0].get("metadata_refresh", {})

                # Handle JSON string format
                if isinstance(metadata_config, str):
                    import json
                    try:
                        metadata_config = json.loads(metadata_config)
                    except:
                        metadata_config = {}

                if not metadata_config.get("enabled", False):
                    recommendations.append(
                        "🟡 INFO: Metadata refresh automation is disabled - enable for automatic collection")

                types = metadata_config.get("types", [])
                if "statistics" not in types:
                    recommendations.append("🟡 WARNING: Statistics collection not included in automation types")
            else:
                recommendations.append("🔴 CRITICAL: No automation configuration found - create automation config")

        except Exception as config_error:
            recommendations.append(f"🔴 ERROR: Could not check automation config: {str(config_error)}")

        # Show recommendations
        if recommendations:
            logger.info("📋 Recommendations:")
            for rec in recommendations:
                logger.info(f"   {rec}")
        else:
            logger.info("🎉 No major issues found with metadata statistics!")

        # Step 10: Summary
        logger.info("Step 10: Final Summary...")

        total_metadata = len(metadata_records)
        statistics_count = len(metadata_by_type.get("statistics", []))
        recent_count = len(recent_records)

        if statistics_count == 0:
            health_status = "🔴 CRITICAL"
            health_msg = "No statistics metadata found - statistics collection is not working"
        elif statistics_count > 0 and recent_count > 0:
            health_status = "🟢 HEALTHY"
            health_msg = "Statistics collection appears to be working"
        elif statistics_count > 0:
            health_status = "🟡 WARNING"
            health_msg = "Statistics exist but no recent collection detected"
        else:
            health_status = "🔴 UNKNOWN"
            health_msg = "Cannot determine statistics collection health"

        logger.info(f"{health_status}: {health_msg}")
        logger.info(f"📊 Metadata Summary:")
        logger.info(f"   - Total metadata records: {total_metadata}")
        logger.info(f"   - Statistics records: {statistics_count}")
        logger.info(f"   - Recent records (15m): {recent_count}")
        logger.info(f"   - Recommendations: {len(recommendations)}")

        logger.info("=== Metadata Statistics Test Completed ===")

        return statistics_count > 0 or recent_count > 0

    except Exception as e:
        logger.error(f"❌ Script failed with error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)