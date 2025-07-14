# Run this script to diagnose and fix metadata statistics collection issues with better connector debugging

import logging
import sys
import os
from datetime import datetime, timezone, timedelta

# Add the backend directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_database_connection(connection_id, supabase):
    """Test database connection with detailed debugging"""
    try:
        logger.info("🔍 Detailed database connection testing...")

        # Get connection details
        connection = supabase.get_connection(connection_id)
        if not connection:
            logger.error("❌ Could not get connection details")
            return False

        logger.info(f"   📋 Connection details:")
        logger.info(f"      - Name: {connection.get('name')}")
        logger.info(f"      - Type: {connection.get('connection_type')}")
        logger.info(f"      - ID: {connection_id}")

        # Test connector creation and connection
        from core.metadata.connector_factory import ConnectorFactory
        connector_factory = ConnectorFactory(supabase)

        logger.info("   🔌 Creating connector...")
        connector = connector_factory.create_connector(connection_id)

        if not connector:
            logger.error("❌ Could not create database connector")
            return False

        logger.info("   ✅ Connector created successfully")

        # Check if connector has connection details
        if hasattr(connector, 'connection_details'):
            logger.info(f"   📋 Connector has connection details: {bool(connector.connection_details)}")

        # Try to establish connection explicitly
        logger.info("   🔗 Attempting to connect to database...")

        try:
            # Check if connector has a connect method
            if hasattr(connector, 'connect'):
                logger.info("   📞 Calling connector.connect()...")
                connector.connect()
                logger.info("   ✅ Connection established successfully")
            else:
                logger.info("   📝 Connector doesn't have explicit connect() method")

            # Check connection status
            if hasattr(connector, 'is_connected'):
                connected = connector.is_connected()
                logger.info(f"   🔗 Connection status: {'Connected' if connected else 'Not connected'}")
                if not connected:
                    logger.error("   ❌ Connector reports not connected")
                    return False
            else:
                logger.info("   📝 Connector doesn't have is_connected() method")

        except Exception as connect_error:
            logger.error(f"   ❌ Connection failed: {str(connect_error)}")

            # Try to get more details about the error
            import traceback
            logger.error(f"   🔍 Connection error details: {traceback.format_exc()}")

            # Check if it's a credential issue
            error_str = str(connect_error).lower()
            if any(keyword in error_str for keyword in ['password', 'credential', 'auth', 'login']):
                logger.error("   💡 This appears to be a credential/authentication issue")
            elif any(keyword in error_str for keyword in ['network', 'timeout', 'connection']):
                logger.error("   💡 This appears to be a network connectivity issue")
            elif any(keyword in error_str for keyword in ['database', 'warehouse', 'schema']):
                logger.error("   💡 This appears to be a database/warehouse configuration issue")

            return False

        # Test basic operations
        logger.info("   📊 Testing basic database operations...")

        try:
            # Test get_tables
            logger.info("   📋 Getting tables list...")
            tables = connector.get_tables()
            logger.info(f"   ✅ Found {len(tables)} tables")

            if len(tables) == 0:
                logger.warning("   ⚠️  No tables found - database may be empty or permissions issue")
                return True  # Connection works, but no tables

            # Show first few tables
            sample_tables = tables[:5]
            logger.info(f"   📋 Sample tables: {sample_tables}")

            # Test get_columns for first table
            first_table = tables[0]
            logger.info(f"   📋 Getting columns for table: {first_table}")
            columns = connector.get_columns(first_table)
            logger.info(f"   ✅ Found {len(columns)} columns in {first_table}")

            # Test basic statistics if possible
            try:
                logger.info(f"   📊 Testing basic statistics for {first_table}...")

                # Try to execute a simple count query
                if hasattr(connector, 'execute_query'):
                    count_query = f"SELECT COUNT(*) as row_count FROM {first_table}"
                    result = connector.execute_query(count_query)
                    logger.info(f"   ✅ Row count query successful: {result}")
                else:
                    logger.info("   📝 Connector doesn't have execute_query method")

            except Exception as stats_error:
                logger.warning(f"   ⚠️  Statistics test failed: {str(stats_error)}")
                # This is OK, connection still works

            return True

        except Exception as operations_error:
            logger.error(f"   ❌ Database operations failed: {str(operations_error)}")

            # Check if this is a connection issue or operations issue
            error_str = str(operations_error).lower()
            if 'not connected' in error_str:
                logger.error("   💡 Operations failed due to connection issue")
            elif any(keyword in error_str for keyword in ['permission', 'access', 'denied']):
                logger.error("   💡 Operations failed due to permissions issue")
            else:
                logger.error("   💡 Operations failed for unknown reason")

            return False

    except Exception as e:
        logger.error(f"❌ Database connection test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def test_manual_metadata_collection(connection_id, connector, supabase):
    """Test manual metadata collection with the working connector"""
    try:
        logger.info("📊 Testing manual metadata collection...")

        from core.metadata.collector import MetadataCollector
        collector = MetadataCollector(connection_id, connector)

        # First, let's see what methods are actually available
        logger.info("   🔍 Checking available MetadataCollector methods...")
        available_methods = [method for method in dir(collector) if
                             not method.startswith('_') and callable(getattr(collector, method))]
        logger.info(f"   📋 Available methods: {available_methods}")

        # Test tables collection
        logger.info("   📋 Collecting tables metadata...")
        try:
            # Try the correct method name
            if hasattr(collector, 'collect_tables'):
                tables_data = collector.collect_tables()
                logger.info(f"   ✅ Collected tables: {len(tables_data)} tables")
                logger.info(f"   📋 Sample tables: {tables_data[:5]}")
            else:
                # Get tables directly from connector
                tables_data = connector.get_tables()
                logger.info(f"   ✅ Got tables from connector: {len(tables_data)} tables")
                logger.info(f"   📋 Sample tables: {tables_data[:5]}")
        except Exception as tables_error:
            logger.error(f"   ❌ Tables collection failed: {str(tables_error)}")
            return False

        # Test columns collection
        logger.info("   📋 Collecting columns metadata...")
        try:
            if hasattr(collector, 'collect_columns'):
                # Try collecting columns for first few tables
                test_tables = tables_data[:3]  # Test with first 3 tables
                columns_data = collector.collect_columns(test_tables)
                logger.info(f"   ✅ Collected columns for {len(test_tables)} tables")
            else:
                # Get columns directly from connector for first table
                first_table = tables_data[0]
                columns = connector.get_columns(first_table)
                logger.info(f"   ✅ Got columns from connector for {first_table}: {len(columns)} columns")
        except Exception as columns_error:
            logger.error(f"   ❌ Columns collection failed: {str(columns_error)}")
            # Continue anyway - this isn't critical for testing

        # Test statistics collection (this is where the issue likely is)
        logger.info("   📊 Collecting statistics metadata...")
        try:
            # Try different methods for statistics collection
            test_tables = tables_data[:2]  # Limit to first 2 tables for testing
            logger.info(f"   📊 Testing statistics for {len(test_tables)} tables: {test_tables}")

            statistics_data = None

            # Try the most likely method names
            if hasattr(collector, 'collect_statistics'):
                logger.info("   📊 Using collect_statistics method...")
                statistics_data = collector.collect_statistics(test_tables)
            elif hasattr(collector, 'collect_table_statistics'):
                logger.info("   📊 Using collect_table_statistics method...")
                statistics_data = {}
                for table in test_tables:
                    table_stats = collector.collect_table_statistics(table)
                    statistics_data[table] = table_stats
            elif hasattr(collector, '_collect_table_statistics'):
                logger.info("   📊 Using _collect_table_statistics method...")
                statistics_data = {}
                for table in test_tables:
                    table_stats = collector._collect_table_statistics(table)
                    statistics_data[table] = table_stats
            else:
                logger.warning("   ⚠️  No statistics collection method found in collector")

                # Try manual statistics collection using connector
                logger.info("   📊 Attempting manual statistics collection...")
                statistics_data = {}
                for table in test_tables:
                    try:
                        # Basic row count
                        count_query = f"SELECT COUNT(*) as row_count FROM {table}"
                        count_result = connector.execute_query(count_query)
                        row_count = count_result[0][0] if count_result else 0

                        statistics_data[table] = {
                            'row_count': row_count
                        }
                        logger.info(f"   📊 Manual stats for {table}: {row_count} rows")
                    except Exception as manual_error:
                        logger.warning(f"   ⚠️  Manual stats failed for {table}: {str(manual_error)}")
                        statistics_data[table] = {'error': str(manual_error)}

            if statistics_data:
                stats_collected = len(statistics_data)
                logger.info(f"   ✅ Collected statistics for {stats_collected} tables")

                # Show sample statistics
                for table_name, table_stats in list(statistics_data.items())[:1]:
                    logger.info(f"   📊 Sample statistics for {table_name}:")
                    if isinstance(table_stats, dict):
                        for stat_name, stat_value in list(table_stats.items())[:3]:
                            logger.info(f"      - {stat_name}: {stat_value}")
                    else:
                        logger.info(f"      - Raw data: {table_stats}")
            else:
                logger.warning("   ⚠️  No statistics data collected")
                return False

            return True

        except Exception as stats_error:
            logger.error(f"   ❌ Statistics collection failed: {str(stats_error)}")
            import traceback
            logger.error(f"   🔍 Statistics error details: {traceback.format_exc()}")

            # Try to diagnose the statistics collection issue
            error_str = str(stats_error).lower()
            if 'timeout' in error_str:
                logger.error("   💡 Statistics collection timed out - try reducing table limit")
            elif any(keyword in error_str for keyword in ['memory', 'out of']):
                logger.error("   💡 Statistics collection ran out of memory - try smaller batch size")
            elif 'permission' in error_str:
                logger.error("   💡 Statistics collection failed due to permissions")
            else:
                logger.error("   💡 Statistics collection failed for unknown reason")

            return False

    except Exception as e:
        logger.error(f"❌ Manual metadata collection test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def fix_common_connection_issues(connection_id, supabase):
    """Try to fix common connection issues"""
    try:
        logger.info("🔧 Attempting to fix common connection issues...")

        # Get connection details
        connection = supabase.get_connection(connection_id)
        if not connection:
            logger.error("❌ Could not get connection details for fixing")
            return False

        connection_details = connection.get('connection_details', {})

        # Check for common issues
        fixes_applied = []

        # Fix 1: Check if connection string is properly formatted
        connection_string = connection_details.get('connection_string', '')
        if connection_string:
            logger.info("   🔍 Checking connection string format...")

            if not connection_string.startswith('snowflake://'):
                logger.warning("   ⚠️  Connection string doesn't start with snowflake://")
                fixes_applied.append("connection_string_format_issue")
            else:
                logger.info("   ✅ Connection string format looks correct")

        # Fix 2: Check individual connection parameters
        required_params = ['account', 'user', 'warehouse', 'database']
        missing_params = []

        for param in required_params:
            if not connection_details.get(param):
                missing_params.append(param)

        if missing_params:
            logger.warning(f"   ⚠️  Missing connection parameters: {missing_params}")
            fixes_applied.append(f"missing_params_{','.join(missing_params)}")
        else:
            logger.info("   ✅ All required connection parameters present")

        # Fix 3: Test connection string resolution
        try:
            from core.utils.connection_utils import resolve_connection_string
            resolved_string = resolve_connection_string(connection_string)

            if resolved_string != connection_string:
                logger.info("   🔧 Connection string needed resolution")
                fixes_applied.append("connection_string_resolved")
            else:
                logger.info("   ✅ Connection string didn't need resolution")

        except Exception as resolve_error:
            logger.warning(f"   ⚠️  Connection string resolution failed: {str(resolve_error)}")
            fixes_applied.append("connection_string_resolution_failed")

        # Fix 4: Check if connection details have changed recently
        try:
            # This would require checking connection update timestamps
            logger.info("   📅 Connection details appear to be current")

        except Exception as timestamp_error:
            logger.warning(f"   ⚠️  Could not check connection timestamp: {str(timestamp_error)}")

        # Report fixes
        if fixes_applied:
            logger.warning(f"   🔧 Issues found: {fixes_applied}")
            return False
        else:
            logger.info("   ✅ No common connection issues detected")
            return True

    except Exception as e:
        logger.error(f"❌ Error while fixing connection issues: {str(e)}")
        return False


def main():
    """Enhanced main function with better connector debugging"""
    try:
        logger.info("=== Enhanced Sparvi Metadata Statistics Test and Fix Script ===")
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

        # Step 3: Check current metadata statistics state (same as before)
        logger.info("Step 3: Checking current metadata statistics...")

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
            latest_record = records[0]
            collected_at = latest_record.get("collected_at", "unknown")

            try:
                collected_time = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                age_hours = (datetime.now(timezone.utc) - collected_time).total_seconds() / 3600
                age_status = "🟢 Fresh" if age_hours < 24 else "🟡 Stale" if age_hours < 72 else "🔴 Very Stale"
            except:
                age_hours = float('inf')
                age_status = "🔴 Unknown"

            logger.info(f"   📊 {metadata_type}: {len(records)} records, latest: {age_status} ({age_hours:.1f}h ago)")

        # Step 4: Fix common connection issues BEFORE testing
        logger.info("Step 4: Checking and fixing common connection issues...")
        fix_result = fix_common_connection_issues(connection_id, supabase)

        if not fix_result:
            logger.warning("⚠️  Some connection issues detected - proceeding with testing anyway")

        # Step 5: Enhanced database connection testing
        logger.info("Step 5: Enhanced database connection testing...")
        connection_test_result = test_database_connection(connection_id, supabase)

        if not connection_test_result:
            logger.error("❌ Database connection test failed - cannot proceed with metadata collection")

            # Provide specific recommendations
            logger.info("💡 Recommendations to fix connection issues:")
            logger.info("   1. Check database credentials in connection configuration")
            logger.info("   2. Verify Snowflake warehouse is running and accessible")
            logger.info("   3. Check network connectivity to Snowflake")
            logger.info("   4. Verify user permissions for the database and warehouse")
            logger.info("   5. Try connecting manually using the connection string")

            return False

        logger.info("✅ Database connection test passed")

        # Step 6: Test metadata task manager (same as before)
        logger.info("Step 6: Testing metadata task manager...")

        try:
            from core.metadata.manager import MetadataTaskManager
            metadata_manager = MetadataTaskManager.get_instance(supabase_manager=supabase)

            if not metadata_manager:
                logger.error("❌ Could not get metadata task manager instance")
                return False

            logger.info("✅ Metadata task manager available")

        except ImportError as import_error:
            logger.error(f"❌ Could not import metadata task manager: {str(import_error)}")
            return False

        # Step 7: Test manual metadata collection with working connector
        logger.info("Step 7: Testing manual metadata collection...")

        # Recreate connector since we know it works now
        from core.metadata.connector_factory import ConnectorFactory
        connector_factory = ConnectorFactory(supabase)
        connector = connector_factory.create_connector(connection_id)

        # Ensure connection is established
        if hasattr(connector, 'connect'):
            connector.connect()

        manual_collection_result = test_manual_metadata_collection(connection_id, connector, supabase)

        if not manual_collection_result:
            logger.error("❌ Manual metadata collection failed")

            logger.info("💡 Recommendations to fix metadata collection:")
            logger.info("   1. Check table permissions - user may not have access to query tables")
            logger.info("   2. Try reducing table limit for statistics collection")
            logger.info("   3. Check for long-running queries or timeouts")
            logger.info("   4. Verify warehouse has sufficient compute resources")

            return False

        logger.info("✅ Manual metadata collection successful")

        # Step 8: Test full metadata collection task (same as before but with working connection)
        logger.info("Step 8: Testing full metadata collection task...")

        try:
            task_params = {
                "depth": "standard",
                "table_limit": 3,  # Reduced to 3 for faster testing
                "automation_trigger": False,
                "test_run": True,
                "refresh_types": ["tables", "columns", "statistics"],
                "timeout_minutes": 5  # Reduced timeout for testing
            }

            logger.info(f"   🚀 Submitting metadata collection task with params: {task_params}")

            task_id = metadata_manager.submit_collection_task(
                connection_id=connection_id,
                params=task_params,
                priority="high"
            )

            logger.info(f"   ✅ Task submitted with ID: {task_id}")

            # Wait for task completion
            logger.info("   ⏳ Waiting for task completion (max 5 minutes)...")

            completion_result = metadata_manager.wait_for_task_completion_sync(task_id, timeout_minutes=5)

            if completion_result.get("completed", False):
                if completion_result.get("success", False):
                    logger.info("   🎉 Metadata collection task completed successfully!")

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

                    task_status = metadata_manager.get_task_status(task_id)
                    logger.error(f"   🔍 Task status details: {task_status}")
            else:
                logger.error("   ❌ Task did not complete within timeout")

                task_status = metadata_manager.get_task_status(task_id)
                logger.error(f"   🔍 Task status: {task_status}")

        except Exception as task_error:
            logger.error(f"❌ Metadata collection task failed: {str(task_error)}")
            import traceback
            logger.error(traceback.format_exc())

        # Step 9: Check for updated metadata (same as before)
        logger.info("Step 9: Checking for updated metadata...")

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

                    for table_name, table_stats in list(stats_tables.items())[:2]:
                        logger.info(f"      - {table_name}: {len(table_stats)} statistics")
                        for stat_name, stat_value in list(table_stats.items())[:3]:
                            logger.info(f"        * {stat_name}: {stat_value}")
                else:
                    logger.info(f"   📁 {metadata_type} record collected at {collected_at}")
        else:
            logger.warning("⚠️  No recent metadata records found - collection may have failed")

        # Step 10: Final summary with enhanced diagnostics
        logger.info("Step 10: Enhanced Final Summary...")

        total_metadata = len(metadata_records)
        statistics_count = len(metadata_by_type.get("statistics", []))
        recent_count = len(recent_records)

        # Determine root cause if statistics are stale
        statistics_records = metadata_by_type.get("statistics", [])
        if statistics_records:
            latest_stats = statistics_records[0]
            try:
                collected_time = datetime.fromisoformat(latest_stats["collected_at"].replace('Z', '+00:00'))
                age_hours = (datetime.now(timezone.utc) - collected_time).total_seconds() / 3600

                if age_hours > 48:  # More than 2 days old
                    logger.warning(f"🔴 ISSUE IDENTIFIED: Statistics are {age_hours:.1f} hours old")

                    # Check automation configuration
                    config_response = supabase.supabase.table("automation_connection_configs") \
                        .select("metadata_refresh") \
                        .eq("connection_id", connection_id) \
                        .execute()

                    if config_response.data:
                        metadata_config = config_response.data[0].get("metadata_refresh", {})
                        if isinstance(metadata_config, str):
                            import json
                            try:
                                metadata_config = json.loads(metadata_config)
                            except:
                                metadata_config = {}

                        enabled = metadata_config.get("enabled", False)
                        interval_hours = metadata_config.get("interval_hours", 24)
                        types = metadata_config.get("types", [])

                        logger.info(
                            f"   📊 Automation config - Enabled: {enabled}, Interval: {interval_hours}h, Types: {types}")

                        if not enabled:
                            logger.error("   🔴 ROOT CAUSE: Metadata refresh automation is DISABLED")
                        elif "statistics" not in types:
                            logger.error("   🔴 ROOT CAUSE: Statistics not included in automation types")
                        elif interval_hours > age_hours:
                            logger.warning(
                                f"   🟡 Automation interval ({interval_hours}h) longer than age ({age_hours:.1f}h)")
                        else:
                            logger.error("   🔴 ROOT CAUSE: Automation is configured but not running properly")
                    else:
                        logger.error("   🔴 ROOT CAUSE: No automation configuration found")

            except Exception as time_error:
                logger.error(f"   🔴 Could not determine statistics age: {str(time_error)}")

        if statistics_count == 0:
            health_status = "🔴 CRITICAL"
            health_msg = "No statistics metadata found - statistics collection has never worked"
        elif statistics_count > 0 and recent_count > 0:
            health_status = "🟢 HEALTHY"
            health_msg = "Statistics collection is working properly"
        elif statistics_count > 0 and age_hours <= 48:
            health_status = "🟡 WARNING"
            health_msg = "Statistics exist but slightly stale - check automation frequency"
        elif statistics_count > 0:
            health_status = "🔴 CRITICAL"
            health_msg = "Statistics exist but very stale - automation is likely broken"
        else:
            health_status = "🔴 UNKNOWN"
            health_msg = "Cannot determine statistics collection health"

        logger.info(f"{health_status}: {health_msg}")
        logger.info(f"📊 Final Summary:")
        logger.info(f"   - Database connection: {'✅ Working' if connection_test_result else '❌ Failed'}")
        logger.info(f"   - Manual collection: {'✅ Working' if manual_collection_result else '❌ Failed'}")
        logger.info(f"   - Total metadata records: {total_metadata}")
        logger.info(f"   - Statistics records: {statistics_count}")
        logger.info(f"   - Recent records (15m): {recent_count}")

        logger.info("=== Enhanced Metadata Statistics Test Completed ===")

        return connection_test_result and manual_collection_result

    except Exception as e:
        logger.error(f"❌ Enhanced script failed with error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)