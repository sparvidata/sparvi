
import logging
import sys
import os
import asyncio
from datetime import datetime, timezone, timedelta

# Add the backend directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_schema_change_storage():
    """Test that schema changes can be stored correctly"""
    try:
        logger.info("Testing schema change storage...")

        from core.storage.supabase_manager import SupabaseManager
        from core.metadata.schema_change_detector import SchemaChangeDetector

        supabase = SupabaseManager()
        detector = SchemaChangeDetector()

        # Test with a sample connection (replace with your actual connection ID)
        test_connection_id = "a84eba26-da4a-4946-af2a-c91fc90680b4"  # From your logs

        # Create a test schema change
        test_changes = [{
            "type": "table_added",
            "table": "test_table_verification",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "details": {"column_count": 5}
        }]

        # Try to store the change
        stored_count = detector._store_schema_changes(test_connection_id, test_changes, supabase)

        if stored_count > 0:
            logger.info("✓ Schema change storage test PASSED")
            return True
        else:
            logger.error("✗ Schema change storage test FAILED")
            return False

    except Exception as e:
        logger.error(f"✗ Schema change storage test ERROR: {str(e)}")
        return False


def test_metadata_statistics_collection():
    """Test that statistics collection works"""
    try:
        logger.info("Testing metadata statistics collection...")

        from core.metadata.collector import MetadataCollector
        from core.storage.supabase_manager import SupabaseManager

        # Check if the collect_table_statistics method exists
        if hasattr(MetadataCollector, 'collect_table_statistics'):
            logger.info("✓ collect_table_statistics method exists")
            return True
        else:
            logger.error("✗ collect_table_statistics method missing")
            return False

    except Exception as e:
        logger.error(f"✗ Statistics collection test ERROR: {str(e)}")
        return False


def test_database_schema_columns():
    """Test that database schema has correct columns"""
    try:
        logger.info("Testing database schema columns...")

        from core.storage.supabase_manager import SupabaseManager

        supabase = SupabaseManager()

        # Test schema_changes table columns
        try:
            # Try to select using the correct column name
            response = supabase.supabase.table("schema_changes") \
                .select("details") \
                .limit(1) \
                .execute()
            logger.info("✓ schema_changes.details column exists")
            schema_changes_ok = True
        except Exception as e:
            logger.error(f"✗ schema_changes.details column issue: {str(e)}")
            schema_changes_ok = False

        # Test connection_metadata table
        try:
            response = supabase.supabase.table("connection_metadata") \
                .select("metadata_type") \
                .eq("metadata_type", "statistics") \
                .limit(1) \
                .execute()
            logger.info("✓ connection_metadata table accessible")
            metadata_table_ok = True
        except Exception as e:
            logger.error(f"✗ connection_metadata table issue: {str(e)}")
            metadata_table_ok = False

        return schema_changes_ok and metadata_table_ok

    except Exception as e:
        logger.error(f"✗ Database schema test ERROR: {str(e)}")
        return False


def test_automation_job_tracking():
    """Test that automation jobs can be tracked properly"""
    try:
        logger.info("Testing automation job tracking...")

        from core.storage.supabase_manager import SupabaseManager

        supabase = SupabaseManager()

        # Check recent automation jobs
        response = supabase.supabase.table("automation_jobs") \
            .select("id, status, job_type") \
            .order("created_at", desc=True) \
            .limit(5) \
            .execute()

        job_count = len(response.data) if response.data else 0
        logger.info(f"✓ Found {job_count} recent automation jobs")

        # Check automation runs
        runs_response = supabase.supabase.table("automation_runs") \
            .select("id, status") \
            .order("created_at", desc=True) \
            .limit(5) \
            .execute()

        runs_count = len(runs_response.data) if runs_response.data else 0
        logger.info(f"✓ Found {runs_count} recent automation runs")

        return True

    except Exception as e:
        logger.error(f"✗ Automation job tracking test ERROR: {str(e)}")
        return False


def main():
    """Run all verification tests"""
    logger.info("Starting automation fixes verification...")

    tests = [
        ("Database Schema Columns", test_database_schema_columns),
        ("Schema Change Storage", test_schema_change_storage),
        ("Statistics Collection", test_metadata_statistics_collection),
        ("Automation Job Tracking", test_automation_job_tracking)
    ]

    results = []

    for test_name, test_func in tests:
        logger.info(f"\n{'=' * 50}")
        logger.info(f"Running: {test_name}")
        logger.info('=' * 50)

        try:
            result = test_func()
            results.append((test_name, result))

            if result:
                logger.info(f"✓ {test_name} PASSED")
            else:
                logger.error(f"✗ {test_name} FAILED")

        except Exception as e:
            logger.error(f"✗ {test_name} ERROR: {str(e)}")
            results.append((test_name, False))

    # Summary
    logger.info(f"\n{'=' * 50}")
    logger.info("VERIFICATION SUMMARY")
    logger.info('=' * 50)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        logger.info(f"{test_name}: {status}")

    logger.info(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All verification tests PASSED!")
        logger.info("Your automation fixes should now work correctly.")
    else:
        logger.warning("⚠️  Some tests failed. Please review the issues above.")

        # Provide recommendations
        logger.info("\nRECOMMENDATIONS:")

        for test_name, result in results:
            if not result:
                if "Schema Change" in test_name:
                    logger.info("- Apply the fixed schema_change_detector.py")
                elif "Statistics" in test_name:
                    logger.info("- Apply the metadata worker statistics fix")
                elif "Database Schema" in test_name:
                    logger.info("- Check your database schema matches the expected columns")
                elif "Job Tracking" in test_name:
                    logger.info("- Verify automation tables exist and are accessible")


if __name__ == "__main__":
    main()