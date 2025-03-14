# test_task_manager.py
import unittest
import time
from unittest.mock import MagicMock, patch
from backend.core.metadata.manager import MetadataTaskManager


class TestTaskManager(unittest.TestCase):
    def setUp(self):
        # Create mock services
        self.mock_storage = MagicMock()
        self.mock_supabase = MagicMock()

        # Mock the connection details method
        self.connection_details = {
            "connection_type": "snowflake",
            "connection_details": {
                "username": "test",
                "password": "pass",
                "account": "acct",
                "database": "db",
                "schema": "public",
                "warehouse": "wh"
            }
        }

        # Initialize task manager with mocks
        self.task_manager = MetadataTaskManager(self.mock_storage, self.mock_supabase)

        # Override the _get_connection_details method
        self.task_manager._get_connection_details = MagicMock(return_value=self.connection_details)

    @patch('backend.core.metadata.connector_factory.ConnectorFactory.create_connector')
    @patch('backend.core.metadata.collector.MetadataCollector')
    def test_submit_collection_task(self, mock_collector_class, mock_create_connector):
        # Configure mocks
        mock_connector = MagicMock()
        mock_create_connector.return_value = mock_connector

        mock_collector = MagicMock()
        mock_collector_class.return_value = mock_collector

        # Mock the comprehensive collection method
        mock_collector.collect_comprehensive_metadata.return_value = {
            "tables": [{"name": "table1"}, {"name": "table2"}],
            "columns_by_table": {
                "table1": [{"name": "col1"}]
            },
            "statistics_by_table": {},
            "collection_metadata": {"duration_seconds": 2.5}
        }

        # Submit a collection task
        task_id = self.task_manager.submit_collection_task(
            "conn-123",
            {"depth": "medium", "table_limit": 50}
        )

        # Wait a moment for task to be processed
        time.sleep(1)

        # Check task history
        task_status = self.task_manager.get_task_status(task_id)
        self.assertIsNotNone(task_status)

        # Verify methods were called
        mock_create_connector.assert_called()
        mock_collector_class.assert_called_with("conn-123", mock_connector)
        mock_collector.collect_comprehensive_metadata.assert_called()

        # Check storage was called to store metadata
        self.mock_storage.store_tables_metadata.assert_called()
        self.mock_storage.store_columns_metadata.assert_called()

    def test_handle_metadata_event(self):
        # Test different event handling
        with patch.object(self.task_manager, 'submit_table_metadata_task') as mock_table_task:
            mock_table_task.return_value = "task-123"

            # Test validation failure event
            result = self.task_manager.handle_metadata_event(
                "validation_failure",
                "conn-123",
                {"reason": "schema_mismatch", "table_name": "table1"}
            )

            self.assertEqual(result, "task-123")
            mock_table_task.assert_called_with("conn-123", "table1", "high")