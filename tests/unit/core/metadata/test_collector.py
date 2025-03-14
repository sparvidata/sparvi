# test_collector.py
import unittest
from unittest.mock import MagicMock, patch
from backend.core.metadata.collector import MetadataCollector


class TestMetadataCollector(unittest.TestCase):
    def setUp(self):
        # Create a mock connector
        self.mock_connector = MagicMock()
        self.mock_connector.get_tables.return_value = ["table1", "table2", "table3"]
        self.mock_connector.get_columns.return_value = [
            {"name": "col1", "type": "int", "nullable": False},
            {"name": "col2", "type": "varchar", "nullable": True}
        ]
        self.mock_connector.get_primary_keys.return_value = ["col1"]
        self.mock_connector.execute_query.return_value = [(100,)]  # Mock row count

        # Create collector with mock connector
        self.collector = MetadataCollector("conn-123", self.mock_connector)

    def test_collect_table_list(self):
        # Test table list collection
        tables = self.collector.collect_table_list()
        self.assertEqual(len(tables), 3)
        self.mock_connector.get_tables.assert_called_once()

    def test_collect_columns(self):
        # Test column collection
        columns = self.collector.collect_columns("table1")
        self.assertEqual(len(columns), 2)
        self.mock_connector.get_columns.assert_called_once_with("table1")

    def test_collect_table_metadata_sync(self):
        # Test complete table metadata collection
        metadata = self.collector.collect_table_metadata_sync("table1")

        # Verify the metadata structure
        self.assertEqual(metadata["table_name"], "table1")
        self.assertEqual(metadata["column_count"], 2)
        self.assertEqual(len(metadata["columns"]), 2)
        self.assertEqual(metadata["primary_keys"], ["col1"])
        self.assertEqual(metadata["row_count"], 100)

        # Verify method calls
        self.mock_connector.get_columns.assert_called_with("table1")
        self.mock_connector.get_primary_keys.assert_called_with("table1")
        self.mock_connector.execute_query.assert_called()

    def test_collect_comprehensive_metadata(self):
        # Test comprehensive collection with different depths
        # Low depth
        metadata_low = self.collector.collect_comprehensive_metadata(table_limit=10, depth="low")
        self.assertEqual(len(metadata_low["tables"]), 3)

        # Medium depth
        metadata_medium = self.collector.collect_comprehensive_metadata(table_limit=10, depth="medium")
        # Verify additional data is collected in medium depth

        # Reset mock call counts between tests
        self.mock_connector.reset_mock()