# test_connector_factory.py
import unittest
from unittest.mock import MagicMock, patch
from backend.core.metadata.connector_factory import ConnectorFactory


class TestConnectorFactory(unittest.TestCase):
    def setUp(self):
        # Create mock supabase manager
        self.mock_supabase_manager = MagicMock()
        self.mock_supabase_manager.get_connection.return_value = {
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

        # Create factory with mock manager
        self.factory = ConnectorFactory(self.mock_supabase_manager)

    @patch('backend.core.metadata.connectors.SnowflakeConnector')
    def test_create_connector_snowflake(self, mock_snowflake_connector):
        # Test creating Snowflake connector
        # Direct connection details
        connection = {
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

        connector = self.factory.create_connector(connection)
        mock_snowflake_connector.assert_called_once_with(connection["connection_details"])

    @patch('backend.core.metadata.connectors.SnowflakeConnector')
    def test_create_connector_by_id(self, mock_snowflake_connector):
        # Test creating connector by ID
        connector = self.factory.create_connector("conn-123")

        # Verify supabase manager was called to get connection
        self.mock_supabase_manager.get_connection.assert_called_once_with("conn-123")

        # Verify connector was created with the right details
        connection_details = self.mock_supabase_manager.get_connection()["connection_details"]
        mock_snowflake_connector.assert_called_once_with(connection_details)

    def test_unsupported_connection_type(self):
        # Test with unsupported connection type
        connection = {
            "connection_type": "unknown_type",
            "connection_details": {}
        }

        with self.assertRaises(ValueError):
            self.factory.create_connector(connection)