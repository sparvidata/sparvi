"""Connection manager for database connections"""

import logging
from core.metadata.connectors import SnowflakeConnector

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manager for creating and managing database connections"""
    
    @staticmethod
    def get_connector_for_connection(connection_details):
        """Create the appropriate connector based on connection type"""
        connection_type = connection_details.get("connection_type")

        if connection_type == "snowflake":
            return SnowflakeConnector(connection_details.get("connection_details", {}))
        elif connection_type == "postgresql":
            # Add PostgreSQL connector when implemented
            raise NotImplementedError("PostgreSQL connector not yet implemented")
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")

    @staticmethod
    def validate_connection_details(connection_type, details):
        """Validate connection details for a given connection type"""
        if connection_type == "snowflake":
            return ConnectionManager._validate_snowflake_details(details)
        elif connection_type == "postgresql":
            return ConnectionManager._validate_postgresql_details(details)
        else:
            return False, f"Unsupported connection type: {connection_type}"

    @staticmethod
    def _validate_snowflake_details(details):
        """Validate Snowflake connection details"""
        required_fields = ["username", "password", "account", "database"]
        missing_fields = [field for field in required_fields if not details.get(field)]
        
        if missing_fields:
            return False, f"Missing required Snowflake fields: {', '.join(missing_fields)}"
        
        return True, "Valid"

    @staticmethod
    def _validate_postgresql_details(details):
        """Validate PostgreSQL connection details"""
        required_fields = ["username", "password", "host", "database"]
        missing_fields = [field for field in required_fields if not details.get(field)]
        
        if missing_fields:
            return False, f"Missing required PostgreSQL fields: {', '.join(missing_fields)}"
        
        return True, "Valid"