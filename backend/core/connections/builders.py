"""Connection string builders for different database types"""

import urllib.parse
import logging

logger = logging.getLogger(__name__)


class ConnectionStringBuilder:
    """Factory for building database connection strings"""
    
    @staticmethod
    def build_connection_string(connection_data):
        """Build connection string from stored connection details"""
        conn_type = connection_data["connection_type"]
        details = connection_data["connection_details"]

        if conn_type == "snowflake":
            return ConnectionStringBuilder._build_snowflake_connection(details)
        elif conn_type == "postgresql":
            return ConnectionStringBuilder._build_postgresql_connection(details)
        else:
            raise ValueError(f"Unsupported connection type: {conn_type}")

    @staticmethod
    def _build_snowflake_connection(details):
        """Build Snowflake connection string"""
        username = details.get("username", "")
        password = details.get("password", "")
        account = details.get("account", "")
        database = details.get("database", "")
        schema = details.get("schema", "PUBLIC")
        warehouse = details.get("warehouse", "")

        # URL encode password
        encoded_password = urllib.parse.quote_plus(password)

        return f"snowflake://{username}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"

    @staticmethod
    def _build_postgresql_connection(details):
        """Build PostgreSQL connection string"""
        username = details.get("username", "")
        password = details.get("password", "")
        host = details.get("host", "localhost")
        port = details.get("port", "5432")
        database = details.get("database", "")

        # URL encode password
        encoded_password = urllib.parse.quote_plus(password)

        return f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"

    @staticmethod
    def get_connection_string(connection):
        """Build and return a connection string from connection details"""
        try:
            connection_details = connection["connection_details"]
            connection_type = connection["connection_type"]

            if connection_type == "snowflake":
                return ConnectionStringBuilder._build_snowflake_connection(connection_details)
            elif connection_type == "postgresql":
                return ConnectionStringBuilder._build_postgresql_connection(connection_details)
            else:
                raise ValueError(f"Unsupported connection type: {connection_type}")
                
        except Exception as e:
            logger.error(f"Error building connection string: {str(e)}")
            return None