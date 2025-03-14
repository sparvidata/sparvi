import logging
import urllib.parse
import sqlalchemy as sa
from sqlalchemy import inspect

# Configure logging
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """Base class for database connectors"""

    def __init__(self, connection_details):
        self.connection_details = connection_details
        self.engine = None
        self.inspector = None

    def connect(self):
        """Establish connection to database - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement connect()")

    def get_tables(self):
        """Get list of tables - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement get_tables()")

    def get_columns(self, table_name):
        """Get columns for a table - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement get_columns()")


class SnowflakeConnector(DatabaseConnector):
    """Snowflake implementation of DatabaseConnector"""

    def connect(self):
        """Implement Snowflake connection logic"""
        try:
            # Extract connection parameters
            username = self.connection_details.get("username")
            password = self.connection_details.get("password")
            account = self.connection_details.get("account")
            database = self.connection_details.get("database")
            schema = self.connection_details.get("schema", "PUBLIC")
            warehouse = self.connection_details.get("warehouse")

            # Validate required parameters
            if not all([username, password, account, database, warehouse]):
                missing = []
                if not username: missing.append("username")
                if not password: missing.append("password")
                if not account: missing.append("account")
                if not database: missing.append("database")
                if not warehouse: missing.append("warehouse")

                raise ValueError(f"Missing required Snowflake connection parameters: {', '.join(missing)}")

            # URL encode password to handle special characters
            encoded_password = urllib.parse.quote_plus(password)

            # Build connection string
            connection_string = f"snowflake://{username}:{encoded_password}@{account}/{database}/{schema}?warehouse={warehouse}"

            # Create SQLAlchemy engine
            self.engine = sa.create_engine(connection_string)

            # Create SQLAlchemy inspector
            self.inspector = inspect(self.engine)

            logger.info(f"Successfully connected to Snowflake database {database}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise  # Re-raise the exception to be handled by the caller

    def get_tables(self):
        """Get list of tables from Snowflake"""
        if not self.inspector:
            raise ValueError("Not connected to database")

        try:
            tables = self.inspector.get_table_names()
            logger.debug(f"Retrieved {len(tables)} tables from Snowflake")
            return tables
        except Exception as e:
            logger.error(f"Error retrieving tables from Snowflake: {str(e)}")
            raise

    def get_columns(self, table_name):
        """Get columns for a specific table from Snowflake"""
        if not self.inspector:
            raise ValueError("Not connected to database")

        try:
            columns = self.inspector.get_columns(table_name)
            logger.debug(f"Retrieved {len(columns)} columns for table {table_name}")
            return columns
        except Exception as e:
            logger.error(f"Error retrieving columns for table {table_name}: {str(e)}")
            raise

    def get_primary_keys(self, table_name):
        """Get primary keys for a specific table"""
        if not self.inspector:
            raise ValueError("Not connected to database")

        try:
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            pk_columns = pk_constraint.get('constrained_columns', []) if pk_constraint else []
            logger.debug(f"Retrieved primary keys for table {table_name}: {pk_columns}")
            return pk_columns
        except Exception as e:
            logger.error(f"Error retrieving primary keys for table {table_name}: {str(e)}")
            raise

    def execute_query(self, query):
        """Execute a SQL query and return results"""
        if not self.engine:
            raise ValueError("Not connected to database")

        try:
            with self.engine.connect() as connection:
                result = connection.execute(sa.text(query))
                return result.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise