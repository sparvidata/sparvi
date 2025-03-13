import logging
import uuid
import time
from datetime import datetime, timedelta
import json

# Configure logging
logger = logging.getLogger(__name__)


class MetadataCollector:
    """Collects metadata from database connections"""

    def __init__(self, connection_id, connector):
        self.connection_id = connection_id
        self.connector = connector
        self.metadata = {}

    async def collect_immediate_metadata(self):
        """Collect high-priority metadata quickly (Tier 1 & 2)"""
        logger.info(f"Collecting immediate metadata for connection {self.connection_id}")

        # Connect to the database
        self.connector.connect()

        # Collect table list (Tier 1)
        tables = self.collect_table_list()

        # Only collect columns for the first few tables to avoid overloading
        column_metadata = {}
        for table in tables[:10]:  # Limit to first 10 tables for immediate collection
            column_metadata[table] = self.collect_columns(table)

        # Return just the essential metadata
        return {
            "table_list": tables,
            "column_metadata": column_metadata
        }

    def collect_table_list(self):
        """Collect list of tables"""
        logger.info(f"Collecting table list for connection {self.connection_id}")
        try:
            tables = self.connector.get_tables()
            logger.info(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            logger.error(f"Error collecting table list: {str(e)}")
            return []

    def collect_columns(self, table_name):
        """Collect column metadata for a specific table"""
        logger.info(f"Collecting column metadata for table {table_name}")
        try:
            columns = self.connector.get_columns(table_name)

            # Convert to a more friendly format
            result = []
            for col in columns:
                column_info = {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col["nullable"]
                }

                # Add additional properties if available
                if "default" in col:
                    column_info["default"] = str(col["default"])

                result.append(column_info)

            logger.info(f"Collected metadata for {len(result)} columns in {table_name}")
            return result
        except Exception as e:
            logger.error(f"Error collecting column metadata for {table_name}: {str(e)}")
            return []

    async def collect_table_metadata(self, table_name):
        """Collect detailed metadata for a specific table"""
        logger.info(f"Collecting detailed metadata for table {table_name}")

        # If not already connected, connect
        if not self.connector.inspector:
            self.connector.connect()

        try:
            # Basic table information
            columns = self.collect_columns(table_name)
            primary_keys = self.connector.get_primary_keys(table_name)

            # Row count - use a query
            row_count = 0
            try:
                result = self.connector.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                if result and len(result) > 0:
                    row_count = result[0][0]
            except Exception as e:
                logger.error(f"Error getting row count for {table_name}: {str(e)}")

            # Compile table metadata
            table_metadata = {
                "table_name": table_name,
                "column_count": len(columns),
                "columns": columns,
                "primary_keys": primary_keys,
                "row_count": row_count,
                "collected_at": datetime.now().isoformat()
            }

            logger.info(f"Successfully collected metadata for table {table_name}")
            return table_metadata
        except Exception as e:
            logger.error(f"Error collecting table metadata for {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "error": str(e),
                "collected_at": datetime.now().isoformat()
            }

    def collect_immediate_metadata_sync(self):
        """Synchronous version of collect_immediate_metadata"""
        logger.info(f"Collecting immediate metadata for connection {self.connection_id}")

        # Connect to the database
        self.connector.connect()

        # Collect table list (Tier 1)
        tables = self.collect_table_list()

        # Only collect columns for the first few tables to avoid overloading
        column_metadata = {}
        for table in tables[:10]:  # Limit to first 10 tables for immediate collection
            column_metadata[table] = self.collect_columns(table)

        # Return just the essential metadata
        return {
            "table_list": tables,
            "column_metadata": column_metadata
        }

    def collect_table_metadata_sync(self, table_name):
        """Synchronous version of collect_table_metadata"""
        logger.info(f"Collecting detailed metadata for table {table_name}")

        # If not already connected, connect
        if not self.connector.inspector:
            self.connector.connect()

        try:
            # Basic table information
            columns = self.collect_columns(table_name)
            primary_keys = self.connector.get_primary_keys(table_name)

            # Row count - use a query
            row_count = 0
            try:
                result = self.connector.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                if result and len(result) > 0:
                    row_count = result[0][0]
            except Exception as e:
                logger.error(f"Error getting row count for {table_name}: {str(e)}")

            # Compile table metadata
            table_metadata = {
                "table_name": table_name,
                "column_count": len(columns),
                "columns": columns,
                "primary_keys": primary_keys,
                "row_count": row_count,
                "collected_at": datetime.now().isoformat()
            }

            logger.info(f"Successfully collected metadata for table {table_name}")
            return table_metadata
        except Exception as e:
            logger.error(f"Error collecting table metadata for {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "error": str(e),
                "collected_at": datetime.now().isoformat()
            }