import logging
import uuid
import time
import asyncio
from datetime import datetime, timedelta, timezone
import json
from typing import List, Dict, Any, Optional, Tuple

# Configure logging
logger = logging.getLogger(__name__)


class MetadataCollector:
    """Collects metadata from database connections with a multi-tiered approach"""

    def __init__(self, connection_id, connector):
        self.connection_id = connection_id
        self.connector = connector
        self.metadata = {}

    async def collect_immediate_metadata(self):
        """
        Collect high-priority metadata quickly (Tier 1 & 2)

        This includes:
        - Table/View names
        - Basic column information for a limited number of tables
        """
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
        """Collect list of tables (Tier 1)"""
        logger.info(f"Collecting table list for connection {self.connection_id}")
        try:
            tables = self.connector.get_tables()
            logger.info(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            logger.error(f"Error collecting table list: {str(e)}")
            return []

    def collect_columns(self, table_name):
        """Collect column metadata for a specific table (Tier 2)"""
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
        """Collect detailed metadata for a specific table (Tier 3-4)"""
        logger.info(f"Collecting detailed metadata for table {table_name}")

        # If not already connected, connect
        if not self.connector.inspector:
            self.connector.connect()

        try:
            # Basic table information
            columns = self.collect_columns(table_name)
            primary_keys = self.connector.get_primary_keys(table_name)

            # Row count - use a query (Tier 4 - basic statistics)
            row_count = 0
            try:
                result = self.connector.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                if result and len(result) > 0:
                    row_count = result[0][0]
            except Exception as e:
                logger.error(f"Error getting row count for {table_name}: {str(e)}")

            # Foreign key relationships (Tier 3)
            foreign_keys = []
            try:
                if hasattr(self.connector.inspector, 'get_foreign_keys'):
                    fks = self.connector.inspector.get_foreign_keys(table_name)
                    for fk in fks:
                        foreign_keys.append({
                            "constrained_columns": fk.get("constrained_columns", []),
                            "referred_table": fk.get("referred_table", ""),
                            "referred_columns": fk.get("referred_columns", [])
                        })
            except Exception as e:
                logger.warning(f"Error collecting foreign keys for {table_name}: {str(e)}")

            # Table indices (Tier 3)
            indices = []
            try:
                if hasattr(self.connector.inspector, 'get_indexes'):
                    idx_info = self.connector.inspector.get_indexes(table_name)
                    indices = [{
                        "name": idx.get("name", ""),
                        "columns": idx.get("column_names", []),
                        "unique": idx.get("unique", False)
                    } for idx in idx_info]
            except Exception as e:
                logger.warning(f"Error collecting indices for {table_name}: {str(e)}")

            # Advanced column statistics (Tier 5)
            column_stats = {}
            for column in columns[:10]:  # Limit to first 10 columns to prevent overload
                try:
                    col_name = column["name"]
                    col_type = column["type"].lower() if isinstance(column["type"], str) else str(
                        column["type"]).lower()

                    # Only collect statistics for certain data types
                    if any(type_str in col_type for type_str in
                           ["int", "float", "num", "dec", "date", "char", "var", "text"]):
                        stats = self._collect_column_statistics(table_name, col_name, col_type)
                        if stats:
                            column_stats[col_name] = stats
                except Exception as e:
                    logger.warning(f"Error collecting statistics for {column['name']}: {str(e)}")

            # Compile table metadata
            table_metadata = {
                "table_name": table_name,
                "column_count": len(columns),
                "columns": columns,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
                "indices": indices,
                "row_count": row_count,
                "column_statistics": column_stats,
                "collected_at": datetime.now(timezone.utc).isoformat()
            }

            logger.info(f"Successfully collected metadata for table {table_name}")
            return table_metadata
        except Exception as e:
            logger.error(f"Error collecting table metadata for {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "error": str(e),
                "collected_at": datetime.now(timezone.utc).isoformat()
            }

    def _collect_column_statistics(self, table_name, column_name, column_type):
        """
        Collect detailed statistics for a specific column (Tier 5)

        Args:
            table_name: Name of the table
            column_name: Name of the column
            column_type: Data type of the column

        Returns:
            Dictionary of statistics or None if error
        """
        try:
            stats = {}

            # Common statistics for all types
            null_query = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"
            total_query = f"SELECT COUNT(*) FROM {table_name}"
            distinct_query = f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name} WHERE {column_name} IS NOT NULL"

            # Execute queries
            null_count = self.connector.execute_query(null_query)[0][0]
            total_count = self.connector.execute_query(total_query)[0][0]
            distinct_count = self.connector.execute_query(distinct_query)[0][0]

            # Calculate percentages
            non_null_count = total_count - null_count
            null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
            distinct_percentage = (distinct_count / non_null_count * 100) if non_null_count > 0 else 0

            # Add to stats
            stats["null_count"] = null_count
            stats["null_percentage"] = null_percentage
            stats["distinct_count"] = distinct_count
            stats["distinct_percentage"] = distinct_percentage
            stats["is_unique"] = non_null_count == distinct_count and non_null_count > 0

            # Type-specific statistics
            if any(type_str in column_type for type_str in ["int", "float", "num", "dec"]):
                # Numeric column statistics
                num_stats_query = f"""
                    SELECT 
                        MIN({column_name}), 
                        MAX({column_name}), 
                        AVG({column_name})
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                """
                result = self.connector.execute_query(num_stats_query)
                if result and len(result) > 0:
                    stats["min_value"] = result[0][0]
                    stats["max_value"] = result[0][1]
                    stats["avg_value"] = result[0][2]

            elif any(type_str in column_type for type_str in ["char", "var", "text"]):
                # String column statistics
                str_stats_query = f"""
                    SELECT 
                        MIN(LENGTH({column_name})), 
                        MAX(LENGTH({column_name})), 
                        AVG(LENGTH({column_name}))
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                """
                result = self.connector.execute_query(str_stats_query)
                if result and len(result) > 0:
                    stats["min_length"] = result[0][0]
                    stats["max_length"] = result[0][1]
                    stats["avg_length"] = result[0][2]

            elif "date" in column_type:
                # Date column statistics
                date_stats_query = f"""
                    SELECT 
                        MIN({column_name}), 
                        MAX({column_name})
                    FROM {table_name}
                    WHERE {column_name} IS NOT NULL
                """
                result = self.connector.execute_query(date_stats_query)
                if result and len(result) > 0:
                    stats["min_date"] = result[0][0].isoformat() if hasattr(result[0][0], 'isoformat') else str(
                        result[0][0])
                    stats["max_date"] = result[0][1].isoformat() if hasattr(result[0][1], 'isoformat') else str(
                        result[0][1])

            return stats
        except Exception as e:
            logger.warning(f"Error collecting statistics for column {column_name}: {str(e)}")
            return None

    def collect_usage_patterns(self, table_name):
        """
        Collect usage patterns metadata (Tier 6)
        Note: This is database-specific and may not be available for all databases
        """
        logger.info(f"Collecting usage patterns for table {table_name}")

        try:
            # Only implement for Snowflake for now
            if hasattr(self.connector, 'get_database_type') and self.connector.get_database_type() == "snowflake":
                # Query Snowflake query history
                query = f"""
                    SELECT 
                        COUNT(*) as access_count,
                        MAX(START_TIME) as last_accessed
                    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
                    WHERE QUERY_TEXT ILIKE '%{table_name}%'
                    AND QUERY_TYPE = 'SELECT'
                """
                result = self.connector.execute_query(query)

                if result and len(result) > 0:
                    return {
                        "access_count": result[0][0],
                        "last_accessed": result[0][1].isoformat() if hasattr(result[0][1], 'isoformat') else str(
                            result[0][1]),
                        "collected_at": datetime.now(timezone.utc).isoformat()
                    }

            # Default empty result if not supported or no data
            return {
                "access_count": 0,
                "last_accessed": None,
                "collected_at": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.warning(f"Error collecting usage patterns for {table_name}: {str(e)}")
            return {
                "error": str(e),
                "collected_at": datetime.now(timezone.utc).isoformat()
            }

    def collect_comprehensive_metadata(self, table_limit=50, depth="medium"):
        """
        Collect comprehensive metadata with controlled depth

        Args:
            table_limit: Maximum number of tables to process
            depth: Collection depth - "low", "medium", or "high"

        Returns:
            Dictionary of collected metadata
        """
        logger.info(f"Starting comprehensive metadata collection with depth={depth} and table_limit={table_limit}")

        # Connect to database if not connected
        if not self.connector.inspector:
            self.connector.connect()

        # Get tables (Tier 1)
        tables = self.collect_table_list()
        logger.info(f"Found {len(tables)} tables, will process up to {table_limit}")

        # Apply table limit
        tables_to_process = tables[:min(len(tables), table_limit)]

        # Initialize results
        results = {
            "tables": [],
            "columns_by_table": {},
            "statistics_by_table": {},
            "relationships": [],
            "collection_metadata": {
                "connection_id": self.connection_id,
                "tables_found": len(tables),
                "tables_processed": len(tables_to_process),
                "start_time": datetime.now(timezone.utc).isoformat(),
                "depth": depth
            }
        }

        # Process each table based on depth
        for idx, table in enumerate(tables_to_process):
            try:
                if idx % 5 == 0:
                    logger.info(f"Processing table {idx + 1}/{len(tables_to_process)}: {table}")

                # Basic table info (Tier 1)
                table_info = {
                    "name": table,
                    "id": str(uuid.uuid4())
                }

                # Get columns (Tier 2)
                columns = self.collect_columns(table)
                results["columns_by_table"][table] = columns
                table_info["column_count"] = len(columns)

                # Get row count and primary keys (Tier 3-4)
                if depth != "low":
                    # Row count
                    try:
                        result = self.connector.execute_query(f"SELECT COUNT(*) FROM {table}")
                        if result and len(result) > 0:
                            table_info["row_count"] = result[0][0]
                    except Exception as e:
                        logger.warning(f"Could not get row count for {table}: {str(e)}")
                        table_info["row_count"] = None

                    # Primary keys
                    try:
                        primary_keys = self.connector.get_primary_keys(table)
                        table_info["primary_key"] = primary_keys
                    except Exception as e:
                        logger.warning(f"Could not get primary keys for {table}: {str(e)}")
                        table_info["primary_key"] = []

                # Collect statistics (Tier 5)
                if depth == "high":
                    table_stats = {
                        "row_count": table_info.get("row_count"),
                        "column_count": table_info.get("column_count", 0),
                        "has_primary_key": len(table_info.get("primary_key", [])) > 0,
                        "column_statistics": {}
                    }

                    # Process a subset of columns for statistics
                    for column in columns[:5]:  # Limit to first 5 columns for high depth
                        col_name = column["name"]
                        col_type = column["type"].lower() if isinstance(column["type"], str) else str(
                            column["type"]).lower()

                        stats = self._collect_column_statistics(table, col_name, col_type)
                        if stats:
                            table_stats["column_statistics"][col_name] = stats

                    results["statistics_by_table"][table] = table_stats

                # Add the table info to results
                results["tables"].append(table_info)

            except Exception as e:
                logger.error(f"Error processing table {table}: {str(e)}")
                # Continue with next table

        # Collection completion timestamp
        results["collection_metadata"]["end_time"] = datetime.now(timezone.utc).isoformat()
        duration = datetime.fromisoformat(results["collection_metadata"]["end_time"]) - \
                   datetime.fromisoformat(results["collection_metadata"]["start_time"])
        results["collection_metadata"]["duration_seconds"] = duration.total_seconds()

        logger.info(f"Comprehensive collection completed in {duration.total_seconds():.2f} seconds")
        return results

    def determine_refresh_strategy(self, object_type, last_refreshed=None, change_frequency="medium"):
        """
        Determine optimal refresh strategy for metadata

        Args:
            object_type: Type of metadata object (table_list, column_metadata, etc.)
            last_refreshed: When the metadata was last refreshed
            change_frequency: Historical change frequency (low, medium, high)

        Returns:
            Dictionary with schedule and priority
        """
        # Default strategies by object type
        strategies = {
            "table_list": {"schedule": "daily", "priority": "high"},
            "column_metadata": {"schedule": "weekly", "priority": "medium"},
            "statistics": {"schedule": "weekly", "priority": "low"},
            "relationships": {"schedule": "weekly", "priority": "medium"},
            "usage_patterns": {"schedule": "monthly", "priority": "low"}
        }

        # If object type is not recognized, use default strategy
        if object_type not in strategies:
            return {"schedule": "weekly", "priority": "low"}

        # Get base strategy
        strategy = strategies[object_type].copy()

        # Adjust based on change frequency
        if change_frequency == "high":
            # More frequent refresh for high-change objects
            if strategy["schedule"] == "monthly":
                strategy["schedule"] = "weekly"
            elif strategy["schedule"] == "weekly":
                strategy["schedule"] = "daily"
            strategy["priority"] = "high"
        elif change_frequency == "low":
            # Less frequent refresh for low-change objects
            if strategy["schedule"] == "daily":
                strategy["schedule"] = "weekly"
            elif strategy["schedule"] == "weekly":
                strategy["schedule"] = "monthly"

        # Adjust if last refresh was too recent (within a day) - prioritize less
        if last_refreshed:
            try:
                refresh_date = datetime.fromisoformat(last_refreshed.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc).isoformat()

                # If refreshed within the last day, lower priority
                if (now - refresh_date).total_seconds() < 86400:
                    if strategy["priority"] == "high":
                        strategy["priority"] = "medium"
                    elif strategy["priority"] == "medium":
                        strategy["priority"] = "low"
            except:
                pass

        return strategy

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
                "collected_at": datetime.now(timezone.utc).isoformat()
            }

            logger.info(f"Successfully collected metadata for table {table_name}")
            return table_metadata
        except Exception as e:
            logger.error(f"Error collecting table metadata for {table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "error": str(e),
                "collected_at": datetime.now(timezone.utc).isoformat()
            }