import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from supabase import create_client

logger = logging.getLogger(__name__)
load_dotenv()


class MetadataStorageService:
    """Enhanced metadata storage service with verification and retry logic"""

    def __init__(self):
        """Initialize with Supabase connection"""
        import os
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Missing Supabase credentials")

        self.supabase = create_client(self.supabase_url, self.supabase_key)
        logger.info("Enhanced metadata storage service initialized")

    def store_tables_metadata(self, connection_id: str, tables_metadata: List[Dict],
                              max_retries: int = 3, verify_storage: bool = True) -> bool:
        """Store table list and basic table metadata with verification"""
        try:
            logger.info(f"Storing tables metadata for connection {connection_id}: {len(tables_metadata)} tables")

            # Validate input data
            if not tables_metadata or not isinstance(tables_metadata, list):
                logger.error("Invalid tables metadata: must be a non-empty list")
                return False

            # Clean and validate table data
            cleaned_tables = []
            for table in tables_metadata:
                if isinstance(table, dict) and "name" in table:
                    cleaned_table = {
                        "name": table["name"],
                        "id": table.get("id", table["name"]),  # Use name as fallback ID
                        "column_count": table.get("column_count", 0),
                        "row_count": table.get("row_count"),
                        "primary_key": table.get("primary_key", [])
                    }
                    # Remove None values
                    cleaned_table = {k: v for k, v in cleaned_table.items() if v is not None}
                    cleaned_tables.append(cleaned_table)
                elif isinstance(table, str):
                    # Handle case where table is just a name string
                    cleaned_tables.append({"name": table, "id": table})
                else:
                    logger.warning(f"Skipping invalid table metadata: {table}")

            if not cleaned_tables:
                logger.error("No valid tables found after cleaning")
                return False

            # Format data for storage
            metadata = {
                "tables": cleaned_tables,
                "count": len(cleaned_tables),
                "stored_at": datetime.now(timezone.utc).isoformat()
            }

            # Attempt storage with retries
            for attempt in range(max_retries):
                try:
                    logger.info(f"Storage attempt {attempt + 1} for tables metadata")

                    # Insert record
                    response = self.supabase.table("connection_metadata").insert({
                        "connection_id": connection_id,
                        "metadata_type": "tables",
                        "metadata": metadata,
                        "collected_at": datetime.now(timezone.utc).isoformat(),
                        "refresh_frequency": "1 day"
                    }).execute()

                    if response.data and len(response.data) > 0:
                        record_id = response.data[0].get("id")
                        logger.info(f"Successfully stored tables metadata with ID: {record_id}")

                        # Verify storage if requested
                        if verify_storage:
                            if self._verify_tables_storage(connection_id, len(cleaned_tables), record_id):
                                logger.info("Tables metadata storage verified successfully")
                                return True
                            else:
                                logger.warning(f"Storage verification failed on attempt {attempt + 1}")
                                if attempt < max_retries - 1:
                                    time.sleep(2)  # Wait before retry
                                    continue
                        else:
                            return True

                    else:
                        logger.error(f"No data returned from storage operation on attempt {attempt + 1}")

                except Exception as storage_error:
                    logger.error(f"Storage error on attempt {attempt + 1}: {str(storage_error)}")
                    if attempt < max_retries - 1:
                        time.sleep(2)  # Wait before retry
                        continue
                    else:
                        raise

            logger.error(f"Failed to store tables metadata after {max_retries} attempts")
            return False

        except Exception as e:
            logger.error(f"Error storing tables metadata: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def store_columns_metadata(self, connection_id: str, columns_by_table: Dict[str, List[Dict]],
                               max_retries: int = 3, verify_storage: bool = True) -> bool:
        """Store column metadata grouped by table with verification"""
        try:
            logger.info(f"Storing columns metadata for connection {connection_id}: {len(columns_by_table)} tables")

            # Validate input data
            if not columns_by_table or not isinstance(columns_by_table, dict):
                logger.error("Invalid columns metadata: must be a non-empty dictionary")
                return False

            # Clean and validate column data
            cleaned_columns = {}
            total_columns = 0

            for table_name, columns in columns_by_table.items():
                if not isinstance(columns, list):
                    logger.warning(f"Skipping invalid columns for table {table_name}: not a list")
                    continue

                cleaned_table_columns = []
                for column in columns:
                    if isinstance(column, dict) and "name" in column:
                        cleaned_column = {
                            "name": column["name"],
                            "type": str(column.get("type", "unknown")),
                            "nullable": column.get("nullable", True)
                        }
                        # Add optional fields if present
                        if "default" in column:
                            cleaned_column["default"] = str(column["default"])
                        if "primary_key" in column:
                            cleaned_column["primary_key"] = bool(column["primary_key"])

                        cleaned_table_columns.append(cleaned_column)
                    else:
                        logger.warning(f"Skipping invalid column in table {table_name}: {column}")

                if cleaned_table_columns:
                    cleaned_columns[table_name] = cleaned_table_columns
                    total_columns += len(cleaned_table_columns)

            if not cleaned_columns:
                logger.error("No valid columns found after cleaning")
                return False

            # Format data for storage
            metadata = {
                "columns_by_table": cleaned_columns,
                "table_count": len(cleaned_columns),
                "total_columns": total_columns,
                "stored_at": datetime.now(timezone.utc).isoformat()
            }

            # Attempt storage with retries
            for attempt in range(max_retries):
                try:
                    logger.info(f"Storage attempt {attempt + 1} for columns metadata")

                    # Insert record
                    response = self.supabase.table("connection_metadata").insert({
                        "connection_id": connection_id,
                        "metadata_type": "columns",
                        "metadata": metadata,
                        "collected_at": datetime.now(timezone.utc).isoformat(),
                        "refresh_frequency": "1 day"
                    }).execute()

                    if response.data and len(response.data) > 0:
                        record_id = response.data[0].get("id")
                        logger.info(f"Successfully stored columns metadata with ID: {record_id}")

                        # Verify storage if requested
                        if verify_storage:
                            if self._verify_columns_storage(connection_id, len(cleaned_columns), record_id):
                                logger.info("Columns metadata storage verified successfully")
                                return True
                            else:
                                logger.warning(f"Storage verification failed on attempt {attempt + 1}")
                                if attempt < max_retries - 1:
                                    time.sleep(2)  # Wait before retry
                                    continue
                        else:
                            return True

                    else:
                        logger.error(f"No data returned from storage operation on attempt {attempt + 1}")

                except Exception as storage_error:
                    logger.error(f"Storage error on attempt {attempt + 1}: {str(storage_error)}")
                    if attempt < max_retries - 1:
                        time.sleep(2)  # Wait before retry
                        continue
                    else:
                        raise

            logger.error(f"Failed to store columns metadata after {max_retries} attempts")
            return False

        except Exception as e:
            logger.error(f"Error storing columns metadata: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def store_statistics_metadata(self, connection_id: str, stats_by_table: Dict[str, Dict],
                                  max_retries: int = 3, verify_storage: bool = True) -> bool:
        """Store statistical metadata for tables with verification"""
        try:
            logger.info(f"Storing statistics for {len(stats_by_table)} tables for connection {connection_id}")

            # Validate input data
            if not stats_by_table or not isinstance(stats_by_table, dict):
                logger.error("Invalid statistics metadata: must be a non-empty dictionary")
                return False

            # Clean and validate statistics data
            cleaned_stats = {}

            for table_name, stats in stats_by_table.items():
                if not isinstance(stats, dict):
                    logger.warning(f"Skipping invalid statistics for table {table_name}: not a dictionary")
                    continue

                # Clean the stats data
                cleaned_table_stats = {}

                # Handle common statistical fields
                for field in ["table_name", "row_count", "column_count", "collected_at"]:
                    if field in stats:
                        value = stats[field]
                        # Convert Decimal objects to float for JSON serialization
                        if hasattr(value, '__float__'):
                            cleaned_table_stats[field] = float(value)
                        elif hasattr(value, 'isoformat'):  # Handle datetime objects
                            cleaned_table_stats[field] = value.isoformat()
                        else:
                            cleaned_table_stats[field] = value

                # Handle nested statistics like column_statistics
                if "column_statistics" in stats and isinstance(stats["column_statistics"], dict):
                    cleaned_column_stats = {}
                    for col_name, col_stats in stats["column_statistics"].items():
                        if isinstance(col_stats, dict):
                            cleaned_col_stats = {}
                            for stat_name, stat_value in col_stats.items():
                                # Convert Decimal objects to float
                                if hasattr(stat_value, '__float__'):
                                    cleaned_col_stats[stat_name] = float(stat_value)
                                elif hasattr(stat_value, 'isoformat'):
                                    cleaned_col_stats[stat_name] = stat_value.isoformat()
                                else:
                                    cleaned_col_stats[stat_name] = stat_value
                            cleaned_column_stats[col_name] = cleaned_col_stats
                    cleaned_table_stats["column_statistics"] = cleaned_column_stats

                # Add any other fields that weren't specifically handled
                for field, value in stats.items():
                    if field not in cleaned_table_stats:
                        try:
                            # Try to JSON serialize to check if it's valid
                            json.dumps(value)
                            cleaned_table_stats[field] = value
                        except (TypeError, ValueError):
                            # Skip fields that can't be serialized
                            logger.warning(f"Skipping non-serializable field {field} in table {table_name}")

                if cleaned_table_stats:
                    cleaned_stats[table_name] = cleaned_table_stats

            if not cleaned_stats:
                logger.error("No valid statistics found after cleaning")
                return False

            # Format data for storage
            metadata = {
                "statistics_by_table": cleaned_stats,
                "table_count": len(cleaned_stats),
                "stored_at": datetime.now(timezone.utc).isoformat()
            }

            # Attempt storage with retries
            for attempt in range(max_retries):
                try:
                    logger.info(f"Storage attempt {attempt + 1} for statistics metadata")

                    # Insert record
                    response = self.supabase.table("connection_metadata").insert({
                        "connection_id": connection_id,
                        "metadata_type": "statistics",
                        "metadata": metadata,
                        "collected_at": datetime.now(timezone.utc).isoformat(),
                        "refresh_frequency": "1 day"
                    }).execute()

                    if response.data and len(response.data) > 0:
                        record_id = response.data[0].get("id")
                        logger.info(f"Successfully stored statistics metadata with ID: {record_id}")

                        # Verify storage if requested
                        if verify_storage:
                            if self._verify_statistics_storage(connection_id, len(cleaned_stats), record_id):
                                logger.info("Statistics metadata storage verified successfully")
                                return True
                            else:
                                logger.warning(f"Storage verification failed on attempt {attempt + 1}")
                                if attempt < max_retries - 1:
                                    time.sleep(2)  # Wait before retry
                                    continue
                        else:
                            return True

                    else:
                        logger.error(f"No data returned from storage operation on attempt {attempt + 1}")

                except Exception as storage_error:
                    logger.error(f"Storage error on attempt {attempt + 1}: {str(storage_error)}")
                    if attempt < max_retries - 1:
                        time.sleep(2)  # Wait before retry
                        continue
                    else:
                        raise

            logger.error(f"Failed to store statistics metadata after {max_retries} attempts")
            return False

        except Exception as e:
            logger.error(f"Error storing statistics metadata: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _verify_tables_storage(self, connection_id: str, expected_count: int, record_id: str = None) -> bool:
        """Verify that tables metadata was actually stored"""
        try:
            time.sleep(1)  # Brief wait for database consistency

            query = self.supabase.table("connection_metadata") \
                .select("metadata") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", "tables")

            if record_id:
                query = query.eq("id", record_id)

            response = query.order("collected_at", desc=True).limit(1).execute()

            if response.data and len(response.data) > 0:
                metadata = response.data[0].get("metadata", {})
                stored_tables = metadata.get("tables", [])
                stored_count = len(stored_tables)

                logger.info(f"Verification: Expected {expected_count} tables, found {stored_count}")
                return stored_count >= expected_count

            logger.warning("Verification failed: No metadata found")
            return False

        except Exception as e:
            logger.error(f"Error verifying tables storage: {str(e)}")
            return False

    def _verify_columns_storage(self, connection_id: str, expected_table_count: int, record_id: str = None) -> bool:
        """Verify that columns metadata was actually stored"""
        try:
            time.sleep(1)  # Brief wait for database consistency

            query = self.supabase.table("connection_metadata") \
                .select("metadata") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", "columns")

            if record_id:
                query = query.eq("id", record_id)

            response = query.order("collected_at", desc=True).limit(1).execute()

            if response.data and len(response.data) > 0:
                metadata = response.data[0].get("metadata", {})
                columns_by_table = metadata.get("columns_by_table", {})
                stored_table_count = len(columns_by_table)

                logger.info(
                    f"Verification: Expected {expected_table_count} tables with columns, found {stored_table_count}")
                return stored_table_count >= expected_table_count

            logger.warning("Verification failed: No columns metadata found")
            return False

        except Exception as e:
            logger.error(f"Error verifying columns storage: {str(e)}")
            return False

    def _verify_statistics_storage(self, connection_id: str, expected_table_count: int, record_id: str = None) -> bool:
        """Verify that statistics metadata was actually stored"""
        try:
            time.sleep(1)  # Brief wait for database consistency

            query = self.supabase.table("connection_metadata") \
                .select("metadata") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", "statistics")

            if record_id:
                query = query.eq("id", record_id)

            response = query.order("collected_at", desc=True).limit(1).execute()

            if response.data and len(response.data) > 0:
                metadata = response.data[0].get("metadata", {})
                stats_by_table = metadata.get("statistics_by_table", {})
                stored_table_count = len(stats_by_table)

                logger.info(
                    f"Verification: Expected {expected_table_count} tables with statistics, found {stored_table_count}")
                return stored_table_count >= expected_table_count

            logger.warning("Verification failed: No statistics metadata found")
            return False

        except Exception as e:
            logger.error(f"Error verifying statistics storage: {str(e)}")
            return False

    def get_metadata(self, connection_id: str, metadata_type: str) -> Optional[Dict]:
        """Get the most recent metadata of a specific type for a connection"""
        import time

        # Simple throttling to prevent excessive calls
        cache_key = f"metadata_check:{connection_id}:{metadata_type}"
        current_time = time.time()

        if not hasattr(self, '_last_checks'):
            self._last_checks = {}
            self._cached_results = {}

        # Only check database once per minute per type
        if cache_key in self._last_checks:
            if current_time - self._last_checks[cache_key] < 60:  # 60 seconds
                cached_result = self._cached_results.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"Using cached {metadata_type} metadata for connection {connection_id}")
                    return cached_result

        try:
            response = self.supabase.table("connection_metadata") \
                .select("id, metadata, collected_at") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", metadata_type) \
                .order("collected_at", desc=True) \
                .limit(1) \
                .maybe_single() \
                .execute()

            if not response.data:
                logger.debug(f"No {metadata_type} metadata found for connection {connection_id}")
                result = None
            else:
                result = response.data
                result["freshness"] = self._calculate_freshness(result.get("collected_at"))

            # Cache the result
            self._last_checks[cache_key] = current_time
            self._cached_results[cache_key] = result

            return result

        except Exception as e:
            logger.error(f"Error getting {metadata_type} metadata: {str(e)}")
            return None

    def get_tables_metadata(self, connection_id: str) -> Optional[List[Dict]]:
        """Get tables metadata for verification"""
        try:
            metadata_result = self.get_metadata(connection_id, "tables")
            if metadata_result and "metadata" in metadata_result:
                return metadata_result["metadata"].get("tables", [])
            return None
        except Exception as e:
            logger.error(f"Error getting tables metadata: {str(e)}")
            return None

    def get_columns_metadata(self, connection_id: str, table_name: str = None) -> Optional[Dict]:
        """Get columns metadata for verification"""
        try:
            metadata_result = self.get_metadata(connection_id, "columns")
            if metadata_result and "metadata" in metadata_result:
                columns_by_table = metadata_result["metadata"].get("columns_by_table", {})
                if table_name:
                    return columns_by_table.get(table_name, [])
                return columns_by_table
            return None
        except Exception as e:
            logger.error(f"Error getting columns metadata: {str(e)}")
            return None

    def get_statistics_metadata(self, connection_id: str, table_name: str = None) -> Optional[Dict]:
        """Get statistics metadata for verification"""
        try:
            metadata_result = self.get_metadata(connection_id, "statistics")
            if metadata_result and "metadata" in metadata_result:
                stats_by_table = metadata_result["metadata"].get("statistics_by_table", {})
                if table_name:
                    return stats_by_table.get(table_name, {})
                return stats_by_table
            return None
        except Exception as e:
            logger.error(f"Error getting statistics metadata: {str(e)}")
            return None

    def verify_tables_stored(self, connection_id: str) -> bool:
        """Verify that tables metadata was actually stored"""
        try:
            tables = self.get_tables_metadata(connection_id)
            return tables is not None and len(tables) > 0
        except Exception:
            return False

    def verify_columns_stored(self, connection_id: str, table_name: str = None) -> bool:
        """Verify that columns metadata was actually stored"""
        try:
            columns = self.get_columns_metadata(connection_id, table_name)
            if table_name:
                return columns is not None and len(columns) > 0
            else:
                return columns is not None and len(columns) > 0
        except Exception:
            return False

    def verify_statistics_stored(self, connection_id: str, table_name: str = None) -> bool:
        """Verify that statistics metadata was actually stored"""
        try:
            stats = self.get_statistics_metadata(connection_id, table_name)
            if table_name:
                return stats is not None and len(stats) > 0
            else:
                return stats is not None and len(stats) > 0
        except Exception:
            return False

    def _calculate_freshness(self, collected_at: str) -> Dict:
        """Calculate metadata freshness"""
        if not collected_at:
            return {"status": "unknown", "age_seconds": None}

        try:
            import datetime
            now = datetime.datetime.now(datetime.timezone.utc)
            collected = datetime.datetime.fromisoformat(collected_at.replace('Z', '+00:00'))

            age_seconds = (now - collected).total_seconds()

            if age_seconds < 3600:  # 1 hour
                status = "fresh"
            elif age_seconds < 86400:  # 1 day
                status = "recent"
            else:
                status = "stale"

            return {
                "status": status,
                "age_seconds": age_seconds,
                "age_hours": age_seconds / 3600,
                "age_days": age_seconds / 86400
            }
        except Exception:
            return {"status": "unknown", "age_seconds": None}