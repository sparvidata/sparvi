# backend/core/metadata/schema_change_detector.py - ENHANCED VERSION

import logging
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)


class SchemaChangeDetector:
    """Enhanced schema change detector with proper storage verification"""

    def __init__(self, storage_service=None):
        """
        Initialize schema change detector

        Args:
            storage_service: MetadataStorageService instance for retrieving metadata
        """
        self.storage_service = storage_service

    def detect_changes_for_connection(self, connection_id: str, connector_factory, supabase_manager) -> Tuple[
        List[Dict], bool]:
        """
        Detect schema changes for a connection and store them in the database

        Returns:
            Tuple of (changes_list, important_changes_detected)
        """
        try:
            logger.info(f"Starting schema change detection for connection {connection_id}")

            # Get current schema
            current_schema = self._get_current_schema(connection_id, connector_factory, supabase_manager)
            if not current_schema:
                logger.warning(f"Could not get current schema for connection {connection_id}")
                return [], False

            # Get previous schema from metadata storage
            previous_schema = self._get_previous_schema(connection_id)
            if not previous_schema:
                logger.info(f"No previous schema found for connection {connection_id}, treating as baseline")
                # Store current schema as baseline
                self._store_schema_baseline(connection_id, current_schema, supabase_manager)
                return [], False

            # Compare schemas
            changes = self.compare_schemas(current_schema, previous_schema)

            # Determine if any changes are important
            important_changes = self._has_important_changes(changes)

            logger.info(f"Detected {len(changes)} schema changes, {important_changes} important")

            # Store changes in database with verification
            if changes:
                stored_count = self._store_schema_changes(connection_id, changes, supabase_manager)
                logger.info(f"Stored {stored_count} out of {len(changes)} schema changes")

                # Verify storage
                if stored_count == 0 and len(changes) > 0:
                    logger.error("Failed to store any schema changes - this is a serious issue!")
                    # Try alternative storage method
                    stored_count = self._store_schema_changes_alternative(connection_id, changes, supabase_manager)
                    logger.info(f"Alternative storage method stored {stored_count} changes")

            return changes, important_changes

        except Exception as e:
            logger.error(f"Error in schema change detection for connection {connection_id}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return [], False

    def _get_current_schema(self, connection_id: str, connector_factory, supabase_manager) -> Optional[Dict]:
        """Get current schema from the database"""
        try:
            # Get connection details
            connection = supabase_manager.get_connection(connection_id)
            if not connection:
                logger.error(f"Connection {connection_id} not found")
                return None

            # Create connector
            connector = connector_factory.create_connector(connection)

            # Get current schema
            current_schema = {}

            # Get tables
            tables = connector.get_tables()
            logger.info(f"Found {len(tables)} tables in current schema")

            for table_name in tables:
                try:
                    # Get columns
                    columns = connector.get_columns(table_name)

                    # Get primary keys
                    primary_keys = connector.get_primary_keys(table_name)

                    # Store table info
                    current_schema[table_name] = {
                        "columns": columns,
                        "primary_keys": primary_keys,
                        "column_count": len(columns)
                    }

                    # Try to get foreign keys (if supported)
                    try:
                        if hasattr(connector.inspector, 'get_foreign_keys'):
                            foreign_keys = connector.inspector.get_foreign_keys(table_name)
                            current_schema[table_name]["foreign_keys"] = foreign_keys
                    except Exception:
                        current_schema[table_name]["foreign_keys"] = []

                    # Try to get indexes (if supported)
                    try:
                        if hasattr(connector.inspector, 'get_indexes'):
                            indexes = connector.inspector.get_indexes(table_name)
                            current_schema[table_name]["indexes"] = indexes
                    except Exception:
                        current_schema[table_name]["indexes"] = []

                except Exception as e:
                    logger.warning(f"Error getting details for table {table_name}: {str(e)}")
                    continue

            logger.info(f"Successfully retrieved current schema with {len(current_schema)} tables")
            return current_schema

        except Exception as e:
            logger.error(f"Error getting current schema: {str(e)}")
            return None

    def _get_previous_schema(self, connection_id: str) -> Optional[Dict]:
        """Get previous schema from metadata storage"""
        try:
            if not self.storage_service:
                logger.warning("No storage service available for getting previous schema")
                return None

            # Get the most recent tables metadata
            tables_metadata = self.storage_service.get_metadata(connection_id, "tables")
            if not tables_metadata or "metadata" not in tables_metadata:
                logger.info("No previous tables metadata found")
                return None

            # Get the most recent columns metadata
            columns_metadata = self.storage_service.get_metadata(connection_id, "columns")
            if not columns_metadata or "metadata" not in columns_metadata:
                logger.info("No previous columns metadata found")
                return None

            # Reconstruct schema from metadata
            previous_schema = {}

            # Get tables list
            tables = tables_metadata["metadata"].get("tables", [])
            columns_by_table = columns_metadata["metadata"].get("columns_by_table", {})

            for table_info in tables:
                table_name = table_info.get("name") if isinstance(table_info, dict) else table_info

                if table_name in columns_by_table:
                    columns = columns_by_table[table_name]

                    # Extract primary keys from columns (if marked)
                    primary_keys = []
                    for col in columns:
                        if isinstance(col, dict) and col.get("primary_key"):
                            primary_keys.append(col["name"])

                    previous_schema[table_name] = {
                        "columns": columns,
                        "primary_keys": primary_keys,
                        "column_count": len(columns),
                        "foreign_keys": [],  # Not typically stored in basic metadata
                        "indexes": []  # Not typically stored in basic metadata
                    }

            logger.info(f"Reconstructed previous schema with {len(previous_schema)} tables")
            return previous_schema

        except Exception as e:
            logger.error(f"Error getting previous schema: {str(e)}")
            return None

    def compare_schemas(self, current_schema: Dict, previous_schema: Dict) -> List[Dict]:
        """Compare two schemas and return list of changes"""
        changes = []

        try:
            # Check for added tables
            current_tables = set(current_schema.keys())
            previous_tables = set(previous_schema.keys())

            added_tables = current_tables - previous_tables
            for table_name in added_tables:
                changes.append({
                    "type": "table_added",
                    "table": table_name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "details": {
                        "column_count": current_schema[table_name].get("column_count", 0)
                    }
                })

            # Check for removed tables
            removed_tables = previous_tables - current_tables
            for table_name in removed_tables:
                changes.append({
                    "type": "table_removed",
                    "table": table_name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "details": {
                        "column_count": previous_schema[table_name].get("column_count", 0)
                    }
                })

            # Check for changes in common tables
            common_tables = current_tables & previous_tables

            for table_name in common_tables:
                current_table = current_schema[table_name]
                previous_table = previous_schema[table_name]

                # Compare columns
                table_changes = self._compare_table_columns(table_name, current_table.get("columns", []),
                                                            previous_table.get("columns", []))
                changes.extend(table_changes)

                # Compare primary keys
                pk_changes = self._compare_primary_keys(table_name, current_table.get("primary_keys", []),
                                                        previous_table.get("primary_keys", []))
                changes.extend(pk_changes)

                # Compare foreign keys (if available)
                fk_changes = self._compare_foreign_keys(table_name, current_table.get("foreign_keys", []),
                                                        previous_table.get("foreign_keys", []))
                changes.extend(fk_changes)

                # Compare indexes (if available)
                index_changes = self._compare_indexes(table_name, current_table.get("indexes", []),
                                                      previous_table.get("indexes", []))
                changes.extend(index_changes)

            logger.info(f"Schema comparison found {len(changes)} changes")
            return changes

        except Exception as e:
            logger.error(f"Error comparing schemas: {str(e)}")
            return []

    def _compare_table_columns(self, table_name: str, current_columns: List[Dict], previous_columns: List[Dict]) -> \
    List[Dict]:
        """Compare columns between current and previous table schemas"""
        changes = []

        try:
            # Create column name mappings
            current_col_names = {col.get("name") for col in current_columns if isinstance(col, dict)}
            previous_col_names = {col.get("name") for col in previous_columns if isinstance(col, dict)}

            # Find added columns
            added_columns = current_col_names - previous_col_names
            for col_name in added_columns:
                col_info = next((col for col in current_columns if col.get("name") == col_name), {})
                changes.append({
                    "type": "column_added",
                    "table": table_name,
                    "column": col_name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "details": {
                        "data_type": str(col_info.get("type", "unknown")),
                        "nullable": col_info.get("nullable", True)
                    }
                })

            # Find removed columns
            removed_columns = previous_col_names - current_col_names
            for col_name in removed_columns:
                col_info = next((col for col in previous_columns if col.get("name") == col_name), {})
                changes.append({
                    "type": "column_removed",
                    "table": table_name,
                    "column": col_name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "details": {
                        "data_type": str(col_info.get("type", "unknown")),
                        "nullable": col_info.get("nullable", True)
                    }
                })

            # Check for changes in common columns
            common_columns = current_col_names & previous_col_names
            for col_name in common_columns:
                current_col = next((col for col in current_columns if col.get("name") == col_name), {})
                previous_col = next((col for col in previous_columns if col.get("name") == col_name), {})

                # Check for type changes
                current_type = str(current_col.get("type", ""))
                previous_type = str(previous_col.get("type", ""))

                if current_type != previous_type:
                    changes.append({
                        "type": "column_type_changed",
                        "table": table_name,
                        "column": col_name,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "details": {
                            "old_type": previous_type,
                            "new_type": current_type
                        }
                    })

                # Check for nullable changes
                current_nullable = current_col.get("nullable", True)
                previous_nullable = previous_col.get("nullable", True)

                if current_nullable != previous_nullable:
                    changes.append({
                        "type": "column_nullable_changed",
                        "table": table_name,
                        "column": col_name,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "details": {
                            "old_nullable": previous_nullable,
                            "new_nullable": current_nullable
                        }
                    })

            return changes

        except Exception as e:
            logger.error(f"Error comparing columns for table {table_name}: {str(e)}")
            return []

    def _compare_primary_keys(self, table_name: str, current_pk: List[str], previous_pk: List[str]) -> List[Dict]:
        """Compare primary keys between schemas"""
        changes = []

        try:
            # Normalize to sets for comparison
            current_pk_set = set(current_pk or [])
            previous_pk_set = set(previous_pk or [])

            if current_pk_set != previous_pk_set:
                changes.append({
                    "type": "primary_key_changed",
                    "table": table_name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "details": {
                        "old_primary_key": list(previous_pk_set),
                        "new_primary_key": list(current_pk_set)
                    }
                })

            return changes

        except Exception as e:
            logger.error(f"Error comparing primary keys for table {table_name}: {str(e)}")
            return []

    def _compare_foreign_keys(self, table_name: str, current_fk: List[Dict], previous_fk: List[Dict]) -> List[Dict]:
        """Compare foreign keys between schemas"""
        changes = []

        try:
            # This is a simplified comparison - you might want to make it more sophisticated
            if len(current_fk) != len(previous_fk):
                changes.append({
                    "type": "foreign_key_changed",
                    "table": table_name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "details": {
                        "old_count": len(previous_fk),
                        "new_count": len(current_fk)
                    }
                })

            return changes

        except Exception as e:
            logger.error(f"Error comparing foreign keys for table {table_name}: {str(e)}")
            return []

    def _compare_indexes(self, table_name: str, current_indexes: List[Dict], previous_indexes: List[Dict]) -> List[
        Dict]:
        """Compare indexes between schemas"""
        changes = []

        try:
            # This is a simplified comparison
            if len(current_indexes) != len(previous_indexes):
                changes.append({
                    "type": "index_changed",
                    "table": table_name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "details": {
                        "old_count": len(previous_indexes),
                        "new_count": len(current_indexes)
                    }
                })

            return changes

        except Exception as e:
            logger.error(f"Error comparing indexes for table {table_name}: {str(e)}")
            return []

    def _has_important_changes(self, changes: List[Dict]) -> bool:
        """Determine if any of the changes are considered important"""
        important_change_types = [
            "table_removed",
            "column_removed",
            "column_type_changed",
            "primary_key_changed"
        ]

        for change in changes:
            if change.get("type") in important_change_types:
                return True

        return False

    def _store_schema_changes(self, connection_id: str, changes: List[Dict[str, Any]], storage_service) -> List[
        Dict[str, Any]]:
        """FIXED: Store schema changes with correct column names"""
        try:
            if not storage_service or not changes:
                return []

            stored_changes = []
            for change in changes:
                try:
                    # FIXED: Use correct column names that match your schema
                    change_record = {
                        "connection_id": connection_id,
                        "change_type": change.get("type", "unknown"),
                        "table_name": change.get("table"),
                        "column_name": change.get("column"),
                        "details": change.get("details", {}),  # FIXED: Use 'details' not 'change_details'
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    }

                    # Get organization_id from connection
                    connection = storage_service.get_connection(connection_id)
                    if connection and connection.get("organization_id"):
                        change_record["organization_id"] = connection["organization_id"]
                    else:
                        logger.warning(f"Could not get organization_id for connection {connection_id}")
                        continue

                    response = storage_service.supabase.table("schema_changes") \
                        .insert(change_record) \
                        .execute()

                    if response.data:
                        stored_changes.extend(response.data)
                    else:
                        logger.warning(f"Failed to store schema change: {change}")

                except Exception as change_error:
                    logger.error(f"Error storing individual schema change: {str(change_error)}")

            return stored_changes

        except Exception as e:
            logger.error(f"Error storing schema changes: {str(e)}")
            return []

    def _store_schema_changes_alternative(self, connection_id: str, changes: List[Dict], supabase_manager) -> int:
        """Alternative method to store schema changes (batch insert)"""
        try:
            logger.info(f"Attempting alternative storage method for {len(changes)} schema changes")

            # Prepare all records
            change_records = []
            for change in changes:
                change_record = {
                    "connection_id": connection_id,
                    "table_name": change.get("table", "unknown"),
                    "column_name": change.get("column"),
                    "change_type": change.get("type", "unknown"),
                    "change_details": change,
                    "detected_at": change.get("timestamp", datetime.now(timezone.utc).isoformat()),
                    "acknowledged": False,
                    "important": change.get("type") in ["table_removed", "column_removed", "column_type_changed",
                                                        "primary_key_changed"]
                }
                change_records.append(change_record)

            # Batch insert
            response = supabase_manager.supabase.table("schema_changes").insert(change_records).execute()

            if response.data:
                stored_count = len(response.data)
                logger.info(f"Alternative storage method successfully stored {stored_count} changes")
                return stored_count
            else:
                logger.error("Alternative storage method failed")
                return 0

        except Exception as e:
            logger.error(f"Error in alternative storage method: {str(e)}")
            return 0

    def _store_schema_baseline(self, connection_id: str, schema: Dict, supabase_manager):
        """Store current schema as baseline for future comparisons"""
        try:
            logger.info(f"Storing schema baseline for connection {connection_id}")

            # Store a baseline record
            baseline_record = {
                "connection_id": connection_id,
                "table_name": "BASELINE",
                "change_type": "baseline_created",
                "change_details": {
                    "type": "baseline_created",
                    "table_count": len(schema),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "tables": list(schema.keys())
                },
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "acknowledged": True,
                "important": False
            }

            supabase_manager.supabase.table("schema_changes").insert(baseline_record).execute()
            logger.info("Schema baseline stored successfully")

        except Exception as e:
            logger.error(f"Error storing schema baseline: {str(e)}")

    def publish_changes_as_events(self, connection_id: str, changes: List[Dict], organization_id: str = None) -> List[
        str]:
        """Publish schema changes as metadata events"""
        task_ids = []

        try:
            # Import here to avoid circular imports
            from .events import publish_metadata_event, MetadataEventType

            for change in changes:
                # Publish event for each change
                task_id = publish_metadata_event(
                    event_type=MetadataEventType.SCHEMA_CHANGE,
                    connection_id=connection_id,
                    details={
                        "table_name": change.get("table"),
                        "column_name": change.get("column"),
                        "change_type": change.get("type"),
                        "change_details": change
                    },
                    organization_id=organization_id
                )

                if task_id:
                    task_ids.append(task_id)

            logger.info(f"Published {len(task_ids)} schema change events")
            return task_ids

        except ImportError:
            logger.warning("Metadata events system not available")
            return []
        except Exception as e:
            logger.error(f"Error publishing schema change events: {str(e)}")
            return []