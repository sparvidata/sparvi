import logging
from typing import Dict, List, Any, Optional, Set, Tuple
import json
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class SchemaChangeDetector:
    """Detects changes in database schema by comparing snapshots"""

    def __init__(self, storage_service=None):
        """
        Initialize the schema change detector

        Args:
            storage_service: Service for retrieving metadata
        """
        self.storage_service = storage_service

    def compare_schemas(self,
                        connection_id: str,
                        current_schema: Dict[str, Any],
                        previous_schema: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Compare current schema against previous snapshot or stored schema

        Args:
            connection_id: Connection ID
            current_schema: Current schema metadata
            previous_schema: Previous schema metadata (optional)

        Returns:
            List of detected changes
        """
        # If previous schema not provided, try to get it from storage
        if previous_schema is None and self.storage_service:
            stored_metadata = self.storage_service.get_metadata(connection_id, "tables")
            if stored_metadata and "metadata" in stored_metadata:
                previous_schema = stored_metadata["metadata"]

        # If we still don't have a previous schema, return empty list
        if not previous_schema:
            logger.warning(f"No previous schema available for comparison for connection {connection_id}")
            return []

        changes = []

        # Extract tables from both schemas
        current_tables = self._extract_tables(current_schema)
        previous_tables = self._extract_tables(previous_schema)

        # Find added tables
        for table_name in set(current_tables.keys()) - set(previous_tables.keys()):
            changes.append({
                "type": "table_added",
                "table": table_name,
                "timestamp": datetime.now().isoformat()
            })

        # Find removed tables
        for table_name in set(previous_tables.keys()) - set(current_tables.keys()):
            changes.append({
                "type": "table_removed",
                "table": table_name,
                "timestamp": datetime.now().isoformat()
            })

        # Check column, primary key, foreign key and index changes in tables that exist in both schemas
        common_tables = set(current_tables.keys()) & set(previous_tables.keys())
        for table_name in common_tables:
            # Column changes
            column_changes = self._compare_table_columns(
                table_name,
                current_tables[table_name].get("columns", []),
                previous_tables[table_name].get("columns", [])
            )
            changes.extend(column_changes)

            # Primary key changes
            pk_changes = self._compare_primary_keys(
                table_name,
                current_tables[table_name].get("primary_key", []),
                previous_tables[table_name].get("primary_key", [])
            )
            changes.extend(pk_changes)

            # Foreign key changes
            fk_changes = self._compare_foreign_keys(
                table_name,
                current_tables[table_name].get("foreign_keys", []),
                previous_tables[table_name].get("foreign_keys", [])
            )
            changes.extend(fk_changes)

            # Index changes
            index_changes = self._compare_indexes(
                table_name,
                current_tables[table_name].get("indices", []),
                previous_tables[table_name].get("indices", [])
            )
            changes.extend(index_changes)

        return changes

    def _extract_tables(self, schema: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        Extract table data from schema metadata

        Args:
            schema: Schema metadata

        Returns:
            Dictionary mapping table names to their metadata
        """
        tables = {}

        # Look for tables list
        if "tables" in schema:
            for table in schema["tables"]:
                if "name" in table:
                    tables[table["name"]] = table

        # Alternative format where tables might be indexed by name directly
        elif isinstance(schema, dict):
            for key, value in schema.items():
                if isinstance(value, dict) and "name" in value:
                    tables[value["name"]] = value

        return tables

    def _compare_table_columns(self,
                               table_name: str,
                               current_columns: List[Dict[str, Any]],
                               previous_columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compare columns between current and previous versions of a table

        Args:
            table_name: Name of the table
            current_columns: List of current columns
            previous_columns: List of previous columns

        Returns:
            List of detected column changes
        """
        changes = []

        # Extract column names and metadata
        current_cols = {col["name"]: col for col in current_columns if "name" in col}
        previous_cols = {col["name"]: col for col in previous_columns if "name" in col}

        # Find added columns
        for col_name in set(current_cols.keys()) - set(previous_cols.keys()):
            col_info = current_cols[col_name]
            changes.append({
                "type": "column_added",
                "table": table_name,
                "column": col_name,
                "details": {
                    "type": col_info.get("type", "unknown"),
                    "nullable": col_info.get("nullable", None)
                },
                "timestamp": datetime.now().isoformat()
            })

        # Find removed columns
        for col_name in set(previous_cols.keys()) - set(current_cols.keys()):
            col_info = previous_cols[col_name]
            changes.append({
                "type": "column_removed",
                "table": table_name,
                "column": col_name,
                "details": {
                    "type": col_info.get("type", "unknown")
                },
                "timestamp": datetime.now().isoformat()
            })

        # Check for column type or property changes
        for col_name in set(current_cols.keys()) & set(previous_cols.keys()):
            current_col = current_cols[col_name]
            previous_col = previous_cols[col_name]

            # Check type changes
            if str(current_col.get("type", "")) != str(previous_col.get("type", "")):
                changes.append({
                    "type": "column_type_changed",
                    "table": table_name,
                    "column": col_name,
                    "details": {
                        "previous_type": previous_col.get("type", "unknown"),
                        "new_type": current_col.get("type", "unknown")
                    },
                    "timestamp": datetime.now().isoformat()
                })

            # Check nullability changes
            if current_col.get("nullable") != previous_col.get("nullable"):
                changes.append({
                    "type": "column_nullability_changed",
                    "table": table_name,
                    "column": col_name,
                    "details": {
                        "previous_nullable": previous_col.get("nullable", None),
                        "new_nullable": current_col.get("nullable", None)
                    },
                    "timestamp": datetime.now().isoformat()
                })

        return changes

    def _compare_primary_keys(self,
                              table_name: str,
                              current_pk: List[str],
                              previous_pk: List[str]) -> List[Dict[str, Any]]:
        """
        Compare primary keys between current and previous versions of a table

        Args:
            table_name: Name of the table
            current_pk: List of current primary key columns
            previous_pk: List of previous primary key columns

        Returns:
            List of detected primary key changes
        """
        changes = []

        # Ensure we're working with lists
        current_pk = current_pk if isinstance(current_pk, list) else []
        previous_pk = previous_pk if isinstance(previous_pk, list) else []

        # Sort to ensure consistent comparison
        current_pk = sorted(current_pk)
        previous_pk = sorted(previous_pk)

        # Case 1: No previous PK, but now there is one
        if not previous_pk and current_pk:
            changes.append({
                "type": "primary_key_added",
                "table": table_name,
                "details": {
                    "columns": current_pk
                },
                "timestamp": datetime.now().isoformat()
            })
            return changes

        # Case 2: Had a PK before, but now it's gone
        if previous_pk and not current_pk:
            changes.append({
                "type": "primary_key_removed",
                "table": table_name,
                "details": {
                    "columns": previous_pk
                },
                "timestamp": datetime.now().isoformat()
            })
            return changes

        # Case 3: PK has changed
        if current_pk != previous_pk:
            changes.append({
                "type": "primary_key_changed",
                "table": table_name,
                "details": {
                    "previous_columns": previous_pk,
                    "new_columns": current_pk
                },
                "timestamp": datetime.now().isoformat()
            })

        return changes

    def _compare_foreign_keys(self,
                              table_name: str,
                              current_fks: List[Dict[str, Any]],
                              previous_fks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compare foreign keys between current and previous versions of a table

        Args:
            table_name: Name of the table
            current_fks: List of current foreign keys
            previous_fks: List of previous foreign keys

        Returns:
            List of detected foreign key changes
        """
        changes = []

        # Ensure we're working with lists
        current_fks = current_fks if isinstance(current_fks, list) else []
        previous_fks = previous_fks if isinstance(previous_fks, list) else []

        # Create fingerprints for each FK to enable comparison
        def fk_fingerprint(fk):
            # Create a unique identifier for a foreign key configuration
            constrained = sorted(fk.get("constrained_columns", []))
            referred = sorted(fk.get("referred_columns", []))
            referred_table = fk.get("referred_table", "")
            return f"{','.join(constrained)}|{referred_table}|{','.join(referred)}"

        # Create mappings with fingerprints
        current_fk_map = {fk_fingerprint(fk): fk for fk in current_fks}
        previous_fk_map = {fk_fingerprint(fk): fk for fk in previous_fks}

        # Find added foreign keys
        for fp in set(current_fk_map.keys()) - set(previous_fk_map.keys()):
            fk = current_fk_map[fp]
            changes.append({
                "type": "foreign_key_added",
                "table": table_name,
                "details": {
                    "constrained_columns": fk.get("constrained_columns", []),
                    "referred_table": fk.get("referred_table", ""),
                    "referred_columns": fk.get("referred_columns", [])
                },
                "timestamp": datetime.now().isoformat()
            })

        # Find removed foreign keys
        for fp in set(previous_fk_map.keys()) - set(current_fk_map.keys()):
            fk = previous_fk_map[fp]
            changes.append({
                "type": "foreign_key_removed",
                "table": table_name,
                "details": {
                    "constrained_columns": fk.get("constrained_columns", []),
                    "referred_table": fk.get("referred_table", ""),
                    "referred_columns": fk.get("referred_columns", [])
                },
                "timestamp": datetime.now().isoformat()
            })

        # Check for foreign key changes
        for fp in set(current_fk_map.keys()) & set(previous_fk_map.keys()):
            # If fingerprints match, the FKs are identical by our definition
            # Could add more detailed attribute comparison here if needed
            pass

        return changes

    def _compare_indexes(self,
                         table_name: str,
                         current_indices: List[Dict[str, Any]],
                         previous_indices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Compare indexes between current and previous versions of a table

        Args:
            table_name: Name of the table
            current_indices: List of current indexes
            previous_indices: List of previous indexes

        Returns:
            List of detected index changes
        """
        changes = []

        # Ensure we're working with lists
        current_indices = current_indices if isinstance(current_indices, list) else []
        previous_indices = previous_indices if isinstance(previous_indices, list) else []

        # Create a unique identifier for each index
        def index_fingerprint(idx):
            # Create a unique identifier combining name, columns, and uniqueness
            name = idx.get("name", "")
            columns = sorted(idx.get("columns", []))
            is_unique = idx.get("unique", False)
            return f"{name}|{','.join(columns)}|{is_unique}"

        # Create mappings with fingerprints
        current_idx_map = {index_fingerprint(idx): idx for idx in current_indices}
        previous_idx_map = {index_fingerprint(idx): idx for idx in previous_indices}

        # Find added indexes by name first, then by definition
        current_names = {idx.get("name", "") for idx in current_indices}
        previous_names = {idx.get("name", "") for idx in previous_indices}

        # Find added indexes
        for fp in set(current_idx_map.keys()) - set(previous_idx_map.keys()):
            idx = current_idx_map[fp]
            changes.append({
                "type": "index_added",
                "table": table_name,
                "details": {
                    "name": idx.get("name", ""),
                    "columns": idx.get("columns", []),
                    "unique": idx.get("unique", False)
                },
                "timestamp": datetime.now().isoformat()
            })

        # Find removed indexes
        for fp in set(previous_idx_map.keys()) - set(current_idx_map.keys()):
            idx = previous_idx_map[fp]
            changes.append({
                "type": "index_removed",
                "table": table_name,
                "details": {
                    "name": idx.get("name", ""),
                    "columns": idx.get("columns", []),
                    "unique": idx.get("unique", False)
                },
                "timestamp": datetime.now().isoformat()
            })

        # Check for changed indexes with the same name but different definition
        for name in current_names & previous_names:
            # Find all indexes with this name in both schemas
            current_with_name = [idx for idx in current_indices if idx.get("name", "") == name]
            previous_with_name = [idx for idx in previous_indices if idx.get("name", "") == name]

            # If multiple indices with same name (shouldn't happen but just in case)
            # We just compare the first ones
            if current_with_name and previous_with_name:
                current_idx = current_with_name[0]
                previous_idx = previous_with_name[0]

                # Check if the definition changed
                if (sorted(current_idx.get("columns", [])) != sorted(previous_idx.get("columns", [])) or
                        current_idx.get("unique", False) != previous_idx.get("unique", False)):
                    changes.append({
                        "type": "index_changed",
                        "table": table_name,
                        "details": {
                            "name": name,
                            "previous_columns": previous_idx.get("columns", []),
                            "new_columns": current_idx.get("columns", []),
                            "previous_unique": previous_idx.get("unique", False),
                            "new_unique": current_idx.get("unique", False)
                        },
                        "timestamp": datetime.now().isoformat()
                    })

        return changes

    def detect_changes_for_connection(self,
                                      connection_id: str,
                                      connector_factory,
                                      supabase_manager=None) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Actively check for schema changes in a database connection

        Args:
            connection_id: Connection ID
            connector_factory: Factory to create database connectors
            supabase_manager: Optional Supabase manager for querying connection details

        Returns:
            Tuple containing (list of changes, whether important changes were detected)
        """
        try:
            logger.info(f"Detecting schema changes for connection {connection_id}")

            # Get connection details if needed
            connection = None
            if supabase_manager:
                connection = supabase_manager.get_connection(connection_id)

            if not connection:
                logger.error(f"Could not get connection details for {connection_id}")
                return [], False

            # Create connector
            connector = connector_factory.create_connector(connection)
            connector.connect()

            # Get current schema
            from .collector import MetadataCollector
            collector = MetadataCollector(connection_id, connector)

            # Collect current comprehensive schema
            current_schema = {
                "tables": []
            }

            # Get table list
            tables = collector.collect_table_list()

            # Limit to a reasonable number of tables to avoid performance issues
            tables_to_check = tables[:50]  # First 50 tables

            # Collect comprehensive metadata for each table
            for table_name in tables_to_check:
                try:
                    # Get column information
                    columns = collector.collect_columns(table_name)

                    # Get primary key information
                    primary_keys = connector.get_primary_keys(table_name)

                    # Get foreign key information (if supported)
                    foreign_keys = []
                    try:
                        if hasattr(connector.inspector, 'get_foreign_keys'):
                            foreign_keys = connector.inspector.get_foreign_keys(table_name)
                    except Exception as e:
                        logger.warning(f"Error getting foreign keys for {table_name}: {str(e)}")

                    # Get index information (if supported)
                    indices = []
                    try:
                        if hasattr(connector.inspector, 'get_indexes'):
                            indices = connector.inspector.get_indexes(table_name)
                    except Exception as e:
                        logger.warning(f"Error getting indexes for {table_name}: {str(e)}")

                    # Combine all information for this table
                    table_data = {
                        "name": table_name,
                        "columns": columns,
                        "primary_key": primary_keys,
                        "foreign_keys": foreign_keys,
                        "indices": indices
                    }

                    current_schema["tables"].append(table_data)
                except Exception as e:
                    logger.error(f"Error collecting schema for table {table_name}: {str(e)}")
                    continue

            # Get previous schema from storage
            previous_schema = None
            if self.storage_service:
                stored_metadata = self.storage_service.get_metadata(connection_id, "tables")
                if stored_metadata and "metadata" in stored_metadata:
                    previous_schema = stored_metadata["metadata"]

            # If no previous schema, return no changes but store the current schema
            if not previous_schema:
                logger.info(f"No previous schema available for comparison for connection {connection_id}")

                # Store the current schema for future comparisons
                if self.storage_service and current_schema["tables"]:
                    self.storage_service.store_tables_metadata(connection_id, current_schema["tables"])
                    logger.info(f"Stored initial schema with {len(current_schema['tables'])} tables")

                # No changes detected, but indicate that schema was collected
                return [], False

            # Compare schemas
            changes = self.compare_schemas(connection_id, current_schema, previous_schema)

            # Determine if there are important changes
            important_change_types = [
                "table_added", "table_removed",
                "column_added", "column_removed", "column_type_changed", "column_nullability_changed",
                "primary_key_added", "primary_key_removed", "primary_key_changed",
                "foreign_key_added", "foreign_key_removed", "foreign_key_changed",
                "index_added", "index_removed", "index_changed"
            ]

            important_changes = len([c for c in changes if c["type"] in important_change_types]) > 0

            logger.info(f"Detected {len(changes)} schema changes, important: {important_changes}")

            # If changes detected, store the new schema
            if changes and self.storage_service:
                self.storage_service.store_tables_metadata(connection_id, current_schema["tables"])
                logger.info(f"Updated schema stored with {len(current_schema['tables'])} tables")

                # Also store the changes in the schema_changes table for historical tracking
                self._store_schema_changes(connection_id, changes, supabase_manager)

            return changes, important_changes

        except Exception as e:
            logger.error(f"Error detecting schema changes: {str(e)}")
            return [], False

    def _store_schema_changes(self, connection_id: str, changes: List[Dict[str, Any]], supabase_manager=None):
        """
        Store detected schema changes in the database

        Args:
            connection_id: Connection ID
            changes: List of detected changes
            supabase_manager: Supabase manager for database operations
        """
        if not supabase_manager:
            logger.warning("No Supabase manager provided, cannot store schema changes")
            return

        try:
            # Get organization ID from connection details
            organization_id = None
            try:
                connection = supabase_manager.get_connection(connection_id)
                if connection and 'organization_id' in connection:
                    organization_id = connection['organization_id']
            except Exception as e:
                logger.warning(f"Could not get organization ID: {str(e)}")
                return

            if not organization_id:
                logger.warning("No organization ID found, cannot store schema changes")
                return

            current_time = datetime.now().isoformat()

            # Store each change with all its data
            for change in changes:
                try:
                    # Create a record for schema_changes table
                    change_record = {
                        "connection_id": connection_id,
                        "organization_id": organization_id,
                        "table_name": change.get("table"),
                        "column_name": change.get("column"),  # Will be None for table-level changes
                        "change_type": change.get("type"),
                        "details": json.dumps(change.get("details", {})),
                        "detected_at": current_time,
                        "acknowledged": False
                    }

                    # Insert into schema_changes table
                    result = supabase_manager.supabase.table("schema_changes").insert(change_record).execute()

                    if not result.data:
                        logger.warning(f"Failed to store schema change of type {change.get('type')}")

                except Exception as e:
                    logger.error(f"Error storing schema change: {str(e)}")
                    continue

            logger.info(f"Stored {len(changes)} schema changes in database")

        except Exception as e:
            logger.error(f"Error in schema change storage: {str(e)}")

    def publish_changes_as_events(self,
                                  connection_id: str,
                                  changes: List[Dict[str, Any]],
                                  organization_id: Optional[str] = None,
                                  user_id: Optional[str] = None) -> List[str]:
        """
        Publish detected schema changes as events

        Args:
            connection_id: Connection ID
            changes: List of detected changes
            organization_id: Optional organization ID
            user_id: Optional user ID

        Returns:
            List of task IDs created
        """
        # Import here to avoid circular imports
        from .events import MetadataEventType, publish_metadata_event

        task_ids = []

        # Process changes by table
        changes_by_table = {}

        for change in changes:
            table_name = change.get("table")
            if table_name:
                if table_name not in changes_by_table:
                    changes_by_table[table_name] = []
                changes_by_table[table_name].append(change)

        # Publish a schema change event for each affected table
        for table_name, table_changes in changes_by_table.items():
            # Get summary of changes for this table
            change_types = [c["type"] for c in table_changes]

            # Publish event
            task_id = publish_metadata_event(
                event_type=MetadataEventType.SCHEMA_CHANGE,
                connection_id=connection_id,
                details={
                    "table_name": table_name,
                    "changes": change_types,
                    "change_count": len(table_changes)
                },
                organization_id=organization_id,
                user_id=user_id
            )

            if task_id:
                task_ids.append(task_id)

        return task_ids