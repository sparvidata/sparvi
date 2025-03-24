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

        # Check column changes in tables that exist in both schemas
        common_tables = set(current_tables.keys()) & set(previous_tables.keys())
        for table_name in common_tables:
            column_changes = self._compare_table_columns(
                table_name,
                current_tables[table_name].get("columns", []),
                previous_tables[table_name].get("columns", [])
            )
            changes.extend(column_changes)

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

            # Collect current basic schema (tables and columns)
            current_schema = {
                "tables": []
            }

            # Get table list
            tables = collector.collect_table_list()

            # Limit to a reasonable number of tables to avoid performance issues
            tables_to_check = tables[:50]  # First 50 tables

            # Collect basic metadata for each table
            for table_name in tables_to_check:
                columns = collector.collect_columns(table_name)
                table_data = {
                    "name": table_name,
                    "columns": columns
                }
                current_schema["tables"].append(table_data)

            # Get previous schema from storage
            previous_schema = None
            if self.storage_service:
                stored_metadata = self.storage_service.get_metadata(connection_id, "tables")
                if stored_metadata and "metadata" in stored_metadata:
                    previous_schema = stored_metadata["metadata"]

            # If no previous schema, return no changes
            if not previous_schema:
                logger.info(f"No previous schema available for comparison for connection {connection_id}")
                # No changes detected, but indicate that schema was collected
                return [], False

            # Compare schemas
            changes = self.compare_schemas(connection_id, current_schema, previous_schema)

            # Check if there are important changes
            important_changes = len([c for c in changes if c["type"] in
                                     ["table_added", "table_removed", "column_added", "column_removed",
                                      "column_type_changed"]]) > 0

            logger.info(f"Detected {len(changes)} schema changes, important: {important_changes}")

            # Track schema changes as metrics
            if changes and important_changes:
                try:
                    from core.analytics.historical_metrics import HistoricalMetricsTracker
                    tracker = HistoricalMetricsTracker(supabase_manager)

                    # Get organization ID from connection details
                    organization_id = connection.get("organization_id")

                    # Track changes by type
                    if organization_id:
                        metrics = []

                        # Count changes by type
                        change_counts = {}
                        for change in changes:
                            change_type = change.get("type", "unknown")
                            if change_type not in change_counts:
                                change_counts[change_type] = 0
                            change_counts[change_type] += 1

                            # Track each table affected
                            table_name = change.get("table")
                            if table_name:
                                metrics.append({
                                    "name": change_type,
                                    "value": 1.0,
                                    "type": "schema_change",
                                    "table_name": table_name,
                                    "source": "schema_detector"
                                })

                        # Track overall change count
                        metrics.append({
                            "name": "schema_changes",
                            "value": len(changes),
                            "type": "schema_change",
                            "source": "schema_detector"
                        })

                        # Track specific change types
                        for change_type, count in change_counts.items():
                            metrics.append({
                                "name": f"schema_change_{change_type}",
                                "value": count,
                                "type": "schema_change",
                                "source": "schema_detector"
                            })

                        # Batch track all metrics
                        if metrics:
                            tracker.track_metrics_batch(
                                organization_id=organization_id,
                                connection_id=connection_id,
                                metrics=metrics
                            )
                            logger.info(f"Tracked {len(metrics)} schema change metrics")

                except Exception as tracking_error:
                    logger.error(f"Error tracking schema change metrics: {str(tracking_error)}")
                    # Continue with return, don't let tracking error affect main operation

            return changes, important_changes

        except Exception as e:
            logger.error(f"Error detecting schema changes: {str(e)}")
            return [], False

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