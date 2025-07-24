"""Validation-related routes - EXACT COPIES from app.py for zero functionality loss"""

import os
import json
import logging
import traceback
import concurrent.futures
import psutil
from flask import request, jsonify

from core.auth.decorators import token_required
from core.connections.utils import connection_access_check
from core.storage.supabase_manager import SupabaseManager
from core.metadata.storage_service import MetadataStorageService
from core.metadata.collector import MetadataCollector
from core.validations.supabase_validation_manager import SupabaseValidationManager
from sparvi.validations.validator import run_validations as sparvi_run_validations

logger = logging.getLogger(__name__)

# Initialize validation manager
validation_manager = SupabaseValidationManager()


def register_validation_routes(app):
    """Register all validation-related routes with the Flask app - EXACT COPIES"""
    
    @app.route("/api/validations", methods=["GET"])
    @token_required
    def get_validations(current_user, organization_id):
        """Get all validation rules for a table"""
        table_name = request.args.get("table")
        connection_id = request.args.get("connection_id")

        if not table_name:
            return jsonify({"error": "Table name is required"}), 400

        try:
            logger.info(
                f"Getting validation rules for organization: {organization_id}, table: {table_name}, connection: {connection_id}")
            rules = validation_manager.get_rules(organization_id, table_name, connection_id)
            logger.info(f"Retrieved {len(rules)} validation rules")
            logger.debug(f"Rules content: {rules}")
            return jsonify({"rules": rules})
        except Exception as e:
            logger.error(f"Error getting validation rules: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    @app.route("/api/validations/summary", methods=["GET"])
    @token_required
    def get_validations_summary(current_user, organization_id):
        """Get a summary of all validation rules and their latest results across tables for a connection"""
        connection_id = request.args.get("connection_id")
        if not connection_id:
            return jsonify({"error": "Connection ID is required"}), 400

        try:
            logger.info(
                f"Getting validation summary for organization: {organization_id}, connection: {connection_id}")

            # Check access to connection
            connection = connection_access_check(connection_id, organization_id)
            if not connection:
                return jsonify({"error": "Connection not found or access denied"}), 404

            # Get tables for this connection using the existing endpoint logic
            storage_service = MetadataStorageService()
            tables_metadata = storage_service.get_metadata(connection_id, "tables")

            tables = []
            if tables_metadata and "metadata" in tables_metadata:
                tables = [table.get("name") for table in tables_metadata["metadata"].get("tables", [])]

            # If no tables in metadata, try to get them directly from database
            if not tables:
                # Create connector for this connection
                from app import get_connector_for_connection  # Import from app.py to avoid circular import
                connector = get_connector_for_connection(connection)
                connector.connect()

                # Create metadata collector
                collector = MetadataCollector(connection_id, connector)

                # Get table list (limited for immediate response)
                tables = collector.collect_table_list()

            # Initialize counters and data structures
            total_validations = 0
            validations_by_table = {}
            passing_count = 0
            failing_count = 0
            unknown_count = 0
            tables_with_validations = 0
            tables_with_failures = 0
            recently_run_tables = 0
            supabase_mgr = SupabaseManager()

            # Get validation data for each table
            for table in tables:
                # Get rules for this table+connection
                rules = validation_manager.get_rules(organization_id, table, connection_id)
                if not rules:
                    continue

                tables_with_validations += 1
                rule_count = len(rules)
                total_validations += rule_count

                # Track results for this table
                table_results = {
                    "total": rule_count,
                    "passing": 0,
                    "failing": 0,
                    "unknown": 0,
                    "last_run": None,
                    "health_score": 0
                }

                # Create a lookup map for rules
                rule_map = {rule["id"]: rule for rule in rules}
                has_failures = False
                most_recent_run = None

                # Check the latest results for each rule
                for rule in rules:
                    rule_id = rule["id"]
                    try:
                        # Query the most recent result for this rule
                        result_response = supabase_mgr.supabase.table("validation_results") \
                            .select("*") \
                            .eq("rule_id", rule_id) \
                            .eq("organization_id", organization_id) \
                            .eq("connection_id", connection_id) \
                            .order("run_at", desc=True) \
                            .limit(1) \
                            .execute()

                        if result_response.data and len(result_response.data) > 0:
                            result = result_response.data[0]

                            # Track the most recent run time
                            if most_recent_run is None or result["run_at"] > most_recent_run:
                                most_recent_run = result["run_at"]

                            # Count based on result status
                            if result["is_valid"]:
                                table_results["passing"] += 1
                                passing_count += 1
                            else:
                                table_results["failing"] += 1
                                failing_count += 1
                                has_failures = True
                        else:
                            # No result found
                            table_results["unknown"] += 1
                            unknown_count += 1

                    except Exception as e:
                        logger.error(f"Error getting result for rule {rule_id}: {str(e)}")
                        # Count as unknown
                        table_results["unknown"] += 1
                        unknown_count += 1

                # Calculate health score for this table (% of passing validations out of known results)
                total_known = table_results["passing"] + table_results["failing"]
                if total_known > 0:
                    table_results["health_score"] = (table_results["passing"] / total_known) * 100

                # Set the last run time
                if most_recent_run:
                    table_results["last_run"] = most_recent_run
                    recently_run_tables += 1

                # Track tables with failures
                if has_failures:
                    tables_with_failures += 1

                validations_by_table[table] = table_results

            # Calculate overall health score
            overall_health_score = 0
            if (passing_count + failing_count) > 0:
                overall_health_score = (passing_count / (passing_count + failing_count)) * 100

            # Check how recently validations were run
            freshness_status = "stale"
            if recently_run_tables > 0:
                # Determine if validations are recent (within the last 24 hours)
                freshness_status = "recent"

            # Build comprehensive summary
            summary = {
                "total_count": total_validations,
                "tables_with_validations": tables_with_validations,
                "tables_with_failures": tables_with_failures,
                "validations_by_table": validations_by_table,
                "passing_count": passing_count,
                "failing_count": failing_count,
                "unknown_count": unknown_count,
                "overall_health_score": overall_health_score,
                "freshness_status": freshness_status,
                "connection_name": connection.get("name", "Unknown")
            }

            return jsonify(summary)
        except Exception as e:
            logger.error(f"Error getting validation summary: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    @app.route("/api/validations", methods=["POST"])
    @token_required
    def add_validation_rule(current_user, organization_id):
        """Add a new validation rule for a table"""
        table_name = request.args.get("table")
        connection_id = request.args.get("connection_id")

        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        if not connection_id:
            return jsonify({"error": "Connection ID is required"}), 400

        rule_data = request.get_json()
        if not rule_data:
            return jsonify({"error": "Rule data is required"}), 400

        required_fields = ["name", "description", "query", "operator", "expected_value"]
        for field in required_fields:
            if field not in rule_data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        try:
            logger.info(
                f"Adding validation rule for organization: {organization_id}, table: {table_name}, connection: {connection_id}")
            rule_id = validation_manager.add_rule(organization_id, table_name, connection_id, rule_data)

            if rule_id:
                return jsonify({"success": True, "id": rule_id})
            else:
                return jsonify({"error": "Failed to add validation rule"}), 500
        except Exception as e:
            logger.error(f"Error adding validation rule: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    @app.route("/api/validations", methods=["DELETE"])
    @token_required
    def delete_validation_rule(current_user, organization_id):
        """Delete a validation rule"""
        table_name = request.args.get("table")
        rule_name = request.args.get("rule_name")
        connection_id = request.args.get("connection_id")

        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        if not rule_name:
            return jsonify({"error": "Rule name is required"}), 400

        try:
            logger.info(f"Deleting validation rule {rule_name} for organization: {organization_id}, table: {table_name}, connection: {connection_id}")
            success = validation_manager.delete_rule(organization_id, table_name, rule_name, connection_id)

            if success:
                return jsonify({"success": True})
            else:
                return jsonify({"error": "Rule not found or delete failed"}), 404
        except Exception as e:
            logger.error(f"Error deleting validation rule: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    @app.route("/api/validations/deactivate", methods=["PUT"])
    @token_required
    def deactivate_validation_rule(current_user, organization_id):
        """Deactivate a validation rule without deleting it"""
        table_name = request.args.get("table")
        rule_name = request.args.get("rule_name")
        connection_id = request.args.get("connection_id")

        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        if not rule_name:
            return jsonify({"error": "Rule name is required"}), 400
        if not connection_id:
            return jsonify({"error": "Connection ID is required"}), 400

        try:
            logger.info(f"Deactivating validation rule {rule_name} for organization: {organization_id}, table: {table_name}, connection: {connection_id}")
            success = validation_manager.deactivate_rule(organization_id, table_name, rule_name, connection_id)

            if success:
                return jsonify({"success": True})
            else:
                return jsonify({"error": "Rule not found or deactivation failed"}), 404
        except Exception as e:
            logger.error(f"Error deactivating validation rule: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    @app.route("/api/validation-history", methods=["GET"])
    @token_required
    def get_validation_history(current_user, organization_id):
        """Get history of validation results for a table"""
        table_name = request.args.get("table")
        connection_id = request.args.get("connection_id")
        limit = request.args.get("limit", 30, type=int)

        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        if not connection_id:
            return jsonify({"error": "Connection ID is required"}), 400

        try:
            logger.info(
                f"Getting validation history for organization: {organization_id}, table: {table_name}, connection: {connection_id}")

            history = validation_manager.get_validation_history(organization_id, table_name, connection_id, limit)

            return jsonify({"history": history})
        except Exception as e:
            logger.error(f"Error getting validation history: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    @app.route("/api/run-validations", methods=["POST"])
    @token_required
    def run_validation_rules(current_user, organization_id):
        """Run all validation rules for a table"""
        data = request.get_json()
        logger.info(f"Run validations request: {data}")

        log_memory_usage()

        if not data or "table" not in data:
            logger.warning("Table name missing in request")
            return jsonify({"error": "Table name is required"}), 400

        connection_string = data.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
        connection_id = data.get("connection_id")
        table_name = data["table"]
        profile_history_id = data.get("profile_history_id")

        if not connection_id:
            return jsonify({"error": "Connection ID is required"}), 400

        logger.info(f"Running validations with profile_history_id: {profile_history_id}")

        try:
            logger.info(f"Running validations for org: {organization_id}, table: {table_name}, connection: {connection_id}")

            log_memory_usage()

            # Use the optimized internal function
            result = run_validation_rules_internal(current_user, organization_id, data)

            # If there was an error, return it
            if "error" in result:
                return jsonify({"error": result["error"]}), 500

            # Otherwise return the results
            return jsonify(result)

        except Exception as e:
            logger.error(f"Error running validations: {str(e)}")
            traceback.print_exc()

            # If exception suggests schema issues and we have connection_id
            if connection_id and ("no such column" in str(e).lower() or
                                  "table not found" in str(e).lower() or
                                  "does not exist" in str(e).lower()):
                try:
                    from core.metadata.events import MetadataEventType, publish_metadata_event
                    # Publish schema mismatch event
                    publish_metadata_event(
                        event_type=MetadataEventType.VALIDATION_FAILURE,
                        connection_id=connection_id,
                        details={
                            "table_name": table_name,
                            "reason": "schema_mismatch",
                            "error": str(e)
                        },
                        organization_id=organization_id,
                        user_id=current_user
                    )
                    logger.info(f"Published schema mismatch event due to exception for table {table_name}")
                except Exception as event_error:
                    logger.error(f"Error publishing schema mismatch event: {str(event_error)}")

            return jsonify({"error": str(e)}), 500

    @app.route("/api/generate-default-validations", methods=["POST"])
    @token_required
    def generate_default_validations(current_user, organization_id):
        data = request.get_json()

        logger.info(f"Received default validations request: {data}")

        if not data or "table" not in data:
            return jsonify({"error": "Table name is required"}), 400

        if not data.get("connection_id"):
            return jsonify({"error": "Connection ID is required"}), 400

        # Extract values
        table_name = data["table"]
        connection_id = data.get("connection_id")

        try:
            # Check access to connection
            connection = connection_access_check(connection_id, organization_id)
            if not connection:
                return jsonify({"error": "Connection not found or access denied"}), 404

            # Build proper connection string
            from app import get_connection_string  # Import from app.py to avoid circular import
            connection_string = get_connection_string(connection)
            if not connection_string:
                return jsonify({"error": "Failed to build connection string"}), 400

            # Get existing rules
            existing_rules = validation_manager.get_rules(organization_id, table_name, connection_id)
            existing_rule_names = {rule['rule_name'] for rule in existing_rules}

            logger.info(f"Found {len(existing_rules)} existing rules for table {table_name}")

            # Try to verify table first
            try:
                # Create a temporary connection to verify table
                from app import get_connector_for_connection  # Import from app.py to avoid circular import
                connector = get_connector_for_connection(connection)
                connector.connect()

                # Try a simple count query to verify access
                try:
                    result = connector.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = result[0][0] if result and len(result) > 0 else 0
                    logger.info(f"Table {table_name} exists with {row_count} rows")
                except Exception as query_error:
                    logger.error(f"Error querying table: {str(query_error)}")
                    return jsonify({"error": f"Error accessing table {table_name}: {str(query_error)}"}), 400

            except Exception as conn_error:
                logger.error(f"Error connecting to database: {str(conn_error)}")
                return jsonify({"error": f"Error connecting to database: {str(conn_error)}"}), 500

            # Generate validations through connector to ensure consistent schema access
            try:
                # Get columns first
                columns = connector.get_columns(table_name)
                logger.info(f"Found {len(columns)} columns for table {table_name}")

                # Get primary keys
                primary_keys = connector.get_primary_keys(table_name)
                logger.info(f"Found primary keys: {primary_keys}")

                # Now generate validations manually instead of using get_default_validations
                validations = []

                # 1. Basic row count validation
                validations.append({
                    "name": f"check_{table_name}_not_empty",
                    "description": f"Ensure {table_name} table has at least one row",
                    "query": f"SELECT COUNT(*) FROM {table_name}",
                    "operator": "greater_than",
                    "expected_value": 0
                })

                # 2. Add validations for each column
                for column in columns:
                    column_name = column.get("name")
                    column_type = str(column.get("type", "")).lower()
                    is_nullable = column.get("nullable", True)

                    # Not null check for non-nullable columns
                    if not is_nullable and column_name not in primary_keys:
                        validations.append({
                            "name": f"check_{column_name}_not_null",
                            "description": f"Ensure {column_name} has no NULL values",
                            "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL",
                            "operator": "equals",
                            "expected_value": 0
                        })

                    # Type-specific checks
                    if "int" in column_type or "float" in column_type or "numeric" in column_type:
                        # Check for negative values in numeric columns
                        validations.append({
                            "name": f"check_{column_name}_not_negative",
                            "description": f"Ensure {column_name} has no negative values",
                            "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} < 0",
                            "operator": "equals",
                            "expected_value": 0
                        })

                    elif "char" in column_type or "text" in column_type or "string" in column_type:
                        # Check for empty strings
                        validations.append({
                            "name": f"check_{column_name}_not_empty_string",
                            "description": f"Ensure {column_name} has no empty strings",
                            "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = ''",
                            "operator": "equals",
                            "expected_value": 0
                        })

                        # Email pattern check if column name suggests email
                        if "email" in column_name.lower():
                            validations.append({
                                "name": f"check_{column_name}_valid_email",
                                "description": f"Ensure {column_name} contains valid email format",
                                "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NOT NULL AND {column_name} NOT LIKE '%@%.%'",
                                "operator": "equals",
                                "expected_value": 0
                            })

                    elif "date" in column_type or "time" in column_type:
                        # Check for future dates
                        validations.append({
                            "name": f"check_{column_name}_not_future",
                            "description": f"Ensure {column_name} contains no future dates",
                            "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} > CURRENT_DATE",
                            "operator": "equals",
                            "expected_value": 0
                        })

                logger.info(f"Generated {len(validations)} validation rules")

            except Exception as gen_error:
                logger.error(f"Error generating validations: {str(gen_error)}")
                return jsonify({"error": f"Error generating validations: {str(gen_error)}"}), 500

            # Add the validations
            count_added = 0
            count_skipped = 0

            for validation in validations:
                try:
                    # Skip if rule with same name already exists
                    if validation['name'] in existing_rule_names:
                        count_skipped += 1
                        continue

                    # Add the rule through the validation manager
                    validation_manager.add_rule(organization_id, table_name, connection_id, validation)
                    count_added += 1
                except Exception as add_error:
                    logger.error(f"Failed to add validation rule {validation['name']}: {str(add_error)}")

            result = {
                "added": count_added,
                "skipped": count_skipped,
                "total": count_added + count_skipped
            }

            logger.info(f"Added {result['added']} default validation rules ({result['skipped']} skipped as duplicates)")
            return jsonify({
                "success": True,
                "message": f"Added {result['added']} default validation rules ({result['skipped']} skipped as duplicates)",
                "count": result['added'],
                "skipped": result['skipped'],
                "total": result['total']
            })
        except Exception as e:
            logger.error(f"Error generating default validations: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    @app.route("/api/validations/latest/<connection_id>/<table_name>", methods=["GET"])
    @token_required
    def get_latest_validation_results(current_user, organization_id, connection_id, table_name):
        """Get the latest validation results for all rules for a specific table"""
        try:
            logger.info(f"Fetching latest validation results for table {table_name} in connection {connection_id}")

            # First check if user has access to connection
            connection = connection_access_check(connection_id, organization_id)
            if not connection:
                return jsonify({"error": "Connection not found or access denied"}), 404

            # Get rules for this table+connection
            rules = validation_manager.get_rules(organization_id, table_name, connection_id)

            if not rules:
                return jsonify({"results": [], "message": "No validation rules found for this table"}), 200

            # Create a rule lookup map
            rule_map = {rule["id"]: rule for rule in rules}

            # Get latest results for each rule
            results = []
            for rule in rules:
                rule_id = rule["id"]
                try:
                    # Query the most recent result for this rule
                    supabase_mgr = SupabaseManager()
                    result_response = supabase_mgr.supabase.table("validation_results") \
                        .select("*") \
                        .eq("rule_id", rule_id) \
                        .eq("organization_id", organization_id) \
                        .eq("connection_id", connection_id) \
                        .order("run_at", desc=True) \
                        .limit(1) \
                        .execute()

                    if result_response.data and len(result_response.data) > 0:
                        result = result_response.data[0]

                        # Parse values from JSON strings
                        try:
                            actual_value = json.loads(result["actual_value"]) if result["actual_value"] else None
                        except (json.JSONDecodeError, TypeError):
                            actual_value = result["actual_value"]

                        try:
                            expected_value = json.loads(rule.get("expected_value", "null"))
                        except (json.JSONDecodeError, TypeError):
                            expected_value = rule.get("expected_value")

                        # Add the formatted result
                        results.append({
                            "id": result["id"],
                            "rule_id": rule_id,
                            "rule_name": rule.get("rule_name", "Unknown"),
                            "description": rule.get("description", ""),
                            "is_valid": result["is_valid"],
                            "actual_value": actual_value,
                            "expected_value": expected_value,
                            "operator": rule.get("operator", "equals"),
                            "run_at": result["run_at"]
                        })
                except Exception as e:
                    logger.error(f"Error getting result for rule {rule_id}: {str(e)}")
                    # Continue with other rules even if one fails

            return jsonify({
                "results": results,
                "table_name": table_name,
                "connection_id": connection_id,
                "count": len(results)
            })

        except Exception as e:
            logger.error(f"Error getting latest validation results: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": str(e)}), 500

    @app.route("/api/validations/<rule_id>", methods=["PUT"])
    @token_required
    def update_validation(current_user, organization_id, rule_id):
        """Update an existing validation rule"""
        table_name = request.args.get("table")
        connection_id = request.args.get("connection_id")

        if not table_name:
            return jsonify({"error": "Table name is required"}), 400
        if not connection_id:
            return jsonify({"error": "Connection ID is required"}), 400

        rule_data = request.get_json()
        if not rule_data:
            return jsonify({"error": "Rule data is required"}), 400

        required_fields = ["name", "query", "operator", "expected_value"]
        for field in required_fields:
            if field not in rule_data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        try:
            logger.info(f"Updating validation rule {rule_id} for table {table_name} in connection {connection_id}")

            # First check if this rule belongs to the connection
            supabase_mgr = SupabaseManager()
            rule_check = supabase_mgr.supabase.table("validation_rules") \
                .select("connection_id") \
                .eq("id", rule_id) \
                .eq("organization_id", organization_id) \
                .single() \
                .execute()

            if not rule_check.data or rule_check.data.get("connection_id") != connection_id:
                return jsonify({"error": "Rule not found or belongs to a different connection"}), 404

            # Now update the rule
            success = validation_manager.update_rule(organization_id, rule_id, rule_data)

            if success:
                return jsonify({"success": True})
            else:
                return jsonify({"error": "Rule not found or update failed"}), 404
        except Exception as e:
            logger.error(f"Error updating validation rule: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500


def run_validation_rules_internal(user_id, organization_id, data):
    """Internal version of run_validation_rules that can be called from other functions"""
    if not data or "table" not in data:
        return {"error": "Table name is required"}

    connection_string = data.get("connection_string")
    connection_id = data.get("connection_id")
    logger.info(f"Run validations request: {data}")
    table_name = data["table"]
    profile_history_id = data.get("profile_history_id")

    if not connection_id:
        return {"error": "Connection ID is required"}

    try:
        force_gc()

        # If no connection string is provided, fetch connection details
        if not connection_string and connection_id:
            # Check access to connection
            supabase_mgr = SupabaseManager()
            connection_check = supabase_mgr.supabase.table("database_connections") \
                .select("*") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_check.data or len(connection_check.data) == 0:
                logger.error(f"Connection not found or access denied: {connection_id}")
                return {"error": "Connection not found or access denied"}

            connection = connection_check.data[0]

            # Create connector for this connection
            try:
                from app import get_connector_for_connection  # Import from app.py to avoid circular import
                connector = get_connector_for_connection(connection)
                connector.connect()
            except Exception as e:
                logger.error(f"Failed to connect to database: {str(e)}")
                return {"error": f"Failed to connect to database: {str(e)}"}

            # Build connection string using the connection details
            from app import build_connection_string  # Import from app.py to avoid circular import
            connection_string = build_connection_string(connection)

        # Validate connection string
        if not connection_string:
            logger.error("No database connection string available")
            return {"error": "No database connection string available"}

        # Get all rules
        rules = validation_manager.get_rules(organization_id, table_name, connection_id)

        if not rules:
            return {"results": []}

        # Convert from Supabase format to sparvi-core format if needed
        validation_rules = []
        for rule in rules:
            validation_rules.append({
                "name": rule["rule_name"],
                "description": rule["description"],
                "query": rule["query"],
                "operator": rule["operator"],
                "expected_value": rule["expected_value"]
            })

        # Log memory usage before validation
        log_memory_usage("Before validation")
        force_gc()

        # OPTIMIZATION: Execute rules in parallel for faster processing
        # Use a thread pool to execute validations in parallel
        def execute_validation_rule(rule, connection_string):
            """Execute a single validation rule"""
            try:
                # Use sparvi_run_validations but with a single rule for better performance
                result = sparvi_run_validations(connection_string, [rule])
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Error executing validation rule {rule['name']}: {str(e)}")
                return {
                    "name": rule["name"],
                    "is_valid": False,
                    "error": str(e)
                }

        # Execute rules in parallel with a limited number of workers
        # We use a smaller batch size here to avoid overloading the database
        max_parallel = min(10, len(validation_rules))
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
            # Submit all tasks
            future_to_rule = {
                executor.submit(execute_validation_rule, rule, connection_string): (i, rule)
                for i, rule in enumerate(validation_rules)
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_rule):
                i, rule = future_to_rule[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)

                        # Store result in Supabase
                        actual_value = result.get("actual_value", None)
                        validation_manager.store_validation_result(
                            organization_id,
                            rules[i]["id"],
                            result["is_valid"],
                            actual_value,
                            connection_id,  # Pass connection_id
                            profile_history_id
                        )
                except Exception as e:
                    logger.error(f"Error processing validation result: {str(e)}")

        # Log memory usage after validation
        log_memory_usage("After validation")

        # Check for failed validations and publish event if necessary
        had_failures = any(not r.get("is_valid", True) for r in results)
        schema_mismatch = any(
            ("column not found" in str(r.get("error", "")).lower() or
             "table not found" in str(r.get("error", "")).lower() or
             ("relation" in str(r.get("error", "")).lower() and "does not exist" in str(r.get("error", "")).lower()))
            for r in results if not r.get("is_valid", True)
        )

        # If any validations failed and we have a connection_id, publish an event
        if had_failures and connection_id:
            try:
                from core.metadata.events import MetadataEventType, publish_metadata_event
                # Publish validation failure event
                publish_metadata_event(
                    event_type=MetadataEventType.VALIDATION_FAILURE,
                    connection_id=connection_id,
                    details={
                        "table_name": table_name,
                        "reason": "schema_mismatch" if schema_mismatch else "data_issue",
                        "validation_count": len(results),
                        "failure_count": sum(1 for r in results if not r.get("is_valid", True))
                    },
                    organization_id=organization_id,
                    user_id=user_id
                )
                logger.info(f"Published validation failure event for table {table_name}")
            except Exception as e:
                logger.error(f"Error publishing validation failure event: {str(e)}")
                logger.error(traceback.format_exc())

        return {"results": results}

    except Exception as e:
        logger.error(f"Error running validations internal: {str(e)}")
        traceback.print_exc()
        return {"error": str(e)}


def log_memory_usage(label=""):
    """Log current memory usage"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        memory_mb = mem_info.rss / (1024 * 1024)
        logger.info(f"Memory Usage [{label}]: {memory_mb:.2f} MB")

        # Alert if memory is getting high (adjust threshold as needed for your environment)
        if memory_mb > 500:  # Alert if using more than 500 MB
            logger.warning(f"High memory usage detected: {memory_mb:.2f} MB")

        return memory_mb
    except ImportError:
        logger.warning("psutil not installed - cannot log memory usage")
        return 0
    except Exception as e:
        logger.warning(f"Error logging memory usage: {str(e)}")
        return 0


def force_gc():
    """Force garbage collection to free memory"""
    try:
        import gc
        gc.collect()
    except Exception as e:
        logger.warning(f"Error during garbage collection: {str(e)}")