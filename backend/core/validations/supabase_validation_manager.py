import json
import logging
import traceback
import uuid
import datetime
from typing import Dict, List, Any, Optional
from sqlalchemy import create_engine, text
import os
import sys

# Add the correct path to the core directory
core_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../core'))
if core_path not in sys.path:
    sys.path.insert(0, core_path)

# Now import from storage
try:
    from ..storage.supabase_manager import SupabaseManager

    # Log success
    logging.info("Successfully imported SupabaseManager")
except ImportError as e:
    # Log the error and try an alternative approach
    logging.error(f"Failed to import SupabaseManager: {e}")

    # Alternative approach using importlib
    import importlib.util

    manager_path = os.path.join(core_path, 'storage', 'supabase_manager.py')
    spec = importlib.util.spec_from_file_location("supabase_manager", manager_path)
    supabase_manager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(supabase_manager)
    SupabaseManager = supabase_manager.SupabaseManager
    logging.info("Successfully imported SupabaseManager using importlib")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='validations.log'
)
logger = logging.getLogger('supabase_validation_manager')


class SupabaseValidationManager:
    """Manages validation rules and executes them against database tables using Supabase storage"""

    def __init__(self):
        """Initialize the validation manager with a Supabase connection"""
        self.supabase = SupabaseManager()
        logger.info("Supabase Validation Manager initialized")

    def get_rules(self, organization_id: str, table_name: str, connection_id: str = None) -> List[Dict[str, Any]]:
        """Get all validation rules for a specific table, optionally filtered by connection_id"""
        try:
            logging.info(
                f"Getting validation rules for org {organization_id}, table {table_name}, connection {connection_id}")

            # Direct Supabase query to ensure we get the right data
            try:
                query = self.supabase.supabase.table("validation_rules") \
                    .select("*") \
                    .eq("organization_id", organization_id) \
                    .eq("table_name", table_name) \
                    .eq("is_active", True)  # Only get active rules

                # Add connection_id filter if provided
                if connection_id:
                    query = query.eq("connection_id", connection_id)

                response = query.execute()

                rules = response.data if response.data else []

                logging.info(f"Found {len(rules)} validation rules for table {table_name}")

                # Log some details about the rules found
                if rules:
                    rule_ids = [rule.get('id') for rule in rules]
                    connection_ids = list(set([rule.get('connection_id') for rule in rules]))
                    logging.info(f"Rule IDs: {rule_ids[:3]}..." if len(rule_ids) > 3 else f"Rule IDs: {rule_ids}")
                    logging.info(f"Connection IDs in rules: {connection_ids}")
                else:
                    # DEBUG: Check if there are ANY rules for this table (without connection filter)
                    debug_response = self.supabase.supabase.table("validation_rules") \
                        .select("id, connection_id, is_active") \
                        .eq("organization_id", organization_id) \
                        .eq("table_name", table_name) \
                        .execute()

                    debug_rules = debug_response.data if debug_response.data else []
                    logging.warning(f"No rules found with connection filter. Total rules for table: {len(debug_rules)}")

                    if debug_rules:
                        active_rules = [r for r in debug_rules if r.get('is_active', True)]
                        inactive_rules = [r for r in debug_rules if not r.get('is_active', True)]
                        connections_in_rules = list(set([r.get('connection_id') for r in debug_rules]))

                        logging.warning(f"Debug - Active rules: {len(active_rules)}, Inactive: {len(inactive_rules)}")
                        logging.warning(f"Debug - Connection IDs found: {connections_in_rules}")
                        logging.warning(f"Debug - Looking for connection: {connection_id}")

                return rules

            except Exception as direct_error:
                logging.error(f"Direct Supabase query failed: {str(direct_error)}")

                # Fallback to the original method
                try:
                    return self.supabase.get_validation_rules(organization_id, table_name, connection_id)
                except TypeError:
                    # If it fails due to unexpected argument, fall back to old method
                    rules = self.supabase.get_validation_rules(organization_id, table_name)

                    # Filter manually if connection_id is provided
                    if connection_id and rules:
                        original_count = len(rules)
                        rules = [rule for rule in rules if rule.get('connection_id') == connection_id]
                        logging.info(
                            f"Manual filtering: {original_count} -> {len(rules)} rules after connection filter")

                    return rules

        except Exception as e:
            logging.error(f"Error getting validation rules: {str(e)}")
            logging.error(traceback.format_exc())
            return []


    def add_rule(self, organization_id: str, table_name: str, connection_id: str, rule: Dict[str, Any]) -> str:
        """
        Add a new validation rule
        Returns the ID of the inserted rule
        """
        try:
            # Always add connection_id to the rule data
            rule_copy = rule.copy()
            rule_copy['connection_id'] = connection_id

            # Log the rule being added
            logger.info(
                f"Adding rule: {rule_copy.get('name')} for table {table_name} with connection_id {connection_id}")

            try:
                # First try with explicit connection_id parameter
                return self.supabase.add_validation_rule(organization_id, table_name, connection_id, rule_copy)
            except (TypeError, ValueError):
                # If that approach fails, try without the separate connection_id parameter
                # since it's already included in rule_copy
                return self.supabase.add_validation_rule(organization_id, table_name, rule_copy)
        except Exception as e:
            logger.error(f"Error adding validation rule: {str(e)}")
            return None

    def delete_rule(self, organization_id: str, table_name: str, rule_name: str, connection_id: str = None) -> bool:
        """
        Delete a validation rule
        Returns True if successful, False if rule not found
        """
        try:
            # Try with connection_id parameter
            try:
                return self.supabase.delete_validation_rule(organization_id, table_name, rule_name, connection_id)
            except TypeError:
                # Fallback to older version without connection_id
                return self.supabase.delete_validation_rule(organization_id, table_name, rule_name)
        except Exception as e:
            logger.error(f"Error deleting validation rule: {str(e)}")
            return False

    def deactivate_rule(self, organization_id: str, table_name: str, rule_name: str, connection_id: str = None) -> bool:
        """
        Deactivate a validation rule by setting is_active=False

        Args:
            organization_id: Organization ID
            table_name: Table name
            rule_name: Name of the rule to deactivate
            connection_id: Optional connection ID to filter by

        Returns:
            bool: True if successfully deactivated, False otherwise
        """
        try:
            # Use the supabase manager's deactivation method
            return self.supabase.deactivate_validation_rule(
                organization_id=organization_id,
                table_name=table_name,
                rule_name=rule_name,
                connection_id=connection_id
            )
        except Exception as e:
            logger.error(f"Error deactivating validation rule: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def check_rule_exists(self, organization_id, table_name, rule_name, connection_id=None):
        """Check if a rule exists"""
        try:
            query = """
            SELECT COUNT(*) as count
            FROM validation_rules
            WHERE organization_id = :org_id
            AND table_name = :table_name
            AND rule_name = :rule_name
            """

            params = {
                "org_id": organization_id,
                "table_name": table_name,
                "rule_name": rule_name
            }

            if connection_id:
                query += " AND connection_id = :connection_id"
                params["connection_id"] = connection_id

            result = self.supabase.query(query, params)
            return result.get('count', 0) > 0
        except Exception as e:
            logger.error(f"Error checking if rule exists: {str(e)}")
            return False

    def execute_rules(self, organization_id: str, connection_string: str, table_name: str, connection_id: str = None) -> \
            List[Dict[str, Any]]:
        """Execute all validation rules for a table against the specified database - FIXED VERSION"""
        # Get all rules for this table
        rules = self.get_rules(organization_id, table_name, connection_id)
        results = []

        logger.info(f"Executing {len(rules)} validation rules for table {table_name}")

        if not rules:
            logger.warning("No rules found for this table")
            return results

        try:
            # Create database engine - do this once for all rules
            logger.info(f"Creating database engine with connection string: {connection_string}")
            engine = create_engine(connection_string, pool_recycle=600, pool_pre_ping=True)

            # Track how many results we store
            stored_results_count = 0

            # Process rules in smaller batches to reduce memory pressure
            batch_size = 5
            for i in range(0, len(rules), batch_size):
                batch_rules = rules[i:i + batch_size]
                logger.info(f"Processing batch {i // batch_size + 1} with {len(batch_rules)} rules")

                for rule in batch_rules:
                    try:
                        logger.info(f"Executing rule: {rule['rule_name']}")

                        with engine.connect() as conn:
                            # Set a query timeout to prevent long-running queries
                            if 'snowflake' in connection_string.lower():
                                conn.execute(text("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 60"))

                            # Execute the query with a timeout context if possible
                            query = rule['query']
                            logger.debug(f"Executing query: {query}")
                            result = conn.execute(text(query)).fetchone()
                            actual_value = result[0] if result else None
                            logger.debug(f"Query result: {actual_value}")

                            # Compare with expected value based on operator
                            is_valid = self._evaluate_rule(rule['operator'], actual_value, rule['expected_value'])
                            logger.info(
                                f"Rule evaluation: {is_valid} (expected: {rule['expected_value']}, actual: {actual_value})")

                            # Create result object
                            validation_result = {
                                'rule_name': rule['rule_name'],
                                'description': rule['description'] or '',
                                'is_valid': is_valid,
                                'actual_value': actual_value,
                                'expected_value': rule['expected_value'],
                                'operator': rule['operator']
                            }

                            results.append(validation_result)

                            # Store result in Supabase using the fixed store method
                            try:
                                logger.info(f"Storing result in Supabase for rule: {rule['rule_name']}")
                                result_id = self.store_validation_result(
                                    organization_id=organization_id,
                                    rule_id=rule['id'],
                                    is_valid=is_valid,
                                    actual_value=actual_value,
                                    connection_id=connection_id
                                )
                                if result_id:
                                    stored_results_count += 1
                                    logger.info(f"Result stored successfully with ID: {result_id}")
                                else:
                                    logger.error(f"Failed to store validation result for rule {rule['rule_name']}")
                            except Exception as storage_error:
                                logger.error(f"Error storing validation result: {str(storage_error)}")
                                logger.error(traceback.format_exc())
                                # Continue execution even if storage fails

                            # Publish automation event for validation failures
                            if not is_valid:
                                try:
                                    self._publish_validation_failure_event(
                                        organization_id=organization_id,
                                        connection_id=connection_id,
                                        table_name=table_name,
                                        rule_name=rule['rule_name'],
                                        actual_value=actual_value,
                                        expected_value=rule['expected_value']
                                    )
                                except Exception as event_error:
                                    logger.error(f"Error publishing validation failure event: {str(event_error)}")

                    except Exception as e:
                        logger.error(f"Error executing validation rule {rule['rule_name']}: {str(e)}")
                        logger.error(traceback.format_exc())
                        results.append({
                            'rule_name': rule['rule_name'],
                            'description': rule.get('description', ''),
                            'is_valid': False,
                            'error': str(e),
                            'expected_value': rule.get('expected_value'),
                            'operator': rule.get('operator')
                        })

                # Release memory between batches
                import gc
                gc.collect()

            logger.info(f"Validation execution complete. Stored {stored_results_count} results out of {len(results)} total results.")
            return results

        except Exception as e:
            logger.error(f"Error in execute_rules: {str(e)}")
            logger.error(traceback.format_exc())
            return results  # Return whatever results we have instead of raising

    def _evaluate_rule(self, operator: str, actual_value: Any, expected_value: Any) -> bool:
        """Evaluate whether the actual value meets the expected value based on the operator"""
        if actual_value is None:
            return False

        try:
            if operator == 'equals':
                # Handle numeric comparisons properly
                if isinstance(actual_value, (int, float)) and isinstance(expected_value, (int, float)):
                    return actual_value == expected_value
                # String comparison
                return str(actual_value) == str(expected_value)

            elif operator == 'greater_than':
                return float(actual_value) > float(expected_value)

            elif operator == 'less_than':
                return float(actual_value) < float(expected_value)

            elif operator == 'between':
                # Expected format: [min, max]
                if isinstance(expected_value, list) and len(expected_value) == 2:
                    return float(expected_value[0]) <= float(actual_value) <= float(expected_value[1])
                return False

            else:
                logger.warning(f"Unknown operator: {operator}")
                return False

        except (ValueError, TypeError) as e:
            logger.error(f"Error evaluating rule: {str(e)}")
            return False

    def _publish_validation_failure_event(self, organization_id: str, connection_id: str, table_name: str,
                                          rule_name: str, actual_value: Any, expected_value: Any):
        """Publish automation event for validation failure"""
        try:
            # Try to import and use automation events
            from core.automation.events import AutomationEventType, publish_automation_event

            publish_automation_event(
                event_type=AutomationEventType.VALIDATION_FAILURES_DETECTED,
                data={
                    "table_name": table_name,
                    "rule_name": rule_name,
                    "actual_value": actual_value,
                    "expected_value": expected_value,
                    "failed_rules": 1
                },
                connection_id=connection_id,
                organization_id=organization_id
            )
            logger.info(f"Published validation failure event for rule {rule_name}")

        except ImportError:
            logger.warning("Automation events not available, skipping event publication")
        except Exception as e:
            logger.error(f"Error publishing validation failure event: {str(e)}")

    def get_validation_history(self, organization_id: str, table_name: str, connection_id: str = None,
                               limit: int = 30) -> List[Dict[str, Any]]:
        """Get the most recent validation results for a table"""
        return self.supabase.get_validation_history(organization_id, table_name, connection_id, limit)

    def update_rule(self, organization_id: str, rule_id: str, rule: Dict[str, Any], connection_id: str = None) -> bool:
        """Update an existing validation rule without deleting and recreating it"""
        try:
            # Ensure expected_value is stored as a JSON string
            expected_value = json.dumps(rule.get("expected_value", ""))

            data = {
                "rule_name": rule.get("name", ""),
                "description": rule.get("description", ""),
                "query": rule.get("query", ""),
                "operator": rule.get("operator", "equals"),
                "expected_value": expected_value
            }

            # Only include connection_id in the update if provided
            if connection_id:
                data["connection_id"] = connection_id

            # Use the supabase client from the manager
            query = self.supabase.supabase.table("validation_rules") \
                .update(data) \
                .eq("id", rule_id) \
                .eq("organization_id", organization_id)

            # If connection_id is provided, add it to the query conditions
            if connection_id:
                query = query.eq("connection_id", connection_id)

            response = query.execute()

            return bool(response.data)  # True if any records were updated

        except Exception as e:
            logger.error(f"Error updating validation rule: {str(e)}")
            return False

    def store_validation_result(
            self,
            organization_id,
            rule_id,
            is_valid,
            actual_value=None,
            connection_id=None,
            profile_history_id=None
    ):
        """Store validation result in Supabase - FIXED VERSION"""
        try:
            logger.info(
                f"Storing validation result with connection_id: {connection_id} and profile_history_id: {profile_history_id}")

            # Create a record to insert - including connection_id
            validation_result = {
                "id": str(uuid.uuid4()),
                "organization_id": organization_id,
                "rule_id": rule_id,
                "is_valid": is_valid,
                "run_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "actual_value": json.dumps(actual_value) if actual_value is not None else None,
                "connection_id": connection_id,  # Include connection_id
                "profile_history_id": profile_history_id
            }

            # Use the Supabase client directly for storage
            response = self.supabase.supabase.table("validation_results").insert(validation_result).execute()

            if not response.data or len(response.data) == 0:
                logger.error("Failed to store validation result: No data returned")
                return None

            result_id = response.data[0].get("id")
            logger.debug(f"Successfully stored validation result with ID: {result_id}")
            return result_id

        except Exception as e:
            logger.error(f"Error storing validation result: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def get_validation_summary(self, organization_id: str, connection_id: str) -> Dict[str, Any]:
        """Get validation summary for automation dashboard"""
        try:
            # Get all validation rules for this connection
            all_rules = []

            # Get all tables with validations for this connection
            response = self.supabase.supabase.table("validation_rules") \
                .select("table_name") \
                .eq("organization_id", organization_id) \
                .eq("connection_id", connection_id) \
                .execute()

            if not response.data:
                return {
                    "total_rules": 0,
                    "tables_with_validations": 0,
                    "passing_rules": 0,
                    "failing_rules": 0,
                    "last_run": None
                }

            # Get unique tables
            tables = list(set(rule["table_name"] for rule in response.data))

            total_rules = 0
            passing_rules = 0
            failing_rules = 0
            last_run = None

            for table_name in tables:
                rules = self.get_rules(organization_id, table_name, connection_id)
                total_rules += len(rules)

                # Get latest validation results for this table
                history = self.get_validation_history(organization_id, table_name, connection_id, limit=len(rules))

                if history:
                    # Group by rule to get latest result for each rule
                    latest_by_rule = {}
                    for result in history:
                        rule_name = result["rule_name"]
                        if rule_name not in latest_by_rule or result["run_at"] > latest_by_rule[rule_name]["run_at"]:
                            latest_by_rule[rule_name] = result

                    # Count passing/failing
                    for result in latest_by_rule.values():
                        if result["is_valid"]:
                            passing_rules += 1
                        else:
                            failing_rules += 1

                        # Update last run time
                        if not last_run or result["run_at"] > last_run:
                            last_run = result["run_at"]

            return {
                "total_rules": total_rules,
                "tables_with_validations": len(tables),
                "passing_rules": passing_rules,
                "failing_rules": failing_rules,
                "last_run": last_run
            }

        except Exception as e:
            logger.error(f"Error getting validation summary: {str(e)}")
            return {
                "total_rules": 0,
                "tables_with_validations": 0,
                "passing_rules": 0,
                "failing_rules": 0,
                "last_run": None,
                "error": str(e)
            }

    def get_tables_with_validations(self, organization_id: str, connection_id: str) -> List[str]:
        """Get list of tables that have validation rules"""
        try:
            response = self.supabase.supabase.table("validation_rules") \
                .select("table_name") \
                .eq("organization_id", organization_id) \
                .eq("connection_id", connection_id) \
                .execute()

            if response.data:
                return list(set(rule["table_name"] for rule in response.data))
            return []

        except Exception as e:
            logger.error(f"Error getting tables with validations: {str(e)}")
            return []