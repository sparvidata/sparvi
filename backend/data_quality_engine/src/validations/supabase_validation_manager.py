import json
import logging
import traceback
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
    from storage.supabase_manager import SupabaseManager

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

    def get_rules(self, organization_id: str, table_name: str) -> List[Dict[str, Any]]:
        """Get all validation rules for a specific table"""
        try:
            logging.info(f"Getting validation rules for org {organization_id}, table {table_name}")
            return self.supabase.get_validation_rules(organization_id, table_name)
        except Exception as e:
            logging.error(f"Error getting validation rules: {str(e)}")
            return []

    def add_rule(self, organization_id: str, table_name: str, rule: Dict[str, Any]) -> str:
        """
        Add a new validation rule
        Returns the ID of the inserted rule
        """
        return self.supabase.add_validation_rule(organization_id, table_name, rule)

    def delete_rule(self, organization_id: str, table_name: str, rule_name: str) -> bool:
        """
        Delete a validation rule
        Returns True if successful, False if rule not found
        """
        return self.supabase.delete_validation_rule(organization_id, table_name, rule_name)

    def execute_rules(self, organization_id: str, connection_string: str, table_name: str) -> List[Dict[str, Any]]:
        """Execute all validation rules for a table against the specified database"""
        # Get all rules for this table
        rules = self.get_rules(organization_id, table_name)
        results = []

        logger.info(f"Executing {len(rules)} validation rules for table {table_name}")

        if not rules:
            logger.warning("No rules found for this table")
            return results

        try:
            # Create database engine
            logger.info(f"Creating database engine with connection string: {connection_string}")
            engine = create_engine(connection_string)

            # Execute each rule
            for rule in rules:
                try:
                    logger.info(f"Executing rule: {rule['rule_name']}")

                    with engine.connect() as conn:
                        # Execute the query
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
                            'description': rule['description'],
                            'is_valid': is_valid,
                            'actual_value': actual_value,
                            'expected_value': rule['expected_value'],
                            'operator': rule['operator']
                        }

                        results.append(validation_result)

                        # Store result in Supabase
                        logger.info(f"Storing result in Supabase for rule: {rule['rule_name']}")
                        self.supabase.store_validation_result(
                            organization_id,
                            rule['id'],
                            is_valid,
                            actual_value
                        )
                        logger.info(f"Result stored successfully")

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

            return results

        except Exception as e:
            logger.error(f"Error in execute_rules: {str(e)}")
            logger.error(traceback.format_exc())
            raise

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

    def get_validation_history(self, organization_id: str, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most recent validation results for a table"""
        return self.supabase.get_validation_history(organization_id, table_name, limit)

    def update_rule(self, organization_id: str, rule_id: str, rule: Dict[str, Any]) -> bool:
        """
        Update an existing validation rule
        Returns True if successful, False if rule not found or update failed
        """
        return self.supabase.update_validation_rule(organization_id, rule_id, rule)