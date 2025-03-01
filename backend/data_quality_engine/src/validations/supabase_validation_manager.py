import json
import logging
from typing import Dict, List, Any, Optional
from sqlalchemy import create_engine, text

# Use relative import for supabase_manager
import sys
import os

# Add the path to find the src directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from src.storage.supabase_manager import SupabaseManager

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
        return self.supabase.get_validation_rules(organization_id, table_name)

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
        """
        Execute all validation rules for a table against the specified database
        Returns a list of validation results
        """
        # Get all rules for this table
        rules = self.get_rules(organization_id, table_name)
        results = []

        if not rules:
            return results

        # Create database engine
        engine = create_engine(connection_string)

        # Execute each rule
        for rule in rules:
            try:
                with engine.connect() as conn:
                    # Execute the query
                    result = conn.execute(text(rule['query'])).fetchone()
                    actual_value = result[0] if result else None

                    # Compare with expected value based on operator
                    is_valid = self._evaluate_rule(rule['operator'], actual_value, rule['expected_value'])

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
                    self.supabase.store_validation_result(
                        organization_id,
                        rule['id'],
                        is_valid,
                        actual_value
                    )

            except Exception as e:
                logger.error(f"Error executing validation rule {rule['rule_name']}: {str(e)}")
                results.append({
                    'rule_name': rule['rule_name'],
                    'description': rule['description'],
                    'is_valid': False,
                    'error': str(e),
                    'expected_value': rule['expected_value'],
                    'operator': rule['operator']
                })

        return results

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