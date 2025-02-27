import json
import os
import sqlite3
from typing import Dict, List, Any, Union, Optional
import logging
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='validations.log'
)
logger = logging.getLogger('validation_manager')


class ValidationManager:
    """Manages validation rules and executes them against database tables"""

    def __init__(self, storage_path="./validations.db"):
        """Initialize the validation manager with a SQLite database path"""
        self.storage_path = storage_path
        self._init_database()

    def _init_database(self):
        """Initialize the SQLite database schema if it doesn't exist"""
        conn = sqlite3.connect(self.storage_path)
        cursor = conn.cursor()

        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS validation_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            rule_name TEXT NOT NULL,
            description TEXT,
            query TEXT NOT NULL,
            operator TEXT NOT NULL,
            expected_value TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS validation_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER NOT NULL,
            run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_valid INTEGER NOT NULL,
            actual_value TEXT,
            FOREIGN KEY (rule_id) REFERENCES validation_rules(id)
        )
        ''')

        # Create indices for faster querying
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rules_table ON validation_rules(table_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rules_name ON validation_rules(rule_name)')

        conn.commit()
        conn.close()

    def get_rules(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all validation rules for a specific table"""
        conn = sqlite3.connect(self.storage_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            'SELECT * FROM validation_rules WHERE table_name = ? ORDER BY id',
            (table_name,)
        )

        rules = [dict(row) for row in cursor.fetchall()]
        conn.close()

        # Convert expected_value from string to Python object
        for rule in rules:
            try:
                rule['expected_value'] = json.loads(rule['expected_value'])
            except json.JSONDecodeError:
                # Keep as string if not valid JSON
                pass

        return rules

    def add_rule(self, table_name: str, rule: Dict[str, Any]) -> int:
        """
        Add a new validation rule
        Returns the ID of the inserted rule
        """
        conn = sqlite3.connect(self.storage_path)
        cursor = conn.cursor()

        # Convert expected_value to JSON string for storage
        expected_value = json.dumps(rule['expected_value'])

        cursor.execute(
            'INSERT INTO validation_rules (table_name, rule_name, description, query, operator, expected_value) VALUES (?, ?, ?, ?, ?, ?)',
            (
                table_name,
                rule['name'],
                rule.get('description', ''),
                rule['query'],
                rule['operator'],
                expected_value
            )
        )

        rule_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return rule_id

    def delete_rule(self, table_name: str, rule_name: str) -> bool:
        """
        Delete a validation rule
        Returns True if successful, False if rule not found
        """
        conn = sqlite3.connect(self.storage_path)
        cursor = conn.cursor()

        cursor.execute(
            'DELETE FROM validation_rules WHERE table_name = ? AND rule_name = ?',
            (table_name, rule_name)
        )

        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()

        return deleted

    def execute_rules(self, connection_string: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Execute all validation rules for a table against the specified database
        Returns a list of validation results
        """
        # Get all rules for this table
        rules = self.get_rules(table_name)
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

                    # Store result in database
                    self._store_result(rule['id'], is_valid, actual_value)

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

    def _store_result(self, rule_id: int, is_valid: bool, actual_value: Any):
        """Store validation result in the database"""
        conn = sqlite3.connect(self.storage_path)
        cursor = conn.cursor()

        cursor.execute(
            'INSERT INTO validation_results (rule_id, is_valid, actual_value) VALUES (?, ?, ?)',
            (
                rule_id,
                1 if is_valid else 0,
                json.dumps(actual_value) if actual_value is not None else None
            )
        )

        conn.commit()
        conn.close()

    def get_validation_history(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most recent validation results for a table"""
        conn = sqlite3.connect(self.storage_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT 
                vr.id, vr.run_timestamp, vr.is_valid, vr.actual_value,
                vru.rule_name, vru.description, vru.operator, vru.expected_value
            FROM validation_results vr
            JOIN validation_rules vru ON vr.rule_id = vru.id
            WHERE vru.table_name = ?
            ORDER BY vr.run_timestamp DESC
            LIMIT ?
        ''', (table_name, limit))

        results = [dict(row) for row in cursor.fetchall()]
        conn.close()

        # Convert values from JSON strings
        for result in results:
            try:
                result['actual_value'] = json.loads(result['actual_value']) if result['actual_value'] else None
                result['expected_value'] = json.loads(result['expected_value'])
            except json.JSONDecodeError:
                # Keep as string if not valid JSON
                pass

        return results