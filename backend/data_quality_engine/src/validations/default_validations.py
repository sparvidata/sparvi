import logging
from typing import List, Dict, Any
import sqlalchemy as sa
from sqlalchemy import inspect
import os


def get_default_validations(connection_string: str, table_name: str) -> List[Dict[str, Any]]:
    """
    Generate default validation rules that can be applied to any table
    """
    # Add debugging
    logging.info(f"Creating engine with connection string: {connection_string}")

    # Fallback to default connection if None is provided
    if connection_string is None:
        connection_string = os.getenv("DEFAULT_CONNECTION_STRING")
        logging.info(f"Using default connection: {connection_string}")

    if not connection_string:
        raise ValueError(
            "No connection string provided and no default connection string found in environment variables")

    try:
        # Create engine and get inspector
        engine = sa.create_engine(connection_string)
        inspector = inspect(engine)

        # Test connection
        with engine.connect() as conn:
            pass  # Just test that we can connect

        # Get column information
        columns = inspector.get_columns(table_name)
        primary_keys = inspector.get_pk_constraint(table_name).get('constrained_columns', [])

        # Initialize validation rules list
        validations = []

        # 1. Row count validation - ensure table is not empty
        validations.append({
            "name": f"check_{table_name}_not_empty",
            "description": f"Ensure {table_name} table has at least one row",
            "query": f"SELECT COUNT(*) FROM {table_name}",
            "operator": "greater_than",
            "expected_value": 0
        })

        # Continue with the rest of the validation rules...

        # 2. Duplicate primary key check (if primary keys exist)
        if primary_keys:
            pk_columns = ", ".join(primary_keys)
            validations.append({
                "name": f"check_{table_name}_pk_unique",
                "description": f"Ensure primary key ({pk_columns}) has no duplicates",
                "query": f"""
                    SELECT COUNT(*) FROM (
                        SELECT {pk_columns}, COUNT(*) as count 
                        FROM {table_name} 
                        GROUP BY {pk_columns} 
                        HAVING COUNT(*) > 1
                    ) AS duplicates
                """,
                "operator": "equals",
                "expected_value": 0
            })

        # Add all the other validation rules with proper indentation...

        return validations

    except Exception as e:
        logging.error(f"Error in get_default_validations: {str(e)}")
        logging.error(f"Connection string: {connection_string}, table: {table_name}")
        raise


def add_default_validations(validation_manager, connection_string: str, table_name: str) -> dict:
    """
    Add default validations for a table to the validation manager, avoiding duplicates

    Args:
        validation_manager: Instance of ValidationManager
        connection_string: Database connection string
        table_name: Name of the table to add validations for

    Returns:
        Dictionary with count of rules added and skipped
    """
    # Get existing rules first
    existing_rules = validation_manager.get_rules(table_name)
    existing_rule_names = {rule['rule_name'] for rule in existing_rules}

    # Generate potential new validations
    validations = get_default_validations(connection_string, table_name)

    count_added = 0
    count_skipped = 0

    for validation in validations:
        try:
            # Skip if rule with same name already exists
            if validation['name'] in existing_rule_names:
                count_skipped += 1
                continue

            validation_manager.add_rule(table_name, validation)
            count_added += 1
        except Exception as e:
            print(f"Failed to add validation rule {validation['name']}: {str(e)}")

    return {
        "added": count_added,
        "skipped": count_skipped,
        "total": count_added + count_skipped
    }