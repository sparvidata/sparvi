from typing import List, Dict, Any
import sqlalchemy as sa
from sqlalchemy import inspect


def get_default_validations(connection_string: str, table_name: str) -> List[Dict[str, Any]]:
    """
    Generate default validation rules that can be applied to any table

    Args:
        connection_string: Database connection string
        table_name: Name of the table to generate validations for

    Returns:
        List of validation rule dictionaries
    """
    # Connect to database and get table metadata
    engine = sa.create_engine(connection_string)
    inspector = inspect(engine)

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

    # 3. NULL checks for non-nullable columns
    for column in columns:
        if not column['nullable'] and column['name'] not in primary_keys:
            validations.append({
                "name": f"check_{column['name']}_not_null",
                "description": f"Ensure {column['name']} has no NULL values",
                "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} IS NULL",
                "operator": "equals",
                "expected_value": 0
            })

    # 4. Check for negative values in numeric columns (if not explicitly allowed)
    for column in columns:
        col_type = str(column['type']).lower()
        if (
                'int' in col_type or 'float' in col_type or 'numeric' in col_type or 'double' in col_type or 'decimal' in col_type) and 'unsigned' not in col_type:
            # Skip columns likely to allow negative values based on common naming patterns
            if not any(neg_term in column['name'].lower() for neg_term in
                       ['balance', 'difference', 'delta', 'change', 'temperature', 'coordinate']):
                validations.append({
                    "name": f"check_{column['name']}_positive",
                    "description": f"Ensure {column['name']} has no negative values",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} < 0",
                    "operator": "equals",
                    "expected_value": 0
                })

    # 5. Check for valid date ranges in date/datetime columns
    for column in columns:
        col_type = str(column['type']).lower()
        if 'date' in col_type or 'time' in col_type:
            # Validate no future dates for columns that typically shouldn't have future dates
            if any(date_term in column['name'].lower() for date_term in
                   ['birth', 'created', 'start', 'registered', 'joined']):
                validations.append({
                    "name": f"check_{column['name']}_not_future",
                    "description": f"Ensure {column['name']} contains no future dates",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} > CURRENT_DATE",
                    "operator": "equals",
                    "expected_value": 0
                })

    # 6. Check for string length constraints in varchar/text columns
    for column in columns:
        col_type = str(column['type']).lower()
        if 'varchar' in col_type or 'char' in col_type or 'text' in col_type:
            # If it's a defined length VARCHAR
            if hasattr(column['type'], 'length') and column['type'].length is not None:
                validations.append({
                    "name": f"check_{column['name']}_max_length",
                    "description": f"Ensure {column['name']} does not exceed max length ({column['type'].length})",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE LENGTH({column['name']}) > {column['type'].length}",
                    "operator": "equals",
                    "expected_value": 0
                })

            # Check for empty strings in required string columns
            if not column['nullable']:
                validations.append({
                    "name": f"check_{column['name']}_not_empty_string",
                    "description": f"Ensure {column['name']} has no empty strings",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} = ''",
                    "operator": "equals",
                    "expected_value": 0
                })

    # 7. Check for outliers in numeric columns (using standard deviation)
    for column in columns:
        col_type = str(column['type']).lower()
        if (
                'int' in col_type or 'float' in col_type or 'numeric' in col_type or 'double' in col_type or 'decimal' in col_type):
            validations.append({
                "name": f"check_{column['name']}_outliers",
                "description": f"Check for extreme outliers in {column['name']} (> 3 std deviations)",
                "query": f"""
                    WITH stats AS (
                        SELECT 
                            AVG({column['name']}) as avg_val,
                            STDDEV({column['name']}) as stddev_val
                        FROM {table_name}
                        WHERE {column['name']} IS NOT NULL
                    )
                    SELECT COUNT(*) FROM {table_name}, stats
                    WHERE {column['name']} > stats.avg_val + 3 * stats.stddev_val
                    OR {column['name']} < stats.avg_val - 3 * stats.stddev_val
                """,
                "operator": "equals",
                "expected_value": 0
            })

    # 8. Check for reasonable row count (based on name patterns suggesting it's a reference table)
    if any(ref_term in table_name.lower() for ref_term in ['ref', 'type', 'status', 'category', 'lookup']):
        # Reference tables should have a reasonable number of rows
        validations.append({
            "name": f"check_{table_name}_ref_table_size",
            "description": f"Ensure reference table {table_name} has a reasonable number of rows",
            "query": f"SELECT COUNT(*) FROM {table_name}",
            "operator": "less_than",
            "expected_value": 1000  # Arbitrary limit for reference tables
        })

    return validations


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