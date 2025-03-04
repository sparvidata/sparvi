"""
Default validation rules generator for Sparvi
"""
from typing import List, Dict, Any, Optional, Union
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
    foreign_keys = []
    try:
        for fk in inspector.get_foreign_keys(table_name):
            if 'constrained_columns' in fk and fk['constrained_columns']:
                foreign_keys.extend(fk['constrained_columns'])
    except Exception:
        # Some databases might not support foreign key inspection
        pass

    # Initialize validation rules list
    validations = []

    # =====================
    # TABLE-LEVEL VALIDATIONS
    # =====================

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

    # 3. Row growth check - detect sudden large changes in row count
    validations.append({
        "name": f"check_{table_name}_row_growth",
        "description": f"Detect unusual growth in {table_name} row count (>20% change)",
        "query": f"""
            WITH current_count AS (
                SELECT COUNT(*) as count FROM {table_name}
            ),
            prev_count AS (
                -- Replace with your historical count logic or reference table
                SELECT 
                    CASE 
                        WHEN COUNT(*) = 0 THEN NULL 
                        ELSE COUNT(*) 
                    END as count 
                FROM {table_name}
            )
            SELECT 
                CASE
                    WHEN prev_count.count IS NULL THEN 0
                    WHEN ABS(current_count.count - prev_count.count) > prev_count.count * 0.2 THEN 1
                    ELSE 0
                END
            FROM current_count, prev_count
        """,
        "operator": "equals",
        "expected_value": 0
    })

    # 4. Duplicate detection for non-PK columns that should be unique
    # Check columns with names suggesting uniqueness
    unique_name_patterns = ['id', 'code', 'number', 'uuid', 'guid', 'key', 'hash', 'identifier']
    for column in columns:
        # Skip primary keys and foreign keys as they're already checked
        if column['name'] in primary_keys or column['name'] in foreign_keys:
            continue

        # Check if column name suggests uniqueness
        if any(unique_pattern in column['name'].lower() for unique_pattern in unique_name_patterns):
            validations.append({
                "name": f"check_{column['name']}_unique",
                "description": f"Check that {column['name']} values are unique",
                "query": f"""
                    SELECT COUNT(*) FROM (
                        SELECT {column['name']}, COUNT(*) as count 
                        FROM {table_name} 
                        WHERE {column['name']} IS NOT NULL
                        GROUP BY {column['name']} 
                        HAVING COUNT(*) > 1
                    ) AS duplicates
                """,
                "operator": "equals",
                "expected_value": 0
            })

    # =====================
    # COLUMN-LEVEL VALIDATIONS
    # =====================

    # 5. NULL checks for non-nullable columns
    for column in columns:
        if not column['nullable'] and column['name'] not in primary_keys:
            validations.append({
                "name": f"check_{column['name']}_not_null",
                "description": f"Ensure {column['name']} has no NULL values",
                "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} IS NULL",
                "operator": "equals",
                "expected_value": 0
            })

    # 6. Check for negative values in numeric columns (if not explicitly allowed)
    for column in columns:
        col_type = str(column['type']).lower()
        if ('int' in col_type or 'float' in col_type or 'numeric' in col_type or
            'double' in col_type or 'decimal' in col_type) and 'unsigned' not in col_type:

            # Skip columns likely to allow negative values based on common naming patterns
            negative_allowed_patterns = [
                'balance', 'difference', 'delta', 'change', 'temperature',
                'coordinate', 'adjustment', 'net', 'profit_loss', 'margin'
            ]
            if not any(neg_term in column['name'].lower() for neg_term in negative_allowed_patterns):
                validations.append({
                    "name": f"check_{column['name']}_positive",
                    "description": f"Ensure {column['name']} has no negative values",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} < 0",
                    "operator": "equals",
                    "expected_value": 0
                })

    # 7. Check for zero values in columns that typically shouldn't be zero
    for column in columns:
        col_type = str(column['type']).lower()
        if ('int' in col_type or 'float' in col_type or 'numeric' in col_type or
                'double' in col_type or 'decimal' in col_type):

            non_zero_patterns = [
                'price', 'amount', 'total', 'cost', 'rate', 'fee', 'tax',
                'revenue', 'salary', 'income', 'expense'
            ]
            if any(term in column['name'].lower() for term in non_zero_patterns):
                validations.append({
                    "name": f"check_{column['name']}_not_zero",
                    "description": f"Ensure {column['name']} has no zero values",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} = 0",
                    "operator": "equals",
                    "expected_value": 0
                })

    # 8. Check for valid date ranges in date/datetime columns
    for column in columns:
        col_type = str(column['type']).lower()
        if 'date' in col_type or 'time' in col_type:
            # Validate no future dates for columns that typically shouldn't have future dates
            past_date_patterns = [
                'birth', 'created', 'start', 'registered', 'joined', 'purchase',
                'transaction', 'order', 'payment', 'issued', 'shipped', 'received'
            ]
            if any(date_term in column['name'].lower() for date_term in past_date_patterns):
                validations.append({
                    "name": f"check_{column['name']}_not_future",
                    "description": f"Ensure {column['name']} contains no future dates",
                    "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} > CURRENT_DATE",
                    "operator": "equals",
                    "expected_value": 0
                })

            # Check for no unreasonably old dates (before 1970)
            validations.append({
                "name": f"check_{column['name']}_reasonable_past",
                "description": f"Ensure {column['name']} contains no unreasonably old dates",
                "query": f"SELECT COUNT(*) FROM {table_name} WHERE {column['name']} < '1970-01-01'",
                "operator": "equals",
                "expected_value": 0
            })

            # For columns that should be in the past (end dates)
            if any(term in column['name'].lower() for term in
                   ['end', 'finish', 'completed', 'closed', 'expiry', 'expiration']):
                validations.append({
                    "name": f"check_{column['name']}_end_date_order",
                    "description": f"Ensure {column['name']} occurs after any start date (if applicable)",
                    "query": f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {column['name']} IS NOT NULL 
                        AND {guess_start_date_column(column['name'], columns)} IS NOT NULL
                        AND {column['name']} < {guess_start_date_column(column['name'], columns)}
                    """,
                    "operator": "equals",
                    "expected_value": 0
                })

    # 9. Check for string length constraints in varchar/text columns
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

            # Check for proper formatting of common data types
            if 'email' in column['name'].lower():
                validations.append({
                    "name": f"check_{column['name']}_valid_email",
                    "description": f"Ensure {column['name']} contains valid email format",
                    "query": f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {column['name']} IS NOT NULL 
                        AND {column['name']} NOT LIKE '%@%.%'
                    """,
                    "operator": "equals",
                    "expected_value": 0
                })

            if 'phone' in column['name'].lower() or 'mobile' in column['name'].lower():
                validations.append({
                    "name": f"check_{column['name']}_valid_phone",
                    "description": f"Ensure {column['name']} contains valid phone number format",
                    "query": f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {column['name']} IS NOT NULL 
                        AND {column['name']} NOT SIMILAR TO '(\\+)?[0-9][0-9 ()-]+'
                    """,
                    "operator": "equals",
                    "expected_value": 0
                })

            if 'zip' in column['name'].lower() or 'postal' in column['name'].lower():
                validations.append({
                    "name": f"check_{column['name']}_valid_postal",
                    "description": f"Ensure {column['name']} follows postal/zip code patterns",
                    "query": f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {column['name']} IS NOT NULL 
                        AND LENGTH(TRIM({column['name']})) < 3
                    """,
                    "operator": "equals",
                    "expected_value": 0
                })

    # 10. Check for outliers in numeric columns (using standard deviation)
    for column in columns:
        col_type = str(column['type']).lower()
        if ('int' in col_type or 'float' in col_type or 'numeric' in col_type or
                'double' in col_type or 'decimal' in col_type):
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
                "operator": "less_than",
                "expected_value": get_outlier_threshold(table_name)
            })

    # 11. Check for reasonable row count for reference tables
    if any(ref_term in table_name.lower() for ref_term in ['ref', 'type', 'status', 'category', 'lookup']):
        # Reference tables should have a reasonable number of rows
        validations.append({
            "name": f"check_{table_name}_ref_table_size",
            "description": f"Ensure reference table {table_name} has a reasonable number of rows",
            "query": f"SELECT COUNT(*) FROM {table_name}",
            "operator": "less_than",
            "expected_value": 1000  # Arbitrary limit for reference tables
        })

    # 12. Check for acceptable NULL rate in columns
    for column in columns:
        # Skip primary keys and required columns
        if column['name'] in primary_keys or not column['nullable']:
            continue

        # For important non-PK columns, check if NULL rate is reasonable
        important_column_patterns = [
            'name', 'description', 'address', 'city', 'state', 'country', 'postal', 'zip',
            'email', 'phone', 'status', 'type', 'category', 'price', 'cost', 'amount'
        ]
        if any(pattern in column['name'].lower() for pattern in important_column_patterns):
            validations.append({
                "name": f"check_{column['name']}_null_rate",
                "description": f"Ensure {column['name']} null rate is below acceptable threshold",
                "query": f"""
                    SELECT (COUNT(*) FILTER (WHERE {column['name']} IS NULL) * 100.0 / NULLIF(COUNT(*), 0)) 
                    FROM {table_name}
                """,
                "operator": "less_than",
                "expected_value": 25.0  # Max 25% NULL rate for important columns
            })

    # 13. Check for distribution of categorical columns
    for column in columns:
        col_type = str(column['type']).lower()
        categorical_column_patterns = [
            'status', 'type', 'category', 'level', 'tier', 'class', 'grade',
            'priority', 'severity', 'state', 'region', 'stage', 'gender'
        ]

        # For string columns with categorical-like names
        if ('varchar' in col_type or 'char' in col_type or 'text' in col_type) and \
                any(pattern in column['name'].lower() for pattern in categorical_column_patterns):
            validations.append({
                "name": f"check_{column['name']}_distribution",
                "description": f"Ensure {column['name']} has a reasonable value distribution",
                "query": f"""
                    WITH val_counts AS (
                        SELECT {column['name']}, COUNT(*) as count,
                        (COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM {table_name}), 0)) as pct
                        FROM {table_name}
                        WHERE {column['name']} IS NOT NULL
                        GROUP BY {column['name']}
                    )
                    SELECT COUNT(*) FROM val_counts
                    WHERE pct > 95.0
                """,
                "operator": "equals",
                "expected_value": 0  # No single value should represent >95% of all values
            })

    # 14. Check for reasonable distinct count in reference columns
    for column in columns:
        if column['name'] in foreign_keys:
            validations.append({
                "name": f"check_{column['name']}_ref_distribution",
                "description": f"Ensure {column['name']} references a reasonable number of distinct values",
                "query": f"""
                    SELECT CASE
                      WHEN (SELECT COUNT(DISTINCT {column['name']}) FROM {table_name} WHERE {column['name']} IS NOT NULL) = 1 
                      THEN 1 ELSE 0 END
                """,
                "operator": "equals",
                "expected_value": 0  # At least 2 distinct values should be referenced
            })

    # 15. For timestamp columns with an 'updated' pattern, check that they're not older than created timestamps
    timestamp_columns = [col['name'] for col in columns if
                         'time' in str(col['type']).lower() or 'date' in str(col['type']).lower()]

    updated_columns = [col for col in timestamp_columns if any(term in col.lower() for term in
                                                               ['updated', 'modified', 'edited', 'changed'])]
    created_columns = [col for col in timestamp_columns if any(term in col.lower() for term in
                                                               ['created', 'inserted', 'added'])]

    # If we have both updated and created timestamps, add a validation
    for updated_col in updated_columns:
        for created_col in created_columns:
            validations.append({
                "name": f"check_{updated_col}_after_{created_col}",
                "description": f"Ensure {updated_col} is not before {created_col}",
                "query": f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE {updated_col} IS NOT NULL 
                    AND {created_col} IS NOT NULL
                    AND {updated_col} < {created_col}
                """,
                "operator": "equals",
                "expected_value": 0
            })

    return validations


def guess_start_date_column(end_date_column, columns):
    """
    Try to guess the corresponding start date column for an end date
    """
    # Convert end terms to start terms
    start_term_map = {
        'end': 'start', 'finish': 'start', 'completed': 'created',
        'closed': 'opened', 'expiry': 'issue', 'expiration': 'issue'
    }

    # Find which end term is in the column name
    found_term = next((term for term in start_term_map.keys() if term in end_date_column.lower()), None)

    if found_term:
        start_term = start_term_map[found_term]
        # Replace end term with start term in column name
        potential_start_column = end_date_column.lower().replace(found_term, start_term)

        # Check if this column exists
        for column in columns:
            if column['name'].lower() == potential_start_column:
                return column['name']

    # Default fallback - find any column with 'start', 'created', etc.
    start_indicators = ['start', 'created', 'opened', 'issue', 'begin']
    date_indicators = ['date', 'time', 'timestamp', 'dt']

    for column in columns:
        col_name = column['name'].lower()
        if any(start in col_name for start in start_indicators) and \
                any(date in col_name for date in date_indicators):
            return column['name']

    # Last resort - just return the end date column itself
    return end_date_column


def get_outlier_threshold(table_name):
    """
    Determine a reasonable threshold for outliers based on table name/size
    """
    # For tables likely to have many rows
    large_table_indicators = ['fact', 'transaction', 'event', 'log', 'history', 'audit', 'detail']

    if any(indicator in table_name.lower() for indicator in large_table_indicators):
        return 50  # Allow up to 50 outliers in large tables

    # For medium tables
    medium_table_indicators = ['order', 'customer', 'user', 'account', 'product', 'item']
    if any(indicator in table_name.lower() for indicator in medium_table_indicators):
        return 20  # Allow up to 20 outliers in medium tables

    # For small/reference tables
    return 5  # Allow only 5 outliers in small tables


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