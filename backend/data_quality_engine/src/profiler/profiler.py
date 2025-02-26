import json
import datetime
from sqlalchemy import create_engine, inspect, text
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np


def profile_table(connection_str: str, table: str, historical_data: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Profile a table for data completeness, uniqueness, distribution, and numeric stats.
    Includes anomaly detection when historical data is provided.
    """
    engine = create_engine(connection_str)
    inspector = inspect(engine)
    columns = inspector.get_columns(table)

    with engine.connect() as conn:
        # Build queries for profiling
        column_names = [col["name"] for col in columns]

        # Data Completeness: Count rows, nulls, blanks
        null_counts = ', '.join(
            [f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls' for col in column_names]
        )
        blank_counts = ', '.join(
            [f'SUM(CASE WHEN TRIM({col["name"]}) = \'\' THEN 1 ELSE 0 END) AS {col["name"]}_blanks'
             for col in columns if str(col["type"]).startswith('VARCHAR') or str(col["type"]).startswith('TEXT')]
        )

        # Uniqueness: Count distinct values and duplicates
        distinct_counts = ', '.join(
            [f'COUNT(DISTINCT {col["name"]}) AS {col["name"]}_distinct' for col in columns]
        )

        # Duplicate check - count records that appear more than once
        dup_check = f"""
        SELECT COUNT(*) AS duplicate_rows FROM (
            SELECT COUNT(*) as count FROM {table} GROUP BY {', '.join(column_names)} HAVING count > 1
        ) AS duplicates
        """

        # Numeric statistics
        numeric_cols = [col["name"] for col in columns if
                        str(col["type"]).startswith("INT") or
                        str(col["type"]).startswith("FLOAT") or
                        str(col["type"]).startswith("NUMERIC")]
        numeric_stats = ', '.join([
            f'''
            MIN({col}) AS {col}_min, 
            MAX({col}) AS {col}_max, 
            AVG({col}) AS {col}_avg, 
            SUM({col}) AS {col}_sum, 
            STDDEV({col}) AS {col}_stdev,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}) AS {col}_q1,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col}) AS {col}_median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS {col}_q3
            '''
            for col in numeric_cols
        ]) if numeric_cols else ''

        # Text Lengths
        text_cols = [col["name"] for col in columns if
                     str(col["type"]).startswith("VARCHAR") or str(col["type"]).startswith("TEXT")]
        text_length_stats = ', '.join([
            f'MIN(LENGTH({col})) AS {col}_min_length, MAX(LENGTH({col})) AS {col}_max_length, AVG(LENGTH({col})) AS {col}_avg_length'
            for col in text_cols
        ]) if text_cols else ''

        # Pattern recognition for text columns
        pattern_checks = []
        for col in text_cols:
            pattern_checks.extend([
                f'SUM(CASE WHEN {col} ~ \'^[0-9]+$\' THEN 1 ELSE 0 END) AS {col}_numeric_pattern',
                f'SUM(CASE WHEN {col} ~ \'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$\' THEN 1 ELSE 0 END) AS {col}_email_pattern',
                f'SUM(CASE WHEN {col} ~ \'^\\d{{4}}-\\d{{2}}-\\d{{2}}$\' THEN 1 ELSE 0 END) AS {col}_date_pattern'
            ])
        pattern_checks_str = ', '.join(pattern_checks) if pattern_checks else ''

        # Most Frequent Values
        freq_queries = []
        for col in column_names:
            freq_queries.append(f"""
            (SELECT '{col}' AS column_name, {col} AS value, COUNT(*) AS frequency,
             (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table})) AS percentage 
             FROM {table}
             GROUP BY {col} ORDER BY frequency DESC LIMIT 5)
            """)
        freq_query = " UNION ALL ".join(freq_queries)

        # Top 100 Data Samples
        sample_query = f"SELECT * FROM {table} LIMIT 100"

        # Date range check for date columns (if any)
        date_cols = [col["name"] for col in columns if
                     str(col["type"]).startswith("DATE") or
                     str(col["type"]).startswith("TIMESTAMP")]
        date_range_checks = ', '.join([
            f'MIN({col}) AS {col}_min_date, MAX({col}) AS {col}_max_date, COUNT(DISTINCT {col}) AS {col}_distinct_dates'
            for col in date_cols
        ]) if date_cols else ''

        # Build a single query for basic profiling metrics
        query = f"""
        SELECT 
            COUNT(*) AS row_count, 
            {null_counts}, 
            {blank_counts}, 
            {distinct_counts}, 
            {numeric_stats}, 
            {text_length_stats},
            {pattern_checks_str},
            {date_range_checks}
        FROM {table}
        """

        # Execute queries
        result = conn.execute(text(query)).fetchone()
        duplicates_result = conn.execute(text(dup_check)).fetchone()
        frequent_values = conn.execute(text(freq_query)).fetchall()
        samples = conn.execute(text(sample_query)).fetchall()

        # Get outliers for numeric columns
        outliers = {}
        for col in numeric_cols:
            outlier_query = f"""
            WITH stats AS (
                SELECT 
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}) AS q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS q3
                FROM {table}
            )
            SELECT {col} AS value FROM {table}, stats
            WHERE {col} < stats.q1 - 1.5 * (stats.q3 - stats.q1)
               OR {col} > stats.q3 + 1.5 * (stats.q3 - stats.q1)
            LIMIT 10
            """
            outlier_results = conn.execute(text(outlier_query)).fetchall()
            if outlier_results:
                outliers[col] = [row[0] for row in outlier_results]

    # Construct the profile dictionary
    profile = {
        "table": table,
        "timestamp": datetime.datetime.now().isoformat(),
        "row_count": result[0],
        "duplicate_count": duplicates_result[0] if duplicates_result else 0,
        "completeness": {
            col: {
                "nulls": result[i + 1],
                "null_percentage": round((result[i + 1] / result[0]) * 100, 2) if result[0] > 0 else 0,
                "blanks": result[i + len(column_names) + 1] if i < len(text_cols) else 0,
                "distinct_count": result[i + 2 * len(column_names) + 1],
                "distinct_percentage": round((result[i + 2 * len(column_names) + 1] / result[0]) * 100, 2) if result[
                                                                                                                  0] > 0 else 0
            }
            for i, col in enumerate(column_names)
        },
        "numeric_stats": {},
        "text_patterns": {},
        "text_length_stats": {},
        "date_stats": {},
        "frequent_values": {
            row[0]: {
                "value": row[1],
                "frequency": row[2],
                "percentage": round(row[3], 2)
            } for row in frequent_values
        },
        "outliers": outliers,
        "samples": [dict(zip(column_names, row)) for row in samples],
    }

    # Populate numeric stats
    idx_offset = 3 * len(column_names) + 1
    for i, col in enumerate(numeric_cols):
        profile["numeric_stats"][col] = {
            "min": result[idx_offset + i * 8],
            "max": result[idx_offset + i * 8 + 1],
            "avg": result[idx_offset + i * 8 + 2],
            "sum": result[idx_offset + i * 8 + 3],
            "stdev": result[idx_offset + i * 8 + 4],
            "q1": result[idx_offset + i * 8 + 5],
            "median": result[idx_offset + i * 8 + 6],
            "q3": result[idx_offset + i * 8 + 7]
        }

    # Populate text length stats
    idx_offset = 3 * len(column_names) + 1 + (8 * len(numeric_cols))
    for i, col in enumerate(text_cols):
        profile["text_length_stats"][col] = {
            "min_length": result[idx_offset + i * 3],
            "max_length": result[idx_offset + i * 3 + 1],
            "avg_length": result[idx_offset + i * 3 + 2]
        }

    # Populate text patterns
    idx_offset = 3 * len(column_names) + 1 + (8 * len(numeric_cols)) + (3 * len(text_cols))
    for i, col in enumerate(text_cols):
        try:
            profile["text_patterns"][col] = {
                "numeric_pattern_count": result[idx_offset + i * 3] if idx_offset + i * 3 < len(result) else 0,
                "email_pattern_count": result[idx_offset + i * 3 + 1] if idx_offset + i * 3 + 1 < len(result) else 0,
                "date_pattern_count": result[idx_offset + i * 3 + 2] if idx_offset + i * 3 + 2 < len(result) else 0
            }
        except IndexError:
            # Fall back to default values if the index is out of range
            profile["text_patterns"][col] = {
                "numeric_pattern_count": 0,
                "email_pattern_count": 0,
                "date_pattern_count": 0
            }

    # Populate date stats
    idx_offset = 3 * len(column_names) + 1 + (8 * len(numeric_cols)) + (3 * len(text_cols)) + (3 * len(text_cols))
    for i, col in enumerate(date_cols):
        profile["date_stats"][col] = {
            "min_date": result[idx_offset + i * 3],
            "max_date": result[idx_offset + i * 3 + 1],
            "distinct_count": result[idx_offset + i * 3 + 2]
        }

    # Compare with historical data to detect anomalies
    anomalies = []
    if historical_data:
        # Check for row count anomalies
        if abs(profile["row_count"] - historical_data["row_count"]) / historical_data["row_count"] > 0.1:
            anomalies.append({
                "type": "row_count",
                "description": f"Row count changed by more than 10%: {historical_data['row_count']} → {profile['row_count']}",
                "severity": "high"
            })

        # Check for completeness anomalies
        for col in profile["completeness"]:
            if col in historical_data["completeness"]:
                hist_null_pct = historical_data["completeness"][col]["null_percentage"]
                curr_null_pct = profile["completeness"][col]["null_percentage"]

                if abs(curr_null_pct - hist_null_pct) > 5:
                    anomalies.append({
                        "type": "null_rate",
                        "column": col,
                        "description": f"Null rate for {col} changed significantly: {hist_null_pct}% → {curr_null_pct}%",
                        "severity": "medium"
                    })

        # Check for numeric anomalies
        for col in profile["numeric_stats"]:
            if col in historical_data.get("numeric_stats", {}):
                hist_avg = historical_data["numeric_stats"][col]["avg"]
                curr_avg = profile["numeric_stats"][col]["avg"]

                if hist_avg and abs((curr_avg - hist_avg) / hist_avg) > 0.2:
                    anomalies.append({
                        "type": "average_value",
                        "column": col,
                        "description": f"Average value of {col} changed by more than 20%: {hist_avg} → {curr_avg}",
                        "severity": "medium"
                    })

    # Add anomalies to profile
    profile["anomalies"] = anomalies

    # Prepare trend data structure (will be populated from historical runs)
    profile["trends"] = {
        "row_counts": [],
        "null_rates": {},
        "duplicates": []
    }

    return profile


def detect_schema_shifts(current_profile: Dict, historical_profile: Dict) -> List[Dict]:
    """
    Detect schema changes between current and historical profiles.
    Returns a list of detected shifts with descriptions.
    """
    shifts = []

    # Get current and historical columns
    current_columns = set(current_profile["completeness"].keys())
    historical_columns = set(historical_profile["completeness"].keys())

    # Check for added columns
    added_columns = current_columns - historical_columns
    for col in added_columns:
        shifts.append({
            "type": "column_added",
            "column": col,
            "description": f"New column added: {col}",
            "severity": "info"
        })

    # Check for removed columns
    removed_columns = historical_columns - current_columns
    for col in removed_columns:
        shifts.append({
            "type": "column_removed",
            "column": col,
            "description": f"Column removed: {col}",
            "severity": "high"
        })

    # Check for type changes (would require more detailed schema info)

    return shifts


def run_custom_validations(connection_str: str, validation_rules: List[Dict]) -> List[Dict]:
    """
    Run custom validation rules defined by the user.
    Each rule should have a name, query, and expected result.
    """
    engine = create_engine(connection_str)
    results = []

    for rule in validation_rules:
        try:
            with engine.connect() as conn:
                query_result = conn.execute(text(rule["query"])).fetchone()
                actual_value = query_result[0] if query_result else None

                is_valid = False
                if rule["operator"] == "equals":
                    is_valid = actual_value == rule["expected_value"]
                elif rule["operator"] == "greater_than":
                    is_valid = actual_value > rule["expected_value"]
                elif rule["operator"] == "less_than":
                    is_valid = actual_value < rule["expected_value"]
                elif rule["operator"] == "between":
                    is_valid = rule["expected_value"][0] <= actual_value <= rule["expected_value"][1]

                results.append({
                    "rule_name": rule["name"],
                    "is_valid": is_valid,
                    "actual_value": actual_value,
                    "expected_value": rule["expected_value"],
                    "description": rule.get("description", "")
                })
        except Exception as e:
            results.append({
                "rule_name": rule["name"],
                "is_valid": False,
                "error": str(e),
                "description": rule.get("description", "")
            })

    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python profiler.py <connection_string> <table_name>")
        sys.exit(1)

    connection_string = sys.argv[1]
    table_name = sys.argv[2]

    try:
        profile = profile_table(connection_string, table_name)
        print(json.dumps(profile, indent=4))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)