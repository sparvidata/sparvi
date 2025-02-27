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
    print(f"DEBUG: Loaded profiler.py from {__file__}")
    print("DEBUG: profile_table function called")

    engine = create_engine(connection_str)
    inspector = inspect(engine)
    columns = inspector.get_columns(table)
    column_names = [col["name"] for col in columns]

    # Categorize columns
    numeric_cols = [col["name"] for col in columns if
                    str(col["type"]).startswith("INT") or
                    str(col["type"]).startswith("FLOAT") or
                    str(col["type"]).startswith("NUMERIC")]

    text_cols = [col["name"] for col in columns if
                 str(col["type"]).startswith("VARCHAR") or str(col["type"]).startswith("TEXT")]

    date_cols = [col["name"] for col in columns if
                 str(col["type"]).startswith("DATE") or
                 str(col["type"]).startswith("TIMESTAMP")]

    with engine.connect() as conn:
        # Create separate queries for basic metrics
        row_count_query = f"SELECT COUNT(*) FROM {table}"
        null_counts_query = f"SELECT {', '.join([f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls' for col in column_names])} FROM {table}"
        distinct_counts_query = f"SELECT {', '.join([f'COUNT(DISTINCT {col}) AS {col}_distinct' for col in column_names])} FROM {table}"

        # Execute separate queries for clarity and reliability
        row_count = conn.execute(text(row_count_query)).fetchone()[0]
        null_counts_result = conn.execute(text(null_counts_query)).fetchone()
        distinct_counts_result = conn.execute(text(distinct_counts_query)).fetchone()

        # Duplicate check
        dup_check = f"""
        SELECT COUNT(*) AS duplicate_rows FROM (
            SELECT COUNT(*) as count FROM {table} GROUP BY {', '.join(column_names)} HAVING count > 1
        ) AS duplicates
        """
        duplicates_result = conn.execute(text(dup_check)).fetchone()
        duplicate_count = duplicates_result[0] if duplicates_result else 0

        # Numeric statistics
        numeric_stats = {}
        for col in numeric_cols:
            stats_query = f"""
            SELECT 
                MIN({col}) AS min_val, 
                MAX({col}) AS max_val, 
                AVG({col}) AS avg_val, 
                SUM({col}) AS sum_val, 
                STDDEV({col}) AS stdev_val,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}) AS q1_val,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col}) AS median_val,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS q3_val
            FROM {table}
            """
            try:
                stats_result = conn.execute(text(stats_query)).fetchone()
                numeric_stats[col] = {
                    "min": stats_result[0],
                    "max": stats_result[1],
                    "avg": stats_result[2],
                    "sum": stats_result[3],
                    "stdev": stats_result[4],
                    "q1": stats_result[5],
                    "median": stats_result[6],
                    "q3": stats_result[7]
                }
            except Exception as e:
                print(f"Error getting numeric stats for {col}: {str(e)}")
                numeric_stats[col] = {
                    "min": None, "max": None, "avg": None, "sum": None,
                    "stdev": None, "q1": None, "median": None, "q3": None
                }

        # Text Lengths
        text_length_stats = {}
        for col in text_cols:
            try:
                length_query = f"""
                SELECT MIN(LENGTH({col})) AS min_length, 
                       MAX(LENGTH({col})) AS max_length, 
                       AVG(LENGTH({col})) AS avg_length
                FROM {table}
                """
                length_result = conn.execute(text(length_query)).fetchone()
                text_length_stats[col] = {
                    "min_length": length_result[0],
                    "max_length": length_result[1],
                    "avg_length": length_result[2]
                }
            except Exception as e:
                print(f"Error getting text length stats for {col}: {str(e)}")
                text_length_stats[col] = {
                    "min_length": None, "max_length": None, "avg_length": None
                }

        # Pattern recognition for text columns
        text_patterns = {}
        for col in text_cols:
            try:
                pattern_query = f"""
                SELECT
                    SUM(CASE WHEN {col} ~ '^[0-9]+$' THEN 1 ELSE 0 END) AS numeric_pattern,
                    SUM(CASE WHEN {col} ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$' THEN 1 ELSE 0 END) AS email_pattern,
                    SUM(CASE WHEN {col} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN 1 ELSE 0 END) AS date_pattern
                FROM {table}
                """
                pattern_result = conn.execute(text(pattern_query)).fetchone()
                text_patterns[col] = {
                    "numeric_pattern_count": pattern_result[0] if pattern_result[0] else 0,
                    "email_pattern_count": pattern_result[1] if pattern_result[1] else 0,
                    "date_pattern_count": pattern_result[2] if pattern_result[2] else 0
                }
            except Exception as e:
                print(f"Error getting text patterns for {col}: {str(e)}")
                text_patterns[col] = {
                    "numeric_pattern_count": 0,
                    "email_pattern_count": 0,
                    "date_pattern_count": 0
                }

        # Date range check for date columns
        date_stats = {}
        for col in date_cols:
            try:
                date_query = f"""
                SELECT MIN({col}) AS min_date, 
                       MAX({col}) AS max_date, 
                       COUNT(DISTINCT {col}) AS distinct_dates
                FROM {table}
                """
                date_result = conn.execute(text(date_query)).fetchone()
                date_stats[col] = {
                    "min_date": date_result[0],
                    "max_date": date_result[1],
                    "distinct_count": date_result[2]
                }
            except Exception as e:
                print(f"Error getting date stats for {col}: {str(e)}")
                date_stats[col] = {
                    "min_date": None, "max_date": None, "distinct_count": 0
                }

        # Most Frequent Values
        frequent_values = []
        for col in column_names:
            try:
                # Handle each column separately to avoid type conversion issues
                if col in date_cols:
                    # Cast dates to strings for consistent handling
                    query = f"""
                    SELECT '{col}' AS column_name, 
                           CAST({col} AS VARCHAR) AS value, 
                           COUNT(*) AS frequency,
                           (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table})) AS percentage 
                    FROM {table}
                    GROUP BY {col} 
                    ORDER BY frequency DESC 
                    LIMIT 5
                    """
                else:
                    query = f"""
                    SELECT '{col}' AS column_name, 
                           {col} AS value, 
                           COUNT(*) AS frequency,
                           (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table})) AS percentage 
                    FROM {table}
                    GROUP BY {col} 
                    ORDER BY frequency DESC 
                    LIMIT 5
                    """

                col_values = conn.execute(text(query)).fetchall()
                frequent_values.extend(col_values)
            except Exception as e:
                print(f"Error getting frequent values for {col}: {str(e)}")

        # Sample Data
        sample_query = f"SELECT * FROM {table} LIMIT 100"
        samples = conn.execute(text(sample_query)).fetchall()

        # Get outliers for numeric columns
        outliers = {}
        for col in numeric_cols:
            try:
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
            except Exception as e:
                print(f"Error getting outliers for {col}: {str(e)}")

    # Process null counts and distinct counts from results
    null_counts = {}
    distinct_counts = {}

    for i, col in enumerate(column_names):
        null_counts[col] = null_counts_result[i] if i < len(null_counts_result) else 0
        distinct_counts[col] = distinct_counts_result[i] if i < len(distinct_counts_result) else 0

    # Construct the profile dictionary
    profile = {
        "table": table,
        "timestamp": datetime.datetime.now().isoformat(),
        "row_count": row_count,
        "duplicate_count": duplicate_count,
        "completeness": {
            col: {
                "nulls": null_counts[col],
                "null_percentage": round((null_counts[col] / row_count) * 100, 2) if row_count > 0 else 0,
                "distinct_count": distinct_counts[col],
                "distinct_percentage": round((distinct_counts[col] / row_count) * 100, 2) if row_count > 0 else 0
            }
            for col in column_names
        },
        "numeric_stats": numeric_stats,
        "text_patterns": text_patterns,
        "text_length_stats": text_length_stats,
        "date_stats": date_stats,
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

    # Compare with historical data to detect anomalies
    anomalies = []
    if historical_data:
        # Check for row count anomalies
        if historical_data["row_count"] > 0 and abs(profile["row_count"] - historical_data["row_count"]) / \
                historical_data["row_count"] > 0.1:
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

    # Add debug prints to see what's actually being stored
    for col in column_names:
        print(f"DEBUG: Column {col} - distinct_count: {profile['completeness'][col]['distinct_count']}")

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