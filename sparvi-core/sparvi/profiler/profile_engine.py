"""
Core profiler engine for Sparvi
"""
import datetime
import json
from typing import Dict, Any, List, Optional, Tuple, Union

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text


def profile_table(
        connection_str: str,
        table: str,
        historical_data: Optional[Dict[str, Any]] = None,
        include_samples: bool = True
) -> Dict[str, Any]:
    """
    Profile a table for data completeness, uniqueness, distribution, and numeric stats.
    Includes anomaly detection when historical data is provided.

    Args:
        connection_str: Database connection string
        table: Table name to profile
        historical_data: Optional dictionary containing historical profile data for comparison
        include_samples: Whether to include sample data in the profile

    Returns:
        Dictionary containing profiling results
    """
    print(f"Starting profiling for table: {table}")
    engine = create_engine(connection_str)
    inspector = inspect(engine)
    columns = inspector.get_columns(table)
    column_names = [col["name"] for col in columns]

    # Categorize columns
    numeric_cols = [col["name"] for col in columns if
                    str(col["type"]).startswith("INT") or
                    str(col["type"]).startswith("FLOAT") or
                    str(col["type"]).startswith("NUMERIC") or
                    str(col["type"]).startswith("DECIMAL") or
                    str(col["type"]).startswith("DOUBLE")]

    text_cols = [col["name"] for col in columns if
                 str(col["type"]).startswith("VARCHAR") or
                 str(col["type"]).startswith("TEXT") or
                 str(col["type"]).startswith("CHAR")]

    date_cols = [col["name"] for col in columns if
                 str(col["type"]).startswith("DATE") or
                 str(col["type"]).startswith("TIMESTAMP") or
                 str(col["type"]).startswith("TIME")]

    with engine.connect() as conn:
        # Create separate queries for basic metrics
        row_count_query = f"SELECT COUNT(*) FROM {table}"
        null_counts_query = f"SELECT {', '.join([f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls' for col in column_names])} FROM {table}"
        distinct_counts_query = f"SELECT {', '.join([f'COUNT(DISTINCT {col}) AS {col}_distinct' for col in column_names])} FROM {table}"

        # Execute separate queries for clarity and reliability
        print("Executing row count query...")
        row_count = conn.execute(text(row_count_query)).fetchone()[0]
        print(f"Row count: {row_count}")

        print("Executing null counts query...")
        null_counts_result = conn.execute(text(null_counts_query)).fetchone()

        print("Executing distinct counts query...")
        distinct_counts_result = conn.execute(text(distinct_counts_query)).fetchone()

        # Duplicate check
        print("Checking for duplicates...")
        dup_check = f"""
        SELECT COUNT(*) AS duplicate_rows FROM (
            SELECT COUNT(*) as count FROM {table} GROUP BY {', '.join(column_names)} HAVING count > 1
        ) AS duplicates
        """
        try:
            duplicates_result = conn.execute(text(dup_check)).fetchone()
            duplicate_count = duplicates_result[0] if duplicates_result else 0
        except Exception as e:
            print(f"Error checking for duplicates: {str(e)}")
            duplicate_count = 0

        # Numeric statistics
        print("Calculating numeric statistics...")
        numeric_stats = {}
        for col in numeric_cols:
            print(f"Processing numeric column: {col}")
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
                # Fallback to simpler statistics query without percentiles if the database doesn't support them
                try:
                    simple_stats_query = f"""
                    SELECT 
                        MIN({col}) AS min_val, 
                        MAX({col}) AS max_val, 
                        AVG({col}) AS avg_val, 
                        SUM({col}) AS sum_val
                    FROM {table}
                    """
                    simple_stats_result = conn.execute(text(simple_stats_query)).fetchone()
                    numeric_stats[col] = {
                        "min": simple_stats_result[0],
                        "max": simple_stats_result[1],
                        "avg": simple_stats_result[2],
                        "sum": simple_stats_result[3],
                        "stdev": None,
                        "q1": None,
                        "median": None,
                        "q3": None
                    }
                except Exception as e2:
                    print(f"Error getting simple numeric stats for {col}: {str(e2)}")
                    numeric_stats[col] = {
                        "min": None, "max": None, "avg": None, "sum": None,
                        "stdev": None, "q1": None, "median": None, "q3": None
                    }

        # Text Lengths
        print("Calculating text statistics...")
        text_length_stats = {}
        for col in text_cols:
            print(f"Processing text column: {col}")
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
        print("Analyzing text patterns...")
        text_patterns = {}
        for col in text_cols:
            try:
                # Use a simpler approach to check patterns that should work across more database types
                email_pattern = f"SELECT COUNT(*) FROM {table} WHERE {col} LIKE '%@%.%'"
                numeric_pattern = f"SELECT COUNT(*) FROM {table} WHERE {col} ~ '^[0-9]+$'"
                date_pattern = f"SELECT COUNT(*) FROM {table} WHERE {col} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$'"

                # Execute each pattern check separately
                try:
                    email_count = conn.execute(text(email_pattern)).fetchone()[0]
                except:
                    email_count = 0

                try:
                    numeric_count = conn.execute(text(numeric_pattern)).fetchone()[0]
                except:
                    numeric_count = 0

                try:
                    date_count = conn.execute(text(date_pattern)).fetchone()[0]
                except:
                    date_count = 0

                text_patterns[col] = {
                    "numeric_pattern_count": numeric_count if numeric_count else 0,
                    "email_pattern_count": email_count if email_count else 0,
                    "date_pattern_count": date_count if date_count else 0
                }
            except Exception as e:
                print(f"Error getting text patterns for {col}: {str(e)}")
                text_patterns[col] = {
                    "numeric_pattern_count": 0,
                    "email_pattern_count": 0,
                    "date_pattern_count": 0
                }

        # Date range check for date columns
        print("Analyzing date columns...")
        date_stats = {}
        for col in date_cols:
            try:
                # First try a database-agnostic approach
                date_query = f"""
                SELECT MIN({col}) AS min_date, 
                       MAX({col}) AS max_date, 
                       COUNT(DISTINCT {col}) AS distinct_dates
                FROM {table}
                """
                date_result = conn.execute(text(date_query)).fetchone()

                # Some databases might not support DATEDIFF directly
                min_date = date_result[0]
                max_date = date_result[1]
                date_range_days = None
                if min_date and max_date:
                    try:
                        # Try to calculate date difference with database function
                        diff_query = f"SELECT DATEDIFF('day', MIN({col}), MAX({col})) FROM {table}"
                        diff_result = conn.execute(text(diff_query)).fetchone()
                        date_range_days = diff_result[0] if diff_result else None
                    except:
                        # If that fails, we'll leave it as None
                        pass

                date_stats[col] = {
                    "min_date": min_date,
                    "max_date": max_date,
                    "distinct_count": date_result[2],
                    "date_range_days": date_range_days
                }
            except Exception as e:
                print(f"Error getting date stats for {col}: {str(e)}")
                date_stats[col] = {
                    "min_date": None, "max_date": None, "distinct_count": 0, "date_range_days": None
                }

        # Most Frequent Values
        print("Finding most frequent values...")
        frequent_values = {}
        for col in column_names:
            try:
                # Handle each column separately to avoid type conversion issues
                if col in date_cols:
                    # Cast dates to strings for consistent handling
                    query = f"""
                    SELECT CAST({col} AS VARCHAR) AS value, 
                           COUNT(*) AS frequency,
                           (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table})) AS percentage 
                    FROM {table}
                    GROUP BY {col} 
                    ORDER BY frequency DESC 
                    LIMIT 5
                    """
                else:
                    query = f"""
                    SELECT {col} AS value, 
                           COUNT(*) AS frequency,
                           (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table})) AS percentage 
                    FROM {table}
                    GROUP BY {col} 
                    ORDER BY frequency DESC 
                    LIMIT 5
                    """

                col_values = conn.execute(text(query)).fetchall()
                if col_values and len(col_values) > 0:
                    frequent_values[col] = {
                        "value": col_values[0][0],
                        "frequency": col_values[0][1],
                        "percentage": round(col_values[0][2], 2) if col_values[0][2] is not None else 0
                    }
            except Exception as e:
                print(f"Error getting frequent values for {col}: {str(e)}")

        # Get outliers for numeric columns
        print("Detecting outliers...")
        outliers = {}
        for col in numeric_cols:
            try:
                # Use a simplified approach for outlier detection that works across more databases
                outlier_query = f"""
                SELECT {col} AS value 
                FROM {table}
                WHERE {col} IS NOT NULL
                ORDER BY {col} DESC
                LIMIT 5
                """
                outlier_results = conn.execute(text(outlier_query)).fetchall()
                if outlier_results:
                    outliers[col] = [row[0] for row in outlier_results]
            except Exception as e:
                print(f"Error getting outliers for {col}: {str(e)}")

        # Sample Data (if requested)
        samples = []
        if include_samples:
            print("Getting data samples...")
            sample_query = f"SELECT * FROM {table} LIMIT 100"
            try:
                sample_results = conn.execute(text(sample_query))
                if sample_results:
                    # Convert to list of dictionaries
                    columns = sample_results.keys()
                    rows = sample_results.fetchall()
                    samples = [dict(zip(columns, row)) for row in rows]
            except Exception as e:
                print(f"Error getting samples: {str(e)}")

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
        "frequent_values": frequent_values,
        "outliers": outliers,
    }

    # Add samples if requested
    if include_samples and samples:
        profile["samples"] = samples

    # Compare with historical data to detect anomalies
    anomalies = []
    schema_shifts = []
    if historical_data:
        print("Comparing with historical data...")
        # Check for row count anomalies
        if historical_data.get("row_count", 0) > 0 and abs(profile["row_count"] - historical_data["row_count"]) / \
                historical_data["row_count"] > 0.1:
            anomalies.append({
                "type": "row_count",
                "description": f"Row count changed by more than 10%: {historical_data['row_count']} → {profile['row_count']}",
                "severity": "high"
            })

        # Check for completeness anomalies
        for col in profile["completeness"]:
            if col in historical_data.get("completeness", {}):
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
                hist_avg = historical_data["numeric_stats"][col].get("avg")
                curr_avg = profile["numeric_stats"][col].get("avg")

                if hist_avg and curr_avg and hist_avg != 0 and abs((curr_avg - hist_avg) / hist_avg) > 0.2:
                    anomalies.append({
                        "type": "average_value",
                        "column": col,
                        "description": f"Average value of {col} changed by more than 20%: {hist_avg} → {curr_avg}",
                        "severity": "medium"
                    })

        # Detect schema changes
        schema_shifts = detect_schema_shifts(profile, historical_data)
        print(f"Detected {len(schema_shifts)} schema shifts")

    # Add anomalies to profile
    profile["anomalies"] = anomalies
    profile["schema_shifts"] = schema_shifts

    # Prepare trend data structure (will be populated from historical runs)
    profile["trends"] = {
        "row_counts": [],
        "null_rates": {},
        "duplicates": []
    }

    print(f"Profiling completed for table: {table}")
    return profile


def detect_schema_shifts(current_profile: Dict, historical_profile: Dict) -> List[Dict]:
    """
    Detect schema changes between current and historical profiles.
    Returns a list of detected shifts with descriptions.
    """
    shifts = []

    # Get current and historical columns
    current_columns = set(current_profile["completeness"].keys())
    historical_columns = set(historical_profile.get("completeness", {}).keys())

    # Check for added columns
    added_columns = current_columns - historical_columns
    for col in added_columns:
        shifts.append({
            "type": "column_added",
            "column": col,
            "description": f"New column added: {col}",
            "severity": "info",
            "timestamp": current_profile["timestamp"]
        })

    # Check for removed columns
    removed_columns = historical_columns - current_columns
    for col in removed_columns:
        shifts.append({
            "type": "column_removed",
            "column": col,
            "description": f"Column removed: {col}",
            "severity": "high",
            "timestamp": current_profile["timestamp"]
        })

    # Check for data type changes (inferred from statistics)
    common_columns = current_columns.intersection(historical_columns)
    for col in common_columns:
        # Check if column shifted between numeric and non-numeric
        was_numeric = col in historical_profile.get("numeric_stats", {})
        is_numeric = col in current_profile.get("numeric_stats", {})

        if was_numeric and not is_numeric:
            shifts.append({
                "type": "type_changed",
                "column": col,
                "description": f"Column {col} changed from numeric to non-numeric",
                "from_type": "numeric",
                "to_type": "non-numeric",
                "severity": "high",
                "timestamp": current_profile["timestamp"]
            })
        elif not was_numeric and is_numeric:
            shifts.append({
                "type": "type_changed",
                "column": col,
                "description": f"Column {col} changed from non-numeric to numeric",
                "from_type": "non-numeric",
                "to_type": "numeric",
                "severity": "high",
                "timestamp": current_profile["timestamp"]
            })

        # Check if column shifted between date and non-date
        was_date = col in historical_profile.get("date_stats", {})
        is_date = col in current_profile.get("date_stats", {})

        if was_date and not is_date:
            shifts.append({
                "type": "type_changed",
                "column": col,
                "description": f"Column {col} changed from date to non-date",
                "from_type": "date",
                "to_type": "non-date",
                "severity": "high",
                "timestamp": current_profile["timestamp"]
            })
        elif not was_date and is_date:
            shifts.append({
                "type": "type_changed",
                "column": col,
                "description": f"Column {col} changed from non-date to date",
                "from_type": "non-date",
                "to_type": "date",
                "severity": "high",
                "timestamp": current_profile["timestamp"]
            })

        # Check if a text column's max length has changed significantly
        if col in current_profile.get("text_length_stats", {}) and col in historical_profile.get("text_length_stats",
                                                                                                 {}):
            current_max = current_profile["text_length_stats"][col].get("max_length")
            historical_max = historical_profile["text_length_stats"][col].get("max_length")

            if current_max is not None and historical_max is not None:
                if current_max > historical_max * 1.5:  # 50% increase in max length
                    shifts.append({
                        "type": "length_increased",
                        "column": col,
                        "description": f"Column {col} max length increased from {historical_max} to {current_max}",
                        "from_length": historical_max,
                        "to_length": current_max,
                        "severity": "medium",
                        "timestamp": current_profile["timestamp"]
                    })

    return shifts