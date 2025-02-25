import json
import datetime
from sqlalchemy import create_engine, inspect, text
from typing import Dict, Any, List

def profile_table(connection_str: str, table: str) -> Dict[str, Any]:
    """Profile a table for data completeness, uniqueness, distribution, and numeric stats."""
    engine = create_engine(connection_str)
    inspector = inspect(engine)
    columns = inspector.get_columns(table)

    with engine.connect() as conn:
        # Build queries for profiling
        # Get just the column names
        column_names = [col["name"] for col in columns]

        # Data Completeness: Count rows, nulls, blanks
        null_counts = ', '.join(
            [f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls' for col in column_names]
        )
        blank_counts = ', '.join(
            [f'SUM(CASE WHEN TRIM({col["name"]}) = \'\' THEN 1 ELSE 0 END) AS {col["name"]}_blanks'
             for col in columns if str(col["type"]).startswith('VARCHAR') or str(col["type"]).startswith('TEXT')]
        )

        # Uniqueness: Count distinct values
        distinct_counts = ', '.join(
            [f'COUNT(DISTINCT {col["name"]}) AS {col["name"]}_distinct' for col in columns]
        )

        # Numeric statistics
        numeric_cols = [col["name"] for col in columns if
                        str(col["type"]).startswith("INT") or
                        str(col["type"]).startswith("FLOAT") or
                        str(col["type"]).startswith("NUMERIC")]
        numeric_stats = ', '.join([
            f'MIN({col}) AS {col}_min, MAX({col}) AS {col}_max, AVG({col}) AS {col}_avg, SUM({col}) AS {col}_sum, STDDEV({col}) AS {col}_stdev'
            for col in numeric_cols
        ]) if numeric_cols else ''

        # Text Lengths
        text_cols = [col["name"] for col in columns if
                     str(col["type"]).startswith("VARCHAR") or str(col["type"]).startswith("TEXT")]
        text_length_stats = ', '.join([
            f'MIN(LENGTH({col})) AS {col}_min_length, MAX(LENGTH({col})) AS {col}_max_length, AVG(LENGTH({col})) AS {col}_avg_length'
            for col in text_cols
        ]) if text_cols else ''

        # Most Frequent Values
        freq_queries = []
        for col in column_names:
            freq_queries.append(f"""
            (SELECT '{col}' AS column_name, {col} AS value, COUNT(*) AS frequency FROM {table}
            GROUP BY {col} ORDER BY frequency DESC LIMIT 5)
            """)
        freq_query = " UNION ALL ".join(freq_queries)

        # Top 100 Data Samples
        sample_query = f"SELECT * FROM {table} LIMIT 100"

        # Build a single query that returns:
        # 1. Row count
        # 2. For each column: null count, blank count, distinct count
        # 3. Then numeric stats and text length stats.
        query = f"""
        SELECT 
            COUNT(*) AS row_count, 
            {null_counts}, 
            {blank_counts}, 
            {distinct_counts}, 
            {numeric_stats}, 
            {text_length_stats}
        FROM {table}
        """
        result = conn.execute(text(query)).fetchone()
        frequent_values = conn.execute(text(freq_query)).fetchall()
        samples = conn.execute(text(sample_query)).fetchall()

    # Now, construct the profile dictionary.
    # The ordering is:
    # result[0] = row_count
    # For i in range(len(column_names)):
    #   result[i + 1] = null count for column i
    # For i in range(len(column_names)):
    #   result[i + len(column_names) + 1] = blank count for column i
    # For i in range(len(column_names)):
    #   result[i + 2*len(column_names) + 1] = distinct count for column i
    profile = {
        "table": table,
        "row_count": result[0],
        "completeness": {
            col: {
                "nulls": result[i + 1],
                "blanks": result[i + len(column_names) + 1],
                "distinct_count": result[i + 2 * len(column_names) + 1]
            }
            for i, col in enumerate(column_names)
        },
        "numeric_stats": {
            col: {
                "min": result[i + 3 * len(column_names) + 1],
                "max": result[i + 3 * len(column_names) + 2],
                "avg": result[i + 3 * len(column_names) + 3],
                "sum": result[i + 3 * len(column_names) + 4],
                "stdev": result[i + 3 * len(column_names) + 5]
            }
            for i, col in enumerate(numeric_cols)
        },
        "text_length_stats": {
            col: {
                "min_length": result[i + 4 * len(column_names) + 1],
                "max_length": result[i + 4 * len(column_names) + 2],
                "avg_length": result[i + 4 * len(column_names) + 3]
            }
            for i, col in enumerate(text_cols)
        },
        "frequent_values": {
            row[0]: {"value": row[1], "frequency": row[2]} for row in frequent_values
        },
        "samples": [dict(zip(column_names, row)) for row in samples],
        "fuzzy_matches": "TODO: Implement fuzzy matching logic"
    }

    return profile


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
