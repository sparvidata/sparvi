# core/anomalies/metrics.py

import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


class MetricExtractor:
    """
    Utility for extracting metrics from databases for anomaly detection
    """

    @staticmethod
    def get_row_count_query(table_name: str) -> str:
        """
        Generate query to track row count for a table

        Args:
            table_name: Name of the table

        Returns:
            SQL query string
        """
        return f"SELECT COUNT(*) AS row_count FROM {table_name}"

    @staticmethod
    def get_null_percentage_query(table_name: str, column_name: str) -> str:
        """
        Generate query to track null percentage for a column

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            SQL query string
        """
        return f"""
        SELECT 
            COUNT(*) AS total_rows,
            COUNT(*) - COUNT({column_name}) AS null_count,
            (COUNT(*) - COUNT({column_name})) * 100.0 / NULLIF(COUNT(*), 0) AS null_percentage
        FROM {table_name}
        """

    @staticmethod
    def get_distinct_count_query(table_name: str, column_name: str) -> str:
        """
        Generate query to track distinct value count for a column

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            SQL query string
        """
        return f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT {column_name}) AS distinct_count,
            COUNT(DISTINCT {column_name}) * 100.0 / NULLIF(COUNT({column_name}), 0) AS distinct_percentage
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """

    @staticmethod
    def get_freshness_query(table_name: str, timestamp_column: str) -> str:
        """
        Generate query to track freshness using a timestamp column

        Args:
            table_name: Name of the table
            timestamp_column: Name of the timestamp column

        Returns:
            SQL query string
        """
        return f"""
        SELECT
            MAX({timestamp_column}) AS latest_timestamp,
            DATEDIFF('hour', MAX({timestamp_column}), CURRENT_TIMESTAMP()) AS hours_since_update
        FROM {table_name}
        """

    @staticmethod
    def get_statistics_query(table_name: str, column_name: str) -> str:
        """
        Generate query to get statistical metrics for a numeric column

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            SQL query string
        """
        return f"""
        SELECT
            MIN({column_name}) AS min_value,
            MAX({column_name}) AS max_value,
            AVG({column_name}) AS avg_value,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column_name}) AS median,
            STDDEV({column_name}) AS std_dev
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """

    @staticmethod
    def extract_metric_from_result(
            result: List[Dict[str, Any]],
            metric_name: str
    ) -> Optional[Tuple[float, Dict[str, Any]]]:
        """
        Extract a specific metric from a query result

        Args:
            result: Query result (list of row dictionaries)
            metric_name: Name of the metric to extract

        Returns:
            Tuple of (metric_value, additional_context) or None if not found
        """
        if not result or not isinstance(result, list) or len(result) == 0:
            return None

        row = result[0]

        # Handle different metric types
        if metric_name == "row_count":
            if "row_count" in row:
                return (float(row["row_count"]), {"total_rows": row["row_count"]})

        elif metric_name == "null_percentage":
            if "null_percentage" in row:
                return (float(row["null_percentage"]), {
                    "total_rows": row.get("total_rows", 0),
                    "null_count": row.get("null_count", 0)
                })

        elif metric_name == "distinct_count":
            if "distinct_count" in row:
                return (float(row["distinct_count"]), {
                    "total_rows": row.get("total_rows", 0),
                    "distinct_percentage": row.get("distinct_percentage", 0)
                })

        elif metric_name == "distinct_percentage":
            if "distinct_percentage" in row:
                return (float(row["distinct_percentage"]), {
                    "total_rows": row.get("total_rows", 0),
                    "distinct_count": row.get("distinct_count", 0)
                })

        elif metric_name == "hours_since_update":
            if "hours_since_update" in row:
                return (float(row["hours_since_update"]), {
                    "latest_timestamp": row.get("latest_timestamp")
                })

        elif metric_name in row:
            # Generic case - if the metric is directly in the row
            try:
                return (float(row[metric_name]), {})
            except (ValueError, TypeError):
                logger.warning(f"Could not convert {metric_name} value to float: {row[metric_name]}")
                return None

        return None

    @staticmethod
    def get_query_for_metric(
            table_name: str,
            column_name: Optional[str],
            metric_name: str
    ) -> Optional[str]:
        """
        Get the appropriate query for a given metric

        Args:
            table_name: Name of the table
            column_name: Name of the column (or None for table-level metrics)
            metric_name: Name of the metric

        Returns:
            SQL query string or None if metric not supported
        """
        # Table-level metrics
        if metric_name == "row_count":
            return MetricExtractor.get_row_count_query(table_name)

        # Column-level metrics that require a column
        if not column_name:
            logger.warning(f"Column name required for metric: {metric_name}")
            return None

        if metric_name == "null_percentage":
            return MetricExtractor.get_null_percentage_query(table_name, column_name)

        elif metric_name in ["distinct_count", "distinct_percentage"]:
            return MetricExtractor.get_distinct_count_query(table_name, column_name)

        elif metric_name == "hours_since_update":
            return MetricExtractor.get_freshness_query(table_name, column_name)

        elif metric_name in ["min_value", "max_value", "avg_value", "median", "std_dev"]:
            # For these metrics, we'll run the statistics query and extract the specific value
            return MetricExtractor.get_statistics_query(table_name, column_name)

        # Custom SQL metric - in this case, return None and let the caller use the custom SQL
        return None