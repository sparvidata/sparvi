import logging
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union
from dotenv import load_dotenv
from supabase import create_client

# Configure logging
logger = logging.getLogger(__name__)
load_dotenv()


class HistoricalMetricsTracker:
    """Service for tracking historical metrics over time"""

    def __init__(self, supabase_manager=None):
        """Initialize with optional Supabase manager"""
        self.supabase_manager = supabase_manager
        # Use supabase manager's client if provided, otherwise create our own
        if supabase_manager:
            self.supabase = supabase_manager.supabase
        # else:
        #     import os
        #     from core.storage.supabase_manager import SupabaseManager
        #     self.supabase_manager = SupabaseManager()
        #     self.supabase = self.supabase_manager.supabase

    def track_metric(
            self,
            organization_id: str,
            connection_id: str,
            metric_name: str,
            metric_value: Union[float, int, str],
            metric_type: str,
            table_name: Optional[str] = None,
            column_name: Optional[str] = None,
            source: Optional[str] = None,
            timestamp: Optional[str] = None
    ) -> bool:
        """
        Track a single metric point

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            metric_name: Name of the metric (e.g., "row_count", "null_percentage")
            metric_value: Value of the metric
            metric_type: Type of metric (e.g., "table_statistic", "column_statistic", "performance")
            table_name: Name of the table (optional)
            column_name: Name of the column (optional)
            source: Source of the metric (e.g., "profiler", "validation", "metadata_change")
            timestamp: Timestamp for the metric (optional, defaults to now)

        Returns:
            bool: True if tracking was successful
        """
        try:
            # Determine where to store the value (numeric or text)
            metric_value_numeric = None
            metric_value_text = None

            if isinstance(metric_value, (int, float)):
                metric_value_numeric = float(metric_value)
            else:
                metric_value_text = str(metric_value)

            # Format timestamp
            if timestamp is None:
                timestamp = datetime.now(timezone.utc).isoformat()

            # Create record data
            record = {
                "organization_id": organization_id,
                "connection_id": connection_id,
                "metric_name": metric_name,
                "metric_value": metric_value_numeric,
                "metric_text": metric_value_text,
                "metric_type": metric_type,
                "timestamp": timestamp,
                "source": source or "system"
            }

            # Add optional fields if provided
            if table_name:
                record["table_name"] = table_name
            if column_name:
                record["column_name"] = column_name

            # Insert the record
            response = self.supabase.table("historical_metrics").insert(record).execute()

            if not response.data:
                logger.error("Failed to insert historical metric")
                return False

            return True

        except Exception as e:
            logger.error(f"Error tracking historical metric: {str(e)}")
            return False

    def track_metrics_batch(
            self,
            organization_id: str,
            connection_id: str,
            metrics: List[Dict[str, Any]]
    ) -> bool:
        """
        Track multiple metrics in a batch

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            metrics: List of metric dictionaries with required fields

        Returns:
            bool: True if tracking was successful
        """
        try:
            # Prepare records for batch insertion
            records = []

            timestamp = datetime.now(timezone.utc).isoformat()

            for metric in metrics:
                # Determine value storage
                metric_value = metric.get("value")
                metric_value_numeric = None
                metric_text = None

                if isinstance(metric_value, (int, float)):
                    metric_value_numeric = float(metric_value)
                else:
                    metric_text = str(metric_value)

                # Create record with ALL possible fields to ensure consistency
                record = {
                    "organization_id": organization_id,
                    "connection_id": connection_id,
                    "metric_name": metric.get("name"),
                    "metric_value": metric_value_numeric,
                    "metric_text": metric_text,
                    "metric_type": metric.get("type"),
                    "table_name": metric.get("table_name", None),  # Ensure these are always present
                    "column_name": metric.get("column_name", None),  # even if null
                    "source": metric.get("source", "system"),
                    "timestamp": metric.get("timestamp", timestamp)
                }

                records.append(record)

            # Batch insert
            if records:
                response = self.supabase.table("historical_metrics").insert(records).execute()
                if not response.data:
                    logger.error("Failed to insert batch of historical metrics")
                    return False

            return True

        except Exception as e:
            logger.error(f"Error tracking batch of historical metrics: {str(e)}")
            return False

    def get_metric_history(
            self,
            organization_id: str,
            connection_id: str,
            metric_name: str,
            table_name: Optional[str] = None,
            column_name: Optional[str] = None,
            days: int = 30,
            limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get historical values for a specific metric

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            metric_name: Metric name
            table_name: Table name (optional)
            column_name: Column name (optional)
            days: Number of days to look back
            limit: Maximum number of data points to return

        Returns:
            List of metric data points
        """
        try:
            # Calculate date range
            start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Start building query
            query = self.supabase.table("historical_metrics") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("connection_id", connection_id) \
                .eq("metric_name", metric_name) \
                .gte("timestamp", start_date) \
                .order("timestamp")

            # Add filters for table and column if provided
            if table_name:
                query = query.eq("table_name", table_name)
            if column_name:
                query = query.eq("column_name", column_name)

            # Execute with limit
            query = query.limit(limit)
            response = query.execute()

            return response.data or []

        except Exception as e:
            logger.error(f"Error getting metric history: {str(e)}")
            return []

    def get_recent_metrics(
            self,
            organization_id: str,
            connection_id: str,
            metric_type: Optional[str] = None,
            limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get most recent metrics

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            metric_type: Filter by metric type (optional)
            limit: Maximum number of metrics to return

        Returns:
            List of recent metrics
        """
        try:
            # Start building query
            query = self.supabase.table("historical_metrics") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("connection_id", connection_id) \
                .order("timestamp", desc=True)

            # Add filter for metric type if provided
            if metric_type:
                query = query.eq("metric_type", metric_type)

            # Execute with limit
            response = query.limit(limit).execute()

            return response.data or []

        except Exception as e:
            logger.error(f"Error getting recent metrics: {str(e)}")
            return []