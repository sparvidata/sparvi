import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class HistoricalMetricsTracker:
    """
    Tracker for historical metrics to support anomaly detection
    """

    def __init__(self, supabase_manager):
        self.supabase = supabase_manager

    def track_metric(self,
                     organization_id: str,
                     connection_id: str,
                     metric_name: str,
                     metric_value: Optional[float] = None,
                     metric_text: Optional[str] = None,
                     table_name: Optional[str] = None,
                     column_name: Optional[str] = None,
                     metric_type: str = "system",
                     source: str = "system") -> Dict[str, Any]:
        """
        Track a single metric

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            metric_name: Name of the metric
            metric_value: Numeric value (optional)
            metric_text: Text value (optional)
            table_name: Name of the table (optional)
            column_name: Name of the column (optional)
            metric_type: Type of metric
            source: Source of the metric

        Returns:
            Created metric record
        """
        try:
            # Create metric record
            metric = {
                "id": str(uuid.uuid4()),
                "organization_id": organization_id,
                "connection_id": connection_id,
                "table_name": table_name,
                "column_name": column_name,
                "metric_name": metric_name,
                "metric_type": metric_type,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Set value based on type
            if metric_value is not None:
                metric["metric_value"] = float(metric_value)
            elif metric_text is not None:
                metric["metric_text"] = str(metric_text)

            # Set source if provided
            if source:
                metric["source"] = source

            # Insert metric
            response = self.supabase.supabase.table("historical_metrics").insert(metric).execute()

            if not response.data:
                logger.error("Failed to insert metric")
                return None

            return response.data[0]

        except Exception as e:
            logger.error(f"Error tracking metric: {str(e)}")
            return None

    def track_metrics_batch(self,
                            organization_id: str,
                            connection_id: str,
                            metrics: List[Dict[str, Any]]) -> bool:
        """
        Track multiple metrics in a batch

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            metrics: List of metric dictionaries

        Returns:
            Success status
        """
        if not metrics:
            return True

        try:
            # Transform metrics to proper format
            records = []

            for metric in metrics:
                record = {
                    "id": str(uuid.uuid4()),
                    "organization_id": organization_id,
                    "connection_id": connection_id,
                    "metric_name": metric.get("name"),
                    "table_name": metric.get("table_name"),
                    "column_name": metric.get("column_name"),
                    "metric_type": metric.get("type", "system"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": metric.get("source", "system")
                }

                # Set value based on type
                if "value" in metric and metric["value"] is not None:
                    try:
                        record["metric_value"] = float(metric["value"])
                    except (ValueError, TypeError):
                        record["metric_text"] = str(metric["value"])

                records.append(record)

            # Insert records in batches of 50
            for i in range(0, len(records), 50):
                batch = records[i:i + 50]
                self.supabase.supabase.table("historical_metrics").insert(batch).execute()

            return True

        except Exception as e:
            logger.error(f"Error tracking metrics batch: {str(e)}")
            return False

    def get_metric_history(self,
                           organization_id: str,
                           connection_id: str,
                           metric_name: str,
                           table_name: Optional[str] = None,
                           column_name: Optional[str] = None,
                           days: int = 30,
                           limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get historical data for a specific metric

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            metric_name: Name of the metric
            table_name: Name of the table (optional)
            column_name: Name of the column (optional)
            days: Number of days to look back
            limit: Maximum number of results

        Returns:
            List of metric records
        """
        try:
            # Calculate date range
            start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Build query
            query = self.supabase.supabase.table("historical_metrics") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("connection_id", connection_id) \
                .eq("metric_name", metric_name) \
                .gte("timestamp", start_date) \
                .order("timestamp")

            if table_name:
                query = query.eq("table_name", table_name)

            if column_name:
                query = query.eq("column_name", column_name)

            # Execute query with limit
            response = query.limit(limit).execute()

            return response.data if response.data else []

        except Exception as e:
            logger.error(f"Error getting metric history: {str(e)}")
            return []

    def get_recent_metrics(self,
                           organization_id: str,
                           connection_id: Optional[str] = None,
                           limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get the most recent metrics

        Args:
            organization_id: Organization ID
            connection_id: Optional connection ID
            limit: Maximum number of results

        Returns:
            List of metric records
        """
        try:
            # Build query
            query = self.supabase.supabase.table("historical_metrics") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .order("timestamp", desc=True)

            if connection_id:
                query = query.eq("connection_id", connection_id)

            # Execute query with limit
            response = query.limit(limit).execute()

            return response.data if response.data else []

        except Exception as e:
            logger.error(f"Error getting recent metrics: {str(e)}")
            return []