# core/anomalies/api.py

import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone

from core.anomalies.detector import AnomalyDetector
from core.anomalies.scheduler import AnomalyDetectionScheduler
from core.anomalies.events import AnomalyEventType, publish_anomaly_event
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


class AnomalyAPI:
    """API for managing anomaly detection configurations and results"""

    def __init__(self):
        self.supabase = SupabaseManager()
        self.detector = AnomalyDetector()
        self.scheduler = AnomalyDetectionScheduler()

    def get_configs(self,
                    organization_id: str,
                    connection_id: str,
                    table_name: Optional[str] = None,
                    metric_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get anomaly detection configurations

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            table_name: Optional table name filter
            metric_name: Optional metric name filter

        Returns:
            List of configuration dictionaries
        """
        query = self.supabase.supabase.table("anomaly_detection_configs") \
            .select("*") \
            .eq("organization_id", organization_id) \
            .eq("connection_id", connection_id)

        if table_name:
            query = query.eq("table_name", table_name)

        if metric_name:
            query = query.eq("metric_name", metric_name)

        response = query.execute()
        return response.data if response.data else []

    def get_config(self,
                   organization_id: str,
                   config_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific anomaly detection configuration

        Args:
            organization_id: Organization ID
            config_id: Configuration ID

        Returns:
            Configuration dictionary or None if not found
        """
        response = self.supabase.supabase.table("anomaly_detection_configs") \
            .select("*") \
            .eq("organization_id", organization_id) \
            .eq("id", config_id) \
            .execute()

        if response.data and len(response.data) > 0:
            return response.data[0]

        return None

    def create_config(self,
                      organization_id: str,
                      user_id: str,
                      config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new anomaly detection configuration

        Args:
            organization_id: Organization ID
            user_id: User ID
            config_data: Configuration data

        Returns:
            Created configuration
        """
        # Validate configuration
        validated_config = self.detector.validate_config(config_data)

        # Add metadata
        validated_config["id"] = validated_config.get("id", str(uuid.uuid4()))
        validated_config["organization_id"] = organization_id
        validated_config["created_by"] = user_id
        validated_config["created_at"] = datetime.now(timezone.utc).isoformat()
        validated_config["updated_at"] = datetime.now(timezone.utc).isoformat()
        validated_config["is_active"] = validated_config.get("is_active", True)

        # Insert into database
        response = self.supabase.supabase.table("anomaly_detection_configs") \
            .insert(validated_config) \
            .execute()

        if not response.data:
            logger.error("Failed to create configuration")
            raise Exception("Failed to create configuration")

        # Publish event
        publish_anomaly_event(
            event_type=AnomalyEventType.CONFIG_CREATED,
            data=response.data[0],
            organization_id=organization_id,
            user_id=user_id
        )

        return response.data[0]

    def update_config(self,
                      organization_id: str,
                      user_id: str,
                      config_id: str,
                      config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an anomaly detection configuration

        Args:
            organization_id: Organization ID
            user_id: User ID
            config_id: Configuration ID
            config_data: Updated configuration data

        Returns:
            Updated configuration
        """
        # Get existing config
        existing_config = self.get_config(organization_id, config_id)
        if not existing_config:
            raise Exception(f"Configuration not found: {config_id}")

        # Merge and validate
        merged_config = {**existing_config, **config_data}
        validated_config = self.detector.validate_config(merged_config)

        # Update metadata
        validated_config["updated_at"] = datetime.now(timezone.utc).isoformat()

        # Update in database
        response = self.supabase.supabase.table("anomaly_detection_configs") \
            .update(validated_config) \
            .eq("id", config_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not response.data:
            logger.error("Failed to update configuration")
            raise Exception("Failed to update configuration")

        # Publish event
        publish_anomaly_event(
            event_type=AnomalyEventType.CONFIG_UPDATED,
            data=response.data[0],
            organization_id=organization_id,
            user_id=user_id
        )

        return response.data[0]

    def delete_config(self,
                      organization_id: str,
                      user_id: str,
                      config_id: str) -> bool:
        """
        Delete an anomaly detection configuration

        Args:
            organization_id: Organization ID
            user_id: User ID
            config_id: Configuration ID

        Returns:
            Success status
        """
        # Delete from database
        response = self.supabase.supabase.table("anomaly_detection_configs") \
            .delete() \
            .eq("id", config_id) \
            .eq("organization_id", organization_id) \
            .execute()

        success = response.data is not None and len(response.data) > 0

        # Publish event if successful
        if success:
            publish_anomaly_event(
                event_type=AnomalyEventType.CONFIG_UPDATED,
                data={"id": config_id, "deleted": True},
                organization_id=organization_id,
                user_id=user_id
            )

        return success

    def get_anomalies(self,
                      organization_id: str,
                      connection_id: str,
                      table_name: Optional[str] = None,
                      status: Optional[str] = None,
                      days: int = 30,
                      limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get detected anomalies

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            table_name: Optional table name filter
            status: Optional status filter ('open', 'acknowledged', 'resolved', 'expected')
            days: Number of days to look back
            limit: Maximum number of results

        Returns:
            List of anomaly dictionaries
        """
        # Calculate date range
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        query = self.supabase.supabase.table("anomaly_results") \
            .select("*, anomaly_detection_configs(*)") \
            .eq("organization_id", organization_id) \
            .eq("connection_id", connection_id) \
            .gte("detected_at", start_date)

        if table_name:
            query = query.eq("table_name", table_name)

        if status:
            query = query.eq("status", status)

        response = query.order("detected_at", desc=True) \
            .limit(limit) \
            .execute()

        return response.data if response.data else []

    def get_anomaly(self,
                    organization_id: str,
                    anomaly_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific anomaly

        Args:
            organization_id: Organization ID
            anomaly_id: Anomaly ID

        Returns:
            Anomaly dictionary or None if not found
        """
        response = self.supabase.supabase.table("anomaly_results") \
            .select("*, anomaly_detection_configs(*)") \
            .eq("organization_id", organization_id) \
            .eq("id", anomaly_id) \
            .execute()

        if response.data and len(response.data) > 0:
            return response.data[0]

        return None

    def update_anomaly_status(self,
                              organization_id: str,
                              user_id: str,
                              anomaly_id: str,
                              status: str,
                              resolution_note: Optional[str] = None) -> Dict[str, Any]:
        """
        Update an anomaly's status

        Args:
            organization_id: Organization ID
            user_id: User ID
            anomaly_id: Anomaly ID
            status: New status ('acknowledged', 'resolved', 'expected')
            resolution_note: Optional note

        Returns:
            Updated anomaly
        """
        # Validate status
        valid_statuses = ['open', 'acknowledged', 'resolved', 'expected']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}")

        update_data = {
            "status": status,
            "resolution_note": resolution_note
        }

        # Set resolved info if status is 'resolved'
        if status == 'resolved':
            update_data["resolved_at"] = datetime.now(timezone.utc).isoformat()
            update_data["resolved_by"] = user_id

        # Update in database
        response = self.supabase.supabase.table("anomaly_results") \
            .update(update_data) \
            .eq("id", anomaly_id) \
            .eq("organization_id", organization_id) \
            .execute()

        if not response.data:
            logger.error("Failed to update anomaly status")
            raise Exception("Failed to update anomaly status")

        # Publish event
        event_type = AnomalyEventType.ANOMALY_ACKNOWLEDGED
        if status == 'resolved':
            event_type = AnomalyEventType.ANOMALY_RESOLVED

        publish_anomaly_event(
            event_type=event_type,
            data=response.data[0],
            organization_id=organization_id,
            user_id=user_id
        )

        return response.data[0]

    def run_detection(self,
                      organization_id: str,
                      connection_id: str,
                      options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run anomaly detection manually

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            options: Optional detection options

        Returns:
            Result dictionary
        """
        # Schedule a detection run
        return self.scheduler.schedule_detection_run(
            organization_id=organization_id,
            connection_id=connection_id,
            trigger_type='manual'
        )

    def get_summary(self,
                    organization_id: str,
                    connection_id: str,
                    days: int = 30) -> Dict[str, Any]:
        """
        Get summary statistics about anomalies

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            days: Number of days to look back

        Returns:
            Summary dictionary
        """
        # Calculate date range
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        # Get count by severity
        severity_query = f"""
        SELECT 
            severity,
            COUNT(*) as count
        FROM 
            anomaly_results
        WHERE 
            organization_id = '{organization_id}'
            AND connection_id = '{connection_id}'
            AND detected_at >= '{start_date}'
        GROUP BY 
            severity
        """

        # Get count by status
        status_query = f"""
        SELECT 
            status,
            COUNT(*) as count
        FROM 
            anomaly_results
        WHERE 
            organization_id = '{organization_id}'
            AND connection_id = '{connection_id}'
            AND detected_at >= '{start_date}'
        GROUP BY 
            status
        """

        # Get count by table
        table_query = f"""
        SELECT 
            table_name,
            COUNT(*) as count
        FROM 
            anomaly_results
        WHERE 
            organization_id = '{organization_id}'
            AND connection_id = '{connection_id}'
            AND detected_at >= '{start_date}'
        GROUP BY 
            table_name
        ORDER BY
            count DESC
        LIMIT 10
        """

        # Get detected today
        today_query = f"""
        SELECT 
            COUNT(*) as count
        FROM 
            anomaly_results
        WHERE 
            organization_id = '{organization_id}'
            AND connection_id = '{connection_id}'
            AND detected_at >= '{datetime.now(timezone.utc).date().isoformat()}'
        """

        # Execute queries
        try:
            severity_results = self.supabase.supabase.rpc(
                'execute_sql',
                {'sql_query': severity_query}
            ).execute()

            status_results = self.supabase.supabase.rpc(
                'execute_sql',
                {'sql_query': status_query}
            ).execute()

            table_results = self.supabase.supabase.rpc(
                'execute_sql',
                {'sql_query': table_query}
            ).execute()

            today_results = self.supabase.supabase.rpc(
                'execute_sql',
                {'sql_query': today_query}
            ).execute()

            # Process results
            severity_counts = {}
            if severity_results.data:
                for item in severity_results.data:
                    severity_counts[item['severity']] = item['count']

            status_counts = {}
            if status_results.data:
                for item in status_results.data:
                    status_counts[item['status']] = item['count']

            table_counts = []
            if table_results.data:
                table_counts = table_results.data

            detected_today = 0
            if today_results.data and len(today_results.data) > 0:
                detected_today = today_results.data[0]['count']

            # Build summary
            return {
                "total_anomalies": sum(status_counts.values()),
                "high_severity": severity_counts.get("high", 0),
                "medium_severity": severity_counts.get("medium", 0),
                "low_severity": severity_counts.get("low", 0),
                "open": status_counts.get("open", 0),
                "acknowledged": status_counts.get("acknowledged", 0),
                "resolved": status_counts.get("resolved", 0),
                "expected": status_counts.get("expected", 0),
                "detected_today": detected_today,
                "by_table": table_counts,
                "days": days
            }

        except Exception as e:
            logger.error(f"Error getting anomaly summary: {str(e)}")
            # Return empty summary as fallback
            return {
                "total_anomalies": 0,
                "high_severity": 0,
                "medium_severity": 0,
                "low_severity": 0,
                "open": 0,
                "acknowledged": 0,
                "resolved": 0,
                "expected": 0,
                "detected_today": 0,
                "by_table": [],
                "days": days,
                "error": str(e)
            }

    def get_dashboard_data(self,
                           organization_id: str,
                           connection_id: str,
                           days: int = 30) -> Dict[str, Any]:
        """
        Get dashboard data for anomalies

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            days: Number of days to look back

        Returns:
            Dashboard data dictionary
        """
        try:
            # Get summary
            summary = self.get_summary(organization_id, connection_id, days)

            # Get recent anomalies
            recent_anomalies = self.get_anomalies(
                organization_id=organization_id,
                connection_id=connection_id,
                status="open",
                limit=10
            )

            # Get trends data
            trends = self._get_anomaly_trends(organization_id, connection_id, days)

            # Get active configs count
            config_response = self.supabase.supabase.table("anomaly_detection_configs") \
                .select("id", count="exact") \
                .eq("organization_id", organization_id) \
                .eq("connection_id", connection_id) \
                .eq("is_active", True) \
                .execute()

            active_configs = config_response.count if hasattr(config_response, 'count') else 0

            # Build dashboard data
            return {
                "summary": summary,
                "recent_anomalies": recent_anomalies,
                "trends": trends,
                "active_configs": active_configs
            }

        except Exception as e:
            logger.error(f"Error getting dashboard data: {str(e)}")
            return {
                "error": str(e),
                "summary": {
                    "total_anomalies": 0,
                    "open": 0
                },
                "recent_anomalies": [],
                "trends": [],
                "active_configs": 0
            }

    def _get_anomaly_trends(self,
                            organization_id: str,
                            connection_id: str,
                            days: int = 30) -> List[Dict[str, Any]]:
        """
        Get daily trends of anomaly detections

        Args:
            organization_id: Organization ID
            connection_id: Connection ID
            days: Number of days to look back

        Returns:
            List of daily trend dictionaries
        """
        # Calculate date range
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        # Query for daily counts
        trends_query = f"""
        WITH days AS (
            SELECT generate_series(
                date_trunc('day', (CURRENT_DATE - INTERVAL '{days} days')),
                date_trunc('day', CURRENT_DATE),
                '1 day'::interval
            )::date as day
        ),
        daily_counts AS (
            SELECT 
                date_trunc('day', detected_at)::date as day,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE severity = 'high') as high,
                COUNT(*) FILTER (WHERE severity = 'medium') as medium,
                COUNT(*) FILTER (WHERE severity = 'low') as low
            FROM 
                anomaly_results
            WHERE 
                organization_id = '{organization_id}'
                AND connection_id = '{connection_id}'
                AND detected_at >= '{start_date}'
            GROUP BY 
                day
        )
        SELECT 
            days.day::text as date,
            COALESCE(daily_counts.total, 0) as total,
            COALESCE(daily_counts.high, 0) as high,
            COALESCE(daily_counts.medium, 0) as medium,
            COALESCE(daily_counts.low, 0) as low
        FROM 
            days
        LEFT JOIN 
            daily_counts ON days.day = daily_counts.day
        ORDER BY 
            days.day
        """

        try:
            trends_results = self.supabase.supabase.rpc(
                'execute_sql',
                {'sql_query': trends_query}
            ).execute()

            if trends_results.data:
                return trends_results.data

            return []

        except Exception as e:
            logger.error(f"Error getting anomaly trends: {str(e)}")
            return []