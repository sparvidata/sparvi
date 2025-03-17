import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

# Configure logging
logger = logging.getLogger(__name__)


class MetadataChangeAnalytics:
    """
    Analyze and track metadata change patterns to optimize refresh strategies
    """

    def __init__(self, supabase_manager=None):
        """
        Initialize the change analytics service

        Args:
            supabase_manager: Manager for Supabase operations
        """
        self.supabase_manager = supabase_manager

    def record_change_detection(
            self,
            connection_id: str,
            object_type: str,
            object_name: str,
            change_detected: bool,
            refresh_interval_hours: int,
            organization_id: Optional[str] = None,
            details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Record a metadata check and whether changes were detected

        Args:
            connection_id: Database connection ID
            object_type: Type of metadata object ('table', 'column', 'statistics', etc.)
            object_name: Name of the object (table or column name)
            change_detected: Whether a change was detected
            refresh_interval_hours: The interval that was used for this check
            organization_id: Organization ID
            details: Additional details about the change

        Returns:
            True if successfully recorded, False otherwise
        """
        try:
            if not self.supabase_manager:
                logger.error("Supabase manager is required to record change analytics")
                return False

            # Prepare record data
            record = {
                "connection_id": connection_id,
                "object_type": object_type,
                "object_name": object_name,
                "change_detected": change_detected,
                "check_timestamp": datetime.now().isoformat(),
                "refresh_interval_hours": refresh_interval_hours,
                "organization_id": organization_id,
                "details": json.dumps(details) if details else json.dumps({})
            }

            # Insert record
            response = self.supabase_manager.supabase.table("metadata_change_analytics").insert(record).execute()

            if not response.data:
                logger.error("Failed to record change analytics")
                return False

            logger.info(f"Recorded metadata change analytics for {object_type} {object_name}")
            return True

        except Exception as e:
            logger.error(f"Error recording change analytics: {str(e)}")
            return False

    def get_change_frequency(
            self,
            connection_id: str,
            object_type: str,
            object_name: str,
            time_period_days: int = 30
    ) -> Dict[str, Any]:
        """
        Calculate change frequency for a specific metadata object

        Args:
            connection_id: Database connection ID
            object_type: Type of metadata object
            object_name: Name of the object
            time_period_days: Number of days to analyze

        Returns:
            Dictionary with change frequency metrics
        """
        try:
            if not self.supabase_manager:
                logger.error("Supabase manager is required to get change frequency")
                return {"frequency": "unknown", "error": "No Supabase manager"}

            # Calculate start date
            start_date = (datetime.now() - timedelta(days=time_period_days)).isoformat()

            # Query change records
            response = self.supabase_manager.supabase.table("metadata_change_analytics") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .eq("object_type", object_type) \
                .eq("object_name", object_name) \
                .gte("check_timestamp", start_date) \
                .order("check_timestamp") \
                .execute()

            if not response.data:
                logger.warning(f"No change analytics found for {object_type} {object_name}")
                return {"frequency": "unknown", "data_points": 0}

            records = response.data

            # Calculate metrics
            total_checks = len(records)
            changes_detected = sum(1 for r in records if r.get("change_detected", False))

            if total_checks == 0:
                change_ratio = 0
            else:
                change_ratio = changes_detected / total_checks

            # Determine frequency category
            if change_ratio >= 0.5:
                frequency = "high"
            elif change_ratio >= 0.1:
                frequency = "medium"
            else:
                frequency = "low"

            # Calculate average time between changes
            if changes_detected >= 2:
                change_timestamps = [
                    datetime.fromisoformat(r.get("check_timestamp").replace('Z', '+00:00'))
                    for r in records
                    if r.get("change_detected", False)
                ]

                change_timestamps.sort()

                time_diffs = [
                    (change_timestamps[i] - change_timestamps[i - 1]).total_seconds() / 3600
                    for i in range(1, len(change_timestamps))
                ]

                avg_hours_between_changes = sum(time_diffs) / len(time_diffs) if time_diffs else None
            else:
                avg_hours_between_changes = None

            # Calculate most recent change
            most_recent_change = next(
                (r.get("check_timestamp") for r in reversed(records) if r.get("change_detected", False)),
                None
            )

            return {
                "frequency": frequency,
                "change_ratio": change_ratio,
                "total_checks": total_checks,
                "changes_detected": changes_detected,
                "avg_hours_between_changes": avg_hours_between_changes,
                "most_recent_change": most_recent_change,
                "data_points": total_checks
            }

        except Exception as e:
            logger.error(f"Error calculating change frequency: {str(e)}")
            return {"frequency": "unknown", "error": str(e)}

    def suggest_refresh_interval(
            self,
            connection_id: str,
            object_type: str,
            object_name: str,
            current_interval_hours: int = 24,
            min_interval_hours: int = 1,
            max_interval_hours: int = 168  # 7 days
    ) -> Dict[str, Any]:
        """
        Suggest an optimal refresh interval based on change analytics

        Args:
            connection_id: Database connection ID
            object_type: Type of metadata object
            object_name: Name of the object
            current_interval_hours: Current refresh interval in hours
            min_interval_hours: Minimum allowed interval in hours
            max_interval_hours: Maximum allowed interval in hours

        Returns:
            Dictionary with suggested interval and reasoning
        """
        try:
            # Get change frequency data
            frequency_data = self.get_change_frequency(connection_id, object_type, object_name)

            # Default response
            result = {
                "current_interval_hours": current_interval_hours,
                "suggested_interval_hours": current_interval_hours,  # Default to no change
                "frequency": frequency_data.get("frequency", "unknown"),
                "reason": "Insufficient data to suggest a change"
            }

            # Check if we have enough data
            if frequency_data.get("data_points", 0) < 5:
                return result

            frequency = frequency_data.get("frequency")
            avg_hours = frequency_data.get("avg_hours_between_changes")

            # Calculate suggested interval based on frequency
            if frequency == "high":
                # For high-frequency changes, check more often
                # Use avg_hours if available, otherwise use a fraction of current interval
                if avg_hours:
                    # Aim to check at about 1/3 of the average time between changes
                    suggested_interval = max(min_interval_hours, min(int(avg_hours / 3), current_interval_hours))
                else:
                    # Without avg_hours data, decrease current interval by 50%
                    suggested_interval = max(min_interval_hours, current_interval_hours // 2)

                reason = "High change frequency detected, decreasing interval for more timely updates"

            elif frequency == "medium":
                # For medium frequency, slight decrease or stay the same
                if avg_hours and avg_hours < current_interval_hours:
                    suggested_interval = max(min_interval_hours, int(avg_hours * 0.75))
                else:
                    # Keep current interval
                    suggested_interval = current_interval_hours

                reason = "Medium change frequency detected, maintaining reasonable refresh interval"

            elif frequency == "low":
                # For low frequency, increase interval to reduce load
                suggested_interval = min(max_interval_hours, current_interval_hours * 2)
                reason = "Low change frequency detected, increasing interval to reduce system load"

            else:
                # Unknown frequency, keep current interval
                suggested_interval = current_interval_hours
                reason = "Unknown change frequency, maintaining current interval"

            # Ensure interval is within bounds
            suggested_interval = max(min_interval_hours, min(max_interval_hours, suggested_interval))

            # Update result
            result["suggested_interval_hours"] = suggested_interval
            result["reason"] = reason
            result["avg_hours_between_changes"] = avg_hours

            return result

        except Exception as e:
            logger.error(f"Error suggesting refresh interval: {str(e)}")
            return {
                "current_interval_hours": current_interval_hours,
                "suggested_interval_hours": current_interval_hours,
                "frequency": "unknown",
                "reason": f"Error calculating suggestion: {str(e)}"
            }

    def get_high_impact_objects(
            self,
            connection_id: str,
            limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get a list of objects with highest change frequency

        Args:
            connection_id: Database connection ID
            limit: Maximum number of objects to return

        Returns:
            List of high-impact objects with their change metrics
        """
        try:
            if not self.supabase_manager:
                logger.error("Supabase manager is required to get high-impact objects")
                return []

            # Get raw analytics data for calculation
            # Note: This SQL uses a 30-day window and aggregates by object
            sql = f"""
            WITH change_stats AS (
                SELECT 
                    object_type,
                    object_name,
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN change_detected = true THEN 1 ELSE 0 END) as changes_detected,
                    (SUM(CASE WHEN change_detected = true THEN 1 ELSE 0 END)::float / COUNT(*)::float) as change_ratio
                FROM metadata_change_analytics
                WHERE connection_id = '{connection_id}'
                AND check_timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days'
                GROUP BY object_type, object_name
                HAVING COUNT(*) >= 5
            )
            SELECT 
                object_type,
                object_name,
                total_checks,
                changes_detected,
                change_ratio,
                CASE 
                    WHEN change_ratio >= 0.5 THEN 'high'
                    WHEN change_ratio >= 0.1 THEN 'medium'
                    ELSE 'low'
                END as frequency
            FROM change_stats
            ORDER BY change_ratio DESC, total_checks DESC
            LIMIT {limit};
            """

            response = self.supabase_manager.supabase.rpc("exec_sql", {"sql_query": sql}).execute()

            if response.data:
                return response.data

            return []

        except Exception as e:
            logger.error(f"Error getting high-impact objects: {str(e)}")
            return []