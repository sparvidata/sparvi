import json
import logging
import uuid
import decimal
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles Decimal and datetime objects"""

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(CustomJSONEncoder, self).default(obj)


class SupabaseProfileHistoryManager:
    """Manager for storing and retrieving profile history from Supabase"""

    def __init__(self, supabase_client=None):
        """Initialize the manager with a Supabase client"""
        from ..storage.supabase_manager import SupabaseManager

        # If no client is provided, create a new one using SupabaseManager
        if supabase_client is None:
            manager = SupabaseManager()
            self.supabase = manager.supabase
        else:
            self.supabase = supabase_client

        # Verify the client is available
        if self.supabase is None:
            logger.error("Failed to initialize Supabase client in SupabaseProfileHistoryManager")
            raise ValueError("Supabase client is required for SupabaseProfileHistoryManager")

    def save_profile(self, user_id: str, organization_id: str, profile_data: Dict[str, Any],
                     connection_string: Optional[str] = None, connection_id: Optional[str] = None) -> Optional[str]:
        """
        Save profile data to Supabase

        Args:
            user_id: ID of the user who ran the profile
            organization_id: Organization ID
            profile_data: The profile data to save
            connection_string: Optional sanitized connection string
            connection_id: Optional connection ID

        Returns:
            The ID of the saved profile or None if an error occurred
        """
        try:
            # Extract table name from profile data
            table_name = profile_data.get("table", profile_data.get("table_name"))
            if not table_name:
                logger.error("No table name found in profile data")
                return None

            logger.info(f"Saving profile for table {table_name}, organization {organization_id}")

            # Generate a unique ID for this profile
            profile_id = str(uuid.uuid4())

            # Convert profile_data to JSON string first with custom encoder, then parse it back
            profile_data_json = json.dumps(profile_data, cls=CustomJSONEncoder)
            profile_data_parsed = json.loads(profile_data_json)

            # Create simplified record with only the fields we know exist in the table
            profile_record = {
                "id": profile_id,
                "organization_id": organization_id,
                "profile_id": user_id,
                "table_name": table_name,
                "connection_id": connection_id,
                "collected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "data": profile_data_parsed  # Use 'data' column instead of 'profile_data'
            }

            # Insert into Supabase
            response = self.supabase.table("profiling_history").insert(profile_record).execute()

            if not response.data:
                logger.error("Failed to save profile to profiling_history")
                return None

            logger.info(f"Successfully saved profile with ID {profile_id}")
            return profile_id

        except Exception as e:
            logger.error(f"Error saving profile: {str(e)}")
            logger.error("Exception details", exc_info=True)
            return None

    def get_profile_history(self, organization_id: str, table_name: str, limit: int = 10,
                            connection_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get profile history for a table from Supabase

        Args:
            organization_id: Organization ID
            table_name: Table name
            limit: Maximum number of profiles to return
            connection_id: Optional connection ID to filter by

        Returns:
            List of profile history items
        """
        try:
            # Start building the query
            query = self.supabase.table("profiling_history") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name)

            # Add connection_id filter if provided
            if connection_id:
                query = query.eq("connection_id", connection_id)

            # Complete the query
            response = query.order("collected_at", desc=True).limit(limit).execute()

            if not response.data:
                logger.info(f"No profile history found for table {table_name}")
                return []

            # Process the results
            history = []
            for item in response.data:
                # Extract relevant fields for the history view
                history_item = {
                    "id": item.get("id"),
                    "collected_at": item.get("collected_at"),
                    "profile_id": item.get("profile_id"),
                    "table_name": item.get("table_name"),
                    "connection_id": item.get("connection_id")
                }

                # Add data from the data JSON field
                profile_data = item.get("data", {})
                if profile_data:
                    history_item["row_count"] = profile_data.get("row_count", 0)

                    # Count columns from completeness data
                    completeness = profile_data.get("completeness", {})
                    history_item["column_count"] = len(completeness) if completeness else 0

                    # Add a summary
                    history_item["summary"] = {
                        "fields_with_nulls": sum(1 for c in completeness.values() if c.get("nulls", 0) > 0),
                        "unique_keys": []  # Add any known unique keys here if available
                    }

                history.append(history_item)

            logger.info(f"Retrieved {len(history)} profile history records")
            return history

        except Exception as e:
            logger.error(f"Error getting profile history: {str(e)}")
            logger.error("Exception details", exc_info=True)
            return []

    def get_latest_profile(self, organization_id: str, table_name: str,
                           connection_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get the most recent profile for a table and connection

        Args:
            organization_id: ID of the organization
            table_name: Name of the table
            connection_id: Optional connection ID to filter by

        Returns:
            The most recent profile data or None if not found
        """
        try:
            # Start building the query
            logger.info(f"Getting latest profile for table {table_name}, connection {connection_id}")
            query = self.supabase.table("profiling_history") \
                .select("data") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name)

            # Add connection_id filter if provided
            if connection_id:
                query = query.eq("connection_id", connection_id)

            # Complete the query
            response = query.order("collected_at", desc=True).limit(1).execute()

            if not response.data or len(response.data) == 0:
                logger.info(f"No profile found for table {table_name}")
                return None

            logger.info(f"Found latest profile for table {table_name}")
            return response.data[0].get("data")

        except Exception as e:
            logger.error(f"Error getting latest profile: {str(e)}")
            logger.exception("Exception details")
            return None

    def get_trends(self, organization_id: str, table_name: str, days: int = 30, connection_id: Optional[str] = None) -> \
    Dict[str, Any]:
        """
        Get trend data for a table from profiling history

        Args:
            organization_id: Organization ID
            table_name: Table name
            days: Number of days to look back
            connection_id: Optional connection ID to filter by

        Returns:
            Trend data dictionary with timestamps and metrics
        """
        try:
            logger.info(f"Getting trend data for table {table_name} over {days} days")

            # Calculate the date range
            start_date = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=days)).isoformat()

            # Query Supabase for trend data - only select collected_at and data
            query = self.supabase.table("profiling_history") \
                .select("collected_at,data") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .gte("collected_at", start_date)

            # Add connection_id filter if provided
            if connection_id:
                query = query.eq("connection_id", connection_id)

            # Complete the query
            response = query.order("collected_at").execute()

            if not response.data:
                logger.info(f"No trend data found for table {table_name}")
                return {
                    "timestamps": [],
                    "row_counts": [],
                    "column_counts": []
                }

            # Process results into time series
            timestamps = []
            row_counts = []
            column_counts = []

            for item in response.data:
                timestamps.append(item.get("collected_at"))

                # Extract data from the data JSON
                profile_data = item.get("data", {})
                row_counts.append(profile_data.get("row_count", 0))

                # Count columns from completeness data or fall back to 0
                completeness = profile_data.get("completeness", {})
                column_counts.append(len(completeness) if completeness else 0)

            # Calculate change metrics
            row_count_change = None
            if len(row_counts) >= 2:
                first_count = row_counts[0]
                last_count = row_counts[-1]
                row_count_change = last_count - first_count

            trends = {
                "timestamps": timestamps,
                "row_counts": row_counts,
                "column_counts": column_counts,
                "row_count_change": row_count_change,
                "count": len(timestamps)
            }

            logger.info(f"Retrieved trend data with {len(timestamps)} points")
            return trends

        except Exception as e:
            logger.error(f"Error getting trends: {str(e)}")
            logger.error("Exception details", exc_info=True)
            return {"error": str(e)}