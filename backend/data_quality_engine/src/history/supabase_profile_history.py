import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
import os
import sys

# Add the correct path to the core directory
core_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../core'))
if core_path not in sys.path:
    sys.path.insert(0, core_path)

# Now import from storage
try:
    from storage.supabase_manager import SupabaseManager

    # Log success
    logging.info("Successfully imported SupabaseManager")
except ImportError as e:
    # Log the error and try an alternative approach
    logging.error(f"Failed to import SupabaseManager: {e}")

    # Alternative approach using importlib
    import importlib.util

    manager_path = os.path.join(core_path, 'storage', 'supabase_manager.py')
    spec = importlib.util.spec_from_file_location("supabase_manager", manager_path)
    supabase_manager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(supabase_manager)
    SupabaseManager = supabase_manager.SupabaseManager
    logging.info("Successfully imported SupabaseManager using importlib")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='profile_history.log'
)
logger = logging.getLogger('supabase_profile_history')


class SupabaseProfileHistoryManager:
    """Manages the storage and retrieval of historical profiling data using Supabase"""

    def __init__(self):
        """Initialize the history manager with a Supabase connection"""
        self.supabase = SupabaseManager()
        logger.info("Supabase Profile History Manager initialized")

    def save_profile(self, user_id: str, organization_id: str, profile: Dict, connection_string: str) -> int:
        """
        Save a profile to Supabase and manage history retention
        Returns the run_id of the saved profile
        """
        try:
            logger.info(f"Attempting to save profile for table {profile['table']} to Supabase")

            # Convert datetime objects to ISO strings in the profile
            import json
            import datetime

            class DateTimeEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, (datetime.datetime, datetime.date)):
                        return obj.isoformat()
                    return super().default(obj)

            # Create a JSON-safe copy of the profile
            serialized_profile = json.loads(json.dumps(profile, cls=DateTimeEncoder))

            # Prepare data for saving
            data = {
                "organization_id": organization_id,
                "profile_id": user_id,
                "connection_string": connection_string,
                "table_name": profile['table'],
                "data": serialized_profile  # Use the serialized version
            }

            # Create a direct Supabase client
            import os
            from supabase import create_client

            # Get credentials from environment
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

            # Create the client and insert data
            direct_client = create_client(supabase_url, supabase_key)
            response = direct_client.table("profiling_history").insert(data).execute()

            if response.data and len(response.data) > 0:
                profile_id = response.data[0].get('id')
                logger.info(f"Successfully saved profile with ID {profile_id}")
                return profile_id
            else:
                logger.warning("No data returned from Supabase after insert")
                return None

        except Exception as e:
            logger.error(f"Error saving profile: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def get_latest_profile(self, organization_id: str, table_name: str) -> Optional[Dict]:
        """Retrieve the latest profile for a given table"""
        try:
            # Create a direct Supabase client
            import os
            from supabase import create_client

            # Get credentials from environment
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

            # Create the client
            direct_client = create_client(supabase_url, supabase_key)

            # Query the data with correct order parameter
            response = direct_client.table("profiling_history") \
                .select("data") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .order("collected_at", desc=True) \
                .limit(1) \
                .execute()

            if response.data and len(response.data) > 0:
                # Return the data field which contains the profile
                return response.data[0]['data']
            return None

        except Exception as e:
            logger.error(f"Error getting latest profile: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def get_trends(self, organization_id: str, table_name: str, num_periods: int = 10) -> Dict[str, Any]:
        """Get time-series trend data for a specific table"""
        try:
            import os
            import json
            import datetime
            from supabase import create_client

            # Get credentials from environment
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

            # Create a direct client
            direct_client = create_client(supabase_url, supabase_key)

            # Query the data with correct order parameter
            response = direct_client.table("profiling_history") \
                .select("collected_at, data") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .order("collected_at", desc=True) \
                .limit(num_periods) \
                .execute()

            if not response.data or len(response.data) == 0:
                logger.warning(f"No historical data found for organization {organization_id}, table {table_name}")
                return {"error": "No historical data found"}

            # Process data into trends format
            trends = {
                "timestamps": [],
                "formatted_timestamps": [],
                "row_counts": [],
                "duplicate_counts": [],
                "null_rates": {},
                "validation_success_rates": []
            }

            # Reverse to get chronological order (oldest to newest)
            profiles = list(reversed(response.data))

            # Process each profile
            for profile in profiles:
                # Format timestamp for display
                timestamp = profile["collected_at"]

                # Add the raw timestamp
                if isinstance(timestamp, str):
                    trends["timestamps"].append(timestamp)
                else:
                    trends["timestamps"].append(timestamp.isoformat())

                # Add a formatted timestamp for display
                try:
                    # Try to parse the timestamp
                    if isinstance(timestamp, str):
                        dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    else:
                        dt = timestamp
                    # Format in a readable way
                    formatted = dt.strftime("%m/%d %H:%M")
                    trends["formatted_timestamps"].append(formatted)
                except Exception as e:
                    # If parsing fails, use the original
                    logger.warning(f"Error formatting timestamp: {e}")
                    trends["formatted_timestamps"].append(str(timestamp))

                # Get the data from the profile
                profile_data = profile["data"]

                # Extract data points
                trends["row_counts"].append(profile_data.get("row_count", 0))
                trends["duplicate_counts"].append(profile_data.get("duplicate_count", 0))

                # Process null rates for each column
                completeness = profile_data.get("completeness", {})
                for col, stats in completeness.items():
                    if col not in trends["null_rates"]:
                        trends["null_rates"][col] = []
                    trends["null_rates"][col].append(stats.get("null_percentage", 0))

                # Process validation results if available
                validation_results = profile_data.get("validation_results", [])
                if validation_results:
                    valid_count = sum(1 for result in validation_results if result.get("is_valid", False))
                    total_count = len(validation_results)
                    success_rate = (valid_count / total_count * 100) if total_count > 0 else 100
                    trends["validation_success_rates"].append(success_rate)
                else:
                    trends["validation_success_rates"].append(None)

            # Make sure all columns have the same number of data points
            # Fill in missing values for columns added later
            for col, values in trends["null_rates"].items():
                if len(values) < len(trends["timestamps"]):
                    # Add nulls to the beginning to match the length
                    padding = [0] * (len(trends["timestamps"]) - len(values))
                    trends["null_rates"][col] = padding + values

            logger.info(f"Retrieved trend data with {len(trends['timestamps'])} data points")
            return trends

        except Exception as e:
            logger.error(f"Error getting trends: {str(e)}")
            logger.error(traceback.format_exc())
            return {"error": f"Error retrieving trend data: {str(e)}"}

    def delete_old_profiles(self, organization_id: str, table_name: str, keep_latest: int = 30):
        """Delete older profiles, keeping only the specified number of latest runs"""
        return self.supabase.delete_old_profiles(organization_id, table_name, keep_latest)