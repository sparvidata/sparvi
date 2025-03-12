import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List
import os
import sys

# Add the correct path to the core directory
core_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../core'))
if core_path not in sys.path:
    sys.path.insert(0, core_path)

# Now import from storage
try:
    from ..storage.supabase_manager import SupabaseManager

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

    # Update the save_profile method to ensure no sample data is stored
    def save_profile(self, user_id: str, organization_id: str, profile: Dict, connection_string: str) -> int:
        """Save a profile to Supabase and manage history retention - without row-level data"""
        try:
            logger.info(f"Attempting to save profile for table {profile.get('table', 'unknown')} to Supabase")

            # Validate inputs to help with debugging
            if not user_id or not organization_id:
                logger.error(f"Missing required parameters: user_id={user_id}, org_id={organization_id}")
                return None

            # Create a sanitized copy of the profile without any row-level data
            sanitized_profile = {}

            # Selectively copy only needed fields to minimize memory usage
            # These are the essential fields we want to keep
            essential_fields = [
                'table', 'row_count', 'timestamp', 'duplicate_count', 'completeness',
                'frequent_values', 'schema_shifts', 'anomalies', 'numeric_stats',
                'date_stats', 'text_length_stats', 'validation_results'
            ]

            # Copy only the essential fields
            for field in essential_fields:
                if field in profile:
                    sanitized_profile[field] = profile[field]

            # Explicitly remove any potential large data fields
            excluded_fields = ['samples', 'sample_data', 'rows', 'data_examples', 'raw_data', 'source_rows', 'preview']
            for field in excluded_fields:
                if field in sanitized_profile:
                    logger.info(f"Removing potential row data field: {field}")
                    del sanitized_profile[field]

            # Force garbage collection before serializing
            import gc
            gc.collect()

            # Convert datetime objects to ISO strings in the profile
            class DateTimeEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, (datetime.datetime, datetime.date)):
                        return obj.isoformat()
                    return super().default(obj)

            # Create a JSON-safe copy of the profile
            try:
                serialized_profile = json.loads(json.dumps(sanitized_profile, cls=DateTimeEncoder))
                logger.info(f"Successfully serialized profile with {len(serialized_profile)} keys")
            except Exception as e:
                logger.error(f"Error serializing profile: {str(e)}")
                # Try again with a more resilient approach - build clean object field by field
                clean_profile = {}
                for k, v in sanitized_profile.items():
                    try:
                        if isinstance(k, str):
                            test_json = json.dumps({k: v}, cls=DateTimeEncoder)
                            clean_profile[k] = v
                    except Exception as e:
                        logger.warning(f"Skipping problematic key {k}: {str(e)}")
                serialized_profile = json.loads(json.dumps(clean_profile, cls=DateTimeEncoder))

            # Sanitize connection string to remove credentials
            sanitized_connection = self.supabase._sanitize_connection_string(connection_string)

            # Prepare data for saving
            data = {
                "organization_id": organization_id,
                "profile_id": user_id,
                "connection_string": sanitized_connection,
                "table_name": sanitized_profile.get('table', 'unknown'),
                "data": serialized_profile
            }

            # Create a direct Supabase client for more reliable insertion
            import os
            from supabase import create_client

            # Get credentials from environment
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

            if not supabase_url or not supabase_key:
                logger.error("Missing Supabase credentials in environment")
                return None

            # Create the client and insert data
            direct_client = create_client(supabase_url, supabase_key)
            logger.info("About to insert data into profiling_history table")

            try:
                response = direct_client.table("profiling_history").insert(data).execute()

                if hasattr(response, 'error') and response.error:
                    logger.error(f"Supabase insert error: {response.error}")
                    return None

                if response.data and len(response.data) > 0:
                    profile_id = response.data[0].get('id')
                    logger.info(f"Successfully saved profile with ID {profile_id}")
                    return profile_id
                else:
                    logger.warning("No data returned from Supabase after insert")
                    return None

            except Exception as save_error:
                logger.error(f"Exception during Supabase insert: {str(save_error)}")
                logger.error(traceback.format_exc())
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

    def get_profile_history(self, organization_id: str, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get complete history of profile runs for a table"""
        try:
            import os
            from supabase import create_client

            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

            direct_client = create_client(supabase_url, supabase_key)

            # Query the full profile data
            response = direct_client.table("profiling_history") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .order("collected_at", desc=True) \
                .limit(limit) \
                .execute()

            if not response.data:
                return []

            # Format the history data - return the full profile data from each record
            history = []
            for item in response.data:
                # Use the data field which contains the complete profile
                profile_data = item["data"]

                # Ensure timestamp is included
                if "timestamp" not in profile_data:
                    profile_data["timestamp"] = item["collected_at"]

                history.append(profile_data)

            return history

        except Exception as e:
            logger.error(f"Error getting profile history: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def get_trends(self, organization_id: str, table_name: str, num_periods: int = 10) -> Dict[str, Any]:
        """Get time-series trend data for a specific table"""
        try:
            import os
            import json
            import datetime
            from supabase import create_client
            import traceback

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

            # First pass: collect all possible column names across all profiles
            all_columns = set()
            for profile in profiles:
                profile_data = profile["data"]
                completeness = profile_data.get("completeness", {})
                all_columns.update(completeness.keys())

            # Initialize null_rates with empty arrays for all columns
            for col in all_columns:
                trends["null_rates"][col] = []

            # Second pass: process each profile
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

                # For each column we know about
                for col in all_columns:
                    if col in completeness:
                        # Column exists in this profile
                        stats = completeness[col]
                        trends["null_rates"][col].append(stats.get("null_percentage", 0))
                    else:
                        # Column doesn't exist in this profile (added later or removed earlier)
                        trends["null_rates"][col].append(None)

                # Process validation results if available
                validation_results = profile_data.get("validation_results", [])
                if validation_results:
                    valid_count = sum(1 for result in validation_results if result.get("is_valid", False))
                    total_count = len(validation_results)
                    success_rate = (valid_count / total_count * 100) if total_count > 0 else 100
                    trends["validation_success_rates"].append(success_rate)
                else:
                    trends["validation_success_rates"].append(None)

            logger.info(
                f"Retrieved trend data with {len(trends['timestamps'])} data points for {len(all_columns)} columns")
            return trends

        except Exception as e:
            logger.error(f"Error getting trends: {str(e)}")
            logger.error(traceback.format_exc())
            return {"error": f"Error retrieving trend data: {str(e)}"}

    def delete_old_profiles(self, organization_id: str, table_name: str, keep_latest: int = 30):
        """Delete older profiles, keeping only the specified number of latest runs"""
        return self.supabase.delete_old_profiles(organization_id, table_name, keep_latest)