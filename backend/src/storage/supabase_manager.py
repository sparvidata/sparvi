import os
from typing import Dict, List, Any, Optional, Union
import json
from dotenv import load_dotenv
from supabase import create_client, Client
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='supabase.log'
)
logger = logging.getLogger('supabase_manager')

# Load environment variables
load_dotenv()


class SupabaseManager:
    """Manager class for Supabase operations, handling data storage and retrieval."""

    def __init__(self):
        """Initialize the Supabase client"""
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

        if not supabase_url or not supabase_key:
            logger.error("Missing Supabase configuration (URL or key)")
            raise ValueError("Missing Supabase configuration. Check environment variables.")

        self.supabase: Client = create_client(supabase_url, supabase_key)
        logger.info("Supabase client initialized")

    def get_user_organization(self, user_id: str) -> Optional[str]:
        """Get the organization ID for a user"""
        try:
            response = self.supabase.table("profiles").select("organization_id").eq("id", user_id).single().execute()
            return response.data.get("organization_id") if response.data else None
        except Exception as e:
            logger.error(f"Error getting user organization: {str(e)}")
            return None

    # Profile History Methods

    def save_profile(self, user_id: str, organization_id: str, connection_string: str, table_name: str,
                     profile_data: Dict) -> int:
        """Save profile data to Supabase"""
        try:
            # Sanitize connection string (don't store passwords in plaintext)
            sanitized_conn = self._sanitize_connection_string(connection_string)

            data = {
                "organization_id": organization_id,
                "profile_id": user_id,
                "connection_string": sanitized_conn,
                "table_name": table_name,
                "data": profile_data
            }

            response = self.supabase.table("profiling_history").insert(data).execute()

            if response.data and len(response.data) > 0:
                logger.info(f"Saved profile for table {table_name}")
                return response.data[0]['id']  # Return the ID of the new record
            else:
                logger.error("Failed to save profile: No data returned")
                return None

        except Exception as e:
            logger.error(f"Error saving profile data: {str(e)}")
            return None

    def get_latest_profile(self, organization_id: str, table_name: str) -> Optional[Dict]:
        """Retrieve the latest profile for a table"""
        try:
            response = self.supabase.table("profiling_history") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .order("collected_at", {"ascending": False}) \
                .limit(1) \
                .execute()

            if response.data and len(response.data) > 0:
                # Return the data field which contains the profile
                return response.data[0]['data']
            return None

        except Exception as e:
            logger.error(f"Error getting latest profile: {str(e)}")
            return None

    def get_trends(self, organization_id: str, table_name: str, num_periods: int = 10) -> Dict:
        """Get time-series trend data for a specific table"""
        try:
            response = self.supabase.table("profiling_history") \
                .select("collected_at, data") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .order("collected_at", {"ascending": False}) \
                .limit(num_periods) \
                .execute()

            if not response.data:
                return {"error": "No historical data found"}

            # Process data into trends format
            trends = {
                "timestamps": [],
                "row_counts": [],
                "duplicate_counts": [],
                "null_rates": {},
                "validation_success_rates": []
            }

            # Reverse to get chronological order (oldest to newest)
            profiles = list(reversed(response.data))

            for profile in profiles:
                trends["timestamps"].append(profile["collected_at"])
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

            return trends

        except Exception as e:
            logger.error(f"Error getting trends: {str(e)}")
            return {"error": f"Error retrieving trend data: {str(e)}"}

    def delete_old_profiles(self, organization_id: str, table_name: str, keep_latest: int = 30) -> bool:
        """Delete older profiles, keeping only the specified number of latest runs"""
        try:
            # First get IDs of all records for this table/org, sorted by date
            response = self.supabase.table("profiling_history") \
                .select("id") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .order("collected_at", {"ascending": False}) \
                .execute()

            if not response.data or len(response.data) <= keep_latest:
                return True  # Nothing to delete

            # Get IDs to delete (all except the most recent 'keep_latest')
            ids_to_delete = [record["id"] for record in response.data[keep_latest:]]

            if ids_to_delete:
                # Delete records with these IDs
                self.supabase.table("profiling_history") \
                    .delete() \
                    .in_("id", ids_to_delete) \
                    .execute()

                logger.info(f"Deleted {len(ids_to_delete)} old profile records for {table_name}")

            return True

        except Exception as e:
            logger.error(f"Error deleting old profiles: {str(e)}")
            return False

    # Validation Rules Methods

    def get_validation_rules(self, organization_id: str, table_name: str) -> List[Dict]:
        """Get all validation rules for a specific table"""
        try:
            response = self.supabase.table("validation_rules") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .execute()

            rules = response.data or []

            # Parse expected_value JSON strings
            for rule in rules:
                try:
                    rule["expected_value"] = json.loads(rule["expected_value"])
                except (json.JSONDecodeError, TypeError):
                    # Keep as string if not valid JSON
                    pass

            return rules

        except Exception as e:
            logger.error(f"Error getting validation rules: {str(e)}")
            return []

    def add_validation_rule(self, organization_id: str, table_name: str, rule: Dict) -> str:
        """Add a new validation rule"""
        try:
            # Ensure expected_value is stored as a JSON string
            expected_value = json.dumps(rule.get("expected_value", ""))

            data = {
                "organization_id": organization_id,
                "table_name": table_name,
                "rule_name": rule.get("name", ""),
                "description": rule.get("description", ""),
                "query": rule.get("query", ""),
                "operator": rule.get("operator", "equals"),
                "expected_value": expected_value
            }

            response = self.supabase.table("validation_rules").insert(data).execute()

            if response.data and len(response.data) > 0:
                return response.data[0]["id"]  # Return the ID of the new rule
            return None

        except Exception as e:
            logger.error(f"Error adding validation rule: {str(e)}")
            return None

    def delete_validation_rule(self, organization_id: str, table_name: str, rule_name: str) -> bool:
        """Delete a validation rule"""
        try:
            response = self.supabase.table("validation_rules") \
                .delete() \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .eq("rule_name", rule_name) \
                .execute()

            return bool(response.data)  # True if any records were deleted

        except Exception as e:
            logger.error(f"Error deleting validation rule: {str(e)}")
            return False

    def store_validation_result(self, organization_id: str, rule_id: str, is_valid: bool, actual_value: Any) -> str:
        """Store a validation result"""
        try:
            # Ensure actual_value is stored as a JSON string
            actual_value_str = json.dumps(actual_value) if actual_value is not None else None

            data = {
                "organization_id": organization_id,
                "rule_id": rule_id,
                "is_valid": is_valid,
                "actual_value": actual_value_str
            }

            response = self.supabase.table("validation_results").insert(data).execute()

            if response.data and len(response.data) > 0:
                return response.data[0]["id"]  # Return the ID of the new result
            return None

        except Exception as e:
            logger.error(f"Error storing validation result: {str(e)}")
            return None

    def get_validation_history(self, organization_id: str, table_name: str, limit: int = 10) -> List[Dict]:
        """Get the most recent validation results for a table"""
        try:
            # First, get the rule IDs for this table
            rules_response = self.supabase.table("validation_rules") \
                .select("id, rule_name, description, operator, expected_value") \
                .eq("organization_id", organization_id) \
                .eq("table_name", table_name) \
                .execute()

            if not rules_response.data:
                return []

            rule_map = {rule["id"]: rule for rule in rules_response.data}
            rule_ids = list(rule_map.keys())

            # Get validation results for these rules
            results_response = self.supabase.table("validation_results") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .in_("rule_id", rule_ids) \
                .order("run_at", {"ascending": False}) \
                .limit(limit) \
                .execute()

            results = []
            for result in results_response.data:
                rule = rule_map.get(result["rule_id"], {})

                # Parse values from JSON strings
                try:
                    actual_value = json.loads(result["actual_value"]) if result["actual_value"] else None
                except (json.JSONDecodeError, TypeError):
                    actual_value = result["actual_value"]

                try:
                    expected_value = json.loads(rule.get("expected_value", "null"))
                except (json.JSONDecodeError, TypeError):
                    expected_value = rule.get("expected_value")

                results.append({
                    "id": result["id"],
                    "rule_name": rule.get("rule_name", "Unknown"),
                    "description": rule.get("description", ""),
                    "operator": rule.get("operator", "equals"),
                    "expected_value": expected_value,
                    "actual_value": actual_value,
                    "is_valid": result["is_valid"],
                    "run_at": result["run_at"]
                })

            return results

        except Exception as e:
            logger.error(f"Error getting validation history: {str(e)}")
            return []

    # Helper methods

    def _sanitize_connection_string(self, connection_string: str) -> str:
        """Sanitize connection string to remove sensitive information"""
        # Basic sanitization - replace password in connection string
        # This is a simple approach and might need to be adjusted based on your connection string format
        import re

        # For connection strings in format "engine://username:password@host:port/database"
        sanitized = re.sub(r'(://[^:]+:)[^@]+(@)', r'\1*****\2', connection_string)

        return sanitized