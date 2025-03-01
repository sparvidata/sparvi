import logging
from typing import Dict, Any, Optional

# Use relative import for supabase_manager
import sys
import os

# Add the path to find the src directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from src.storage.supabase_manager import SupabaseManager

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
        Save a profile to Supabase
        Returns the run_id of the saved profile
        """
        return self.supabase.save_profile(
            user_id,
            organization_id,
            connection_string,
            profile['table'],
            profile
        )

    def get_latest_profile(self, organization_id: str, table_name: str) -> Optional[Dict]:
        """Retrieve the latest profile for a given table"""
        return self.supabase.get_latest_profile(organization_id, table_name)

    def get_trends(self, organization_id: str, table_name: str, num_periods: int = 10) -> Dict[str, Any]:
        """Get time-series trend data for a specific table"""
        return self.supabase.get_trends(organization_id, table_name, num_periods)

    def delete_old_profiles(self, organization_id: str, table_name: str, keep_latest: int = 30):
        """Delete older profiles, keeping only the specified number of latest runs"""
        return self.supabase.delete_old_profiles(organization_id, table_name, keep_latest)