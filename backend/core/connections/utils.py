"""Connection utility functions"""

import logging
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


def connection_access_check(connection_id, organization_id):
    """Check if the organization has access to this connection"""
    supabase_mgr = SupabaseManager()
    connection_check = supabase_mgr.supabase.table("database_connections") \
        .select("*") \
        .eq("id", connection_id) \
        .eq("organization_id", organization_id) \
        .execute()

    if not connection_check.data or len(connection_check.data) == 0:
        logger.error(f"Connection not found or access denied: {connection_id}")
        return None

    return connection_check.data[0]