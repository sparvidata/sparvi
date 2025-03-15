import json
import logging
import datetime
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from supabase import create_client

logger = logging.getLogger(__name__)
load_dotenv()


class MetadataStorageService:
    """Efficient metadata storage service using JSON-based structure"""

    def __init__(self):
        """Initialize with Supabase connection"""
        import os
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Missing Supabase credentials")

        self.supabase = create_client(self.supabase_url, self.supabase_key)
        logger.info("Metadata storage service initialized")

    def store_tables_metadata(self, connection_id: str, tables_metadata: List[Dict]) -> bool:
        """Store table list and basic table metadata"""
        try:
            # Format data for storage
            metadata = {
                "tables": tables_metadata,
                "count": len(tables_metadata)
            }

            # Upsert into connection_metadata - explicitly handle potential conflict
            try:
                # Try the upsert, which should handle updates automatically
                response = self.supabase.table("connection_metadata").upsert({
                    "connection_id": connection_id,
                    "metadata_type": "tables",
                    "metadata": metadata,
                    "collected_at": datetime.datetime.now().isoformat(),
                    "refresh_frequency": "1 day"
                }).execute()
            except Exception as conflict_error:
                # If upsert fails, try an explicit update
                if "duplicate key value" in str(conflict_error):
                    logger.info(f"Entry exists, updating connection_metadata for {connection_id}")
                    response = self.supabase.table("connection_metadata") \
                        .update({
                        "metadata": metadata,
                        "collected_at": datetime.datetime.now().isoformat()
                    }) \
                        .eq("connection_id", connection_id) \
                        .eq("metadata_type", "tables") \
                        .execute()
                else:
                    # Re-raise if it's not a duplicate key error
                    raise

            # If response has data, it was successful
            if hasattr(response, 'data') and response.data:
                logger.info(f"Stored metadata for {len(tables_metadata)} tables")
                return True

            logger.error("Failed to store tables metadata: No data returned")
            return False

        except Exception as e:
            logger.error(f"Error storing tables metadata: {str(e)}")
            return False

    def store_columns_metadata(self, connection_id: str, columns_by_table: Dict[str, List[Dict]]) -> bool:
        """Store column metadata grouped by table"""
        try:
            # Format data for storage
            metadata = {
                "columns_by_table": columns_by_table,
                "table_count": len(columns_by_table),
                "total_columns": sum(len(cols) for cols in columns_by_table.values())
            }

            # Upsert into connection_metadata
            response = self.supabase.table("connection_metadata").upsert({
                "connection_id": connection_id,
                "metadata_type": "columns",
                "metadata": metadata,
                "collected_at": datetime.datetime.now().isoformat(),
                "refresh_frequency": "1 day"
            }).execute()

            if not response.data:
                logger.error("Failed to store columns metadata")
                return False

            logger.info(f"Stored metadata for columns across {len(columns_by_table)} tables")
            return True

        except Exception as e:
            logger.error(f"Error storing columns metadata: {str(e)}")
            return False

    def store_statistics_metadata(self, connection_id: str, stats_by_table: Dict[str, Dict]) -> bool:
        """Store statistical metadata for tables"""
        try:
            logger.info(f"Storing statistics for {len(stats_by_table)} tables for connection {connection_id}")

            # Format data for storage
            metadata = {
                "statistics_by_table": stats_by_table,
                "table_count": len(stats_by_table)
            }

            # Convert Decimal objects to float for JSON serialization
            import decimal
            import json

            class CustomJSONEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, decimal.Decimal):
                        return float(obj)
                    if hasattr(obj, 'isoformat'):  # Handle datetime objects
                        return obj.isoformat()
                    return super(CustomJSONEncoder, self).default(obj)

            # First convert to JSON string with custom encoder, then parse back to dict
            metadata_json = json.dumps(metadata, cls=CustomJSONEncoder)
            metadata = json.loads(metadata_json)

            # Use upsert with ON CONFLICT DO UPDATE
            try:
                # Check if record exists first
                check_response = self.supabase.table("connection_metadata") \
                    .select("id") \
                    .eq("connection_id", connection_id) \
                    .eq("metadata_type", "statistics") \
                    .execute()

                if check_response.data and len(check_response.data) > 0:
                    # Record exists, update it
                    response = self.supabase.table("connection_metadata") \
                        .update({
                        "metadata": metadata,
                        "collected_at": datetime.datetime.now().isoformat()
                    }) \
                        .eq("connection_id", connection_id) \
                        .eq("metadata_type", "statistics") \
                        .execute()
                else:
                    # Record doesn't exist, insert it
                    response = self.supabase.table("connection_metadata").insert({
                        "connection_id": connection_id,
                        "metadata_type": "statistics",
                        "metadata": metadata,
                        "collected_at": datetime.datetime.now().isoformat(),
                        "refresh_frequency": "1 day"
                    }).execute()
            except Exception as e:
                logger.error(f"Error upserting statistics metadata: {str(e)}")
                return False

            if not response.data:
                logger.error("Failed to store statistics metadata")
                return False

            logger.info(f"Stored statistics metadata for {len(stats_by_table)} tables")
            return True

        except Exception as e:
            logger.error(f"Error storing statistics metadata: {str(e)}")
            return False

    def get_metadata(self, connection_id: str, metadata_type: str) -> Optional[Dict]:
        """Get metadata of a specific type for a connection"""
        try:
            response = self.supabase.table("connection_metadata") \
                .select("metadata, collected_at") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", metadata_type) \
                .single() \
                .execute()

            if not response.data:
                logger.info(f"No {metadata_type} metadata found for connection {connection_id}")
                return None

            result = response.data
            result["freshness"] = self._calculate_freshness(response.data.get("collected_at"))
            return result

        except Exception as e:
            logger.error(f"Error getting {metadata_type} metadata: {str(e)}")
            return None

    def _calculate_freshness(self, collected_at: str) -> Dict:
        """Calculate metadata freshness"""
        if not collected_at:
            return {"status": "unknown", "age_seconds": None}

        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            collected = datetime.datetime.fromisoformat(collected_at.replace('Z', '+00:00'))

            age_seconds = (now - collected).total_seconds()

            if age_seconds < 3600:  # 1 hour
                status = "fresh"
            elif age_seconds < 86400:  # 1 day
                status = "recent"
            else:
                status = "stale"

            return {
                "status": status,
                "age_seconds": age_seconds,
                "age_hours": age_seconds / 3600,
                "age_days": age_seconds / 86400
            }
        except Exception:
            return {"status": "unknown", "age_seconds": None}