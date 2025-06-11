import logging
import json
import uuid
from datetime import datetime, timedelta, timezone
import os
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
from supabase import create_client

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class MetadataStorage:
    """Handles storing and retrieving metadata from Supabase"""

    def __init__(self):
        """Initialize the storage with Supabase connection"""
        # Get Supabase credentials
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Missing Supabase credentials")

        # Create Supabase client
        self.supabase = create_client(self.supabase_url, self.supabase_key)
        logger.info("Metadata storage initialized with Supabase")

    async def store_metadata(self, connection_id, metadata_type, object_id, property_id, value):
        """
        Store a piece of metadata in the star schema

        Args:
            connection_id: UUID of the database connection
            metadata_type: Type of metadata (schema, statistics, etc.)
            object_id: UUID of the database object
            property_id: ID of the property being stored
            value: The value to store
        """
        try:
            # Get metadata type ID
            type_response = self.supabase.table("metadata_types").select("id").eq("type_name", metadata_type).execute()

            if not type_response.data or len(type_response.data) == 0:
                raise ValueError(f"Unknown metadata type: {metadata_type}")

            metadata_type_id = type_response.data[0]["id"]

            # Determine value type based on Python type
            value_text = None
            value_numeric = None
            value_json = None

            if isinstance(value, (int, float)):
                value_numeric = value
            elif isinstance(value, (dict, list)):
                value_json = value
            else:
                value_text = str(value)

            # Create data to insert
            data = {
                "connection_id": connection_id,
                "metadata_type_id": metadata_type_id,
                "object_id": object_id,
                "property_id": property_id,
                "value_text": value_text,
                "value_numeric": value_numeric,
                "value_json": json.dumps(value_json) if value_json else None,
                "collected_at": datetime.now(timezone.utc).isoformat()
            }

            # Insert or update (using upsert)
            response = self.supabase.table("metadata_facts").upsert(data).execute()

            if not response.data:
                logger.error("No data returned from metadata storage operation")
                return False

            logger.debug(f"Stored metadata: {metadata_type}/{property_id} for object {object_id}")
            return True

        except Exception as e:
            logger.error(f"Error storing metadata: {str(e)}")
            return False

    async def get_metadata(self, connection_id, metadata_type=None, object_type=None):
        """
        Retrieve metadata from cache

        Args:
            connection_id: UUID of the database connection
            metadata_type: Optional type of metadata to filter by
            object_type: Optional type of database object to filter by
        """
        try:
            # Start with basic query
            query = self.supabase.table("metadata_facts").select("""
                *,
                metadata_types(type_name),
                metadata_objects(object_type, object_name, object_schema),
                metadata_properties(property_name, value_type)
            """).eq("connection_id", connection_id)

            # Add filters if provided
            if metadata_type:
                query = query.eq("metadata_types.type_name", metadata_type)

            if object_type:
                query = query.eq("metadata_objects.object_type", object_type)

            # Execute query
            response = query.execute()

            if not response.data:
                return []

            # Process and return the data
            return response.data

        except Exception as e:
            logger.error(f"Error retrieving metadata: {str(e)}")
            return []

    async def store_object(self, connection_id, object_type, object_name, object_schema=None, parent_id=None,
                           is_system=False):
        """
        Store a database object metadata record

        Args:
            connection_id: UUID of the database connection
            object_type: Type of object (table, view, column, etc.)
            object_name: Name of the object
            object_schema: Schema the object belongs to
            parent_id: UUID of parent object (e.g., table for a column)
            is_system: Whether this is a system object

        Returns:
            UUID of the created/updated object
        """
        try:
            # Check if object already exists
            query = self.supabase.table("metadata_objects") \
                .select("id") \
                .eq("connection_id", connection_id) \
                .eq("object_type", object_type) \
                .eq("object_name", object_name)

            if object_schema:
                query = query.eq("object_schema", object_schema)
            else:
                query = query.is_("object_schema", "null")

            response = query.execute()

            # If object exists, return its ID
            if response.data and len(response.data) > 0:
                return response.data[0]["id"]

            # Otherwise, create a new object
            data = {
                "connection_id": connection_id,
                "object_type": object_type,
                "object_name": object_name,
                "object_schema": object_schema,
                "parent_id": parent_id,
                "is_system": is_system,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }

            response = self.supabase.table("metadata_objects").insert(data).execute()

            if not response.data or len(response.data) == 0:
                raise ValueError("Failed to create metadata object")

            return response.data[0]["id"]

        except Exception as e:
            logger.error(f"Error storing metadata object: {str(e)}")
            raise

    async def get_refresh_candidates(self):
        """Find metadata due for refresh"""
        try:
            # Find records that are due for refresh
            query = f"""
            SELECT mf.id, mf.connection_id, mt.type_name, mo.object_type, mo.object_name, mo.object_schema, mp.property_name
            FROM metadata_facts mf
            JOIN metadata_types mt ON mf.metadata_type_id = mt.id
            JOIN metadata_objects mo ON mf.object_id = mo.id
            JOIN metadata_properties mp ON mf.property_id = mp.id
            WHERE mf.collected_at + mf.refresh_frequency < NOW()
            ORDER BY mf.collected_at ASC
            LIMIT 100
            """

            # Execute raw query using RPC
            response = self.supabase.rpc("exec_sql", {"sql_query": query}).execute()

            if hasattr(response, 'error') and response.error:
                logger.error(f"Error executing refresh candidates query: {response.error}")
                return []

            # Process and return data
            return response.data

        except Exception as e:
            logger.error(f"Error getting refresh candidates: {str(e)}")
            return []

    def get_metadata_sync(self, connection_id, metadata_type=None, object_type=None):
        """
        Synchronous version of get_metadata
        Retrieve metadata from cache

        Args:
            connection_id: UUID of the database connection
            metadata_type: Optional type of metadata to filter by
            object_type: Optional type of database object to filter by
        """
        try:
            # First, if metadata_type is specified, get its ID
            metadata_type_id = None
            if metadata_type:
                type_response = self.supabase.table("metadata_types").select("id").eq("type_name",
                                                                                      metadata_type).execute()
                if type_response.data and len(type_response.data) > 0:
                    metadata_type_id = type_response.data[0]["id"]
                else:
                    # If no metadata type found, return empty result
                    return []

            # Build the facts query
            facts_query = self.supabase.table("metadata_facts").select("*").eq("connection_id", connection_id)

            # Apply metadata type filter if specified
            if metadata_type_id:
                facts_query = facts_query.eq("metadata_type_id", metadata_type_id)

            # Execute the query
            facts_response = facts_query.execute()

            if not facts_response.data:
                return []

            # Now get the related data for each fact
            result = []
            for fact in facts_response.data:
                # Get the metadata type info
                type_info = self.supabase.table("metadata_types").select("*").eq("id",
                                                                                 fact["metadata_type_id"]).execute()

                # Get the object info
                object_info = self.supabase.table("metadata_objects").select("*").eq("id", fact["object_id"]).execute()

                # Apply object_type filter if specified
                if object_type and (not object_info.data or not object_info.data[0] or object_info.data[0].get(
                        "object_type") != object_type):
                    continue

                # Get the property info
                property_info = self.supabase.table("metadata_properties").select("*").eq("id",
                                                                                          fact["property_id"]).execute()

                # Build the enriched fact object
                enriched_fact = fact.copy()
                if type_info.data and len(type_info.data) > 0:
                    enriched_fact["metadata_type"] = type_info.data[0]
                if object_info.data and len(object_info.data) > 0:
                    enriched_fact["metadata_object"] = object_info.data[0]
                if property_info.data and len(property_info.data) > 0:
                    enriched_fact["metadata_property"] = property_info.data[0]

                result.append(enriched_fact)

            return result

        except Exception as e:
            logger.error(f"Error retrieving metadata: {str(e)}")
            return []

    # In backend/core/metadata/storage_service.py - add these methods

    def get_table_columns(self, connection_id: str, table_name: str) -> Optional[List[Dict]]:
        """Get columns for a specific table"""
        try:
            # Get columns metadata
            columns_metadata = self.get_metadata(connection_id, "columns")
            if not columns_metadata or "metadata" not in columns_metadata:
                return None

            # Extract columns for the specified table
            columns_by_table = columns_metadata["metadata"].get("columns_by_table", {})
            if table_name not in columns_by_table:
                return None

            return columns_by_table[table_name]
        except Exception as e:
            logger.error(f"Error getting table columns: {str(e)}")
            return None

    def get_table_statistics(self, connection_id: str, table_name: str) -> Optional[Dict]:
        """Get statistics for a specific table"""
        try:
            # Get statistics metadata
            statistics_metadata = self.get_metadata(connection_id, "statistics")
            if not statistics_metadata or "metadata" not in statistics_metadata:
                return None

            # Extract statistics for the specified table
            statistics_by_table = statistics_metadata["metadata"].get("statistics_by_table", {})
            if table_name not in statistics_by_table:
                return None

            return statistics_by_table[table_name]
        except Exception as e:
            logger.error(f"Error getting table statistics: {str(e)}")
            return None

    def get_metadata_history(self, connection_id: str, metadata_type: str, limit: int = 10) -> List[Dict]:
        """Get historical metadata records for a specific type"""
        try:
            # Create direct Supabase client
            import os
            from supabase import create_client

            # Get credentials from environment
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

            # Create client
            direct_client = create_client(supabase_url, supabase_key)

            # Query historical metadata
            response = direct_client.table("connection_metadata") \
                .select("metadata, collected_at") \
                .eq("connection_id", connection_id) \
                .eq("metadata_type", metadata_type) \
                .order("collected_at", desc=True) \
                .limit(limit) \
                .execute()

            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error getting metadata history: {str(e)}")
            return []