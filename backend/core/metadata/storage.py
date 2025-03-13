import logging
import json
import uuid
from datetime import datetime, timedelta
import os
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
                "collected_at": datetime.now().isoformat()
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
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
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