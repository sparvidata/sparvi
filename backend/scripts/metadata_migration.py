# backend/scripts/init_metadata_tables.py
import os
import logging
from dotenv import load_dotenv
from supabase import create_client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def main():
    # Get Supabase credentials
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

    if not supabase_url or not supabase_key:
        logger.error("Missing Supabase configuration. Check your .env file.")
        return

    logger.info("Connecting to Supabase...")
    supabase = create_client(supabase_url, supabase_key)

    # SQL to create the connection_metadata table
    sql = """
    -- Create the connection_metadata table if it doesn't exist
    CREATE TABLE IF NOT EXISTS connection_metadata (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      connection_id UUID REFERENCES database_connections(id) NOT NULL,
      metadata_type VARCHAR(50) NOT NULL,
      metadata JSONB NOT NULL DEFAULT '{}',
      collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      refresh_frequency INTERVAL DEFAULT '1 day'::INTERVAL,
      UNIQUE(connection_id, metadata_type)
    );

    -- Create index for efficient querying
    CREATE INDEX IF NOT EXISTS idx_connection_metadata_conn_type 
    ON connection_metadata(connection_id, metadata_type);

    -- Enable RLS (Row Level Security)
    ALTER TABLE connection_metadata ENABLE ROW LEVEL SECURITY;

    -- Create RLS policy for metadata access
    DROP POLICY IF EXISTS "Users can view metadata for their connections" ON connection_metadata;

    CREATE POLICY "Users can view metadata for their connections"
      ON connection_metadata FOR SELECT
      USING (
        EXISTS (
          SELECT 1 FROM database_connections
          WHERE database_connections.id = connection_metadata.connection_id
          AND database_connections.organization_id IN (
            SELECT organization_id FROM profiles
            WHERE profiles.id = auth.uid()
          )
        )
      );
    """

    try:
        # Execute the SQL
        response = supabase.rpc('exec_sql', {"sql_query": sql}).execute()

        # Check if there was any error
        if hasattr(response, 'error') and response.error:
            logger.error(f"Error creating table: {response.error}")
            return

        logger.info("✅ Successfully set up connection_metadata table!")

        # Verify the table exists
        try:
            response = supabase.table("connection_metadata").select("count", count="exact").limit(1).execute()
            logger.info(f"Verification successful. Found connection_metadata table with {response.count} records.")
        except Exception as e:
            logger.error(f"Error verifying table: {str(e)}")

    except Exception as e:
        logger.error(f"Error creating connection_metadata table: {str(e)}")


if __name__ == "__main__":
    main()