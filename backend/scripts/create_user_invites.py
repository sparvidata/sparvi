#!/usr/bin/env python3
"""
Script to create the user_invites table and related policies in Supabase
Run this script to set up the necessary database structure for the invite feature
"""

import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


def main():
    # Get Supabase credentials
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

    if not supabase_url or not supabase_key:
        logger.error("Missing Supabase URL or service key. Please check your .env file.")
        sys.exit(1)

    logger.info("Connecting to Supabase...")
    supabase = create_client(supabase_url, supabase_key)

    # SQL to create the user_invites table and policies
    sql = """
    -- Check if the user_invites table already exists
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'user_invites') THEN
            -- Create table for user invites
            CREATE TABLE user_invites (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                organization_id UUID REFERENCES organizations(id) NOT NULL,
                email TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                first_name TEXT,
                last_name TEXT,
                invite_token TEXT NOT NULL UNIQUE,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                accepted_at TIMESTAMP WITH TIME ZONE,
                accepted_by UUID REFERENCES auth.users(id)
            );

            -- Create index for faster lookups by token
            CREATE INDEX idx_user_invites_token ON user_invites(invite_token);

            -- Create index for organization lookups
            CREATE INDEX idx_user_invites_org ON user_invites(organization_id);

            -- Enable Row Level Security
            ALTER TABLE user_invites ENABLE ROW LEVEL SECURITY;

            -- Create policy for users to view invites in their organization
            CREATE POLICY "Users can view invites in their organization"
                ON user_invites FOR SELECT
                USING (
                    organization_id IN (
                        SELECT organization_id FROM profiles
                        WHERE profiles.id = auth.uid()
                    )
                );

            -- Create policy for admins to manage invites in their organization
            CREATE POLICY "Admins can manage invites in their organization"
                ON user_invites FOR ALL
                USING (
                    organization_id IN (
                        SELECT organization_id FROM profiles
                        WHERE profiles.id = auth.uid() AND profiles.role = 'admin'
                    )
                );

            -- Check if update_updated_at_column function exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_proc 
                WHERE proname = 'update_updated_at_column'
            ) THEN
                -- Create function for managing timestamps
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                   NEW.updated_at = NOW();
                   RETURN NEW;
                END;
                $$ language 'plpgsql';
            END IF;

            -- Add an updated_at timestamp trigger to the users_invites table
            CREATE TRIGGER update_user_invites_updated_at
                BEFORE UPDATE ON user_invites
                FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

            RAISE NOTICE 'Created user_invites table and policies';
        ELSE
            RAISE NOTICE 'user_invites table already exists';
        END IF;
    END $$;
    """

    try:
        logger.info("Creating user_invites table...")

        # Execute the SQL using RPC or raw query as applicable
        # Using rpc method
        response = supabase.rpc('exec_sql', {"sql_query": sql}).execute()

        # Check if there was any error
        if hasattr(response, 'error') and response.error:
            logger.error(f"Error creating table: {response.error}")
            sys.exit(1)

        logger.info("✅ Successfully set up user_invites table!")

        # Verify the table exists
        test_query = "SELECT COUNT(*) FROM user_invites"
        test_response = supabase.table("user_invites").select("count", count="exact").execute()

        logger.info(f"Verification successful. Found user_invites table with {test_response.count} records.")

    except Exception as e:
        logger.error(f"Error creating user_invites table: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()