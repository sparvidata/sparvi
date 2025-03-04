#!/usr/bin/env python3
"""
Script to initialize Supabase database tables for Sparvi
This script creates all the required tables if they don't exist
"""

import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('init_supabase')

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

    # Use the SQL query to create tables and indexes
    sql_file_path = os.path.join(os.path.dirname(__file__), 'supabase_schema.sql')

    try:
        with open(sql_file_path, 'r') as file:
            sql_queries = file.read()

        logger.info("Executing database schema creation...")

        # This will depend on what method Supabase client supports for raw SQL.
        # If raw SQL execution is not supported, you might need to:
        # 1. Either split the script into individual commands and run them
        # 2. Or use a more direct PostgreSQL connection
        # For now, assuming there's a method to execute raw SQL:

        response = supabase.rpc('exec_sql', {"sql_query": sql_queries}).execute()

        # Check if the operation was successful
        if hasattr(response, 'error') and response.error:
            logger.error(f"Error creating schema: {response.error}")
            sys.exit(1)

        logger.info("Database schema created successfully!")

    except Exception as e:
        logger.error(f"Error initializing Supabase database: {str(e)}")
        sys.exit(1)

    logger.info("Verifying tables existence...")

    # List of tables that should exist
    expected_tables = [
        "organizations",
        "profiles",
        "profiling_history",
        "validation_rules",
        "validation_results"
    ]

    # Since there's no direct way to list tables via the Supabase client,
    # we'll check by making a simple query to each table
    for table in expected_tables:
        try:
            # Try to select a single row from the table
            response = supabase.table(table).select("*").limit(1).execute()
            logger.info(f"Table '{table}' exists and is accessible.")
        except Exception as e:
            logger.error(f"Error accessing table '{table}': {str(e)}")

    logger.info("Supabase initialization completed.")


if __name__ == "__main__":
    main()