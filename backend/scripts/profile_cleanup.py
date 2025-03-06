"""
Script to remove all sample data from existing profile histories
"""
import os
import json
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
        logger.error("Missing Supabase URL or service key. Please check your .env file.")
        return

    logger.info("Connecting to Supabase...")
    supabase = create_client(supabase_url, supabase_key)

    # Get all profile history records
    logger.info("Fetching profile history records...")
    response = supabase.table("profiling_history").select("id, data").execute()

    if not response.data:
        logger.info("No profile history records found.")
        return

    logger.info(f"Found {len(response.data)} profile history records.")

    updated_count = 0
    for record in response.data:
        record_id = record.get("id")
        profile_data = record.get("data")

        if not profile_data:
            continue

        # Check if the profile contains sample data
        if "samples" in profile_data:
            logger.info(f"Removing sample data from record {record_id}")

            # Create a clean copy without samples
            clean_data = profile_data.copy()
            del clean_data["samples"]

            # Update the record
            update_response = supabase.table("profiling_history").update({"data": clean_data}).eq("id",
                                                                                                  record_id).execute()

            if update_response.data:
                updated_count += 1
            else:
                logger.warning(f"Failed to update record {record_id}")

    logger.info(f"Updated {updated_count} records to remove sample data.")

    # Set default preview settings for all organizations
    logger.info("Setting default preview settings for all organizations...")

    default_settings = {
        "preview_settings": {
            "enable_previews": True,
            "max_preview_rows": 50,
            "restricted_preview_columns": {}
        }
    }

    # Get all organizations
    org_response = supabase.table("organizations").select("id, settings").execute()

    if not org_response.data:
        logger.info("No organizations found.")
        return

    for org in org_response.data:
        org_id = org.get("id")
        settings = org.get("settings") or {}

        # Add preview settings if not present
        if "preview_settings" not in settings:
            settings["preview_settings"] = default_settings["preview_settings"]

            # Update organization settings
            update_response = supabase.table("organizations").update({"settings": settings}).eq("id", org_id).execute()

            if update_response.data:
                logger.info(f"Updated organization {org_id} with default preview settings")
            else:
                logger.warning(f"Failed to update organization {org_id}")

    logger.info("Migration completed.")


if __name__ == "__main__":
    main()