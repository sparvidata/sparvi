#!/usr/bin/env python3
"""
Quick script to assign an organization to a user in Supabase
"""
import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('fix_organization')

# Load environment variables
load_dotenv()


def main():
    if len(sys.argv) < 2:
        print("Usage: python fix_organization.py <user_id>")
        sys.exit(1)

    user_id = sys.argv[1]
    org_name = "Default Organization"

    # Initialize Supabase
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

    if not supabase_url or not supabase_key:
        logger.error("Supabase URL or service key is missing. Check your .env file.")
        sys.exit(1)

    supabase = create_client(supabase_url, supabase_key)

    try:
        # First, create the organization
        print(f"Creating organization: {org_name}")
        org_response = supabase.table("organizations").insert({
            "name": org_name
        }).execute()

        if not org_response.data or len(org_response.data) == 0:
            print("Failed to create organization")
            sys.exit(1)

        org_id = org_response.data[0]["id"]
        print(f"Created organization with ID: {org_id}")

        # Now get the user profile
        profile_response = supabase.table("profiles").select("*").eq("id", user_id).execute()

        if not profile_response.data or len(profile_response.data) == 0:
            # Profile doesn't exist, create it
            print(f"Creating profile for user: {user_id}")

            # Get user email from auth.users
            user_response = supabase.auth.admin.get_user_by_id(user_id)
            email = user_response.user.email

            if not email:
                print("Could not get user email. Using a placeholder.")
                email = f"user_{user_id}@example.com"

            profile_response = supabase.table("profiles").insert({
                "id": user_id,
                "email": email,
                "organization_id": org_id,
                "role": "admin"
            }).execute()

            if not profile_response.data or len(profile_response.data) == 0:
                print("Failed to create profile")
                sys.exit(1)

            print(f"Created profile for user {user_id} with organization {org_id}")
        else:
            # Profile exists, update it
            print(f"Updating profile for user: {user_id}")
            profile_response = supabase.table("profiles").update({
                "organization_id": org_id
            }).eq("id", user_id).execute()

            if not profile_response.data or len(profile_response.data) == 0:
                print("Failed to update profile")
                sys.exit(1)

            print(f"Updated profile for user {user_id} with organization {org_id}")

        print("Organization assignment completed successfully")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()