# fix_existing_user.py
import os
import sys
from dotenv import load_dotenv
from supabase import create_client


def fix_user(user_id=None, email=None):
    # Load environment variables
    load_dotenv()

    # Initialize Supabase client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

    if not supabase_url or not supabase_key:
        print("Missing Supabase configuration. Check environment variables.")
        sys.exit(1)

    print(f"Connecting to Supabase: {supabase_url}")
    print(f"Service key available: {bool(supabase_key)}")
    supabase = create_client(supabase_url, supabase_key)

    # If no user specified, get all users and fix them
    if not user_id and not email:
        print("No user specified. Will get all users from Auth...")
        try:
            users_response = supabase.auth.admin.list_users()

            if not hasattr(users_response, 'users') or not users_response.users:
                print("No users found in auth system.")
                return

            users = users_response.users
            print(f"Found {len(users)} users in Auth system.")

            for user in users:
                print(f"\nFixing user: {user.email} ({user.id})")
                fix_specific_user(supabase, user.id, user.email)

        except Exception as e:
            print(f"Error listing users: {str(e)}")
            return

    # Fix a specific user if provided
    elif user_id:
        try:
            # Try to get user details from Auth
            user_response = supabase.auth.admin.get_user_by_id(user_id)

            if hasattr(user_response, 'user') and user_response.user:
                email = user_response.user.email
                print(f"Found user: {email} ({user_id})")
                fix_specific_user(supabase, user_id, email)
            else:
                print(f"User with ID {user_id} not found in Auth system.")
        except Exception as e:
            print(f"Error getting user details: {str(e)}")

    # If only email provided, try to find by email
    elif email:
        try:
            users_response = supabase.auth.admin.list_users()

            if not hasattr(users_response, 'users') or not users_response.users:
                print("No users found in auth system.")
                return

            for user in users_response.users:
                if user.email.lower() == email.lower():
                    print(f"Found user by email: {user.email} ({user.id})")
                    fix_specific_user(supabase, user.id, user.email)
                    return

            print(f"No user found with email: {email}")
        except Exception as e:
            print(f"Error finding user by email: {str(e)}")
            return


def fix_specific_user(supabase, user_id, email):
    """Fix a specific user's profile and organization"""
    # Check if profile exists
    profile_check = supabase.table("profiles").select("*").eq("id", user_id).execute()

    if profile_check.data and len(profile_check.data) > 0:
        print(f"User already has a profile: {profile_check.data[0]}")

        # Check if organization exists
        if profile_check.data[0].get("organization_id"):
            org_id = profile_check.data[0].get("organization_id")
            org_check = supabase.table("organizations").select("*").eq("id", org_id).execute()

            if org_check.data and len(org_check.data) > 0:
                print(f"User has an organization: {org_check.data[0]}")
                return

            print(f"Organization {org_id} referenced by profile doesn't exist. Creating new organization...")

    # Create organization
    org_name = f"{email.split('@')[0]}'s Organization"
    print(f"Creating organization: {org_name}")

    try:
        org_response = supabase.table("organizations").insert({"name": org_name}).execute()

        if not org_response.data or len(org_response.data) == 0:
            print("Failed to create organization: No data returned")
            print(f"Response: {org_response}")
            return

        org_id = org_response.data[0]["id"]
        print(f"Created organization with ID: {org_id}")

        # Create or update profile
        if profile_check.data and len(profile_check.data) > 0:
            print(f"Updating existing profile with org_id: {org_id}")
            profile_response = supabase.table("profiles").update({
                "organization_id": org_id
            }).eq("id", user_id).execute()
        else:
            print(f"Creating new profile for user: {user_id}")
            profile_response = supabase.table("profiles").insert({
                "id": user_id,
                "email": email,
                "organization_id": org_id,
                "role": "admin"
            }).execute()

        if not profile_response.data or len(profile_response.data) == 0:
            print("Failed to create/update profile: No data returned")
            print(f"Response: {profile_response}")
            return

        print(f"Profile operation successful: {profile_response.data[0]}")

        # Verify success
        verification_profile = supabase.table("profiles").select("*").eq("id", user_id).execute()
        if verification_profile.data and len(verification_profile.data) > 0:
            print(f"Verification successful - user now has profile with organization: {verification_profile.data[0]}")
        else:
            print("Verification failed - profile still not visible")

    except Exception as e:
        print(f"Error fixing user: {str(e)}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Assume first argument is user_id
        fix_user(user_id=sys.argv[1])
    else:
        # Fix all users if no arguments provided
        fix_user()