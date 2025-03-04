import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Supabase client
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(url, key)

def get_user_organization(user_id):
    """Get the organization ID for a user"""
    response = supabase.table("profiles").select("organization_id").eq("id", user_id).single().execute()
    return response.data.get("organization_id") if response.data else None

def save_profile_data(organization_id, connection_string, table_name, profile_data):
    """Save profile data to Supabase"""
    data = {
        "organization_id": organization_id,
        "connection_string": connection_string,
        "table_name": table_name,
        "data": profile_data
    }
    response = supabase.table("profiling_history").insert(data).execute()
    return response.data