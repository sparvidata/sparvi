# In auth_config.py
import os

# Default to Supabase's standard token lifetime (3600 seconds or 1 hour)
DEFAULT_ACCESS_TOKEN_LIFETIME = 3600
DEFAULT_REFRESH_TOKEN_LIFETIME = 86400 * 14  # 14 days

# Load from environment variables
ACCESS_TOKEN_LIFETIME = int(os.getenv("ACCESS_TOKEN_LIFETIME", DEFAULT_ACCESS_TOKEN_LIFETIME))
REFRESH_TOKEN_LIFETIME = int(os.getenv("REFRESH_TOKEN_LIFETIME", DEFAULT_REFRESH_TOKEN_LIFETIME))

# Enable session expiry by default
ENABLE_SESSION_EXPIRY = os.getenv("ENABLE_SESSION_EXPIRY", "True").lower() == "true"