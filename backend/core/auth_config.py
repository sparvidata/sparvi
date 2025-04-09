import os

# Default values
DEFAULT_ACCESS_TOKEN_LIFETIME = 3600  # 1 hour in seconds
DEFAULT_REFRESH_TOKEN_LIFETIME = 86400 * 14  # 14 days in seconds

# Load from environment variables if available
ACCESS_TOKEN_LIFETIME = int(os.getenv("ACCESS_TOKEN_LIFETIME", DEFAULT_ACCESS_TOKEN_LIFETIME))
REFRESH_TOKEN_LIFETIME = int(os.getenv("REFRESH_TOKEN_LIFETIME", DEFAULT_REFRESH_TOKEN_LIFETIME))

# Session settings
ENABLE_SESSION_EXPIRY = os.getenv("ENABLE_SESSION_EXPIRY", "True").lower() == "true"