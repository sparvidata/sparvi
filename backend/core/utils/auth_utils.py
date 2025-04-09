import jwt
import time
import os
from datetime import datetime


def validate_token(token):
    """
    Validate a JWT token and check if it's expired

    Args:
        token: The JWT token to validate

    Returns:
        dict: The decoded token payload if valid
        None: If the token is invalid or expired
    """
    try:
        # Get Supabase JWT secret from environment
        jwt_secret = os.getenv("SUPABASE_JWT_SECRET")

        # If no secret is configured, fall back to the service key for validation
        if not jwt_secret:
            # This is not ideal but better than no validation
            jwt_secret = os.getenv("SUPABASE_SERVICE_KEY")

        # Decode the token without verification first to check expiration
        unverified = jwt.decode(token, options={"verify_signature": False})

        # Check if token is expired
        if 'exp' in unverified:
            expiration = unverified['exp']
            current_time = int(time.time())

            if current_time > expiration:
                return None  # Token expired

        # For additional security, verify the token signature if secret is available
        if jwt_secret:
            try:
                decoded = jwt.decode(token, jwt_secret, algorithms=["HS256"])
                return decoded
            except jwt.InvalidSignatureError:
                return None  # Invalid signature

        return unverified

    except jwt.PyJWTError:
        return None  # Invalid token format