import jwt
import time
import os
import logging
from datetime import datetime, timedelta

from .token_utils import log_token_details

logger = logging.getLogger(__name__)

def validate_token(token):
    """
    Validate a JWT token and check if it's expired

    Args:
        token: The JWT token to validate

    Returns:
        dict: The decoded token payload if valid
        None: If the token is invalid or expired
    """
    # Log token details for debugging
    log_token_details(token)

    try:
        # Get Supabase JWT secret from environment
        jwt_secret = os.getenv("SUPABASE_JWT_SECRET")
        service_key = os.getenv("SUPABASE_SERVICE_KEY")

        # Log available secrets (without revealing them)
        logger.info(f"JWT Secret available: {bool(jwt_secret)}")
        logger.info(f"Service Key available: {bool(service_key)}")

        # Determine which secret to use
        secret_to_use = jwt_secret or service_key

        if not secret_to_use:
            logger.error("No JWT secret or service key available for token validation")
            return None

        # More flexible validation
        options = {
            "verify_signature": True,
            "require_exp": True,
            "verify_iat": False,  # Disable strict IAT check
            "verify_aud": True,  # Enable audience verification
            "leeway": 300,  # 5 minutes of clock skew
        }

        # Decode and verify the token
        decoded = jwt.decode(
            token,
            secret_to_use,
            algorithms=["HS256"],
            options=options,
            audience='authenticated'  # Match Supabase's default audience
        )

        logger.info("Token successfully validated")
        return decoded

    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.error(f"Token validation error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected token validation error: {str(e)}")
        return None