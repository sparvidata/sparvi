import jwt
import logging

logger = logging.getLogger(__name__)


def log_token_details(token):
    """
    Log basic token information for debugging (without sensitive content)

    Args:
        token (str): JWT token to decode and log
    """
    try:
        # Only log in DEBUG mode and don't dump full token contents
        if not logger.isEnabledFor(logging.DEBUG):
            return

        unverified = jwt.decode(token, options={"verify_signature": False})

        # Only log essential, non-sensitive information
        logger.debug("Token validation request")
        logger.debug(f"Issuer: {unverified.get('iss', 'unknown')}")
        logger.debug(f"Subject: {unverified.get('sub', 'unknown')}")
        logger.debug(f"Audience: {unverified.get('aud', 'unknown')}")

        # Log expiration info for debugging token issues
        exp = unverified.get('exp')
        if exp:
            from datetime import datetime, timezone
            exp_time = datetime.fromtimestamp(exp, tz=timezone.utc)
            logger.debug(f"Token expires: {exp_time.isoformat()}")

        # Never log the full token contents or any sensitive data

    except Exception as e:
        logger.debug(f"Error decoding token for logging: {e}")