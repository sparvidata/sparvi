import jwt
import logging

logger = logging.getLogger(__name__)


def log_token_details(token):
    """
    Log detailed information about a JWT token without verifying its signature

    Args:
        token (str): JWT token to decode and log
    """
    try:
        unverified = jwt.decode(token, options={"verify_signature": False})
        logger.info("Token Details:")
        logger.info(f"Issuer: {unverified.get('iss')}")
        logger.info(f"Subject: {unverified.get('sub')}")
        logger.info(f"Audience: {unverified.get('aud')}")
        logger.info(f"Expiration: {unverified.get('exp')}")
        logger.info(f"Issued At: {unverified.get('iat')}")
        logger.info(f"Full Token Contents: {unverified}")
    except Exception as e:
        logger.error(f"Error decoding token: {e}")