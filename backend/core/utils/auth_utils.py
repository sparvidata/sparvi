import jwt
import time
import os
import logging
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify, current_app

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
    try:
        # Get Supabase JWT secret from environment
        jwt_secret = os.getenv("SUPABASE_JWT_SECRET")
        service_key = os.getenv("SUPABASE_SERVICE_KEY")

        # Log availability once at DEBUG level (not every request)
        logger.debug(f"JWT Secret available: {bool(jwt_secret)}")
        logger.debug(f"Service Key available: {bool(service_key)}")

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

        # Only log successful validation at DEBUG level
        logger.debug("Token successfully validated")
        return decoded

    except jwt.ExpiredSignatureError:
        logger.debug("Token has expired")  # Changed from WARNING to DEBUG
        return None
    except jwt.InvalidTokenError as e:
        logger.debug(f"Token validation error: {str(e)}")  # Changed from ERROR to DEBUG
        return None
    except Exception as e:
        logger.error(f"Unexpected token validation error: {str(e)}")
        return None


def token_required(f):
    """
    Enhanced decorator for token validation with better error handling
    """

    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        request_id = getattr(request, 'request_id', 'unknown')

        # Get token from header
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]

        if not token:
            logger.warning(f"[{request_id}] Missing authorization token")
            return jsonify({
                'error': 'authorization_required',
                'message': 'Authorization token is required'
            }), 401

        # Validate token using your existing function
        payload = validate_token(token)

        if not payload:
            logger.warning(f"[{request_id}] Token validation failed")
            return jsonify({
                'error': 'invalid_token',
                'message': 'Invalid or expired token'
            }), 401

        try:
            # Get user ID from payload
            user_id = payload.get('sub')
            if not user_id:
                logger.error(f"[{request_id}] No user ID in token payload")
                return jsonify({
                    'error': 'invalid_token_payload',
                    'message': 'Invalid token payload'
                }), 401

            # ENHANCED: Get user organization with retry logic
            organization_id = None
            max_retries = 3

            for attempt in range(max_retries):
                try:
                    from core.storage.supabase_manager import SupabaseManager
                    supabase_manager = SupabaseManager()
                    organization_id = supabase_manager.get_user_organization(user_id)

                    if organization_id:
                        break  # Success, exit retry loop
                    elif attempt == max_retries - 1:
                        # Last attempt and still no organization
                        logger.error(f"[{request_id}] No organization found for user: {user_id}")
                        return jsonify({
                            'error': 'organization_not_found',
                            'message': 'User organization not found'
                        }), 403

                except Exception as org_error:
                    logger.warning(f"[{request_id}] Organization lookup attempt {attempt + 1} failed: {str(org_error)}")

                    if attempt == max_retries - 1:
                        # Last attempt failed
                        logger.error(
                            f"[{request_id}] Failed to get organization after {max_retries} attempts for user: {user_id}")

                        # Check if it's a connection issue vs missing organization
                        error_str = str(org_error).lower()
                        if any(keyword in error_str for keyword in ["connection", "ssl", "timeout", "network"]):
                            return jsonify({
                                'error': 'service_unavailable',
                                'message': 'Service temporarily unavailable. Please try again.'
                            }), 503
                        else:
                            return jsonify({
                                'error': 'organization_not_found',
                                'message': 'User organization not found'
                            }), 403

                    # Wait before retry (exponential backoff)
                    import time
                    time.sleep(0.1 * (2 ** attempt))

            # Add user info to request context
            request.current_user_id = user_id
            request.current_organization_id = organization_id

            return f(*args, **kwargs)

        except Exception as e:
            logger.error(f"[{request_id}] Unexpected error in token validation: {str(e)}")
            return jsonify({
                'error': 'authentication_error',
                'message': 'Authentication failed'
            }), 500

    return decorated