"""Authentication decorators"""

import os
import logging
import traceback
import uuid
from functools import wraps
from flask import request, jsonify, g

logger = logging.getLogger(__name__)


def token_required(f):
    """Decorator that validates JWT tokens and extracts user/org info"""
    @wraps(f)
    def decorated(*args, **kwargs):
        # Add request correlation ID for tracing
        request_id = str(uuid.uuid4())[:8]

        # Special case for testing
        if os.environ.get('TESTING') == 'True' and request.headers.get('Authorization') == 'Bearer test-token':
            logger.debug(f"[{request_id}] Test token detected, bypassing authentication")
            return f("test-user-id", "test-org-id", *args, **kwargs)

        # Normal token check
        token = None
        auth_header = request.headers.get("Authorization", None)
        if auth_header:
            parts = auth_header.split()
            if len(parts) == 2 and parts[0] == "Bearer":
                token = parts[1]
        if not token:
            logger.debug(f"[{request_id}] No token provided")
            return jsonify({"error": "Token is missing!", "code": "token_missing"}), 401

        try:
            # Use the new validation utility - ONLY CALL ONCE
            from core.utils.auth_utils import validate_token
            decoded = validate_token(token)

            if not decoded:
                logger.debug(f"[{request_id}] Token validation failed")
                return jsonify({
                    "error": "Token is expired or invalid!",
                    "code": "token_expired"
                }), 401

            current_user = decoded.get("sub")
            if not current_user:
                logger.error(f"[{request_id}] User ID not found in token")
                return jsonify({"error": "Invalid token format", "code": "token_invalid"}), 401

            # Only log successful auth at DEBUG level, not INFO
            logger.debug(f"[{request_id}] Token valid for user: {current_user}")

            # Cache organization lookup per request using Flask g
            if not hasattr(g, 'user_org_cache'):
                g.user_org_cache = {}

            if current_user not in g.user_org_cache:
                from core.storage.supabase_manager import SupabaseManager
                supabase_mgr = SupabaseManager()
                g.user_org_cache[current_user] = supabase_mgr.get_user_organization(current_user)

            organization_id = g.user_org_cache[current_user]

            if not organization_id:
                logger.error(f"[{request_id}] No organization found for user: {current_user}")
                return jsonify({"error": "User has no associated organization", "code": "org_missing"}), 403

        except Exception as e:
            logger.error(f"[{request_id}] Token verification error: {str(e)}")
            # Don't log full traceback unless in DEBUG mode
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(traceback.format_exc())
            return jsonify({"error": "Token validation failed!", "message": str(e), "code": "token_error"}), 401

        # Pass both user_id and organization_id to the decorated function
        return f(current_user, organization_id, *args, **kwargs)

    return decorated