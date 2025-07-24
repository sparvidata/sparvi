"""Profile-related routes"""

import os
import logging
import traceback
import datetime
from datetime import timezone
from flask import request, jsonify

from core.auth.decorators import token_required
from core.connections.builders import ConnectionStringBuilder
from core.connections.utils import connection_access_check
from core.history.supabase_profile_history import SupabaseProfileHistoryManager
from utils.caching import request_cache

logger = logging.getLogger(__name__)


def register_profile_routes(app):
    """Register all profile-related routes with the Flask app"""
    
    @app.route("/api/profile", methods=["GET"])
    @token_required
    def get_profile(current_user, organization_id):
        """Get profile data for a table"""
        connection_string = request.args.get("connection_string", os.getenv("DEFAULT_CONNECTION_STRING"))
        table_name = request.args.get("table", "employees")
        connection_id = request.args.get("connection_id")

        # Explicitly set include_samples to false - no row data in profiles
        include_samples = False

        try:
            logger.info(f"========== PROFILING STARTED ==========")
            logger.info(f"Profiling table {table_name}...")
            logger.info(f"User ID: {current_user}, Organization ID: {organization_id}, Connection ID: {connection_id}")

            # If connection_id provided but no connection string, get connection string from ID
            if connection_id:
                connection = connection_access_check(connection_id, organization_id)
                if not connection:
                    return jsonify({"error": "Connection not found or access denied"}), 404

                # Log connection details (without sensitive info)
                connection_type = connection.get("connection_type", "unknown")
                connection_name = connection.get("name", "unknown")
                logger.info(f"Using connection: {connection_name} (Type: {connection_type})")

                # Use connection string builder
                connection_string = ConnectionStringBuilder.get_connection_string(connection)
                if not connection_string:
                    return jsonify({"error": "Failed to build connection string"}), 400

            if not connection_string:
                return jsonify({"error": "No connection string provided"}), 400

            # Check if connection string is properly formatted
            if "://" not in connection_string:
                logger.error(f"Invalid connection string format: {connection_string[:20]}...")
                return jsonify({"error": "Invalid connection string format"}), 400

            # Verify table existence and run profiling
            # (This would contain the rest of the profiling logic)
            # For now, return a simplified response
            
            # Create profile history manager
            profile_history = SupabaseProfileHistoryManager()
            logger.info("Created SupabaseProfileHistoryManager")

            # Get previous profile to detect changes
            previous_profile = profile_history.get_latest_profile(organization_id, table_name, connection_id)
            logger.info(f"Previous profile found: {previous_profile is not None}")

            # Run the profiler (simplified for demo)
            from sparvi.profiler.profile_engine import profile_table
            result = profile_table(connection_string, table_name, previous_profile, include_samples=include_samples)
            result["timestamp"] = datetime.datetime.now(timezone.utc).isoformat()
            result["table_name"] = table_name

            # Save the profile to Supabase
            from core.utils.connection_utils import sanitize_connection_string
            sanitized_connection = sanitize_connection_string(connection_string)
            
            profile_id = profile_history.save_profile(current_user, organization_id, result, sanitized_connection, connection_id)
            logger.info(f"Profile saved with ID: {profile_id}")

            # Get trend data from history
            try:
                trends = profile_history.get_trends(organization_id, table_name, connection_id=connection_id)
                if isinstance(trends, dict) and "error" not in trends:
                    result["trends"] = trends
                    logger.info(f"Added trends data with {len(trends.get('timestamps', []))} points")
            except Exception as e:
                logger.warning(f"Could not get trends: {str(e)}")

            logger.info(f"========== PROFILING COMPLETED ==========")
            return jsonify(result)

        except Exception as e:
            logger.error(f"Error during profiling: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": str(e)}), 500

    @app.route("/api/profile-history", methods=["GET"])
    @token_required
    def get_profile_history(current_user, organization_id):
        """Get history of profile runs for a table"""
        table_name = request.args.get("table")
        connection_id = request.args.get("connection_id")
        limit = request.args.get("limit", 10, type=int)

        if not table_name:
            return jsonify({"error": "Table name is required"}), 400

        try:
            logger.info(f"Getting profile history for organization: {organization_id}, table: {table_name}, limit: {limit}")

            # Create profile history manager
            profile_history = SupabaseProfileHistoryManager()

            # Get profile history
            history_data = profile_history.get_profile_history(
                organization_id=organization_id,
                table_name=table_name,
                limit=limit,
                connection_id=connection_id
            )

            logger.info(f"Retrieved {len(history_data)} profile history records")
            return jsonify({"history": history_data})
            
        except Exception as e:
            logger.error(f"Error getting profile history: {str(e)}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500