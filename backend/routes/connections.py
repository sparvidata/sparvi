"""Connection management routes"""

import logging
import traceback
from flask import request, jsonify

from core.auth.decorators import token_required
from core.connections.builders import ConnectionStringBuilder
from core.connections.manager import ConnectionManager
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


def register_connection_routes(app):
    """Register all connection-related routes with the Flask app"""
    
    @app.route("/api/connections", methods=["GET"])
    @token_required
    def get_connections(current_user, organization_id):
        """Get all connections for the organization"""
        try:
            # Query connections for this organization
            supabase_mgr = SupabaseManager()

            # Use the Supabase client to query the database_connections table
            connections_response = supabase_mgr.supabase.table("database_connections") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .order("created_at") \
                .execute()

            connections = connections_response.data if connections_response.data else []

            # For security, remove passwords from the response
            for conn in connections:
                if 'connection_details' in conn and 'password' in conn['connection_details']:
                    conn['connection_details'].pop('password', None)

            return jsonify({"connections": connections})
        except Exception as e:
            logger.error(f"Error fetching connections: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": str(e)}), 500

    @app.route("/api/connections/<connection_id>", methods=["GET"])
    @token_required
    def get_connection_by_id(current_user, organization_id, connection_id):
        """Get a specific connection by ID for the organization"""
        try:
            # Initialize Supabase manager
            supabase_mgr = SupabaseManager()

            # Query the specific connection
            connection_response = supabase_mgr.supabase.table("database_connections") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("id", connection_id) \
                .single() \
                .execute()

            connection = connection_response.data

            if not connection:
                return jsonify({"error": "Connection not found"}), 404

            # Remove password before returning
            if 'connection_details' in connection and 'password' in connection['connection_details']:
                connection['connection_details'].pop('password', None)

            return jsonify({"connection": connection})
        except Exception as e:
            logger.error(f"Error fetching connection {connection_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": str(e)}), 500

    @app.route("/api/connections", methods=["POST"])
    @token_required
    def create_connection(current_user, organization_id):
        """Create a new database connection"""
        data = request.get_json()

        # Validate input
        required_fields = ["name", "connection_type", "connection_details"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Validate connection details
        connection_type = data["connection_type"]
        connection_details = data["connection_details"]
        
        is_valid, error_msg = ConnectionManager.validate_connection_details(connection_type, connection_details)
        if not is_valid:
            return jsonify({"error": error_msg}), 400

        try:
            # Check if this is the first connection - if so, make it the default
            is_default = False
            supabase_mgr = SupabaseManager()

            # Count existing connections
            count_response = supabase_mgr.supabase.table("database_connections") \
                .select("id", count="exact") \
                .eq("organization_id", organization_id) \
                .execute()

            # If no connections exist, make this one the default
            if count_response.count == 0:
                is_default = True

            # Insert the new connection
            connection_data = {
                "organization_id": organization_id,
                "created_by": current_user,
                "name": data["name"],
                "connection_type": data["connection_type"],
                "connection_details": data["connection_details"],
                "is_default": is_default
            }

            response = supabase_mgr.supabase.table("database_connections") \
                .insert(connection_data) \
                .execute()

            if not response.data:
                return jsonify({"error": "Failed to create connection"}), 500

            new_connection = response.data[0]
            
            # Remove password before returning
            if 'connection_details' in new_connection and 'password' in new_connection['connection_details']:
                new_connection['connection_details'].pop('password', None)

            return jsonify({"connection": new_connection}), 201

        except Exception as e:
            logger.error(f"Error creating connection: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": str(e)}), 500

    @app.route("/api/connections/<connection_id>", methods=["PUT"])
    @token_required
    def update_connection(current_user, organization_id, connection_id):
        """Update an existing database connection"""
        data = request.get_json()

        try:
            supabase_mgr = SupabaseManager()

            # Verify the connection exists and belongs to this organization
            existing_response = supabase_mgr.supabase.table("database_connections") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("id", connection_id) \
                .execute()

            if not existing_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Validate connection details if they're being updated
            if "connection_details" in data and "connection_type" in data:
                is_valid, error_msg = ConnectionManager.validate_connection_details(
                    data["connection_type"], 
                    data["connection_details"]
                )
                if not is_valid:
                    return jsonify({"error": error_msg}), 400

            # Update the connection
            update_data = {}
            allowed_fields = ["name", "connection_type", "connection_details"]
            
            for field in allowed_fields:
                if field in data:
                    update_data[field] = data[field]

            if update_data:
                update_data["updated_at"] = "now()"
                
                response = supabase_mgr.supabase.table("database_connections") \
                    .update(update_data) \
                    .eq("organization_id", organization_id) \
                    .eq("id", connection_id) \
                    .execute()

                if not response.data:
                    return jsonify({"error": "Failed to update connection"}), 500

                updated_connection = response.data[0]
                
                # Remove password before returning
                if 'connection_details' in updated_connection and 'password' in updated_connection['connection_details']:
                    updated_connection['connection_details'].pop('password', None)

                return jsonify({"connection": updated_connection})
            else:
                return jsonify({"error": "No valid fields to update"}), 400

        except Exception as e:
            logger.error(f"Error updating connection {connection_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": str(e)}), 500

    @app.route("/api/connections/<connection_id>", methods=["DELETE"])
    @token_required
    def delete_connection(current_user, organization_id, connection_id):
        """Delete a database connection"""
        try:
            supabase_mgr = SupabaseManager()

            # Verify the connection exists and belongs to this organization
            existing_response = supabase_mgr.supabase.table("database_connections") \
                .select("*") \
                .eq("organization_id", organization_id) \
                .eq("id", connection_id) \
                .execute()

            if not existing_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Delete the connection
            response = supabase_mgr.supabase.table("database_connections") \
                .delete() \
                .eq("organization_id", organization_id) \
                .eq("id", connection_id) \
                .execute()

            return jsonify({"message": "Connection deleted successfully"})

        except Exception as e:
            logger.error(f"Error deleting connection {connection_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": str(e)}), 500

    @app.route("/api/connections/test", methods=["POST"])
    @token_required
    def test_connection(current_user, organization_id):
        """Test a database connection"""
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "Request body is required"}), 400

        connection_type = data.get("connection_type")
        connection_details = data.get("connection_details")

        if not connection_type or not connection_details:
            return jsonify({"error": "connection_type and connection_details are required"}), 400

        try:
            # Validate connection details first
            is_valid, error_msg = ConnectionManager.validate_connection_details(connection_type, connection_details)
            if not is_valid:
                return jsonify({"error": error_msg}), 400

            # Build connection string and test it
            connection = {
                "connection_type": connection_type,
                "connection_details": connection_details
            }

            connection_string = ConnectionStringBuilder.get_connection_string(connection)
            if not connection_string:
                return jsonify({"error": "Failed to build connection string"}), 400

            # Test the connection with a simple query
            from sqlalchemy import create_engine, text
            engine = create_engine(connection_string)

            with engine.connect() as conn:
                if connection_type == "snowflake":
                    result = conn.execute(text("SELECT CURRENT_USER()"))
                    user = result.scalar()
                    return jsonify({
                        "message": "Connection successful!",
                        "details": {
                            "user": user,
                            "connection_type": "snowflake"
                        }
                    })
                elif connection_type == "postgresql":
                    result = conn.execute(text("SELECT current_user"))
                    user = result.scalar()
                    return jsonify({
                        "message": "Connection successful!",
                        "details": {
                            "user": user,
                            "connection_type": "postgresql"
                        }
                    })

        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return jsonify({"error": f"Connection test failed: {str(e)}"}), 400