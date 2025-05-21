# core/anomalies/routes.py

from flask import request, jsonify
from functools import wraps
import uuid

from core.anomalies.api import AnomalyAPI

# Initialize API
anomaly_api = AnomalyAPI()


def register_anomaly_routes(app, token_required):
    """
    Register anomaly-related routes with the Flask app

    Args:
        app: Flask application
        token_required: Authentication decorator
    """

    # Get all configurations for a connection
    @app.route("/api/connections/<connection_id>/anomalies/configs", methods=["GET"])
    @token_required
    def get_anomaly_configs(current_user, organization_id, connection_id):
        try:
            table_name = request.args.get("table_name")
            metric_name = request.args.get("metric_name")

            configs = anomaly_api.get_configs(
                organization_id=organization_id,
                connection_id=connection_id,
                table_name=table_name,
                metric_name=metric_name
            )

            return jsonify({"configs": configs})
        except Exception as e:
            app.logger.error(f"Error getting anomaly configs: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Create a new configuration
    @app.route("/api/connections/<connection_id>/anomalies/configs", methods=["POST"])
    @token_required
    def create_anomaly_config(current_user, organization_id, connection_id):
        try:
            config_data = request.json

            # Add connection ID to config data
            config_data["connection_id"] = connection_id

            config = anomaly_api.create_config(
                organization_id=organization_id,
                user_id=current_user,
                config_data=config_data
            )

            return jsonify({"config": config})
        except Exception as e:
            app.logger.error(f"Error creating anomaly config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Get a specific configuration
    @app.route("/api/connections/<connection_id>/anomalies/configs/<config_id>", methods=["GET"])
    @token_required
    def get_anomaly_config(current_user, organization_id, connection_id, config_id):
        try:
            config = anomaly_api.get_config(
                organization_id=organization_id,
                config_id=config_id
            )

            if not config:
                return jsonify({"error": "Configuration not found"}), 404

            # Check if config belongs to the specified connection
            if config["connection_id"] != connection_id:
                return jsonify({"error": "Configuration does not belong to this connection"}), 403

            return jsonify({"config": config})
        except Exception as e:
            app.logger.error(f"Error getting anomaly config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Update a configuration
    @app.route("/api/connections/<connection_id>/anomalies/configs/<config_id>", methods=["PUT"])
    @token_required
    def update_anomaly_config(current_user, organization_id, connection_id, config_id):
        try:
            config_data = request.json

            # Add connection ID to config data
            config_data["connection_id"] = connection_id

            # Get existing config to check ownership
            existing_config = anomaly_api.get_config(
                organization_id=organization_id,
                config_id=config_id
            )

            if not existing_config:
                return jsonify({"error": "Configuration not found"}), 404

            # Check if config belongs to the specified connection
            if existing_config["connection_id"] != connection_id:
                return jsonify({"error": "Configuration does not belong to this connection"}), 403

            # Update config
            config = anomaly_api.update_config(
                organization_id=organization_id,
                user_id=current_user,
                config_id=config_id,
                config_data=config_data
            )

            return jsonify({"config": config})
        except Exception as e:
            app.logger.error(f"Error updating anomaly config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Delete a configuration
    @app.route("/api/connections/<connection_id>/anomalies/configs/<config_id>", methods=["DELETE"])
    @token_required
    def delete_anomaly_config(current_user, organization_id, connection_id, config_id):
        try:
            # Get existing config to check ownership
            existing_config = anomaly_api.get_config(
                organization_id=organization_id,
                config_id=config_id
            )

            if not existing_config:
                return jsonify({"error": "Configuration not found"}), 404

            # Check if config belongs to the specified connection
            if existing_config["connection_id"] != connection_id:
                return jsonify({"error": "Configuration does not belong to this connection"}), 403

            # Delete config
            success = anomaly_api.delete_config(
                organization_id=organization_id,
                user_id=current_user,
                config_id=config_id
            )

            if success:
                return jsonify({"success": True})
            else:
                return jsonify({"error": "Failed to delete configuration"}), 500
        except Exception as e:
            app.logger.error(f"Error deleting anomaly config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Get anomalies for a connection
    @app.route("/api/connections/<connection_id>/anomalies", methods=["GET"])
    @token_required
    def get_anomalies(current_user, organization_id, connection_id):
        try:
            table_name = request.args.get("table_name")
            status = request.args.get("status")
            days = int(request.args.get("days", 30))
            limit = int(request.args.get("limit", 100))

            anomalies = anomaly_api.get_anomalies(
                organization_id=organization_id,
                connection_id=connection_id,
                table_name=table_name,
                status=status,
                days=days,
                limit=limit
            )

            return jsonify({"anomalies": anomalies})
        except Exception as e:
            app.logger.error(f"Error getting anomalies: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Get a specific anomaly
    @app.route("/api/connections/<connection_id>/anomalies/<anomaly_id>", methods=["GET"])
    @token_required
    def get_anomaly(current_user, organization_id, connection_id, anomaly_id):
        try:
            anomaly = anomaly_api.get_anomaly(
                organization_id=organization_id,
                anomaly_id=anomaly_id
            )

            if not anomaly:
                return jsonify({"error": "Anomaly not found"}), 404

            # Check if anomaly belongs to the specified connection
            if anomaly["connection_id"] != connection_id:
                return jsonify({"error": "Anomaly does not belong to this connection"}), 403

            return jsonify({"anomaly": anomaly})
        except Exception as e:
            app.logger.error(f"Error getting anomaly: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Update anomaly status
    @app.route("/api/connections/<connection_id>/anomalies/<anomaly_id>/status", methods=["PUT"])
    @token_required
    def update_anomaly_status(current_user, organization_id, connection_id, anomaly_id):
        try:
            data = request.json

            if not data or "status" not in data:
                return jsonify({"error": "Status is required"}), 400

            # Get existing anomaly to check ownership
            existing_anomaly = anomaly_api.get_anomaly(
                organization_id=organization_id,
                anomaly_id=anomaly_id
            )

            if not existing_anomaly:
                return jsonify({"error": "Anomaly not found"}), 404

            # Check if anomaly belongs to the specified connection
            if existing_anomaly["connection_id"] != connection_id:
                return jsonify({"error": "Anomaly does not belong to this connection"}), 403

            # Update status
            updated_anomaly = anomaly_api.update_anomaly_status(
                organization_id=organization_id,
                user_id=current_user,
                anomaly_id=anomaly_id,
                status=data["status"],
                resolution_note=data.get("resolution_note")
            )

            return jsonify({"anomaly": updated_anomaly})
        except Exception as e:
            app.logger.error(f"Error updating anomaly status: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Run anomaly detection manually
    @app.route("/api/connections/<connection_id>/anomalies/detect", methods=["POST"])
    @token_required
    def run_anomaly_detection(current_user, organization_id, connection_id):
        try:
            options = request.json or {}

            result = anomaly_api.run_detection(
                organization_id=organization_id,
                connection_id=connection_id,
                options=options
            )

            return jsonify(result)
        except Exception as e:
            app.logger.error(f"Error running anomaly detection: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Get anomaly summary
    @app.route("/api/connections/<connection_id>/anomalies/summary", methods=["GET"])
    @token_required
    def get_anomaly_summary(current_user, organization_id, connection_id):
        try:
            days = int(request.args.get("days", 30))

            summary = anomaly_api.get_summary(
                organization_id=organization_id,
                connection_id=connection_id,
                days=days
            )

            return jsonify(summary)
        except Exception as e:
            app.logger.error(f"Error getting anomaly summary: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Get anomaly dashboard data
    @app.route("/api/connections/<connection_id>/anomalies/dashboard", methods=["GET"])
    @token_required
    def get_anomaly_dashboard(current_user, organization_id, connection_id):
        try:
            days = int(request.args.get("days", 30))

            dashboard_data = anomaly_api.get_dashboard_data(
                organization_id=organization_id,
                connection_id=connection_id,
                days=days
            )

            return jsonify(dashboard_data)
        except Exception as e:
            app.logger.error(f"Error getting anomaly dashboard: {str(e)}")
            return jsonify({"error": str(e)}), 500