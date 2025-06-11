from flask import Blueprint, request, jsonify
import logging
from datetime import datetime

from .api import AutomationAPI

logger = logging.getLogger(__name__)

# Initialize API
automation_api = AutomationAPI()


def register_automation_routes(app, token_required):
    """
    Register automation-related routes with the Flask app

    Args:
        app: Flask application
        token_required: Authentication decorator
    """

    # Global Configuration Endpoints
    @app.route("/api/automation/global-config", methods=["GET"])
    @token_required
    def get_global_automation_config(current_user, organization_id):
        """Get global automation configuration"""
        try:
            config = automation_api.get_global_config()
            return jsonify({"config": config}), 200

        except Exception as e:
            logger.error(f"Error getting global automation config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/global-config", methods=["PUT"])
    @token_required
    def update_global_automation_config(current_user, organization_id):
        """Update global automation configuration"""
        try:
            config_data = request.json
            if not config_data:
                return jsonify({"error": "Request body is required"}), 400

            config = automation_api.update_global_config(config_data, current_user)

            if "error" in config:
                return jsonify({"error": config["error"]}), 500

            return jsonify({"config": config}), 200

        except Exception as e:
            logger.error(f"Error updating global automation config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/global-toggle", methods=["POST"])
    @token_required
    def toggle_global_automation(current_user, organization_id):
        """Toggle global automation on/off"""
        try:
            data = request.json
            enabled = data.get("enabled", True)

            config = automation_api.toggle_global_automation(enabled, current_user)

            if "error" in config:
                return jsonify({"error": config["error"]}), 500

            return jsonify({"config": config}), 200

        except Exception as e:
            logger.error(f"Error toggling global automation: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Connection-Level Configuration Endpoints
    @app.route("/api/automation/connection-configs", methods=["GET"])
    @token_required
    def get_automation_connection_configs(current_user, organization_id):
        """Get all automation configurations for organization connections"""
        try:
            configs = automation_api.get_connection_configs(organization_id)
            return jsonify({"configs": configs}), 200

        except Exception as e:
            logger.error(f"Error getting connection configs: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/connection-configs/<connection_id>", methods=["GET"])
    @token_required
    def get_automation_connection_config(current_user, organization_id, connection_id):
        """Get automation configuration for a specific connection"""
        try:
            config = automation_api.get_connection_config(connection_id)

            if config is None:
                return jsonify({"error": "Connection not found"}), 404

            return jsonify({"config": config}), 200

        except Exception as e:
            logger.error(f"Error getting connection config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/connection-configs/<connection_id>", methods=["PUT"])
    @token_required
    def update_automation_connection_config(current_user, organization_id, connection_id):
        """Update automation configuration for a connection"""
        try:
            config_data = request.json
            if not config_data:
                return jsonify({"error": "Request body is required"}), 400

            config = automation_api.update_connection_config(connection_id, config_data, current_user)

            if "error" in config:
                return jsonify({"error": config["error"]}), 500

            return jsonify({"config": config}), 200

        except Exception as e:
            logger.error(f"Error updating connection config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Table-Level Configuration Endpoints
    @app.route("/api/automation/table-configs/<connection_id>/<table_name>", methods=["GET"])
    @token_required
    def get_automation_table_config(current_user, organization_id, connection_id, table_name):
        """Get automation configuration for a specific table"""
        try:
            config = automation_api.get_table_config(connection_id, table_name)

            if config is None:
                return jsonify({"error": "Table config not found"}), 404

            return jsonify({"config": config}), 200

        except Exception as e:
            logger.error(f"Error getting table config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/table-configs/<connection_id>/<table_name>", methods=["PUT"])
    @token_required
    def update_automation_table_config(current_user, organization_id, connection_id, table_name):
        """Update automation configuration for a table"""
        try:
            config_data = request.json
            if not config_data:
                return jsonify({"error": "Request body is required"}), 400

            config = automation_api.update_table_config(connection_id, table_name, config_data, current_user)

            if "error" in config:
                return jsonify({"error": config["error"]}), 500

            return jsonify({"config": config}), 200

        except Exception as e:
            logger.error(f"Error updating table config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Status and Monitoring Endpoints
    @app.route("/api/automation/status", methods=["GET"])
    @token_required
    def get_automation_status(current_user, organization_id):
        """Get automation system status"""
        try:
            status = automation_api.get_automation_status(organization_id=organization_id)
            return jsonify({"status": status}), 200

        except Exception as e:
            logger.error(f"Error getting automation status: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/status/<connection_id>", methods=["GET"])
    @token_required
    def get_connection_automation_status(current_user, organization_id, connection_id):
        """Get automation status for a specific connection"""
        try:
            status = automation_api.get_automation_status(connection_id=connection_id)
            return jsonify({"status": status}), 200

        except Exception as e:
            logger.error(f"Error getting connection automation status: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/jobs", methods=["GET"])
    @token_required
    def get_automation_jobs(current_user, organization_id):
        """Get automation jobs"""
        try:
            connection_id = request.args.get("connection_id")
            status = request.args.get("status")
            limit = int(request.args.get("limit", 50))

            jobs = automation_api.get_jobs(connection_id=connection_id, status=status, limit=limit)
            return jsonify({"jobs": jobs}), 200

        except Exception as e:
            logger.error(f"Error getting automation jobs: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Control Operation Endpoints
    @app.route("/api/automation/toggle/<connection_id>", methods=["POST"])
    @token_required
    def toggle_connection_automation(current_user, organization_id, connection_id):
        """Toggle automation for a specific connection"""
        try:
            data = request.json
            enabled = data.get("enabled", True)

            result = automation_api.toggle_connection_automation(connection_id, enabled, current_user)

            if "error" in result:
                return jsonify({"error": result["error"]}), 500

            return jsonify({"result": result}), 200

        except Exception as e:
            logger.error(f"Error toggling connection automation: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/trigger/<connection_id>", methods=["POST"])
    @token_required
    def trigger_automation(current_user, organization_id, connection_id):
        """Manually trigger automation for a connection"""
        try:
            data = request.json or {}
            automation_type = data.get("automation_type")  # metadata_refresh, schema_detection, validation_run

            result = automation_api.trigger_automation(connection_id, automation_type, current_user)

            if "error" in result:
                return jsonify({"error": result["error"]}), 500

            return jsonify({"result": result}), 200

        except Exception as e:
            logger.error(f"Error triggering automation: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/jobs/<job_id>/cancel", methods=["POST"])
    @token_required
    def cancel_automation_job(current_user, organization_id, job_id):
        """Cancel a scheduled or running automation job"""
        try:
            result = automation_api.cancel_job(job_id, current_user)

            if "error" in result:
                return jsonify({"error": result["error"]}), 500

            return jsonify({"result": result}), 200

        except Exception as e:
            logger.error(f"Error cancelling automation job: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Dashboard and Analytics Endpoints
    @app.route("/api/automation/dashboard", methods=["GET"])
    @token_required
    def get_automation_dashboard(current_user, organization_id):
        """Get automation dashboard data"""
        try:
            # Get overall status
            status = automation_api.get_automation_status(organization_id=organization_id)

            # Get recent jobs
            recent_jobs = automation_api.get_jobs(limit=10)

            # Get connection configs
            connection_configs = automation_api.get_connection_configs(organization_id)

            # Calculate summary statistics
            enabled_connections = len([c for c in connection_configs if
                                       c.get("metadata_refresh", {}).get("enabled") or
                                       c.get("schema_change_detection", {}).get("enabled") or
                                       c.get("validation_automation", {}).get("enabled")])

            dashboard_data = {
                "status": status,
                "recent_jobs": recent_jobs,
                "summary": {
                    "total_connections": len(connection_configs),
                    "enabled_connections": enabled_connections,
                    "active_jobs": status.get("active_jobs", 0),
                    "failed_jobs_24h": status.get("failed_jobs_24h", 0)
                }
            }

            return jsonify({"dashboard": dashboard_data}), 200

        except Exception as e:
            logger.error(f"Error getting automation dashboard: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Configuration Templates Endpoint
    @app.route("/api/automation/templates", methods=["GET"])
    @token_required
    def get_automation_templates(current_user, organization_id):
        """Get automation configuration templates"""
        try:
            templates = {
                "basic_monitoring": {
                    "name": "Basic Monitoring",
                    "description": "Basic metadata refresh and schema change detection",
                    "config": {
                        "metadata_refresh": {
                            "enabled": True,
                            "interval_hours": 24,
                            "types": ["tables", "columns"]
                        },
                        "schema_change_detection": {
                            "enabled": True,
                            "interval_hours": 12,
                            "auto_acknowledge_safe_changes": True
                        },
                        "validation_automation": {
                            "enabled": False
                        }
                    }
                },
                "comprehensive_monitoring": {
                    "name": "Comprehensive Monitoring",
                    "description": "Full metadata refresh, schema detection, and validation automation",
                    "config": {
                        "metadata_refresh": {
                            "enabled": True,
                            "interval_hours": 12,
                            "types": ["tables", "columns", "statistics"]
                        },
                        "schema_change_detection": {
                            "enabled": True,
                            "interval_hours": 6,
                            "auto_acknowledge_safe_changes": False
                        },
                        "validation_automation": {
                            "enabled": True,
                            "interval_hours": 24,
                            "auto_generate_for_new_tables": True
                        }
                    }
                },
                "validation_only": {
                    "name": "Validation Only",
                    "description": "Only automated validation runs",
                    "config": {
                        "metadata_refresh": {
                            "enabled": False
                        },
                        "schema_change_detection": {
                            "enabled": False
                        },
                        "validation_automation": {
                            "enabled": True,
                            "interval_hours": 12,
                            "auto_generate_for_new_tables": False
                        }
                    }
                }
            }

            return jsonify({"templates": templates}), 200

        except Exception as e:
            logger.error(f"Error getting automation templates: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Bulk Operations Endpoint
    @app.route("/api/automation/bulk-update", methods=["POST"])
    @token_required
    def bulk_update_automation_configs(current_user, organization_id):
        """Bulk update automation configurations for multiple connections"""
        try:
            data = request.json
            if not data:
                return jsonify({"error": "Request body is required"}), 400

            connection_ids = data.get("connection_ids", [])
            config_updates = data.get("config", {})

            if not connection_ids or not config_updates:
                return jsonify({"error": "connection_ids and config are required"}), 400

            results = []
            for connection_id in connection_ids:
                try:
                    result = automation_api.update_connection_config(connection_id, config_updates, current_user)
                    results.append({
                        "connection_id": connection_id,
                        "success": "error" not in result,
                        "result": result
                    })
                except Exception as e:
                    results.append({
                        "connection_id": connection_id,
                        "success": False,
                        "error": str(e)
                    })

            successful_updates = len([r for r in results if r["success"]])

            return jsonify({
                "results": results,
                "summary": {
                    "total": len(connection_ids),
                    "successful": successful_updates,
                    "failed": len(connection_ids) - successful_updates
                }
            }), 200

        except Exception as e:
            logger.error(f"Error in bulk automation config update: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Scheduler Management Endpoints (Admin only)
    @app.route("/api/automation/scheduler/status", methods=["GET"])
    @token_required
    def get_scheduler_status(current_user, organization_id):
        """Get automation scheduler status (admin only)"""
        try:
            # Check if user is admin
            from core.storage.supabase_manager import SupabaseManager
            supabase = SupabaseManager()
            user_role = supabase.get_user_role(current_user)

            if user_role not in ['admin', 'owner']:
                return jsonify({"error": "Insufficient permissions"}), 403

            scheduler_stats = automation_api.scheduler.get_scheduler_stats()
            return jsonify({"scheduler": scheduler_stats}), 200

        except Exception as e:
            logger.error(f"Error getting scheduler status: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/scheduler/restart", methods=["POST"])
    @token_required
    def restart_scheduler(current_user, organization_id):
        """Restart the automation scheduler (admin only)"""
        try:
            # Check if user is admin
            from core.storage.supabase_manager import SupabaseManager
            supabase = SupabaseManager()
            user_role = supabase.get_user_role(current_user)

            if user_role not in ['admin', 'owner']:
                return jsonify({"error": "Insufficient permissions"}), 403

            # Stop and restart scheduler
            automation_api.scheduler.stop()
            automation_api.scheduler.start()

            return jsonify({"message": "Scheduler restarted successfully"}), 200

        except Exception as e:
            logger.error(f"Error restarting scheduler: {str(e)}")
            return jsonify({"error": str(e)}), 500

    logger.info("Automation routes registered successfully")

    # Add these routes to backend/core/automation/routes.py

    @app.route("/api/automation/connections/<connection_id>/next-runs", methods=["GET"])
    @token_required
    def get_connection_next_runs(current_user, organization_id, connection_id):
        """Get next run times for a specific connection"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            next_runs = automation_api.get_next_run_times(organization_id, connection_id)
            return jsonify(next_runs), 200

        except Exception as e:
            logger.error(f"Error getting connection next runs: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/next-runs", methods=["GET"])
    @token_required
    def get_all_next_runs(current_user, organization_id):
        """Get next run times for all connections in the organization"""
        try:
            # Get all connections for the organization
            connections_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id, name") \
                .eq("organization_id", organization_id) \
                .execute()

            if not connections_response.data:
                return jsonify({"connections": [], "message": "No connections found"}), 200

            all_next_runs = {}

            for connection in connections_response.data:
                connection_id = connection["id"]
                connection_name = connection["name"]

                next_runs_data = automation_api.get_next_run_times(organization_id, connection_id)

                if next_runs_data.get("next_runs"):
                    all_next_runs[connection_id] = {
                        "connection_name": connection_name,
                        "next_runs": next_runs_data["next_runs"]
                    }

            return jsonify({
                "next_runs_by_connection": all_next_runs,
                "generated_at": datetime.now().isoformat()
            }), 200

        except Exception as e:
            logger.error(f"Error getting all next runs: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/status-enhanced", methods=["GET"])
    @token_required
    def get_enhanced_automation_status(current_user, organization_id):
        """Get automation status with next run times included"""
        try:
            connection_id = request.args.get("connection_id")

            status = automation_api.get_automation_status_with_next_runs(
                organization_id=organization_id,
                connection_id=connection_id
            )

            return jsonify({"status": status}), 200

        except Exception as e:
            logger.error(f"Error getting enhanced automation status: {str(e)}")
            return jsonify({"error": str(e)}), 500