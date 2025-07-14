from flask import Blueprint, request, jsonify
import logging
from datetime import datetime, timezone
from .events import get_automation_events, get_automation_event_stats
from .api import AutomationAPI
from core.utils.automation_diagnostics import AutomationDiagnosticUtility
from .schedule_manager import ScheduleManager, migrate_interval_to_schedule_configs
import time
from functools import wraps

logger = logging.getLogger(__name__)

# Initialize API (now uses SimplifiedAutomationScheduler internally)
automation_api = AutomationAPI()

# Initialize schedule manager
schedule_manager = ScheduleManager(automation_api.supabase)


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
            automation_type = data.get("automation_type")  # metadata_refresh, schema_change_detection, validation_automation

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
            enabled_connections = 0
            for config in connection_configs:
                # Check both new schedule format and legacy format
                if config.get("schedule_config"):
                    schedule_config = config["schedule_config"]
                    if any(schedule_config.get(auto_type, {}).get("enabled", False)
                          for auto_type in ["metadata_refresh", "schema_change_detection", "validation_automation"]):
                        enabled_connections += 1
                else:
                    # Legacy format check
                    if (config.get("metadata_refresh", {}).get("enabled") or
                        config.get("schema_change_detection", {}).get("enabled") or
                        config.get("validation_automation", {}).get("enabled")):
                        enabled_connections += 1

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

            scheduler_stats = automation_api.scheduler.get_scheduler_stats() if automation_api.scheduler else {"status": "not_available"}
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
            if automation_api.scheduler:
                automation_api.scheduler.stop()
                automation_api.scheduler.start()

            return jsonify({"message": "Scheduler restarted successfully"}), 200

        except Exception as e:
            logger.error(f"Error restarting scheduler: {str(e)}")
            return jsonify({"error": str(e)}), 500

    logger.info("Automation routes registered successfully")

    _rate_limit_cache = {}

    def rate_limit(seconds=30):
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                # Get client identifier (could be IP + user)
                client_id = f"{request.remote_addr}_{kwargs.get('connection_id', 'unknown')}"
                now = time.time()

                if client_id in _rate_limit_cache:
                    if now - _rate_limit_cache[client_id] < seconds:
                        return jsonify({"error": "Rate limit exceeded"}), 429

                _rate_limit_cache[client_id] = now
                return f(*args, **kwargs)

            return decorated_function

        return decorator

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
                "generated_at": datetime.now(timezone.utc).isoformat()
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

    @app.route("/api/automation/runs", methods=["GET"])
    @token_required
    def get_automation_runs(current_user, organization_id):
        """Get automation runs for the organization"""
        try:
            connection_id = request.args.get("connection_id")
            status = request.args.get("status")
            limit = int(request.args.get("limit", 50))

            # Build query
            query = automation_api.supabase.supabase.table("automation_runs").select("""
                *,
                automation_jobs!inner(connection_id, job_type),
                database_connections!inner(name, organization_id)
            """)

            # Filter by organization
            query = query.eq("database_connections.organization_id", organization_id)

            if connection_id:
                query = query.eq("connection_id", connection_id)

            if status:
                query = query.eq("status", status)

            response = query.order("created_at", desc=True).limit(limit).execute()

            runs = response.data if response.data else []

            # Clean up the data structure
            for run in runs:
                if 'automation_jobs' in run:
                    run['job_type'] = run['automation_jobs']['job_type']
                    del run['automation_jobs']
                if 'database_connections' in run:
                    run['connection_name'] = run['database_connections']['name']
                    del run['database_connections']

            return jsonify({
                "runs": runs,
                "count": len(runs)
            }), 200

        except Exception as e:
            logger.error(f"Error getting automation runs: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/runs/<run_id>", methods=["GET"])
    @token_required
    def get_automation_run_details(current_user, organization_id, run_id):
        """Get detailed information about a specific automation run"""
        try:
            response = automation_api.supabase.supabase.table("automation_runs") \
                .select("""
                    *,
                    automation_jobs!inner(connection_id, job_type, job_config),
                    database_connections!inner(name, organization_id)
                """) \
                .eq("id", run_id) \
                .eq("database_connections.organization_id", organization_id) \
                .single() \
                .execute()

            if not response.data:
                return jsonify({"error": "Run not found"}), 404

            run = response.data

            # Clean up the data structure
            if 'automation_jobs' in run:
                run['job_type'] = run['automation_jobs']['job_type']
                run['job_config'] = run['automation_jobs']['job_config']
                del run['automation_jobs']
            if 'database_connections' in run:
                run['connection_name'] = run['database_connections']['name']
                del run['database_connections']

            return jsonify({"run": run}), 200

        except Exception as e:
            logger.error(f"Error getting automation run details: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/events", methods=["GET"])
    @token_required
    def get_automation_events_endpoint(current_user, organization_id):
        """Get automation events for the organization"""
        try:
            connection_id = request.args.get("connection_id")
            event_type = request.args.get("event_type")
            limit = int(request.args.get("limit", 50))

            events = get_automation_events(
                connection_id=connection_id,
                organization_id=organization_id,
                event_type=event_type,
                limit=limit
            )

            return jsonify({
                "events": events,
                "count": len(events)
            }), 200

        except Exception as e:
            logger.error(f"Error getting automation events: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/events/stats", methods=["GET"])
    @token_required
    def get_automation_event_stats_endpoint(current_user, organization_id):
        """Get automation event statistics for the organization"""
        try:
            days = int(request.args.get("days", 7))

            stats = get_automation_event_stats(organization_id, days)

            return jsonify({"stats": stats}), 200

        except Exception as e:
            logger.error(f"Error getting automation event stats: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/dashboard/monitoring", methods=["GET"])
    @token_required
    def get_automation_monitoring_dashboard(current_user, organization_id):
        """Get comprehensive monitoring dashboard data"""
        try:
            connection_id = request.args.get("connection_id")

            # Get recent runs
            runs_query = automation_api.supabase.supabase.table("automation_runs") \
                .select("""
                    *,
                    automation_jobs!inner(connection_id, job_type),
                    database_connections!inner(name, organization_id)
                """) \
                .eq("database_connections.organization_id", organization_id)

            if connection_id:
                runs_query = runs_query.eq("connection_id", connection_id)

            recent_runs = runs_query.order("created_at", desc=True).limit(10).execute()

            # Get recent events
            recent_events = get_automation_events(
                connection_id=connection_id,
                organization_id=organization_id,
                limit=20
            )

            # Get event stats
            event_stats = get_automation_event_stats(organization_id, days=7)

            # Calculate run statistics
            runs = recent_runs.data if recent_runs.data else []
            run_stats = {
                "total_runs": len(runs),
                "successful_runs": len([r for r in runs if r["status"] == "completed"]),
                "failed_runs": len([r for r in runs if r["status"] == "failed"]),
                "running_runs": len([r for r in runs if r["status"] == "running"]),
                "runs_by_type": {}
            }

            # Count runs by type
            for run in runs:
                if 'automation_jobs' in run:
                    job_type = run['automation_jobs']['job_type']
                    if job_type not in run_stats["runs_by_type"]:
                        run_stats["runs_by_type"][job_type] = {"total": 0, "successful": 0, "failed": 0}
                    run_stats["runs_by_type"][job_type]["total"] += 1
                    if run["status"] == "completed":
                        run_stats["runs_by_type"][job_type]["successful"] += 1
                    elif run["status"] == "failed":
                        run_stats["runs_by_type"][job_type]["failed"] += 1

            # Clean up runs data
            for run in runs:
                if 'automation_jobs' in run:
                    run['job_type'] = run['automation_jobs']['job_type']
                    del run['automation_jobs']
                if 'database_connections' in run:
                    run['connection_name'] = run['database_connections']['name']
                    del run['database_connections']

            dashboard_data = {
                "recent_runs": runs,
                "recent_events": recent_events,
                "run_statistics": run_stats,
                "event_statistics": event_stats,
                "summary": {
                    "automation_health": "healthy" if run_stats["failed_runs"] < run_stats[
                        "successful_runs"] else "warning",
                    "recent_activity": len(recent_events),
                    "active_automations": run_stats["running_runs"]
                }
            }

            return jsonify({"dashboard": dashboard_data}), 200

        except Exception as e:
            logger.error(f"Error getting automation monitoring dashboard: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/runs/<run_id>/logs", methods=["GET"])
    @token_required
    def get_automation_run_logs(current_user, organization_id, run_id):
        """Get logs for a specific automation run"""
        try:
            # Verify the run belongs to the organization
            run_response = automation_api.supabase.supabase.table("automation_runs") \
                .select("""
                    id,
                    database_connections!inner(organization_id)
                """) \
                .eq("id", run_id) \
                .eq("database_connections.organization_id", organization_id) \
                .single() \
                .execute()

            if not run_response.data:
                return jsonify({"error": "Run not found"}), 404

            # Get related automation job for additional context
            job_response = automation_api.supabase.supabase.table("automation_jobs") \
                .select("*") \
                .eq("id", run_response.data["id"]) \
                .single() \
                .execute()

            # For now, return the job data as "logs" since we don't have separate log storage
            # In a full implementation, you'd have a separate logs table
            logs = {
                "run_id": run_id,
                "job_details": job_response.data if job_response.data else None,
                "message": "Detailed logging not yet implemented. Check job details and results for information."
            }

            return jsonify({"logs": logs}), 200

        except Exception as e:
            logger.error(f"Error getting automation run logs: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Add trigger endpoint for manual runs with better error handling
    @app.route("/api/automation/trigger-immediate/<connection_id>", methods=["POST"])
    @token_required
    def trigger_immediate_automation(current_user, organization_id, connection_id):
        """Trigger immediate automation run with detailed response"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id, name") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            data = request.json or {}
            automation_type = data.get("automation_type")  # metadata_refresh, schema_change_detection, validation_automation

            # Trigger the automation
            result = automation_api.trigger_automation(connection_id, automation_type, current_user)

            if "error" in result:
                return jsonify({"error": result["error"]}), 500

            # Get the connection name for response
            connection_name = connection_response.data[0]["name"]

            return jsonify({
                "success": True,
                "message": f"Automation triggered successfully for {connection_name}",
                "connection_id": connection_id,
                "connection_name": connection_name,
                "automation_type": automation_type or "all",
                "triggered_by": current_user,
                "result": result
            }), 200

        except Exception as e:
            logger.error(f"Error triggering immediate automation: {str(e)}")
            return jsonify({"error": str(e)}), 500

    logger.info("Automation monitoring routes registered successfully")

    @app.route("/api/automation/verify-storage/<connection_id>", methods=["POST"])
    @token_required
    def verify_automation_storage(current_user, organization_id, connection_id):
        """Verify that automation results are being stored properly"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Create verification utility
            from core.utils.storage_verification import StorageVerificationUtility
            verification_util = StorageVerificationUtility(automation_api.supabase)

            # Get verification parameters
            verification_type = request.json.get("type", "all")  # all, metadata, validation, schema
            expected_count = request.json.get("expected_count")

            results = {}

            if verification_type in ["all", "metadata"]:
                # Verify metadata storage
                for metadata_type in ["tables", "columns", "statistics"]:
                    verification = verification_util.verify_metadata_storage(
                        connection_id, metadata_type, expected_count
                    )
                    results[f"metadata_{metadata_type}"] = verification

            if verification_type in ["all", "validation"]:
                # Verify validation results storage
                verification = verification_util.verify_validation_results_storage(
                    connection_id, expected_count
                )
                results["validation_results"] = verification

            if verification_type in ["all", "schema"]:
                # Verify schema changes storage
                verification = verification_util.verify_schema_changes_storage(
                    connection_id, expected_count
                )
                results["schema_changes"] = verification

            # Get comprehensive health check
            health_check = verification_util.comprehensive_storage_health_check(connection_id)
            results["health_check"] = health_check

            return jsonify({
                "verification_results": results,
                "connection_id": connection_id
            }), 200

        except Exception as e:
            logger.error(f"Error verifying storage: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/storage-stats/<connection_id>", methods=["GET"])
    @token_required
    def get_automation_storage_stats(current_user, organization_id, connection_id):
        """Get storage statistics for automation"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Create verification utility
            from core.utils.storage_verification import StorageVerificationUtility
            verification_util = StorageVerificationUtility(automation_api.supabase)

            # Get parameters
            days = int(request.args.get("days", 7))

            # Get storage statistics
            stats = verification_util.get_storage_statistics(connection_id, days)

            return jsonify({
                "storage_statistics": stats,
                "connection_id": connection_id,
                "analysis_period_days": days
            }), 200

        except Exception as e:
            logger.error(f"Error getting storage statistics: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/diagnostics/<connection_id>", methods=["GET"])
    @token_required
    def diagnose_automation_issues(current_user, organization_id, connection_id):
        """Diagnose automation issues for a specific connection"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Create diagnostic utility
            diagnostic_util = AutomationDiagnosticUtility(automation_api.supabase)

            # Get diagnosis parameters
            days = int(request.args.get("days", 3))

            # Run diagnosis
            diagnosis = diagnostic_util.diagnose_automation_issues(connection_id, days)

            return jsonify({
                "diagnosis": diagnosis,
                "connection_id": connection_id,
                "organization_id": organization_id
            }), 200

        except Exception as e:
            logger.error(f"Error diagnosing automation issues: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/test-storage/<connection_id>", methods=["POST"])
    @token_required
    def test_automation_storage(current_user, organization_id, connection_id):
        """Test storage operations for automation"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Create diagnostic utility
            diagnostic_util = AutomationDiagnosticUtility(automation_api.supabase)

            # Test storage operations
            test_results = diagnostic_util.test_storage_operations(connection_id)

            return jsonify({
                "test_results": test_results,
                "connection_id": connection_id
            }), 200

        except Exception as e:
            logger.error(f"Error testing storage operations: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/fix-issues/<connection_id>", methods=["POST"])
    @token_required
    def fix_automation_issues(current_user, organization_id, connection_id):
        """Attempt to fix common automation issues"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Create diagnostic utility
            diagnostic_util = AutomationDiagnosticUtility(automation_api.supabase)

            # Attempt fixes
            fix_results = diagnostic_util.fix_common_issues(connection_id)

            return jsonify({
                "fix_results": fix_results,
                "connection_id": connection_id
            }), 200

        except Exception as e:
            logger.error(f"Error fixing automation issues: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/comprehensive-report/<connection_id>", methods=["GET"])
    @token_required
    def get_comprehensive_automation_report(current_user, organization_id, connection_id):
        """Get a comprehensive automation diagnostic report"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id, name") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            connection_name = connection_response.data[0]["name"]

            # Create diagnostic utility
            diagnostic_util = AutomationDiagnosticUtility(automation_api.supabase)

            # Generate comprehensive report
            report = diagnostic_util.create_comprehensive_report(connection_id)

            # Add connection info
            report["connection_name"] = connection_name
            report["organization_id"] = organization_id

            return jsonify({
                "report": report,
                "connection_id": connection_id,
                "connection_name": connection_name
            }), 200

        except Exception as e:
            logger.error(f"Error generating comprehensive report: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/debug-info", methods=["GET"])
    @token_required
    def get_automation_debug_info(current_user, organization_id):
        """Get debug information about the automation system"""
        try:
            debug_info = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "organization_id": organization_id,
                "scheduler_stats": {},
                "global_config": {},
                "environment_info": {},
                "service_health": {}
            }

            # Get scheduler stats
            try:
                debug_info["scheduler_stats"] = automation_api.scheduler.get_scheduler_stats() if automation_api.scheduler else {"status": "not_available"}
            except Exception as e:
                debug_info["scheduler_stats"] = {"error": str(e)}

            # Get global config
            try:
                debug_info["global_config"] = automation_api.get_global_config()
            except Exception as e:
                debug_info["global_config"] = {"error": str(e)}

            # Get environment info
            import os
            import sys
            debug_info["environment_info"] = {
                "environment": os.getenv("ENVIRONMENT", "unknown"),
                "scheduler_enabled": os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false"),
                "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                "utc_time": datetime.now(timezone.utc).isoformat()
            }

            # Get service health
            try:
                from core.automation.service import automation_service
                debug_info["service_health"] = automation_service.get_status()
            except Exception as e:
                debug_info["service_health"] = {"error": str(e)}

            return jsonify(debug_info), 200

        except Exception as e:
            logger.error(f"Error getting automation debug info: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Schedule Management Routes
    @app.route("/api/automation/schedules/<connection_id>", methods=["GET"])
    @token_required
    def get_connection_schedule(current_user, organization_id, connection_id):
        """Get schedule configuration for a connection"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id, name") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            connection_name = connection_response.data[0]["name"]

            # Get schedule configuration
            schedule_data = schedule_manager.get_connection_schedule(connection_id)

            if "error" in schedule_data:
                return jsonify({"error": schedule_data["error"]}), 500

            schedule_data["connection_name"] = connection_name
            return jsonify(schedule_data), 200

        except Exception as e:
            logger.error(f"Error getting connection schedule: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/schedules/<connection_id>", methods=["PUT"])
    @token_required
    def update_connection_schedule(current_user, organization_id, connection_id):
        """Update schedule configuration for a connection"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Get schedule configuration from request
            schedule_config = request.json.get("schedule_config")
            if not schedule_config:
                return jsonify({"error": "schedule_config is required"}), 400

            # Update schedule
            result = schedule_manager.update_connection_schedule(connection_id, schedule_config, current_user)

            if "error" in result:
                return jsonify({"error": result["error"]}), 400

            # Update the scheduler with new configuration
            if automation_api.scheduler:
                automation_api.scheduler.update_connection_schedule(connection_id, schedule_config)

            return jsonify(result), 200

        except Exception as e:
            logger.error(f"Error updating connection schedule: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/schedules/<connection_id>/next-runs", methods=["GET"])
    @token_required
    def get_connection_next_runs(current_user, organization_id, connection_id):
        """Get next run times for a connection's scheduled automations"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id, name") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Get scheduled jobs for this connection
            response = automation_api.supabase.supabase.table("automation_scheduled_jobs") \
                .select("*") \
                .eq("connection_id", connection_id) \
                .eq("enabled", True) \
                .execute()

            scheduled_jobs = response.data or []

            next_runs = {}
            for job in scheduled_jobs:
                automation_type = job["automation_type"]
                next_run_at = job.get("next_run_at")

                if next_run_at:
                    next_run_time = datetime.fromisoformat(next_run_at.replace('Z', '+00:00'))
                    time_until = schedule_manager._format_time_until(next_run_time)

                    next_runs[automation_type] = {
                        "next_run_iso": next_run_at,
                        "next_run_timestamp": next_run_time.timestamp(),
                        "time_until_next": time_until,
                        "schedule_type": job["schedule_type"],
                        "scheduled_time": job["scheduled_time"],
                        "timezone": job["timezone"],
                        "days_of_week": job.get("days_of_week")
                    }

            return jsonify({
                "connection_id": connection_id,
                "connection_name": connection_response.data[0]["name"],
                "next_runs": next_runs,
                "generated_at": datetime.now(timezone.utc).isoformat()
            }), 200

        except Exception as e:
            logger.error(f"Error getting next runs: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/schedules/all", methods=["GET"])
    @token_required
    def get_all_connection_schedules(current_user, organization_id):
        """Get schedule configurations for all connections in the organization"""
        try:
            # Get all connections for the organization
            connections_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id, name") \
                .eq("organization_id", organization_id) \
                .execute()

            if not connections_response.data:
                return jsonify({"connections": [], "message": "No connections found"}), 200

            all_schedules = {}

            for connection in connections_response.data:
                connection_id = connection["id"]
                connection_name = connection["name"]

                try:
                    schedule_data = schedule_manager.get_connection_schedule(connection_id)

                    if "error" not in schedule_data:
                        all_schedules[connection_id] = {
                            "connection_name": connection_name,
                            "schedule_config": schedule_data.get("schedule_config", {}),
                            "next_runs": schedule_data.get("next_runs", {})
                        }
                except Exception as conn_error:
                    logger.warning(f"Error getting schedule for connection {connection_id}: {str(conn_error)}")
                    continue

            return jsonify({
                "schedules_by_connection": all_schedules,
                "generated_at": datetime.now(timezone.utc).isoformat()
            }), 200

        except Exception as e:
            logger.error(f"Error getting all connection schedules: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/schedules/validate", methods=["POST"])
    @token_required
    def validate_schedule_config(current_user, organization_id):
        """Validate a schedule configuration without saving it"""
        try:
            schedule_config = request.json.get("schedule_config")
            if not schedule_config:
                return jsonify({"error": "schedule_config is required"}), 400

            # Validate the configuration
            validation_result = schedule_manager._validate_schedule_config(schedule_config)

            return jsonify(validation_result), 200

        except Exception as e:
            logger.error(f"Error validating schedule config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/schedules/templates", methods=["GET"])
    @token_required
    def get_schedule_templates(current_user, organization_id):
        """Get predefined schedule templates"""
        try:
            templates = {
                "business_hours_daily": {
                    "name": "Business Hours Daily",
                    "description": "Run automations daily during business hours",
                    "schedule_config": {
                        "metadata_refresh": {
                            "enabled": True,
                            "schedule_type": "daily",
                            "time": "06:00",
                            "timezone": "America/New_York"
                        },
                        "schema_change_detection": {
                            "enabled": True,
                            "schedule_type": "daily",
                            "time": "07:00",
                            "timezone": "America/New_York"
                        },
                        "validation_automation": {
                            "enabled": True,
                            "schedule_type": "daily",
                            "time": "08:00",
                            "timezone": "America/New_York"
                        }
                    }
                },
                "off_hours_daily": {
                    "name": "Off Hours Daily",
                    "description": "Run automations daily during off-peak hours",
                    "schedule_config": {
                        "metadata_refresh": {
                            "enabled": True,
                            "schedule_type": "daily",
                            "time": "02:00",
                            "timezone": "UTC"
                        },
                        "schema_change_detection": {
                            "enabled": True,
                            "schedule_type": "daily",
                            "time": "03:00",
                            "timezone": "UTC"
                        },
                        "validation_automation": {
                            "enabled": False,
                            "schedule_type": "daily",
                            "time": "04:00",
                            "timezone": "UTC"
                        }
                    }
                },
                "weekend_only": {
                    "name": "Weekend Only",
                    "description": "Run automations only on weekends",
                    "schedule_config": {
                        "metadata_refresh": {
                            "enabled": True,
                            "schedule_type": "weekly",
                            "time": "02:00",
                            "timezone": "UTC",
                            "days": ["saturday"]
                        },
                        "schema_change_detection": {
                            "enabled": True,
                            "schedule_type": "weekly",
                            "time": "03:00",
                            "timezone": "UTC",
                            "days": ["saturday"]
                        },
                        "validation_automation": {
                            "enabled": True,
                            "schedule_type": "weekly",
                            "time": "01:00",
                            "timezone": "UTC",
                            "days": ["sunday"]
                        }
                    }
                },
                "minimal_weekly": {
                    "name": "Minimal Weekly",
                    "description": "Minimal automation - once per week",
                    "schedule_config": {
                        "metadata_refresh": {
                            "enabled": True,
                            "schedule_type": "weekly",
                            "time": "02:00",
                            "timezone": "UTC",
                            "days": ["sunday"]
                        },
                        "schema_change_detection": {
                            "enabled": False,
                            "schedule_type": "weekly",
                            "time": "03:00",
                            "timezone": "UTC",
                            "days": ["sunday"]
                        },
                        "validation_automation": {
                            "enabled": False,
                            "schedule_type": "weekly",
                            "time": "04:00",
                            "timezone": "UTC",
                            "days": ["sunday"]
                        }
                    }
                }
            }

            return jsonify({"templates": templates}), 200

        except Exception as e:
            logger.error(f"Error getting schedule templates: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/migration/interval-to-schedule", methods=["POST"])
    @token_required
    def migrate_interval_configs(current_user, organization_id):
        """Migrate existing interval-based configs to schedule-based configs"""
        try:
            # Check if user has admin permissions
            user_role = automation_api.supabase.get_user_role(current_user)
            if user_role not in ['admin', 'owner']:
                return jsonify({"error": "Insufficient permissions"}), 403

            # Run migration
            result = migrate_interval_to_schedule_configs(automation_api.supabase)

            return jsonify({
                "migration_result": result,
                "message": "Migration completed successfully" if "error" not in result else "Migration failed"
            }), 200

        except Exception as e:
            logger.error(f"Error running migration: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/schedules/<connection_id>/disable", methods=["POST"])
    @token_required
    def disable_connection_automation(current_user, organization_id, connection_id):
        """Disable all automation for a connection"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Disable all scheduled jobs for this connection
            automation_api.supabase.supabase.table("automation_scheduled_jobs") \
                .update({"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()}) \
                .eq("connection_id", connection_id) \
                .execute()

            # Update schedule config to disable all
            disabled_config = {
                "metadata_refresh": {"enabled": False},
                "schema_change_detection": {"enabled": False},
                "validation_automation": {"enabled": False}
            }

            result = schedule_manager.update_connection_schedule(connection_id, disabled_config, current_user)

            return jsonify({
                "success": True,
                "message": "All automation disabled for connection",
                "result": result
            }), 200

        except Exception as e:
            logger.error(f"Error disabling connection automation: {str(e)}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/automation/schedules/<connection_id>/enable-default", methods=["POST"])
    @token_required
    def enable_default_schedule(current_user, organization_id, connection_id):
        """Enable default schedule for a connection"""
        try:
            # Verify connection belongs to organization
            connection_response = automation_api.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("id", connection_id) \
                .eq("organization_id", organization_id) \
                .execute()

            if not connection_response.data:
                return jsonify({"error": "Connection not found"}), 404

            # Get timezone from request or default to UTC
            user_timezone = request.json.get("timezone", "UTC") if request.json else "UTC"

            # Create default schedule configuration
            default_config = {
                "metadata_refresh": {
                    "enabled": True,
                    "schedule_type": "daily",
                    "time": "02:00",
                    "timezone": user_timezone
                },
                "schema_change_detection": {
                    "enabled": True,
                    "schedule_type": "daily",
                    "time": "03:00",
                    "timezone": user_timezone
                },
                "validation_automation": {
                    "enabled": False,  # Start with this disabled
                    "schedule_type": "weekly",
                    "time": "01:00",
                    "timezone": user_timezone,
                    "days": ["sunday"]
                }
            }

            # Update schedule
            result = schedule_manager.update_connection_schedule(connection_id, default_config, current_user)

            if "error" in result:
                return jsonify({"error": result["error"]}), 400

            return jsonify({
                "success": True,
                "message": "Default schedule enabled for connection",
                "schedule_config": default_config,
                "next_runs": result.get("next_runs", {})
            }), 200

        except Exception as e:
            logger.error(f"Error enabling default schedule: {str(e)}")
            return jsonify({"error": str(e)}), 500

    # Enhanced connection config route to handle both old and new formats
    @app.route("/api/automation/connection-configs/<connection_id>", methods=["PUT"])
    @token_required
    def update_automation_connection_config_enhanced(current_user, organization_id, connection_id):
        """Enhanced connection config update that handles both interval and schedule configs"""
        try:
            config_data = request.json
            if not config_data:
                return jsonify({"error": "Request body is required"}), 400

            # Check if this is a schedule-based update
            if "schedule_config" in config_data:
                # Use new schedule manager
                result = schedule_manager.update_connection_schedule(
                    connection_id,
                    config_data["schedule_config"],
                    current_user
                )

                if "error" in result:
                    return jsonify({"error": result["error"]}), 400

                return jsonify({"config": result}), 200
            else:
                # Use legacy automation API for backwards compatibility
                config = automation_api.update_connection_config(connection_id, config_data, current_user)

                if "error" in config:
                    return jsonify({"error": config["error"]}), 500

                return jsonify({"config": config}), 200

        except Exception as e:
            logger.error(f"Error updating connection config: {str(e)}")
            return jsonify({"error": str(e)}), 500

    logger.info("Schedule management routes registered successfully")