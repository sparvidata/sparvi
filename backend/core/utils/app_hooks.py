import logging
import atexit
import signal
import sys
from typing import Optional

logger = logging.getLogger(__name__)

# Global reference to automation service
_automation_service: Optional[object] = None


def initialize_automation_system():
    """
    Initialize the automation system when the Flask app starts
    Call this from your app.py after creating the Flask app
    """
    try:
        global _automation_service

        # Import automation service
        from core.automation.service import automation_service
        _automation_service = automation_service

        logger.info("Starting automation system...")

        # Start the automation service
        success = _automation_service.start()

        if success:
            logger.info("Automation system started successfully")

            # Only register signal handlers if we're in the main thread
            try:
                import threading
                if threading.current_thread() is threading.main_thread():
                    # Register cleanup handlers only in main thread
                    atexit.register(cleanup_automation_system)
                    signal.signal(signal.SIGTERM, _signal_handler)
                    signal.signal(signal.SIGINT, _signal_handler)
                    logger.info("Signal handlers registered successfully")
                else:
                    logger.warning("Skipping signal handler registration (not in main thread)")
                    # Just register atexit cleanup which works from any thread
                    atexit.register(cleanup_automation_system)
            except Exception as signal_error:
                logger.warning(f"Could not register signal handlers: {signal_error}")
                # Still register atexit cleanup
                atexit.register(cleanup_automation_system)

        else:
            logger.warning("Automation system failed to start")

        return success

    except Exception as e:
        logger.error(f"Error initializing automation system: {str(e)}")
        return False


def cleanup_automation_system():
    """
    Clean up the automation system when the Flask app shuts down
    This is called automatically via atexit
    """
    try:
        global _automation_service

        if _automation_service and _automation_service.is_running():
            logger.info("Shutting down automation system...")
            _automation_service.stop()
            logger.info("Automation system shutdown complete")

    except Exception as e:
        logger.error(f"Error during automation cleanup: {str(e)}")


def get_automation_health():
    """
    Get automation system health status
    Use this for health check endpoints
    """
    try:
        global _automation_service

        if not _automation_service:
            return {
                "healthy": False,
                "message": "Automation service not initialized",
                "status": {}
            }

        status = _automation_service.get_status()

        # Determine health based on status
        healthy = (
                status.get("running", False) and
                status.get("global_enabled", False) and
                status.get("scheduler_active", False) and
                not status.get("error")
        )

        return {
            "healthy": healthy,
            "message": "Automation system is healthy" if healthy else "Automation system has issues",
            "status": status
        }

    except Exception as e:
        logger.error(f"Error checking automation health: {str(e)}")
        return {
            "healthy": False,
            "message": f"Health check failed: {str(e)}",
            "status": {}
        }


def restart_automation_system():
    """
    Restart the automation system
    Use this for admin endpoints
    """
    try:
        global _automation_service

        if not _automation_service:
            logger.error("Automation service not initialized")
            return False

        logger.info("Restarting automation system...")
        success = _automation_service.restart()

        if success:
            logger.info("Automation system restarted successfully")
        else:
            logger.error("Failed to restart automation system")

        return success

    except Exception as e:
        logger.error(f"Error restarting automation system: {str(e)}")
        return False


def _signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down automation system...")
    cleanup_automation_system()
    sys.exit(0)


# Health check endpoint function
def automation_health_endpoint():
    """
    Flask endpoint function for automation health checks
    Add this to your Flask app:

    @app.route('/health/automation')
    def automation_health():
        return jsonify(automation_health_endpoint())
    """
    health = get_automation_health()
    status_code = 200 if health["healthy"] else 503
    return health, status_code


# Admin control endpoint functions
def automation_control_endpoint(action: str):
    """
    Flask endpoint function for automation control
    Add this to your Flask app:

    @app.route('/admin/automation/<action>', methods=['POST'])
    @admin_required
    def automation_control(action):
        return jsonify(automation_control_endpoint(action))
    """
    if action == "restart":
        success = restart_automation_system()
        return {
            "success": success,
            "message": "Automation system restarted" if success else "Failed to restart automation system"
        }
    elif action == "status":
        return get_automation_health()
    elif action == "stop":
        cleanup_automation_system()
        return {"success": True, "message": "Automation system stopped"}
    elif action == "start":
        success = initialize_automation_system()
        return {
            "success": success,
            "message": "Automation system started" if success else "Failed to start automation system"
        }
    else:
        return {"success": False, "message": f"Unknown action: {action}"}


# Integration with existing metadata system
def integrate_with_metadata_system():
    """
    Integrate automation system with existing metadata task manager
    Call this after both systems are initialized
    """
    try:
        from core.metadata.manager import MetadataTaskManager
        from core.automation.events import set_event_handler_supabase
        from core.storage.supabase_manager import SupabaseManager

        # Get metadata task manager instance
        metadata_manager = MetadataTaskManager.get_instance()

        # Set up automation event handler with Supabase
        supabase_manager = SupabaseManager()
        set_event_handler_supabase(supabase_manager)

        logger.info("Automation system integrated with metadata system")
        return True

    except Exception as e:
        logger.error(f"Error integrating automation with metadata system: {str(e)}")
        return False


# Example usage in app.py:
"""
from core.utils.app_hooks import initialize_automation_system, integrate_with_metadata_system

# After creating Flask app and initializing other systems
def create_app():
    app = Flask(__name__)

    # ... other initialization ...

    # Initialize automation system
    initialize_automation_system()

    # Integrate with metadata system
    integrate_with_metadata_system()

    # Add health check endpoint
    @app.route('/health/automation')
    def automation_health():
        return jsonify(automation_health_endpoint())

    # Add admin control endpoint
    @app.route('/admin/automation/<action>', methods=['POST'])
    @token_required  # Your auth decorator
    def automation_control(current_user, organization_id, action):
        # Check if user is admin
        if not user_is_admin(current_user):
            return jsonify({"error": "Insufficient permissions"}), 403

        result, status_code = automation_control_endpoint(action)
        return jsonify(result), status_code

    return app
"""