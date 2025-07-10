import logging
import atexit
import signal
import sys
import traceback
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

        logger.info("Starting simplified automation system...")

        # Import automation service with error handling
        try:
            from core.automation.service import automation_service
            _automation_service = automation_service
        except ImportError as import_error:
            logger.error(f"Failed to import automation service: {str(import_error)}")
            return False
        except Exception as import_error:
            logger.error(f"Error importing automation service: {str(import_error)}")
            return False

        # Start the automation service with new scheduler
        try:
            success = _automation_service.start()

            if success:
                logger.info("Simplified automation system started successfully")

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
                logger.warning("Simplified automation system failed to start")

            return success

        except Exception as start_error:
            logger.error(f"Error starting automation service: {str(start_error)}")
            return False

    except Exception as e:
        logger.error(f"Error initializing simplified automation system: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def cleanup_automation_system():
    """
    Clean up the automation system when the Flask app shuts down
    This is called automatically via atexit
    """
    try:
        global _automation_service

        if _automation_service and _automation_service.is_running():
            logger.info("Shutting down simplified automation system...")
            _automation_service.stop()
            logger.info("Simplified automation system shutdown complete")

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
            "message": "Simplified automation system is healthy" if healthy else "Simplified automation system has issues",
            "status": status,
            "version": "simplified_user_schedule"
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

        logger.info("Restarting simplified automation system...")
        success = _automation_service.restart()

        if success:
            logger.info("Simplified automation system restarted successfully")
        else:
            logger.error("Failed to restart simplified automation system")

        return success

    except Exception as e:
        logger.error(f"Error restarting automation system: {str(e)}")
        return False


def _signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down simplified automation system...")
    cleanup_automation_system()
    sys.exit(0)


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

        logger.info("Simplified automation system integrated with metadata system")
        return True

    except Exception as e:
        logger.error(f"Error integrating automation with metadata system: {str(e)}")
        logger.error(traceback.format_exc())
        # Don't fail the entire startup - just log and continue
        logger.warning("Continuing automation startup without metadata integration")
        return False


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
            "message": "Simplified automation system restarted" if success else "Failed to restart simplified automation system"
        }
    elif action == "status":
        return get_automation_health()
    elif action == "stop":
        cleanup_automation_system()
        return {"success": True, "message": "Simplified automation system stopped"}
    elif action == "start":
        success = initialize_automation_system()
        return {
            "success": success,
            "message": "Simplified automation system started" if success else "Failed to start simplified automation system"
        }
    else:
        return {"success": False, "message": f"Unknown action: {action}"}