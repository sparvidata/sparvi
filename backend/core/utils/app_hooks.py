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
    FIXED: Better error handling and environment logic
    """
    try:
        global _automation_service

        logger.info("Starting automation system initialization...")

        # Import automation service with better error handling
        try:
            from core.automation.service import automation_service
            _automation_service = automation_service
            logger.debug("Successfully imported automation service")
        except ImportError as import_error:
            logger.error(f"Failed to import automation service: {str(import_error)}")
            logger.warning("Automation system will not be available")
            return False
        except Exception as import_error:
            logger.error(f"Error importing automation service: {str(import_error)}")
            logger.warning("Automation system will not be available")
            return False

        # FIXED: Better service startup with proper error handling
        try:
            logger.info("Attempting to start automation service...")
            success = _automation_service.start()

            if success:
                logger.info("✓ Automation system started successfully")

                # Register cleanup handlers (only if successfully started)
                try:
                    import threading
                    if threading.current_thread() is threading.main_thread():
                        # Register cleanup handlers only in main thread
                        atexit.register(cleanup_automation_system)
                        signal.signal(signal.SIGTERM, _signal_handler)
                        signal.signal(signal.SIGINT, _signal_handler)
                        logger.debug("Signal handlers registered successfully")
                    else:
                        logger.debug("Skipping signal handler registration (not in main thread)")
                        # Just register atexit cleanup which works from any thread
                        atexit.register(cleanup_automation_system)
                except Exception as signal_error:
                    logger.warning(f"Could not register signal handlers: {signal_error}")
                    # Still register atexit cleanup
                    atexit.register(cleanup_automation_system)

                return True
            else:
                # FIXED: Don't treat this as an error - it's expected in some environments
                status = _automation_service.get_status()
                environment = status.get("environment", "unknown")
                explicitly_enabled = status.get("explicitly_enabled", False)

                if environment == "development" and not explicitly_enabled:
                    logger.info("Automation system not started (disabled in development environment)")
                    logger.info("This is expected behavior. Set ENABLE_AUTOMATION_SCHEDULER=true to enable.")
                else:
                    logger.warning("Automation system failed to start")

                return False

        except Exception as start_error:
            logger.error(f"Error starting automation service: {str(start_error)}")
            logger.warning("Automation system will not be available")
            return False

    except Exception as e:
        logger.error(f"Error initializing automation system: {str(e)}")
        logger.error(traceback.format_exc())
        logger.warning("Automation system will not be available")
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

        # Use the improved health check from the service
        from core.automation.service import automation_health_check
        return automation_health_check()

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


# Integration with existing metadata system
def integrate_with_metadata_system():
    """
    Integrate automation system with existing metadata task manager
    FIXED: Better error handling and validation
    """
    try:
        logger.info("Integrating automation system with metadata system...")

        # Test imports first
        try:
            from core.metadata.manager import MetadataTaskManager
            from core.automation.events import set_event_handler_supabase
            from core.storage.supabase_manager import SupabaseManager
        except ImportError as import_error:
            logger.warning(f"Could not import required modules for integration: {str(import_error)}")
            logger.warning("Automation will continue without metadata integration")
            return False

        # Get metadata task manager instance
        try:
            metadata_manager = MetadataTaskManager.get_instance()
            if not metadata_manager:
                logger.warning("Could not get metadata task manager instance")
                return False
        except Exception as e:
            logger.warning(f"Error getting metadata task manager: {str(e)}")
            return False

        # Set up automation event handler with Supabase
        try:
            supabase_manager = SupabaseManager()
            set_event_handler_supabase(supabase_manager)
        except Exception as e:
            logger.warning(f"Error setting up event handler: {str(e)}")
            return False

        logger.info("✓ Automation system integrated with metadata system")
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
    """
    health = get_automation_health()
    status_code = 200 if health["healthy"] else 503
    return health, status_code


# Admin control endpoint functions
def automation_control_endpoint(action: str):
    """
    Flask endpoint function for automation control
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


def get_automation_status():
    """Get detailed automation status for debugging"""
    try:
        global _automation_service

        if not _automation_service:
            return {
                "service_initialized": False,
                "message": "Automation service not initialized"
            }

        status = _automation_service.get_status()
        status["service_initialized"] = True
        return status

    except Exception as e:
        return {
            "service_initialized": False,
            "error": str(e),
            "message": "Error getting automation status"
        }