"""
UPDATED: AutomationService now uses the clean AutomationOrchestrator
instead of the complex SimplifiedAutomationScheduler.
"""

import logging
import threading
import time
from typing import Optional, Dict, Any
import os

from .orchestrator import AutomationOrchestrator  # NEW: Use clean orchestrator
from .events import set_event_handler_supabase
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


class AutomationService:
    """Main service class for managing the clean automation system"""

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        """Get singleton instance of AutomationService"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        """Initialize the automation service"""
        if AutomationService._instance is not None:
            raise Exception("AutomationService is a singleton. Use get_instance() instead.")

        try:
            self.supabase = SupabaseManager()
            self.orchestrator: Optional[AutomationOrchestrator] = None  # NEW: Use orchestrator
            self.running = False
            self.initialization_error = None

            # Set up event handler with Supabase
            set_event_handler_supabase(self.supabase)

            logger.info("AutomationService initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing AutomationService: {str(e)}")
            self.initialization_error = str(e)

    def start(self):
        """Start the automation service with clean orchestrator"""
        try:
            if self.initialization_error:
                logger.error(f"Cannot start automation service - initialization error: {self.initialization_error}")
                return False

            if self.running:
                logger.info("Automation service already running")
                return True

            # Check environment configuration
            environment = os.getenv("ENVIRONMENT", "development")

            # Check if explicitly disabled
            if os.getenv("DISABLE_AUTOMATION", "false").lower() == "true":
                logger.info("Automation explicitly disabled via DISABLE_AUTOMATION environment variable")
                return False

            # Allow enabling in development
            scheduler_enabled = os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"

            # Environment logic
            if environment == "development" and not scheduler_enabled:
                logger.info("Automation scheduler disabled in development environment")
                logger.info("Set ENABLE_AUTOMATION_SCHEDULER=true to enable automation in development")
                return False
            elif environment == "production":
                scheduler_enabled = True
                logger.info("Automation enabled in production environment")

            if not scheduler_enabled:
                logger.info(f"Automation scheduler not enabled for environment: {environment}")
                return False

            # Check global configuration
            try:
                global_config = self._get_global_config()
                if not global_config.get("automation_enabled", True):
                    logger.info("Global automation is disabled in configuration")
                    return False

                logger.info("Global automation is enabled")
            except Exception as config_error:
                logger.warning(f"Could not check global config, using defaults: {str(config_error)}")
                global_config = {"automation_enabled": True, "max_concurrent_jobs": 3}

            # NEW: Initialize clean orchestrator
            try:
                max_workers = global_config.get("max_concurrent_jobs", 3)
                logger.info(f"Starting clean automation orchestrator with {max_workers} max workers")

                self.orchestrator = AutomationOrchestrator(max_workers=max_workers)
                self.orchestrator.start()

                # Verify orchestrator started
                if not self.orchestrator.running:
                    raise Exception("Orchestrator failed to start properly")

                self.running = True
                logger.info(f"✓ Automation service started successfully in {environment} environment")
                return True

            except Exception as orchestrator_error:
                logger.error(f"Failed to start automation orchestrator: {str(orchestrator_error)}")
                self.orchestrator = None
                return False

        except Exception as e:
            logger.error(f"Error starting automation service: {str(e)}")
            return False

    def stop(self):
        """Stop the automation service with proper cleanup"""
        try:
            if not self.running:
                logger.debug("Automation service not running")
                return True

            # Stop orchestrator
            if self.orchestrator:
                try:
                    logger.info("Stopping automation orchestrator...")
                    self.orchestrator.stop()
                    logger.info("Automation orchestrator stopped successfully")
                except Exception as e:
                    logger.error(f"Error stopping orchestrator: {str(e)}")
                finally:
                    self.orchestrator = None

            self.running = False
            logger.info("Automation service stopped successfully")
            return True

        except Exception as e:
            logger.error(f"Error stopping automation service: {str(e)}")
            return False

    def restart(self):
        """Restart the automation service"""
        logger.info("Restarting automation service...")
        success = self.stop()
        if success:
            time.sleep(1)  # Brief pause
            return self.start()
        return False

    def is_running(self) -> bool:
        """Check if the automation service is running"""
        return self.running and self.orchestrator is not None

    def get_status(self) -> dict:
        """Get detailed status of the automation service"""
        status = {
            "running": self.running,
            "global_enabled": True,
            "orchestrator_active": False,
            "scheduler_type": "clean_orchestrator_v1",  # NEW: Updated type
            "initialization_error": self.initialization_error,
            "environment": os.getenv("ENVIRONMENT", "development"),
            "explicitly_enabled": os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true",
            "error": None
        }

        try:
            # Get global configuration
            global_config = self._get_global_config()
            status["global_enabled"] = global_config.get("automation_enabled", True)

            # Get orchestrator status
            if self.orchestrator:
                try:
                    orchestrator_stats = self.orchestrator.get_scheduler_stats()
                    status["orchestrator_active"] = orchestrator_stats.get("running", False)
                    status["orchestrator_stats"] = orchestrator_stats
                except Exception as orchestrator_error:
                    status["error"] = f"Error getting orchestrator stats: {str(orchestrator_error)}"

        except Exception as e:
            status["error"] = str(e)
            logger.error(f"Error getting automation service status: {str(e)}")

        return status

    def _get_global_config(self) -> dict:
        """Get global automation configuration"""
        try:
            if not self.supabase:
                logger.warning("No Supabase manager available, using default config")
                return {"automation_enabled": True, "max_concurrent_jobs": 3}

            response = self.supabase.supabase.table("automation_global_config") \
                .select("*") \
                .order("created_at", desc=True) \
                .limit(1) \
                .execute()

            if response.data and len(response.data) > 0:
                config = response.data[0]
                logger.debug("Retrieved global automation configuration from database")
                return config

            logger.info("No global automation config found, using defaults")
            return {
                "automation_enabled": True,
                "max_concurrent_jobs": 3,
                "default_retry_attempts": 2
            }

        except Exception as e:
            logger.warning(f"Error getting global config, using defaults: {str(e)}")
            return {"automation_enabled": True, "max_concurrent_jobs": 3}

    def update_global_config(self, config: dict):
        """Update global configuration and restart service if needed"""
        try:
            # Check if automation enabled status changed
            current_config = self._get_global_config()
            current_enabled = current_config.get("automation_enabled", True)
            new_enabled = config.get("automation_enabled", True)

            # If automation was disabled, stop the service
            if current_enabled and not new_enabled:
                logger.info("Automation disabled in config, stopping service")
                self.stop()

            # If automation was enabled, start the service
            elif not current_enabled and new_enabled:
                logger.info("Automation enabled in config, starting service")
                self.start()

            # If max workers changed, restart orchestrator
            elif (self.running and
                  current_config.get("max_concurrent_jobs") != config.get("max_concurrent_jobs")):
                logger.info("Max concurrent jobs changed, restarting orchestrator")
                self.restart()

        except Exception as e:
            logger.error(f"Error updating global config: {str(e)}")

    # NEW: Expose orchestrator methods for external access
    def schedule_immediate_run(self, connection_id: str, automation_type: str = None, trigger_user: str = None) -> Dict[
        str, Any]:
        """Schedule an immediate automation run"""
        if not self.orchestrator or not self.running:
            return {
                "success": False,
                "error": "Automation orchestrator not available"
            }

        return self.orchestrator.schedule_immediate_run(connection_id, automation_type, trigger_user)

    def update_connection_schedule(self, connection_id: str, schedule_config: Dict[str, Any]):
        """Update connection schedule"""
        if not self.orchestrator:
            raise Exception("Automation orchestrator not available")

        return self.orchestrator.update_connection_schedule(connection_id, schedule_config)


# Global service instance
automation_service = AutomationService.get_instance()


# Public API functions (unchanged for compatibility)
def start_automation_service():
    """Start the global automation service"""
    try:
        return automation_service.start()
    except Exception as e:
        logger.error(f"Error starting automation service: {str(e)}")
        return False


def stop_automation_service():
    """Stop the global automation service"""
    try:
        return automation_service.stop()
    except Exception as e:
        logger.error(f"Error stopping automation service: {str(e)}")
        return False


def get_automation_service_status():
    """Get the status of the global automation service"""
    try:
        return automation_service.get_status()
    except Exception as e:
        logger.error(f"Error getting automation service status: {str(e)}")
        return {"error": str(e), "running": False}


def initialize_automation_on_startup():
    """Initialize automation service when the application starts"""
    try:
        logger.info("Initializing automation service on startup...")
        success = automation_service.start()

        if success:
            logger.info("Automation service started successfully on startup")
        else:
            logger.warning("Automation service did not start on startup (this may be expected in development)")

        return success

    except Exception as e:
        logger.error(f"Error initializing automation on startup: {str(e)}")
        return False


def cleanup_automation_on_shutdown():
    """Clean up automation service when the application shuts down"""
    try:
        logger.info("Cleaning up automation service on shutdown...")
        automation_service.stop()
        logger.info("Automation service cleaned up successfully")

    except Exception as e:
        logger.error(f"Error cleaning up automation on shutdown: {str(e)}")


def automation_health_check():
    """Health check for automation service"""
    try:
        status = automation_service.get_status()

        environment = status.get("environment", "development")
        explicitly_enabled = status.get("explicitly_enabled", False)
        global_enabled = status.get("global_enabled", True)
        running = status.get("running", False)
        orchestrator_active = status.get("orchestrator_active", False)

        # In development, it's OK if automation is not running unless explicitly enabled
        if environment == "development" and not explicitly_enabled:
            return {
                "healthy": True,
                "message": "Automation disabled in development (set ENABLE_AUTOMATION_SCHEDULER=true to enable)",
                "status": status
            }

        # Service should be running if globally enabled and environment allows it
        if global_enabled and explicitly_enabled and not running:
            return {
                "healthy": False,
                "message": "Automation is enabled but service is not running",
                "status": status
            }

        # If running, orchestrator should be active
        if running and not orchestrator_active:
            return {
                "healthy": False,
                "message": "Service is running but orchestrator is not active",
                "status": status
            }

        # Check for initialization errors
        if status.get("initialization_error"):
            return {
                "healthy": False,
                "message": f"Initialization error: {status['initialization_error']}",
                "status": status
            }

        return {
            "healthy": True,
            "message": "Automation service is healthy" if running else "Automation service is properly disabled",
            "status": status
        }

    except Exception as e:
        return {
            "healthy": False,
            "message": f"Health check failed: {str(e)}",
            "status": {}
        }