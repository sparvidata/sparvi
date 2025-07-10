import logging
import threading
import time
from typing import Optional
import os

from .simplified_scheduler import SimplifiedAutomationScheduler
from .events import set_event_handler_supabase
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


class AutomationService:
    """Main service class for managing the simplified automation system"""

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
            self.scheduler: Optional[SimplifiedAutomationScheduler] = None
            self.running = False
            self.initialization_error = None

            # Set up event handler with Supabase
            set_event_handler_supabase(self.supabase)

            logger.info("AutomationService initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing AutomationService: {str(e)}")
            self.initialization_error = str(e)
            # Don't raise - allow service to exist but track the error

    def start(self):
        """Start the automation service with improved error handling"""
        try:
            if self.initialization_error:
                logger.error(f"Cannot start automation service - initialization error: {self.initialization_error}")
                return False

            if self.running:
                logger.info("Automation service already running")
                return True

            # FIXED: More flexible environment handling
            environment = os.getenv("ENVIRONMENT", "development")

            # Check if explicitly disabled
            if os.getenv("DISABLE_AUTOMATION", "false").lower() == "true":
                logger.info("Automation explicitly disabled via DISABLE_AUTOMATION environment variable")
                return False

            # Allow enabling in development
            scheduler_enabled = os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"

            # FIXED: Less restrictive logic - allow in development if explicitly enabled
            if environment == "development" and not scheduler_enabled:
                logger.info("Automation scheduler disabled in development environment")
                logger.info("Set ENABLE_AUTOMATION_SCHEDULER=true to enable automation in development")
                return False
            elif environment == "production":
                # In production, default to enabled unless explicitly disabled
                scheduler_enabled = True
                logger.info("Automation enabled in production environment")

            if not scheduler_enabled:
                logger.info(f"Automation scheduler not enabled for environment: {environment}")
                return False

            # Check if automation is globally enabled (with better error handling)
            try:
                global_config = self._get_global_config()
                if not global_config.get("automation_enabled", True):
                    logger.info("Global automation is disabled in configuration")
                    return False

                logger.info("Global automation is enabled")
            except Exception as config_error:
                logger.warning(f"Could not check global config, using defaults: {str(config_error)}")
                global_config = {"automation_enabled": True, "max_concurrent_jobs": 3}

            # FIXED: Better scheduler initialization with error handling
            try:
                max_workers = global_config.get("max_concurrent_jobs", 3)
                logger.info(f"Starting automation scheduler with {max_workers} max workers")

                self.scheduler = SimplifiedAutomationScheduler(max_workers=max_workers)

                # Test scheduler initialization before marking as started
                if not self.scheduler:
                    raise Exception("Failed to create scheduler instance")

                self.scheduler.start()

                # Verify scheduler actually started
                if hasattr(self.scheduler, 'running') and not self.scheduler.running:
                    raise Exception("Scheduler failed to start properly")

                self.running = True
                logger.info(f"Automation service started successfully in {environment} environment")
                return True

            except Exception as scheduler_error:
                logger.error(f"Failed to start automation scheduler: {str(scheduler_error)}")
                self.scheduler = None
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

            # Stop scheduler
            if self.scheduler:
                try:
                    logger.info("Stopping automation scheduler...")
                    self.scheduler.stop()
                    logger.info("Automation scheduler stopped successfully")
                except Exception as e:
                    logger.error(f"Error stopping scheduler: {str(e)}")
                finally:
                    self.scheduler = None

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
        return self.running and self.scheduler is not None

    def get_status(self) -> dict:
        """Get detailed status of the automation service"""
        status = {
            "running": self.running,
            "global_enabled": True,
            "scheduler_active": False,
            "active_jobs": 0,
            "scheduler_type": "simplified_user_schedule",
            "initialization_error": self.initialization_error,
            "environment": os.getenv("ENVIRONMENT", "development"),
            "explicitly_enabled": os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true",
            "error": None
        }

        try:
            # Get global configuration
            global_config = self._get_global_config()
            status["global_enabled"] = global_config.get("automation_enabled", True)

            # Get scheduler status
            if self.scheduler:
                try:
                    scheduler_stats = self.scheduler.get_scheduler_stats()
                    status["scheduler_active"] = scheduler_stats.get("running", False)
                    status["active_jobs"] = scheduler_stats.get("active_jobs", 0)
                    status["scheduler_stats"] = scheduler_stats
                except Exception as scheduler_error:
                    status["error"] = f"Error getting scheduler stats: {str(scheduler_error)}"

        except Exception as e:
            status["error"] = str(e)
            logger.error(f"Error getting automation service status: {str(e)}")

        return status

    def _get_global_config(self) -> dict:
        """Get global automation configuration with improved error handling"""
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

            # Return default config if none exists
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

            # If max workers changed, restart scheduler
            elif (self.running and
                  current_config.get("max_concurrent_jobs") != config.get("max_concurrent_jobs")):
                logger.info("Max concurrent jobs changed, restarting scheduler")
                self.restart()

        except Exception as e:
            logger.error(f"Error updating global config: {str(e)}")


# Global service instance
automation_service = AutomationService.get_instance()


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


# Application startup hook
def initialize_automation_on_startup():
    """Initialize automation service when the application starts"""
    try:
        logger.info("Initializing automation service on startup...")

        # Start the service
        success = automation_service.start()

        if success:
            logger.info("Automation service started successfully on startup")
        else:
            logger.warning("Automation service did not start on startup (this may be expected in development)")

        return success

    except Exception as e:
        logger.error(f"Error initializing automation on startup: {str(e)}")
        return False


# Application shutdown hook
def cleanup_automation_on_shutdown():
    """Clean up automation service when the application shuts down"""
    try:
        logger.info("Cleaning up automation service on shutdown...")
        automation_service.stop()
        logger.info("Automation service cleaned up successfully")

    except Exception as e:
        logger.error(f"Error cleaning up automation on shutdown: {str(e)}")


# Health check function
def automation_health_check():
    """Health check for automation service"""
    try:
        status = automation_service.get_status()

        # FIXED: More nuanced health checking
        environment = status.get("environment", "development")
        explicitly_enabled = status.get("explicitly_enabled", False)
        global_enabled = status.get("global_enabled", True)
        running = status.get("running", False)
        scheduler_active = status.get("scheduler_active", False)

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

        # If running, scheduler should be active
        if running and not scheduler_active:
            return {
                "healthy": False,
                "message": "Service is running but scheduler is not active",
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