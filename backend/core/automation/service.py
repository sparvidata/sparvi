import logging
import threading
import time
from typing import Optional
import os

from .scheduler import AutomationScheduler
from .events import set_event_handler_supabase
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


class AutomationService:
    """Main service class for managing the automation system"""

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

        self.supabase = SupabaseManager()
        self.scheduler: Optional[AutomationScheduler] = None
        self.running = False

        # Set up event handler with Supabase
        set_event_handler_supabase(self.supabase)

        logger.info("AutomationService initialized")

    def start(self):
        """Start the automation service with environment protection"""
        try:
            if self.running:
                logger.warning("Automation service already running")
                return False

            # CRITICAL: Environment protection
            environment = os.getenv("ENVIRONMENT", "development")
            scheduler_enabled = os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"

            if environment == "development" and not scheduler_enabled:
                logger.info("Automation scheduler disabled in development environment")
                logger.info("Set ENABLE_AUTOMATION_SCHEDULER=true to enable in development")
                return False

            # Check if automation is globally enabled
            global_config = self._get_global_config()
            if not global_config.get("automation_enabled", True):
                logger.info("Global automation is disabled, not starting service")
                return False

            # Initialize and start scheduler
            max_workers = global_config.get("max_concurrent_jobs", 3)
            self.scheduler = AutomationScheduler(max_workers=max_workers)
            self.scheduler.start()

            self.running = True
            logger.info(f"Automation service started successfully in {environment} environment")
            return True

        except Exception as e:
            logger.error(f"Error starting automation service: {str(e)}")
            return False

    def stop(self):
        """Stop the automation service"""
        try:
            if not self.running:
                logger.warning("Automation service not running")
                return False

            # Stop scheduler
            if self.scheduler:
                self.scheduler.stop()
                self.scheduler = None

            self.running = False
            logger.info("Automation service stopped successfully")
            return True

        except Exception as e:
            logger.error(f"Error stopping automation service: {str(e)}")
            return False

    def restart(self):
        """Restart the automation service"""
        logger.info("Restarting automation service")
        self.stop()
        time.sleep(1)  # Brief pause
        return self.start()

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
            "error": None
        }

        try:
            # Get global configuration
            global_config = self._get_global_config()
            status["global_enabled"] = global_config.get("automation_enabled", True)

            # Get scheduler status
            if self.scheduler:
                scheduler_stats = self.scheduler.get_scheduler_stats()
                status["scheduler_active"] = scheduler_stats.get("running", False)
                status["active_jobs"] = scheduler_stats.get("active_jobs", 0)

        except Exception as e:
            status["error"] = str(e)
            logger.error(f"Error getting automation service status: {str(e)}")

        return status

    def _get_global_config(self) -> dict:
        """Get global automation configuration"""
        try:
            response = self.supabase.supabase.table("automation_global_config") \
                .select("*") \
                .order("created_at", desc=True) \
                .limit(1) \
                .execute()

            if response.data and len(response.data) > 0:
                return response.data[0]

            # Return default config if none exists
            return {
                "automation_enabled": True,
                "max_concurrent_jobs": 3,
                "default_retry_attempts": 2
            }

        except Exception as e:
            logger.error(f"Error getting global config: {str(e)}")
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
                logger.info("Automation disabled, stopping service")
                self.stop()

            # If automation was enabled, start the service
            elif not current_enabled and new_enabled:
                logger.info("Automation enabled, starting service")
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
    return automation_service.start()


def stop_automation_service():
    """Stop the global automation service"""
    return automation_service.stop()


def get_automation_service_status():
    """Get the status of the global automation service"""
    return automation_service.get_status()


# Application startup hook
def initialize_automation_on_startup():
    """Initialize automation service when the application starts"""
    try:
        logger.info("Initializing automation service on startup")

        # Start the service
        success = automation_service.start()

        if success:
            logger.info("Automation service started successfully on startup")
        else:
            logger.warning("Automation service did not start on startup")

    except Exception as e:
        logger.error(f"Error initializing automation on startup: {str(e)}")


# Application shutdown hook
def cleanup_automation_on_shutdown():
    """Clean up automation service when the application shuts down"""
    try:
        logger.info("Cleaning up automation service on shutdown")
        automation_service.stop()
        logger.info("Automation service cleaned up successfully")

    except Exception as e:
        logger.error(f"Error cleaning up automation on shutdown: {str(e)}")


# Health check function
def automation_health_check():
    """Health check for automation service"""
    try:
        status = automation_service.get_status()

        # Service should be running if globally enabled
        if status["global_enabled"] and not status["running"]:
            return {
                "healthy": False,
                "message": "Automation is enabled but service is not running",
                "status": status
            }

        # If running, scheduler should be active
        if status["running"] and not status["scheduler_active"]:
            return {
                "healthy": False,
                "message": "Service is running but scheduler is not active",
                "status": status
            }

        return {
            "healthy": True,
            "message": "Automation service is healthy",
            "status": status
        }

    except Exception as e:
        return {
            "healthy": False,
            "message": f"Health check failed: {str(e)}",
            "status": {}
        }