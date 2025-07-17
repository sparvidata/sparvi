import logging
import threading
import time
from typing import Optional, Dict, Any
import os

logger = logging.getLogger(__name__)


class UnifiedAutomationManager:
    """
    Single point of control for ALL automation systems.
    Prevents duplicate schedulers and job conflicts.
    """

    _instance = None
    _lock = threading.Lock()
    _initialization_lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        """Thread-safe singleton instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        if UnifiedAutomationManager._instance is not None:
            raise Exception("UnifiedAutomationManager is a singleton!")

        # Initialization flags to prevent duplicate setup
        self.initialized = False
        self.routes_registered = False  # NEW: Track route registration
        self.main_scheduler_running = False
        self.anomaly_scheduler_running = False

        # Scheduler instances
        self.main_scheduler = None
        self.anomaly_scheduler = None

        # Configuration
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.automation_enabled = self._should_enable_automation()

        logger.info(
            f"UnifiedAutomationManager created - Environment: {self.environment}, Enabled: {self.automation_enabled}")

    def _should_enable_automation(self) -> bool:
        """Centralized logic for determining if automation should be enabled"""
        # Check if explicitly disabled
        if os.getenv("DISABLE_AUTOMATION", "false").lower() == "true":
            return False

        # In development, require explicit enabling
        if self.environment == "development":
            return os.getenv("ENABLE_AUTOMATION_SCHEDULER", "false").lower() == "true"

        # In production, default to enabled
        return True

    def initialize_all_systems(self, flask_app=None, token_required=None) -> Dict[str, bool]:
        """
        Initialize all automation systems in the correct order.

        Args:
            flask_app: Flask application instance (required for route registration)
            token_required: Authentication decorator (required for route registration)

        Returns:
            Dictionary with status of each system.
        """
        with self._initialization_lock:
            if self.initialized:
                logger.info("Automation systems already initialized")
                return self.get_system_status()

            logger.info("=== INITIALIZING UNIFIED AUTOMATION SYSTEMS ===")

            results = {
                "main_automation": False,
                "anomaly_detection": False,
                "anomaly_routes": False,  # NEW: Track route registration
                "metadata_integration": False
            }

            try:
                # 1. Register anomaly routes FIRST (if Flask app provided)
                if flask_app and token_required:
                    logger.info("Step 1: Registering anomaly detection routes...")
                    results["anomaly_routes"] = self._register_anomaly_routes(flask_app, token_required)
                else:
                    logger.warning("Flask app or token_required not provided - skipping route registration")

                if not self.automation_enabled:
                    logger.info("Automation disabled - skipping scheduler initialization")
                    self.initialized = True
                    return results

                # 2. Initialize main automation scheduler
                logger.info("Step 2: Initializing main automation scheduler...")
                results["main_automation"] = self._initialize_main_scheduler()

                # 3. Initialize anomaly detection scheduler
                logger.info("Step 3: Initializing anomaly detection scheduler...")
                results["anomaly_detection"] = self._initialize_anomaly_scheduler()

                # 4. Set up metadata integration
                logger.info("Step 4: Setting up metadata integration...")
                results["metadata_integration"] = self._setup_metadata_integration()

                self.initialized = True

                # Log final status
                successful_systems = sum(1 for success in results.values() if success)
                logger.info(f"=== AUTOMATION INITIALIZATION COMPLETE ===")
                logger.info(f"Successfully initialized {successful_systems}/4 systems")

                for system, success in results.items():
                    status = "✓ SUCCESS" if success else "✗ FAILED"
                    logger.info(f"  {system}: {status}")

                return results

            except Exception as e:
                logger.error(f"Critical error in automation initialization: {str(e)}")
                self.initialized = True  # Mark as initialized to prevent retry loops
                return results

    def _register_anomaly_routes(self, flask_app, token_required) -> bool:
        """
        Register anomaly detection routes with Flask app.
        Enhanced with detailed error logging.
        """
        try:
            if self.routes_registered:
                logger.info("Anomaly routes already registered")
                return True

            logger.info("Starting anomaly route registration...")

            # Check if required parameters are valid
            if not flask_app:
                logger.error("Flask app is None - cannot register routes")
                return False

            if not token_required:
                logger.error("token_required decorator is None - cannot register routes")
                return False

            logger.info("Flask app and token_required are valid")

            # Try to import the registration function
            try:
                from core.anomalies.routes import register_anomaly_routes
                logger.info("Successfully imported register_anomaly_routes")
            except ImportError as import_error:
                logger.error(f"Failed to import register_anomaly_routes: {import_error}")
                return False

            # Check current route count before registration
            routes_before = len(list(flask_app.url_map.iter_rules()))
            logger.info(f"Route count before registration: {routes_before}")

            # Try to register the routes
            try:
                register_anomaly_routes(flask_app, token_required)
                logger.info("register_anomaly_routes function called successfully")
            except Exception as reg_error:
                logger.error(f"Error calling register_anomaly_routes: {reg_error}")
                logger.error(f"Error type: {type(reg_error)}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                return False

            # Check route count after registration
            routes_after = len(list(flask_app.url_map.iter_rules()))
            new_routes = routes_after - routes_before
            logger.info(f"Route count after registration: {routes_after} (+{new_routes} new routes)")

            # Verify anomaly routes were actually added
            anomaly_routes = [
                rule.rule for rule in flask_app.url_map.iter_rules()
                if 'anomalies' in rule.rule
            ]

            if len(anomaly_routes) == 0:
                logger.error("No anomaly routes found after registration!")
                return False

            logger.info(f"Successfully registered {len(anomaly_routes)} anomaly routes:")
            for route in anomaly_routes:
                logger.info(f"  - {route}")

            self.routes_registered = True
            logger.info("✓ Anomaly detection routes registered successfully")
            return True

        except Exception as e:
            logger.error(f"Unexpected error in route registration: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    # Also add this debug method to your UnifiedAutomationManager class:

    def debug_route_registration(self, flask_app, token_required):
        """Debug method to test route registration in isolation"""
        logger.info("=== DEBUGGING ROUTE REGISTRATION ===")

        # Test 1: Check imports
        try:
            from core.anomalies.routes import register_anomaly_routes
            from core.anomalies.api import AnomalyAPI
            logger.info("✓ All anomaly modules import successfully")
        except Exception as e:
            logger.error(f"✗ Import error: {e}")
            return False

        # Test 2: Check Flask app
        logger.info(f"Flask app type: {type(flask_app)}")
        logger.info(f"Flask app name: {flask_app.name if flask_app else 'None'}")

        # Test 3: Check token_required
        logger.info(f"token_required type: {type(token_required)}")
        logger.info(f"token_required callable: {callable(token_required) if token_required else 'None'}")

        # Test 4: Try to create AnomalyAPI instance
        try:
            api = AnomalyAPI()
            logger.info("✓ AnomalyAPI instance created successfully")
        except Exception as e:
            logger.error(f"✗ AnomalyAPI creation failed: {e}")
            return False

        # Test 5: Try route registration
        try:
            register_anomaly_routes(flask_app, token_required)
            logger.info("✓ Route registration completed without errors")
            return True
        except Exception as e:
            logger.error(f"✗ Route registration failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def _initialize_main_scheduler(self) -> bool:
        """Initialize the main automation scheduler (SimplifiedAutomationScheduler)"""
        try:
            if self.main_scheduler_running:
                logger.info("Main scheduler already running")
                return True

            from .service import AutomationService

            # Get the singleton service
            automation_service = AutomationService.get_instance()

            # Start the service
            success = automation_service.start()

            if success:
                self.main_scheduler = automation_service
                self.main_scheduler_running = True
                logger.info("✓ Main automation scheduler started successfully")
                return True
            else:
                logger.warning("Main automation scheduler failed to start")
                return False

        except Exception as e:
            logger.error(f"Error initializing main scheduler: {str(e)}")
            return False

    def _initialize_anomaly_scheduler(self) -> bool:
        """Initialize anomaly detection scheduler (separate system)"""
        try:
            if self.anomaly_scheduler_running:
                logger.info("Anomaly scheduler already running")
                return True

            from core.anomalies.scheduler_service import AnomalyDetectionSchedulerService

            self.anomaly_scheduler = AnomalyDetectionSchedulerService()
            self.anomaly_scheduler.start()

            # Verify it started
            if hasattr(self.anomaly_scheduler, 'running') and self.anomaly_scheduler.running:
                self.anomaly_scheduler_running = True
                logger.info("✓ Anomaly detection scheduler started successfully")
                return True
            else:
                logger.warning("Anomaly detection scheduler failed to start properly")
                return False

        except Exception as e:
            logger.error(f"Error initializing anomaly scheduler: {str(e)}")
            return False

    def _setup_metadata_integration(self) -> bool:
        """Set up integration between automation and metadata systems"""
        try:
            from core.utils.app_hooks import integrate_with_metadata_system
            from core.automation.events import set_event_handler_supabase
            from core.storage.supabase_manager import SupabaseManager

            # Set up event handler
            supabase_manager = SupabaseManager()
            set_event_handler_supabase(supabase_manager)

            # Integrate with metadata system
            integration_success = integrate_with_metadata_system()

            if integration_success:
                logger.info("✓ Metadata integration set up successfully")
                return True
            else:
                logger.warning("Metadata integration failed")
                return False

        except Exception as e:
            logger.error(f"Error setting up metadata integration: {str(e)}")
            return False

    def stop_all_systems(self) -> bool:
        """Stop all automation systems"""
        logger.info("=== STOPPING ALL AUTOMATION SYSTEMS ===")

        success = True

        # Stop main scheduler
        if self.main_scheduler and self.main_scheduler_running:
            try:
                self.main_scheduler.stop()
                self.main_scheduler_running = False
                logger.info("✓ Main scheduler stopped")
            except Exception as e:
                logger.error(f"Error stopping main scheduler: {str(e)}")
                success = False

        # Stop anomaly scheduler
        if self.anomaly_scheduler and self.anomaly_scheduler_running:
            try:
                self.anomaly_scheduler.stop()
                self.anomaly_scheduler_running = False
                logger.info("✓ Anomaly scheduler stopped")
            except Exception as e:
                logger.error(f"Error stopping anomaly scheduler: {str(e)}")
                success = False

        self.initialized = False
        self.routes_registered = False  # Reset route registration flag
        return success

    def restart_all_systems(self, flask_app=None, token_required=None) -> Dict[str, bool]:
        """Restart all automation systems"""
        logger.info("=== RESTARTING ALL AUTOMATION SYSTEMS ===")

        # Stop everything first
        self.stop_all_systems()

        # Wait a moment
        time.sleep(2)

        # Reinitialize
        self.initialized = False
        return self.initialize_all_systems(flask_app, token_required)

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive status of all automation systems"""
        status = {
            "initialized": self.initialized,
            "routes_registered": self.routes_registered,  # NEW: Include route status
            "automation_enabled": self.automation_enabled,
            "environment": self.environment,
            "main_scheduler": {
                "running": self.main_scheduler_running,
                "instance_exists": self.main_scheduler is not None,
                "details": {}
            },
            "anomaly_scheduler": {
                "running": self.anomaly_scheduler_running,
                "instance_exists": self.anomaly_scheduler is not None,
                "details": {}
            }
        }

        # Get detailed status from each system
        try:
            if self.main_scheduler:
                status["main_scheduler"]["details"] = self.main_scheduler.get_status()
        except Exception as e:
            status["main_scheduler"]["error"] = str(e)

        try:
            if self.anomaly_scheduler:
                status["anomaly_scheduler"]["details"] = {
                    "running": getattr(self.anomaly_scheduler, 'running', False)
                }
        except Exception as e:
            status["anomaly_scheduler"]["error"] = str(e)

        return status

    def schedule_immediate_job(self, connection_id: str, job_type: str, trigger_user: str = None) -> Dict[str, Any]:
        """
        Schedule an immediate job through the appropriate scheduler.
        This prevents duplicate job submission.
        """
        try:
            if not self.automation_enabled:
                return {
                    "success": False,
                    "error": "Automation system not available"
                }

            # Route to appropriate scheduler
            if job_type in ["metadata_refresh", "schema_change_detection", "validation_automation"]:
                if not self.main_scheduler_running:
                    return {"success": False, "error": "Main scheduler not running"}

                if hasattr(self.main_scheduler, 'scheduler'):
                    return self.main_scheduler.scheduler.schedule_immediate_run(
                        connection_id, job_type, trigger_user
                    )
                else:
                    return {"success": False, "error": "Main scheduler not available"}

            elif job_type == "anomaly_detection":
                if not self.anomaly_scheduler_running:
                    return {"success": False, "error": "Anomaly scheduler not running"}

                if self.anomaly_scheduler:
                    # Use the anomaly scheduler's detection run method
                    from core.anomalies.scheduler import AnomalyDetectionScheduler
                    scheduler = AnomalyDetectionScheduler()
                    return scheduler.schedule_detection_run(
                        organization_id=None,  # Will need to get from connection
                        connection_id=connection_id,
                        trigger_type='manual'
                    )
                else:
                    return {"success": False, "error": "Anomaly scheduler not available"}

            else:
                return {"success": False, "error": f"Unknown job type: {job_type}"}

        except Exception as e:
            logger.error(f"Error scheduling immediate job: {str(e)}")
            return {"success": False, "error": str(e)}

    def register_routes_only(self, flask_app, token_required) -> bool:
        """
        Public method to register only the anomaly routes without starting schedulers.
        Useful when you want API access but not background processing.
        """
        return self._register_anomaly_routes(flask_app, token_required)


# Global instance
unified_manager = UnifiedAutomationManager.get_instance()


# Updated public API functions
def initialize_unified_automation(flask_app=None, token_required=None):
    """
    Initialize all automation systems through unified manager.

    Args:
        flask_app: Flask application instance (required for route registration)
        token_required: Authentication decorator (required for route registration)
    """
    return unified_manager.initialize_all_systems(flask_app, token_required)


def register_anomaly_routes_only(flask_app, token_required):
    """
    Register only the anomaly detection routes without starting schedulers.
    Use this in your Flask app initialization.
    """
    return unified_manager.register_routes_only(flask_app, token_required)


def stop_unified_automation():
    """Stop all automation systems"""
    return unified_manager.stop_all_systems()


def restart_unified_automation(flask_app=None, token_required=None):
    """Restart all automation systems"""
    return unified_manager.restart_all_systems(flask_app, token_required)


def get_unified_automation_status():
    """Get status of all automation systems"""
    return unified_manager.get_system_status()


def schedule_job_safely(connection_id: str, job_type: str, trigger_user: str = None):
    """Schedule a job safely without duplication"""
    return unified_manager.schedule_immediate_job(connection_id, job_type, trigger_user)