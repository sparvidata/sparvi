# core/anomalies/scheduler_service.py

import time
import logging
import threading
import traceback
import schedule
from datetime import datetime, timedelta, timezone

from core.anomalies.scheduler import AnomalyDetectionScheduler
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


class AnomalyDetectionSchedulerService:
    """
    Service for scheduling anomaly detection runs at regular intervals
    """

    def __init__(self):
        self.scheduler = AnomalyDetectionScheduler()
        self.supabase = SupabaseManager()
        self.running = False
        self.thread = None

    def start(self):
        """Start the scheduler service"""
        if self.running:
            logger.warning("Scheduler service already running")
            return

        self.running = True

        # Set up schedules
        schedule.clear()

        # Run daily at midnight
        schedule.every().day.at("00:00").do(self.run_daily_detection)

        # Run hourly for more frequent metrics
        schedule.every().hour.do(self.run_hourly_detection)

        # Start the scheduler thread
        self.thread = threading.Thread(target=self._run_scheduler)
        self.thread.daemon = True
        self.thread.start()

        logger.info("Anomaly detection scheduler service started")

    def stop(self):
        """Stop the scheduler service"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
        logger.info("Anomaly detection scheduler service stopped")

    def _run_scheduler(self):
        """Run the scheduler loop"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(5)  # Wait a bit before retrying

    def run_daily_detection(self):
        """Run detection for all organizations (daily metrics)"""
        try:
            logger.info("Running daily anomaly detection")

            # Get all active organizations
            orgs = self._get_active_organizations()

            # Run detection for each organization
            for org in orgs:
                try:
                    # Get all connections for this organization
                    connections = self._get_connections(org["id"])

                    # Run detection for each connection
                    for conn in connections:
                        try:
                            logger.info(f"Running daily detection for org {org['id']}, connection {conn['id']}")
                            self.scheduler.schedule_detection_run(
                                organization_id=org["id"],
                                connection_id=conn["id"],
                                trigger_type="scheduled"
                            )
                        except Exception as conn_e:
                            logger.error(f"Error running detection for connection {conn['id']}: {str(conn_e)}")
                except Exception as org_e:
                    logger.error(f"Error processing organization {org['id']}: {str(org_e)}")

            logger.info("Daily anomaly detection completed")
            return True
        except Exception as e:
            logger.error(f"Error in daily anomaly detection: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def run_hourly_detection(self):
        """
        Run detection for hourly metrics only
        This is optimized to run more frequently but with a smaller scope
        """
        try:
            logger.info("Running hourly anomaly detection")

            # Get all configurations with 'hourly' refresh frequency
            configs = self._get_hourly_configs()

            # Group configs by organization and connection
            org_conn_configs = {}
            for config in configs:
                org_id = config["organization_id"]
                conn_id = config["connection_id"]

                if org_id not in org_conn_configs:
                    org_conn_configs[org_id] = {}

                if conn_id not in org_conn_configs[org_id]:
                    org_conn_configs[org_id][conn_id] = []

                org_conn_configs[org_id][conn_id].append(config)

            # Run detection for each organization and connection
            for org_id, connections in org_conn_configs.items():
                for conn_id, configs in connections.items():
                    try:
                        logger.info(f"Running hourly detection for org {org_id}, connection {conn_id}")
                        self.scheduler.schedule_detection_run(
                            organization_id=org_id,
                            connection_id=conn_id,
                            trigger_type="scheduled"
                        )
                    except Exception as conn_e:
                        logger.error(f"Error running detection for connection {conn_id}: {str(conn_e)}")

            logger.info("Hourly anomaly detection completed")
            return True
        except Exception as e:
            logger.error(f"Error in hourly anomaly detection: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _get_active_organizations(self):
        """Get all active organizations"""
        try:
            response = self.supabase.supabase.table("organizations").select("id").execute()
            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error getting active organizations: {str(e)}")
            return []

    def _get_connections(self, organization_id):
        """Get all connections for an organization"""
        try:
            response = self.supabase.supabase.table("database_connections") \
                .select("id") \
                .eq("organization_id", organization_id) \
                .execute()

            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error getting connections for org {organization_id}: {str(e)}")
            return []

    def _get_hourly_configs(self):
        """Get configurations with hourly refresh frequency"""
        try:
            # In the MVP, we don't have a specific 'refresh_frequency' field yet,
            # so we'll just get all active configs that have been recently updated
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

            response = self.supabase.supabase.table("anomaly_detection_configs") \
                .select("organization_id,connection_id,id") \
                .eq("is_active", True) \
                .gte("updated_at", cutoff_date) \
                .execute()

            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error getting hourly configs: {str(e)}")
            return []