# core/anomalies/scheduler.py

import logging
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional

from core.anomalies.detector import AnomalyDetector
from core.anomalies.events import AnomalyEventType, publish_anomaly_event
from core.storage.supabase_manager import SupabaseManager

logger = logging.getLogger(__name__)


class AnomalyDetectionScheduler:
    """
    Scheduler for running anomaly detection jobs
    """

    def __init__(self, max_workers: int = 5):
        """
        Initialize the scheduler

        Args:
            max_workers: Maximum number of worker threads for parallel processing
        """
        self.detector = AnomalyDetector()
        self.supabase = SupabaseManager()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def schedule_detection_run(self,
                               organization_id: str,
                               connection_id: Optional[str] = None,
                               trigger_type: str = 'scheduled') -> Dict[str, Any]:
        """
        Schedule anomaly detection for all active configs

        Args:
            organization_id: Organization ID
            connection_id: Optional connection ID to limit scope
            trigger_type: Type of trigger ('scheduled', 'manual', 'event')

        Returns:
            Result dictionary with status and statistics
        """
        run_id = None
        try:
            # Create a detection run record
            run_id = self._create_run_record(organization_id, connection_id, trigger_type)

            # Get active configs
            configs = self._get_active_configs(organization_id, connection_id)

            if not configs:
                self._complete_run(run_id, 'completed', 0, 0)
                return {"status": "success", "message": "No active configurations found"}

            # Process configs in parallel
            futures = []
            for config in configs:
                future = self.executor.submit(
                    self._process_config,
                    run_id,
                    organization_id,
                    config
                )
                futures.append(future)

            # Wait for all futures to complete
            metrics_processed = 0
            anomalies_detected = 0

            for future in futures:
                result = future.result()
                metrics_processed += result.get('metrics_processed', 0)
                anomalies_detected += result.get('anomalies_detected', 0)

            # Mark run as completed
            self._complete_run(run_id, 'completed', metrics_processed, anomalies_detected)

            return {
                "status": "success",
                "run_id": run_id,
                "metrics_processed": metrics_processed,
                "anomalies_detected": anomalies_detected
            }

        except Exception as e:
            logger.error(f"Error in anomaly detection scheduler: {str(e)}")
            if run_id:
                self._complete_run(run_id, 'failed', 0, 0, error=str(e))
            return {"status": "error", "message": str(e)}

    def _create_run_record(self,
                           organization_id: str,
                           connection_id: Optional[str],
                           trigger_type: str) -> str:
        """
        Create a record for this detection run

        Args:
            organization_id: Organization ID
            connection_id: Optional connection ID
            trigger_type: Type of trigger

        Returns:
            Run ID
        """
        data = {
            "id": str(uuid.uuid4()),
            "organization_id": organization_id,
            "connection_id": connection_id or str(uuid.uuid4()),  # Use a default UUID if None
            "trigger_type": trigger_type,
            "status": "running"
        }

        response = self.supabase.supabase.table("anomaly_detection_runs").insert(data).execute()
        if not response.data:
            logger.error("Failed to create run record")
            raise Exception("Failed to create run record")

        return response.data[0]["id"]

    def _complete_run(self,
                      run_id: str,
                      status: str,
                      metrics_processed: int,
                      anomalies_detected: int,
                      error: Optional[str] = None):
        """
        Update the run record with completion status

        Args:
            run_id: Run ID
            status: Status ('completed', 'failed')
            metrics_processed: Number of metrics processed
            anomalies_detected: Number of anomalies detected
            error: Optional error message
        """
        data = {
            "status": status,
            "completed_at": datetime.datetime.now(timezone.utc).isoformat(),
            "metrics_processed": metrics_processed,
            "anomalies_detected": anomalies_detected,
            "execution_time_ms": self._calculate_execution_time(run_id)
        }

        if error:
            data["error"] = error

        self.supabase.supabase.table("anomaly_detection_runs").update(data).eq("id", run_id).execute()

    def _calculate_execution_time(self, run_id: str) -> int:
        """
        Calculate execution time for a run

        Args:
            run_id: Run ID

        Returns:
            Execution time in milliseconds
        """
        try:
            # Get run record to find start time
            response = self.supabase.supabase.table("anomaly_detection_runs") \
                .select("started_at") \
                .eq("id", run_id) \
                .execute()

            if not response.data:
                return 0

            started_at = response.data[0].get("started_at")
            if not started_at:
                return 0

            # Parse start time
            start_time = datetime.datetime.fromisoformat(started_at.replace('Z', '+00:00'))

            # Calculate time difference
            time_diff = datetime.datetime.now(timezone.utc) - start_time
            return int(time_diff.total_seconds() * 1000)

        except Exception as e:
            logger.error(f"Error calculating execution time: {str(e)}")
            return 0

    def _get_active_configs(self,
                            organization_id: str,
                            connection_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all active anomaly detection configurations

        Args:
            organization_id: Organization ID
            connection_id: Optional connection ID to filter

        Returns:
            List of configuration dictionaries
        """
        query = self.supabase.supabase.table("anomaly_detection_configs") \
            .select("*") \
            .eq("organization_id", organization_id) \
            .eq("is_active", True)

        if connection_id:
            query = query.eq("connection_id", connection_id)

        response = query.execute()
        return response.data if response.data else []

    def _process_config(self,
                        run_id: str,
                        organization_id: str,
                        config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single anomaly detection configuration

        Args:
            run_id: Run ID
            organization_id: Organization ID
            config: Configuration dictionary

        Returns:
            Result dictionary
        """
        try:
            # Get historical metrics for this config
            metrics = self._get_historical_metrics(config)

            if not metrics or len(metrics) < config.get("min_data_points", 7):
                logger.info(
                    f"Not enough data points for config {config.get('id')}: found {len(metrics) if metrics else 0}")
                return {"metrics_processed": 0, "anomalies_detected": 0}

            # Run anomaly detection
            results = self.detector.detect_anomalies(config, metrics)

            # Save results to database
            anomalies_detected = self._save_detection_results(organization_id, config, results)

            # Publish events for detected anomalies
            if anomalies_detected > 0:
                self._publish_anomaly_events(organization_id, config, results)

            return {"metrics_processed": 1, "anomalies_detected": anomalies_detected}

        except Exception as e:
            logger.error(f"Error processing config {config.get('id')}: {str(e)}")
            return {"metrics_processed": 0, "anomalies_detected": 0, "error": str(e)}

    def _get_historical_metrics(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get historical metrics data for a config

        Args:
            config: Configuration dictionary

        Returns:
            List of metric dictionaries
        """
        try:
            from core.analytics.historical_metrics import HistoricalMetricsTracker

            tracker = HistoricalMetricsTracker(self.supabase)

            # Calculate how many days of data to request
            days = max(config.get("baseline_window_days", 14), 30)  # At least 30 days

            # Get metrics from the tracker
            metrics = tracker.get_metric_history(
                organization_id=config["organization_id"],
                connection_id=config["connection_id"],
                metric_name=config["metric_name"],
                table_name=config["table_name"],
                column_name=config.get("column_name"),
                days=days,
                limit=1000  # High limit to ensure we get all data
            )

            return metrics

        except ImportError:
            logger.error("Historical metrics tracker not available")
            return []
        except Exception as e:
            logger.error(f"Error getting historical metrics: {str(e)}")
            return []

    def _save_detection_results(self,
                                organization_id: str,
                                config: Dict[str, Any],
                                results: List[Dict[str, Any]]) -> int:
        """
        Save detection results to the database

        Args:
            organization_id: Organization ID
            config: Configuration dictionary
            results: List of anomaly results

        Returns:
            Number of anomalies saved
        """
        # Only save anomalies, not all results
        anomalies = [r for r in results if r.get("is_anomaly", False)]

        if not anomalies:
            return 0

        # Convert to database records
        records = []
        for anomaly in anomalies:
            # Determine severity based on score
            severity = anomaly.get("severity", "medium")

            records.append({
                "id": str(uuid.uuid4()),
                "organization_id": organization_id,
                "connection_id": config["connection_id"],
                "config_id": config["id"],
                "table_name": config["table_name"],
                "column_name": config.get("column_name"),
                "metric_name": config["metric_name"],
                "metric_value": anomaly.get("value"),
                "severity": severity,
                "score": anomaly.get("score", 0),
                "threshold": anomaly.get("threshold", 0),
                "detected_at": datetime.datetime.now(timezone.utc).isoformat(),
                "status": "open"
            })

        # Insert records in batches of 50
        for i in range(0, len(records), 50):
            batch = records[i:i + 50]
            try:
                self.supabase.supabase.table("anomaly_results").insert(batch).execute()
            except Exception as e:
                logger.error(f"Error inserting anomaly results: {str(e)}")
                # Continue with next batch even if this one fails

        return len(anomalies)

    def _publish_anomaly_events(self,
                                organization_id: str,
                                config: Dict[str, Any],
                                results: List[Dict[str, Any]]) -> None:
        """
        Publish events for detected anomalies

        Args:
            organization_id: Organization ID
            config: Configuration dictionary
            results: List of anomaly results
        """
        try:
            # Only publish events for anomalies
            anomalies = [r for r in results if r.get("is_anomaly", False)]

            if not anomalies:
                return

            # Create event data
            event_data = {
                "config_id": config["id"],
                "connection_id": config["connection_id"],
                "table_name": config["table_name"],
                "column_name": config.get("column_name"),
                "metric_name": config["metric_name"],
                "anomaly_count": len(anomalies),
                "detection_method": config["detection_method"],
                "high_severity_count": len([a for a in anomalies if a.get("severity") == "high"]),
                "medium_severity_count": len([a for a in anomalies if a.get("severity") == "medium"]),
                "low_severity_count": len([a for a in anomalies if a.get("severity") == "low"])
            }

            # Publish the event
            publish_anomaly_event(
                event_type=AnomalyEventType.ANOMALY_DETECTED,
                data=event_data,
                organization_id=organization_id
            )

        except Exception as e:
            logger.error(f"Error publishing anomaly events: {str(e)}")