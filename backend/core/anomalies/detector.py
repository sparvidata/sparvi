# core/anomalies/detector.py

import logging
from typing import List, Dict, Any, Optional
from core.anomalies.algorithms import (
    detect_zscore_anomalies,
    detect_iqr_anomalies,
    detect_moving_average_anomalies,
    format_anomaly_results
)

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Class for detecting anomalies in time-series metrics
    based on statistical methods.
    """

    def detect_anomalies(self,
                         config: Dict[str, Any],
                         metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect anomalies based on a configuration and historical metrics

        Args:
            config: Anomaly detection configuration
            metrics: List of historical metrics

        Returns:
            List of anomaly results
        """
        # Sort metrics by timestamp to ensure chronological order
        metrics.sort(key=lambda m: m.get("timestamp", ""))

        # Extract values and timestamps from metrics
        values = []
        timestamps = []

        for metric in metrics:
            # Handle numeric and text metrics appropriately
            if "metric_value" in metric and metric["metric_value"] is not None:
                values.append(float(metric["metric_value"]))
            elif "metric_text" in metric and metric["metric_text"] is not None:
                try:
                    # Try to convert text value to float
                    values.append(float(metric["metric_text"]))
                except (ValueError, TypeError):
                    # Skip metrics with non-numeric values
                    continue
            else:
                # Skip metrics with no value
                continue

            timestamps.append(metric.get("timestamp", ""))

        # Check if we have enough data points
        min_data_points = config.get("min_data_points", 7)
        if len(values) < min_data_points:
            logger.info(f"Not enough data points for detection: {len(values)} < {min_data_points}")
            return []

        # Select detection method based on config
        method = config.get("detection_method", "zscore")
        sensitivity = config.get("sensitivity", 1.0)
        config_params = config.get("config_params", {}) or {}

        # Detect anomalies using the appropriate method
        if method == "zscore":
            window = config_params.get("window")
            raw_results = detect_zscore_anomalies(values, sensitivity, window)
        elif method == "iqr":
            window = config_params.get("window")
            raw_results = detect_iqr_anomalies(values, sensitivity, window)
        elif method == "moving_average":
            window = config_params.get("window", 7)
            std_window = config_params.get("std_window")
            raw_results = detect_moving_average_anomalies(values, sensitivity, window, std_window)
        else:
            logger.error(f"Unknown detection method: {method}")
            return []

        # Format the results
        formatted_results = format_anomaly_results(raw_results, values, timestamps, method)

        # Check for and apply expected patterns filtering
        # (in an MVP, we'll just return the results without this filtering)

        return formatted_results

    def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a configuration and set default values if needed

        Args:
            config: Anomaly detection configuration

        Returns:
            Validated configuration with defaults filled in
        """
        # Create a copy of the config to avoid modifying the original
        validated = config.copy()

        # Set default values if not provided
        if "detection_method" not in validated:
            validated["detection_method"] = "zscore"

        if "sensitivity" not in validated:
            validated["sensitivity"] = 1.0

        if "min_data_points" not in validated:
            validated["min_data_points"] = 7

        if "baseline_window_days" not in validated:
            validated["baseline_window_days"] = 14

        if "config_params" not in validated:
            validated["config_params"] = {}

        # Method-specific validation
        method = validated["detection_method"]
        if method == "moving_average":
            if "window" not in validated["config_params"]:
                validated["config_params"]["window"] = 7

        return validated