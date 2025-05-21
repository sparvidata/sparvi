# core/anomalies/algorithms.py

import numpy as np
import logging
from typing import List, Tuple, Dict, Any, Optional

logger = logging.getLogger(__name__)


def detect_zscore_anomalies(values: List[float],
                            sensitivity: float = 1.0,
                            window: Optional[int] = None) -> List[Tuple[int, float, bool, float]]:
    """
    Detect anomalies using Z-score method.

    Args:
        values: List of metric values
        sensitivity: Multiplier for threshold (lower = more sensitive)
        window: Optional rolling window size (None = use all data)

    Returns:
        List of (index, score, is_anomaly, threshold) tuples
    """
    if len(values) < 2:
        return []

    results = []

    # Use all data if window not specified
    if window is None or window >= len(values):
        mean = np.mean(values)
        std = np.std(values)
        threshold = 3.0 / sensitivity  # Default threshold is 3 sigma

        for i, value in enumerate(values):
            if std == 0:  # Handle case where all values are the same
                score = 0
            else:
                score = abs((value - mean) / std)

            is_anomaly = score > threshold
            results.append((i, score, is_anomaly, threshold))

    else:
        # Rolling window approach
        for i in range(window, len(values)):
            window_values = values[i - window:i]
            mean = np.mean(window_values)
            std = np.std(window_values)
            threshold = 3.0 / sensitivity

            value = values[i]
            if std == 0:  # Handle case where all values in window are the same
                score = 0
            else:
                score = abs((value - mean) / std)

            is_anomaly = score > threshold
            results.append((i, score, is_anomaly, threshold))

    return results


def detect_iqr_anomalies(values: List[float],
                         sensitivity: float = 1.0,
                         window: Optional[int] = None) -> List[Tuple[int, float, bool, float]]:
    """
    Detect anomalies using IQR method.

    Args:
        values: List of metric values
        sensitivity: Multiplier for threshold (lower = more sensitive)
        window: Optional rolling window size (None = use all data)

    Returns:
        List of (index, score, is_anomaly, threshold) tuples
    """
    if len(values) < 4:  # Need at least 4 points for meaningful quartiles
        return []

    results = []

    # Use all data if window not specified
    if window is None or window >= len(values):
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1
        threshold = 1.5 / sensitivity  # Default threshold is 1.5 * IQR

        lower_bound = q1 - (iqr * threshold)
        upper_bound = q3 + (iqr * threshold)

        for i, value in enumerate(values):
            # Calculate how many IQRs away from the nearest bound
            if value < lower_bound:
                score = abs((lower_bound - value) / iqr) if iqr > 0 else float('inf')
                is_anomaly = True
            elif value > upper_bound:
                score = abs((value - upper_bound) / iqr) if iqr > 0 else float('inf')
                is_anomaly = True
            else:
                score = 0
                is_anomaly = False

            results.append((i, score, is_anomaly, threshold))

    else:
        # Rolling window approach
        for i in range(window, len(values)):
            window_values = values[i - window:i]
            q1 = np.percentile(window_values, 25)
            q3 = np.percentile(window_values, 75)
            iqr = q3 - q1
            threshold = 1.5 / sensitivity

            lower_bound = q1 - (iqr * threshold)
            upper_bound = q3 + (iqr * threshold)

            value = values[i]
            if value < lower_bound:
                score = abs((lower_bound - value) / iqr) if iqr > 0 else float('inf')
                is_anomaly = True
            elif value > upper_bound:
                score = abs((value - upper_bound) / iqr) if iqr > 0 else float('inf')
                is_anomaly = True
            else:
                score = 0
                is_anomaly = False

            results.append((i, score, is_anomaly, threshold))

    return results


def detect_moving_average_anomalies(values: List[float],
                                    sensitivity: float = 1.0,
                                    window: int = 7,
                                    std_window: Optional[int] = None) -> List[Tuple[int, float, bool, float]]:
    """
    Detect anomalies using moving average with standard deviation.

    Args:
        values: List of metric values
        sensitivity: Multiplier for threshold (lower = more sensitive)
        window: Window size for moving average
        std_window: Window for calculating standard deviation (default: same as window)

    Returns:
        List of (index, score, is_anomaly, threshold) tuples
    """
    if len(values) < window + 1:
        return []

    if std_window is None:
        std_window = window

    results = []

    # Calculate moving averages
    moving_avgs = []
    for i in range(window, len(values)):
        window_values = values[i - window:i]
        moving_avgs.append(np.mean(window_values))

    # Calculate standard deviation of moving averages
    if len(moving_avgs) < std_window:
        std = np.std(moving_avgs)
        stds = [std] * len(moving_avgs)
    else:
        # Calculate rolling standard deviation
        stds = []
        for i in range(std_window, len(moving_avgs) + 1):
            stds.append(np.std(moving_avgs[i - std_window:i]))

    threshold = 2.0 / sensitivity  # Default threshold is 2 stdevs

    # Detect anomalies
    for i in range(len(moving_avgs)):
        actual_idx = i + window
        value = values[actual_idx]
        moving_avg = moving_avgs[i]

        # Get appropriate standard deviation
        if i < len(stds):
            current_std = stds[i]
        else:
            current_std = stds[-1]  # Use last calculated std

        if current_std == 0:  # Handle case where std is zero
            score = 0
            is_anomaly = False
        else:
            score = abs((value - moving_avg) / current_std)
            is_anomaly = score > threshold

        results.append((actual_idx, score, is_anomaly, threshold))

    return results


def get_anomaly_severity(score: float, method: str = 'zscore') -> str:
    """
    Determine anomaly severity based on score and method.

    Args:
        score: Anomaly score
        method: Detection method

    Returns:
        Severity level ('low', 'medium', 'high')
    """
    if method == 'zscore':
        if score > 5.0:
            return 'high'
        elif score > 3.5:
            return 'medium'
        else:
            return 'low'
    elif method == 'iqr':
        if score > 3.0:
            return 'high'
        elif score > 1.5:
            return 'medium'
        else:
            return 'low'
    elif method == 'moving_average':
        if score > 4.0:
            return 'high'
        elif score > 2.5:
            return 'medium'
        else:
            return 'low'
    else:
        # Default severity logic
        if score > 5.0:
            return 'high'
        elif score > 2.5:
            return 'medium'
        else:
            return 'low'


def format_anomaly_results(raw_results: List[Tuple[int, float, bool, float]],
                           values: List[float],
                           timestamps: List[str],
                           method: str) -> List[Dict[str, Any]]:
    """
    Format raw detection results into a standardized structure.

    Args:
        raw_results: List of (index, score, is_anomaly, threshold) tuples
        values: Original values list
        timestamps: List of timestamps
        method: Detection method used

    Returns:
        List of formatted anomaly result dictionaries
    """
    formatted_results = []

    for idx, score, is_anomaly, threshold in raw_results:
        if is_anomaly:
            # Ensure index is within bounds for values and timestamps
            if idx < len(values) and idx < len(timestamps):
                severity = get_anomaly_severity(score, method)

                formatted_results.append({
                    "timestamp": timestamps[idx],
                    "value": values[idx],
                    "score": float(score),  # Convert numpy types to Python native
                    "is_anomaly": is_anomaly,
                    "threshold": float(threshold),
                    "method": method,
                    "severity": severity
                })
            else:
                logger.warning(
                    f"Index {idx} out of bounds for values/timestamps with lengths {len(values)}/{len(timestamps)}")

    return formatted_results