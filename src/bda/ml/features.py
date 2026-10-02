"""
Feature extraction from event-time window aggregates for IoT anomaly detection.

Features are computed strictly from window aggregate summaries (start, DeviceId, Sensor, count, sum, avg, min, max),
never from raw arrival order.
"""

from typing import Any, Dict, List, Tuple
import numpy as np


FEATURE_NAMES = [
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "val_range",
    "avg_ratio",
    "window_hour",
    "window_minute",
    "lag_avg_diff",
]


def extract_aggregate_features(
    rows: List[Dict[str, Any]],
) -> Tuple[np.ndarray, List[str], List[Dict[str, Any]]]:
    """
    Extract modular feature matrix X from window aggregate rows.

    Returns:
        X: 2D numpy array of shape (N, num_features)
        feature_names: List of feature names
        ordered_rows: List of rows corresponding to X
    """
    if not rows:
        return np.empty((0, len(FEATURE_NAMES))), FEATURE_NAMES, []

    # Sort deterministically by (DeviceId, Sensor, start) to compute temporal lag features
    sorted_rows = sorted(
        rows,
        key=lambda r: (str(r.get("DeviceId")), str(r.get("Sensor")), int(r.get("start", 0))),
    )

    feature_matrix = []
    prev_series_val: Dict[Tuple[str, str], float] = {}

    for row in sorted_rows:
        dev = str(row.get("DeviceId", ""))
        sens = str(row.get("Sensor", ""))
        start = int(row.get("start", 0))

        cnt = float(row.get("count", 0))
        sm = float(row.get("sum", 0.0))
        avg = float(row.get("avg", 0.0))
        mn = float(row.get("min", 0.0))
        mx = float(row.get("max", 0.0))

        # Group B: Statistical
        val_range = mx - mn
        avg_ratio = (avg - mn) / (val_range + 1e-6)

        # Group C: Temporal
        hour = float((start // 3600) % 24)
        minute = float((start // 60) % 60)

        # Lag difference
        series_key = (dev, sens)
        if series_key in prev_series_val:
            lag_diff = avg - prev_series_val[series_key]
        else:
            lag_diff = 0.0
        prev_series_val[series_key] = avg

        feature_vector = [
            cnt,
            sm,
            avg,
            mn,
            mx,
            val_range,
            avg_ratio,
            hour,
            minute,
            lag_diff,
        ]
        feature_matrix.append(feature_vector)

    X = np.array(feature_matrix, dtype=np.float64)
    # Replace any nan or inf
    X = np.nan_to_num(X, nan=0.0, posinf=1e6, neginf=-1e6)
    return X, FEATURE_NAMES, sorted_rows
