"""
Unsupervised Isolation Forest anomaly detection for aggregated IoT stream windows.

Enforces:
1. Deterministic seeding (random_state=42).
2. Unsupervised metrics only (anomaly count/%, score distribution, histogram).
3. Graceful handling of insufficient-data cases.
"""

from typing import Any, Dict, List, Optional
import numpy as np
from sklearn.ensemble import IsolationForest

from src.bda.ml.features import extract_aggregate_features


class StreamAnomalyDetector:
    """
    Online/window-level anomaly detector using seeded Isolation Forest.
    """

    def __init__(
        self,
        contamination: float = 0.05,
        random_state: int = 42,
        n_estimators: int = 100,
        min_samples_required: int = 5,
    ):
        self.contamination = contamination
        self.random_state = random_state
        self.n_estimators = n_estimators
        self.min_samples_required = min_samples_required

    def detect(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Run anomaly detection over aggregated window rows.

        Returns comprehensive unsupervised anomaly results dictionary.
        """
        if not rows or len(rows) < self.min_samples_required:
            return {
                "status": "insufficient_data",
                "message": f"Insufficient data: {len(rows) if rows else 0} window records (minimum {self.min_samples_required} required).",
                "observations_scored": len(rows) if rows else 0,
                "anomaly_count": 0,
                "anomaly_percentage": 0.0,
                "model_metadata": {
                    "algorithm": "IsolationForest",
                    "contamination": self.contamination,
                    "random_state": self.random_state,
                    "n_estimators": self.n_estimators,
                },
                "score_distribution": {
                    "min": 0.0,
                    "max": 0.0,
                    "mean": 0.0,
                    "histogram": [],
                },
                "anomalous_records": [],
            }

        X, feature_names, ordered_rows = extract_aggregate_features(rows)

        # Fit model deterministically
        clf = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=self.n_estimators,
        )
        clf.fit(X)

        # Raw scores: lower = more abnormal. Invert so higher score = more anomalous.
        raw_scores = clf.decision_function(X)
        anomaly_scores = -raw_scores

        preds = clf.predict(X)  # -1 for anomaly, 1 for normal
        is_anomaly_mask = preds == -1

        total_samples = len(ordered_rows)
        anomaly_count = int(np.sum(is_anomaly_mask))
        anomaly_pct = round((anomaly_count / total_samples) * 100.0, 2)

        min_s = float(np.min(anomaly_scores))
        max_s = float(np.max(anomaly_scores))
        mean_s = float(np.mean(anomaly_scores))

        # Compute histogram bins for visualization (10 bins)
        hist_counts, bin_edges = np.histogram(anomaly_scores, bins=10)
        histogram_bins = []
        for i in range(len(hist_counts)):
            histogram_bins.append(
                {
                    "bin_start": round(float(bin_edges[i]), 3),
                    "bin_end": round(float(bin_edges[i + 1]), 3),
                    "count": int(hist_counts[i]),
                }
            )

        # Attach scores to rows
        annotated_records = []
        for i, r in enumerate(ordered_rows):
            item = dict(r)
            item["anomaly_score"] = round(float(anomaly_scores[i]), 4)
            item["is_anomaly"] = bool(is_anomaly_mask[i])
            annotated_records.append(item)

        # Top anomalous records sorted descending by anomaly score
        anomalies_only = [r for r in annotated_records if r["is_anomaly"]]
        anomalies_sorted = sorted(
            anomalies_only, key=lambda x: x["anomaly_score"], reverse=True
        )

        return {
            "status": "success",
            "message": f"Successfully evaluated {total_samples} window records.",
            "observations_scored": total_samples,
            "anomaly_count": anomaly_count,
            "anomaly_percentage": anomaly_pct,
            "model_metadata": {
                "algorithm": "IsolationForest",
                "contamination": self.contamination,
                "random_state": self.random_state,
                "n_estimators": self.n_estimators,
                "feature_names": feature_names,
                "unsupervised": True,
            },
            "score_distribution": {
                "min": round(min_s, 4),
                "max": round(max_s, 4),
                "mean": round(mean_s, 4),
                "histogram": histogram_bins,
            },
            "anomalous_records": anomalies_sorted[:50],  # Top 50 for display
            "all_scored_preview": annotated_records[:20],
        }
