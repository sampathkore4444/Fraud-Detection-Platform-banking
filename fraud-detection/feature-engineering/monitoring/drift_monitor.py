"""
Feature Drift Monitoring

Detects feature distribution drift that may indicate model degradation.
Per SPEC §3.5: "Feature drift detected → Automated alert, manual model review gate"
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass

import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


@dataclass
class DriftAlert:
    feature_name: str
    ks_statistic: float
    p_value: float
    drift_detected: bool
    severity: str  # "info", "warning", "critical"
    message: str


class DriftMonitor:
    """Monitors feature distribution drift over time."""

    DRIFT_THRESHOLD = 0.05      # p-value threshold for KS test
    CRITICAL_DRIFT_THRESHOLD = 0.01  # p-value for critical drift
    MIN_SAMPLES = 1000          # Minimum samples for reliable drift detection

    def __init__(self, reference_stats: Optional[Dict] = None):
        """
        Initialize with reference distribution statistics.
        reference_stats: dict of {feature_name: {mean, std, min, max, histogram}}
        """
        self.reference_stats = reference_stats or {}
        self.alerts: List[DriftAlert] = []

    def check_drift(
        self,
        current_features: Dict[str, List[float]],
        feature_names: List[str]
    ) -> List[DriftAlert]:
        """
        Check for drift in each feature against reference distribution.
        Returns list of DriftAlert objects.
        """
        self.alerts = []

        for feature_name in feature_names:
            if feature_name not in current_features:
                continue

            values = current_features[feature_name]
            if len(values) < self.MIN_SAMPLES:
                continue

            alert = self._check_single_feature(feature_name, values)
            if alert:
                self.alerts.append(alert)

        return self.alerts

    def _check_single_feature(
        self, feature_name: str, current_values: List[float]
    ) -> Optional[DriftAlert]:
        """Check drift for a single feature."""
        if feature_name not in self.reference_stats:
            return None

        ref = self.reference_stats[feature_name]
        ref_values = ref.get("values", [])

        if len(ref_values) < self.MIN_SAMPLES:
            return None

        # Kolmogorov-Smirnov test
        ks_stat, p_value = stats.ks_2samp(ref_values, current_values)

        # Determine severity
        if p_value < self.CRITICAL_DRIFT_THRESHOLD:
            severity = "critical"
            drift_detected = True
        elif p_value < self.DRIFT_THRESHOLD:
            severity = "warning"
            drift_detected = True
        else:
            severity = "info"
            drift_detected = False

        if drift_detected:
            message = (
                f"Drift detected in {feature_name}: "
                f"KS={ks_stat:.4f}, p={p_value:.6f}, "
                f"ref_mean={np.mean(ref_values):.4f}, "
                f"current_mean={np.mean(current_values):.4f}"
            )
            logger.warning(message)

            return DriftAlert(
                feature_name=feature_name,
                ks_statistic=ks_stat,
                p_value=p_value,
                drift_detected=drift_detected,
                severity=severity,
                message=message,
            )

        return None

    def compute_reference_stats(
        self, features: Dict[str, List[float]]
    ) -> Dict:
        """Compute reference statistics from historical data."""
        stats_dict = {}

        for feature_name, values in features.items():
            values_array = np.array(values)
            stats_dict[feature_name] = {
                "mean": float(np.mean(values_array)),
                "std": float(np.std(values_array)),
                "min": float(np.min(values_array)),
                "max": float(np.max(values_array)),
                "median": float(np.median(values_array)),
                "p5": float(np.percentile(values_array, 5)),
                "p95": float(np.percentile(values_array, 95)),
                "count": len(values),
                "values": values,  # Store raw values for KS test
            }

        self.reference_stats = stats_dict
        return stats_dict

    def generate_report(self) -> Dict:
        """Generate drift monitoring report."""
        critical_alerts = [a for a in self.alerts if a.severity == "critical"]
        warning_alerts = [a for a in self.alerts if a.severity == "warning"]

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_features_checked": len(self.alerts),
            "critical_drifts": len(critical_alerts),
            "warning_drifts": len(warning_alerts),
            "alerts": [
                {
                    "feature": a.feature_name,
                    "severity": a.severity,
                    "ks_statistic": a.ks_statistic,
                    "p_value": a.p_value,
                    "message": a.message,
                }
                for a in self.alerts
            ],
            "action_required": len(critical_alerts) > 0,
            "recommendation": (
                "Manual model review gate required" if critical_alerts
                else "Continue monitoring" if warning_alerts
                else "No action required"
            ),
        }


# ── Prometheus Metrics Export ─────────────────────────────────

class DriftMetricsExporter:
    """Exports drift metrics to Prometheus."""

    @staticmethod
    def export_to_prometheus(alerts: List[DriftAlert], output_path: str):
        """Export drift alerts as Prometheus metrics."""
        lines = []

        # Feature drift gauge
        lines.append("# HELP fraud_feature_drift Detected feature drift (1=yes)")
        lines.append("# TYPE fraud_feature_drift gauge")

        for alert in alerts:
            labels = f'feature="{alert.feature_name}",severity="{alert.severity}"'
            value = 1 if alert.drift_detected else 0
            lines.append(f"fraud_feature_drift{{{labels}}} {value}")

        # KS statistic
        lines.append("# HELP fraud_feature_ks_statistic Kolmogorov-Smirnov statistic")
        lines.append("# TYPE fraud_feature_ks_statistic gauge")

        for alert in alerts:
            labels = f'feature="{alert.feature_name}"'
            lines.append(f"fraud_feature_ks_statistic{{{labels}}} {alert.ks_statistic:.6f}")

        # P-value
        lines.append("# HELP fraud_feature_drift_pvalue KS test p-value")
        lines.append("# TYPE fraud_feature_drift_pvalue gauge")

        for alert in alerts:
            labels = f'feature="{alert.feature_name}"'
            lines.append(f"fraud_feature_drift_pvalue{{{labels}}} {alert.p_value:.6f}")

        with open(output_path, "w") as f:
            f.write("\n".join(lines) + "\n")
