"""
Model Evaluation Pipeline

Weekly automated evaluation of the fraud detection model per SPEC §3.5.
Evaluates AUC-ROC, precision@5%FPR, recall, and detects drift.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import roc_auc_score, precision_score, recall_score
from scipy import stats

logger = logging.getLogger(__name__)


FEATURE_NAMES = [
    "velocity_tx_count_1h", "velocity_tx_count_24h",
    "velocity_amount_sum_1h", "velocity_amount_sum_24h",
    "velocity_decline_count_24h", "velocity_unique_countries_1h",
    "velocity_unique_merchants_24h", "velocity_avg_amount_7d",
    "velocity_stddev_amount_7d", "velocity_time_since_last_tx",
    "behavioral_typical_amount_ratio", "behavioral_typical_hour_score",
    "behavioral_typical_day_score", "behavioral_merchant_category_diversity",
    "behavioral_amount_zscore", "behavioral_is_recipient_new",
    "behavioral_velocity_direction", "behavioral_time_between_tx_stddev",
    "behavioral_country_change_freq", "behavioral_night_tx_ratio",
    "device_is_known", "device_last_seen_hours_ago",
    "device_unique_accounts_24h", "device_is_emulator_detected",
    "device_rooted_jailbroken", "device_ip_country_match",
    "device_ip_is_vpn", "device_browser_fingerprint_match",
    "device_latency_anomaly", "device_is_new_os_version",
]


class ModelEvaluator:
    """Evaluates model performance and detects drift."""

    def __init__(self, model_path: str, reference_data_path: str = None):
        self.model = xgb.XGBClassifier()
        self.model.load_model(model_path)
        self.reference_data = None
        if reference_data_path and os.path.exists(reference_data_path):
            self.reference_data = pd.read_csv(reference_data_path)

    def evaluate(self, eval_data: pd.DataFrame) -> Dict:
        """Full evaluation suite."""
        X = eval_data[FEATURE_NAMES].values
        y = eval_data["is_fraud"].values

        y_proba = self.model.predict_proba(X)[:, 1]
        y_pred = self.model.predict(X)

        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "n_samples": len(eval_data),
            "fraud_rate": float(y.mean()),
            "auc_roc": float(roc_auc_score(y, y_proba)),
            "precision": float(precision_score(y, y_pred)),
            "recall": float(recall_score(y, y_pred)),
        }

        # Feature drift detection
        if self.reference_data is not None:
            metrics["drift"] = self._detect_drift(eval_data)

        # Score distribution analysis
        metrics["score_distribution"] = {
            "mean": float(y_proba.mean()),
            "std": float(y_proba.std()),
            "p50": float(np.percentile(y_proba, 50)),
            "p95": float(np.percentile(y_proba, 95)),
            "p99": float(np.percentile(y_proba, 99)),
        }

        return metrics

    def _detect_drift(self, current_data: pd.DataFrame) -> Dict:
        """Detect feature distribution drift using KS test."""
        drift_results = {}

        for feature in FEATURE_NAMES:
            if feature in current_data.columns and feature in self.reference_data.columns:
                # Kolmogorov-Smirnov test
                stat, p_value = stats.ks_2samp(
                    self.reference_data[feature].dropna(),
                    current_data[feature].dropna()
                )

                drift_detected = p_value < 0.05  # 95% confidence
                drift_results[feature] = {
                    "ks_statistic": float(stat),
                    "p_value": float(p_value),
                    "drift_detected": drift_detected,
                }

        n_drifted = sum(1 for v in drift_results.values() if v["drift_detected"])
        drift_results["_summary"] = {
            "n_features_drifted": n_drifted,
            "total_features": len(FEATURE_NAMES),
            "drift_rate": n_drifted / len(FEATURE_NAMES),
        }

        return drift_results

    def check_performance_regression(
        self, current_metrics: Dict, baseline_metrics: Dict
    ) -> Dict:
        """Check for performance regression against baseline."""
        regressions = []

        checks = {
            "auc_roc": ("min", 0.001),
            "precision": ("min", 0.005),
            "recall": ("min", 0.005),
        }

        for metric_name, (direction, threshold) in checks.items():
            current = current_metrics.get(metric_name, 0)
            baseline = baseline_metrics.get(metric_name, 0)
            diff = baseline - current

            if diff > threshold:
                regressions.append({
                    "metric": metric_name,
                    "current": current,
                    "baseline": baseline,
                    "regression": diff,
                })

        return {
            "regressions": regressions,
            "passed": len(regressions) == 0,
        }


def main():
    """Main evaluation entrypoint."""
    import argparse

    parser = argparse.ArgumentParser(description="Evaluate fraud detection model")
    parser.add_argument("--model", type=str, required=True, help="Path to trained model")
    parser.add_argument("--eval-data", type=str, required=True, help="Evaluation dataset")
    parser.add_argument("--reference-data", type=str, default=None, help="Reference data for drift")
    parser.add_argument("--baseline-metrics", type=str, default=None, help="Baseline metrics JSON")
    parser.add_argument("--output", type=str, default="evaluation_results.json")
    args = parser.parse_args()

    eval_data = pd.read_csv(args.eval_data)

    evaluator = ModelEvaluator(args.model, args.reference_data)
    metrics = evaluator.evaluate(eval_data)

    # Check regression
    if args.baseline_metrics:
        with open(args.baseline_metrics) as f:
            baseline = json.load(f)
        regression_check = evaluator.check_performance_regression(metrics, baseline)
        metrics["regression_check"] = regression_check

        if not regression_check["passed"]:
            logger.warning("⚠️ Performance regression detected!")
            for r in regression_check["regressions"]:
                logger.warning(f"  {r['metric']}: {r['baseline']:.4f} → {r['current']:.4f}")

    with open(args.output, "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    logger.info(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
