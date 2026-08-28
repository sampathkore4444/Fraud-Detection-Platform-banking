"""
Scoring module — mirrors Go internal/scoring/ package

Implements:
- XGBoost model loading from native JSON format
- StandardScaler loading and application
- Feature extraction and prediction
- Decision classification (APPROVE/REVIEW/DECLINE)
- Reason code identification
"""

import json
import math
import time
import logging
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from threading import RLock

logger = logging.getLogger(__name__)


# ─── Decision Types ───────────────────────────────────────────────────────────

class Decision(IntEnum):
    APPROVE = 0
    REVIEW = 1
    DECLINE = 2


class ReasonCode:
    UNKNOWN = "UNKNOWN"
    VELOCITY_BURST = "VELOCITY_BURST"
    NEW_DEVICE = "NEW_DEVICE"
    AMOUNT_ANOMALY = "AMOUNT_ANOMALY"
    GEO_ANOMALY = "GEO_ANOMALY"
    IMPOSSIBLE_TRAVEL = "IMPOSSIBLE_TRAVEL"
    VPN_PROXY = "VPN_PROXY"
    EMULATOR_DETECTED = "EMULATOR_DETECTED"
    HIGH_RISK_MCC = "HIGH_RISK_MCC"
    MODEL_SCORE_HIGH = "MODEL_SCORE_HIGH"
    MODEL_SCORE_MEDIUM = "MODEL_SCORE_MEDIUM"
    RULE_VIOLATION = "RULE_VIOLATION"
    BLOCKED_COUNTRY = "BLOCKED_COUNTRY"


# ─── Data Structures ──────────────────────────────────────────────────────────

@dataclass
class Thresholds:
    approve: float = 0.30
    review: float = 0.70
    decline: float = 1.00


@dataclass
class ScalerParams:
    version: str = ""
    feature_names: List[str] = field(default_factory=list)
    mean: List[float] = field(default_factory=list)
    std: List[float] = field(default_factory=list)
    n_features: int = 0


@dataclass
class ScoreResult:
    transaction_id: str = ""
    decision: Decision = Decision.APPROVE
    fraud_probability: float = 0.0
    model_version: str = ""
    latency_ms: int = 0
    top_features: Dict[str, float] = field(default_factory=dict)
    reason_code: str = ReasonCode.UNKNOWN
    timestamp_ms: int = 0


# ─── Compiled Tree (for fast inference) ──────────────────────────────────────

@dataclass
class CompiledNode:
    feature_idx: int = 0
    threshold: float = 0.0
    left: int = -1
    right: int = -1
    value: float = 0.0
    is_leaf: bool = False


@dataclass
class CompiledTree:
    nodes: List[CompiledNode] = field(default_factory=list)

    def predict(self, features: List[float]) -> float:
        """Walk tree from root to leaf, return leaf value."""
        if not self.nodes:
            return 0.0
        node = self.nodes[0]
        while not node.is_leaf:
            if features[node.feature_idx] <= node.threshold:
                node = self.nodes[node.left]
            else:
                node = self.nodes[node.right]
        return node.value


# ─── Model ────────────────────────────────────────────────────────────────────

# Default feature names (same order as training pipeline)
DEFAULT_FEATURES = [
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


class Model:
    """
    XGBoost model loaded from native JSON format.
    Mirrors Go scoring.Model.

    Inference pipeline:
    1. Extract raw features from dict → List[float]
    2. Apply StandardScaler: (x - μ) / σ
    3. Sum tree predictions: logit = base_score + Σ tree_i.predict(scaled)
    4. Apply sigmoid: probability = 1 / (1 + exp(-logit))
    """

    def __init__(self):
        self.version = "v1.0.0"
        self.loaded_at = time.time()
        self.thresholds = Thresholds()
        self.features = DEFAULT_FEATURES
        self.trees: List[CompiledTree] = []
        self.base_score = 0.0
        self.num_trees = 0
        self.scaler: Optional[ScalerParams] = None
        self.feature_importance: Dict[str, float] = {
            "velocity_tx_count_1h": 0.15,
            "device_is_known": 0.12,
            "behavioral_amount_zscore": 0.11,
            "device_ip_country_match": 0.10,
            "velocity_unique_countries_1h": 0.09,
        }
        self._lock = RLock()

    def predict(self, features: Dict[str, str]) -> Tuple[float, Dict[str, float]]:
        """
        Compute fraud probability from feature vector.
        Returns (probability, top_features).
        Mirrors Go Model.Predict().
        """
        with self._lock:
            # Step 1: Extract raw feature values
            raw = self._extract_features(features)

            # Step 2: Apply StandardScaler
            scaled = self._scale_features(raw)

            # Step 3: Sum tree predictions
            logit = self.base_score
            for tree in self.trees:
                logit += tree.predict(scaled)

            # Step 4: Sigmoid
            probability = self._sigmoid(logit)
            probability = max(0.0, min(1.0, probability))

            # Step 5: Feature contributions
            top_features = self._compute_contributions(raw)

            return probability, top_features

    def _extract_features(self, features: Dict[str, str]) -> List[float]:
        """Convert string feature map to float array."""
        vals = []
        for name in self.features:
            v = features.get(name, "0")
            try:
                vals.append(float(v))
            except (ValueError, TypeError):
                vals.append(0.0)
        return vals

    def _scale_features(self, raw: List[float]) -> List[float]:
        """Apply StandardScaler: (x - mean) / std"""
        if self.scaler is None:
            return raw
        scaled = []
        for i, v in enumerate(raw):
            std = self.scaler.std[i]
            if std > 1e-10:
                scaled.append((v - self.scaler.mean[i]) / std)
            else:
                scaled.append(0.0)
        return scaled

    def _compute_contributions(self, features: List[float]) -> Dict[str, float]:
        """Estimate feature contributions for explainability."""
        contributions = {}
        for name, importance in self.feature_importance.items():
            if name in self.features:
                idx = self.features.index(name)
                contributions[name] = importance * features[idx]
        return contributions

    @staticmethod
    def _sigmoid(x: float) -> float:
        return 1.0 / (1.0 + math.exp(-x))


# ─── Model Loading ────────────────────────────────────────────────────────────

def load_model(path: str) -> Model:
    """
    Load XGBoost model from native JSON format.
    Mirrors Go scoring.LoadModel().

    JSON structure:
      { "learner": { "gradient_booster": { "model": { "trees": [...] } } } }
    """
    with open(path, "r") as f:
        data = json.load(f)

    learner = data.get("learner", {})
    model_param = learner.get("learner_model_param", {})
    gb_model = learner.get("gradient_booster", {}).get("model", {})
    raw_trees = gb_model.get("trees", [])

    # Parse base_score
    base_score = 0.5
    bs_str = model_param.get("base_score", "0.5")
    try:
        base_score = float(bs_str)
    except (ValueError, TypeError):
        base_score = 0.5

    # Compile trees from parallel arrays
    compiled = []
    total_nodes = 0

    for raw_tree in raw_trees:
        split_indices = raw_tree.get("split_indices", [])
        split_conditions = raw_tree.get("split_conditions", [])
        left_children = raw_tree.get("left_children", [])
        right_children = raw_tree.get("right_children", [])
        base_weights = raw_tree.get("base_weights", [])

        nodes = []
        n = len(split_indices)
        for j in range(n):
            left = left_children[j]
            right = right_children[j]
            is_leaf = (left == -1 and right == -1)
            nodes.append(CompiledNode(
                feature_idx=split_indices[j],
                threshold=split_conditions[j],
                left=left,
                right=right,
                value=base_weights[j],
                is_leaf=is_leaf,
            ))

        compiled.append(CompiledTree(nodes=nodes))
        total_nodes += n

    model = Model()
    model.trees = compiled
    model.base_score = base_score
    model.num_trees = len(compiled)

    logger.info(
        f"XGBoost model loaded: {model.num_trees} trees, "
        f"{total_nodes} nodes, base_score={base_score}"
    )

    return model


def load_scaler(path: str) -> ScalerParams:
    """
    Load StandardScaler parameters from JSON.
    Mirrors Go scoring.LoadScaler().
    """
    with open(path, "r") as f:
        data = json.load(f)

    scaler = ScalerParams(
        version=data.get("version", ""),
        feature_names=data.get("feature_names", []),
        mean=data.get("mean", []),
        std=data.get("std", []),
        n_features=data.get("n_features", 0),
    )

    if len(scaler.mean) != len(scaler.std):
        raise ValueError(
            f"Scaler mean/std length mismatch: {len(scaler.mean)} vs {len(scaler.std)}"
        )

    logger.info(f"Scaler loaded: {scaler.n_features} features, version={scaler.version}")
    return scaler


# ─── Scorer (Model + Rules orchestrator) ──────────────────────────────────────

class Scorer:
    """
    Orchestrates model prediction and rule-based fallback.
    Mirrors Go scoring.Scorer.
    """

    def __init__(self, model: Model, rules_engine):
        self.model = model
        self.rules = rules_engine

    @property
    def model_version(self) -> str:
        return self.model.version

    @property
    def model_loaded_at(self) -> float:
        return self.model.loaded_at

    def score(
        self,
        transaction_id: str,
        features: Dict[str, str],
        model_version: str,
        timestamp_ms: int,
    ) -> ScoreResult:
        """
        Evaluate a transaction for fraud.
        Mirrors Go Scorer.Score().
        """
        start = time.time()

        # 1. XGBoost model prediction
        probability, top_features = self.model.predict(features)

        # 2. Determine decision from threshold
        decision, reason_code = self._classify(probability, features)

        # 3. Rules engine override (can only escalate, never downgrade)
        rule_decision, rule_reason = self.rules.evaluate(features)
        if rule_decision > decision:
            decision = rule_decision
            reason_code = rule_reason

        latency_ms = int((time.time() - start) * 1000)

        result = ScoreResult(
            transaction_id=transaction_id,
            decision=decision,
            fraud_probability=probability,
            model_version=model_version,
            latency_ms=latency_ms,
            top_features=top_features,
            reason_code=reason_code,
            timestamp_ms=timestamp_ms,
        )

        logger.info(
            f"TX={transaction_id} | Decision={decision.name} | "
            f"Prob={probability:.4f} | Latency={latency_ms}ms | Reason={reason_code}"
        )

        return result

    def _classify(self, probability: float, features: Dict[str, str]) -> Tuple[Decision, str]:
        """Map probability to decision and assign reason code."""
        if probability < self.model.thresholds.approve:
            return Decision.APPROVE, ReasonCode.UNKNOWN

        if probability < self.model.thresholds.review:
            reason = self._identify_review_reason(features)
            return Decision.REVIEW, reason

        reason = self._identify_decline_reason(features)
        return Decision.DECLINE, reason

    def _identify_review_reason(self, features: Dict[str, str]) -> str:
        tx_count = _parse_feature_int(features, "velocity_tx_count_1h")
        device_known = _parse_feature_int(features, "device_is_known")
        country_match = _parse_feature_int(features, "device_ip_country_match")

        if tx_count > 5:
            return ReasonCode.VELOCITY_BURST
        if device_known == 0:
            return ReasonCode.NEW_DEVICE
        if country_match == 0:
            return ReasonCode.GEO_ANOMALY
        return ReasonCode.MODEL_SCORE_MEDIUM

    def _identify_decline_reason(self, features: Dict[str, str]) -> str:
        tx_count = _parse_feature_int(features, "velocity_tx_count_1h")
        device_known = _parse_feature_int(features, "device_is_known")
        emulator = _parse_feature_int(features, "device_is_emulator_detected")
        countries = _parse_feature_int(features, "velocity_unique_countries_1h")
        vpn = _parse_feature_int(features, "device_ip_is_vpn")

        if tx_count > 10:
            return ReasonCode.VELOCITY_BURST
        if countries > 2:
            return ReasonCode.IMPOSSIBLE_TRAVEL
        if emulator == 1:
            return ReasonCode.EMULATOR_DETECTED
        if device_known == 0 and vpn == 1:
            return ReasonCode.VPN_PROXY
        return ReasonCode.MODEL_SCORE_HIGH


# ─── Utilities ────────────────────────────────────────────────────────────────

def _parse_feature_int(features: Dict[str, str], key: str) -> int:
    """Parse a feature value as integer."""
    v = features.get(key, "0")
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0
