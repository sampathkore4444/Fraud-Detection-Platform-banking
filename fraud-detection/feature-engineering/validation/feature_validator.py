"""
Feature Vector Validation

Validates assembled feature vectors before scoring per SPEC §3.2.5.
Ensures all 30 features are present, within expected ranges, and type-safe.
"""

import json
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ── Feature Definitions ───────────────────────────────────────

@dataclass
class FeatureDefinition:
    name: str
    feature_type: str  # "int", "float", "binary"
    min_value: float
    max_value: float
    required: bool = True
    default_value: float = 0.0


FEATURE_DEFINITIONS: Dict[str, FeatureDefinition] = {
    # Velocity Features
    "velocity_tx_count_1h": FeatureDefinition("int", "int", 0, 1000),
    "velocity_tx_count_24h": FeatureDefinition("int", "int", 0, 10000),
    "velocity_amount_sum_1h": FeatureDefinition("float", "float", 0, 1000000),
    "velocity_amount_sum_24h": FeatureDefinition("float", "float", 0, 10000000),
    "velocity_decline_count_24h": FeatureDefinition("int", "int", 0, 1000),
    "velocity_unique_countries_1h": FeatureDefinition("int", "int", 0, 50),
    "velocity_unique_merchants_24h": FeatureDefinition("int", "int", 0, 500),
    "velocity_avg_amount_7d": FeatureDefinition("float", "float", 0, 1000000),
    "velocity_stddev_amount_7d": FeatureDefinition("float", "float", 0, 1000000),
    "velocity_time_since_last_tx": FeatureDefinition("long", "float", 0, 31536000),

    # Behavioral Features
    "behavioral_typical_amount_ratio": FeatureDefinition("float", "float", 0, 100),
    "behavioral_typical_hour_score": FeatureDefinition("float", "float", 0, 1),
    "behavioral_typical_day_score": FeatureDefinition("float", "float", 0, 1),
    "behavioral_merchant_category_diversity": FeatureDefinition("int", "int", 0, 500),
    "behavioral_amount_zscore": FeatureDefinition("float", "float", -10, 10),
    "behavioral_is_recipient_new": FeatureDefinition("binary", "int", 0, 1),
    "behavioral_velocity_direction": FeatureDefinition("float", "float", 0, 100),
    "behavioral_time_between_tx_stddev": FeatureDefinition("float", "float", 0, 86400),
    "behavioral_country_change_freq": FeatureDefinition("float", "float", 0, 30),
    "behavioral_night_tx_ratio": FeatureDefinition("float", "float", 0, 1),

    # Device Features
    "device_is_known": FeatureDefinition("binary", "int", 0, 1),
    "device_last_seen_hours_ago": FeatureDefinition("float", "float", 0, 999999),
    "device_unique_accounts_24h": FeatureDefinition("int", "int", 0, 100),
    "device_is_emulator_detected": FeatureDefinition("binary", "int", 0, 1),
    "device_rooted_jailbroken": FeatureDefinition("binary", "int", 0, 1),
    "device_ip_country_match": FeatureDefinition("binary", "int", 0, 1),
    "device_ip_is_vpn": FeatureDefinition("binary", "int", 0, 1),
    "device_browser_fingerprint_match": FeatureDefinition("binary", "int", 0, 1),
    "device_latency_anomaly": FeatureDefinition("binary", "int", 0, 1),
    "device_is_new_os_version": FeatureDefinition("binary", "int", 0, 1),
}

EXPECTED_FEATURE_COUNT = 30


# ── Validation Result ─────────────────────────────────────────

@dataclass
class ValidationResult:
    valid: bool
    errors: List[str]
    warnings: List[str]
    missing_features: List[str]
    out_of_range_features: List[str]
    type_errors: List[str]


def validate_feature_vector(features: Dict[str, str]) -> ValidationResult:
    """
    Validate a feature vector before scoring.
    Returns ValidationResult with errors and warnings.
    """
    errors = []
    warnings = []
    missing_features = []
    out_of_range_features = []
    type_errors = []

    # Check feature count
    provided_features = set(features.keys()) - {"transaction_id", "account_id", "amount", "currency",
                                                  "merchant_id", "merchant_category_code", "channel",
                                                  "country_code", "timestamp_ms"}

    if len(provided_features) < EXPECTED_FEATURE_COUNT:
        warnings.append(
            f"Expected {EXPECTED_FEATURE_COUNT} features, got {len(provided_features)}"
        )

    # Validate each expected feature
    for name, definition in FEATURE_DEFINITIONS.items():
        if name not in features:
            if definition.required:
                missing_features.append(name)
                errors.append(f"Missing required feature: {name}")
            continue

        value_str = features[name]

        # Type validation
        try:
            value = float(value_str)
        except (ValueError, TypeError):
            type_errors.append(name)
            errors.append(f"Invalid type for feature {name}: '{value_str}'")
            continue

        # Range validation
        if value < definition.min_value or value > definition.max_value:
            out_of_range_features.append(name)
            warnings.append(
                f"Feature {name} out of range: {value} not in [{definition.min_value}, {definition.max_value}]"
            )

    # Check for unexpected features
    unexpected = set(features.keys()) - set(FEATURE_DEFINITIONS.keys()) - {
        "transaction_id", "account_id", "amount", "currency",
        "merchant_id", "merchant_category_code", "channel",
        "country_code", "timestamp_ms"
    }
    if unexpected:
        warnings.append(f"Unexpected features ignored: {unexpected}")

    valid = len(errors) == 0

    result = ValidationResult(
        valid=valid,
        errors=errors,
        warnings=warnings,
        missing_features=missing_features,
        out_of_range_features=out_of_range_features,
        type_errors=type_errors,
    )

    if not valid:
        logger.warning(f"Feature validation failed: {errors}")
    if warnings:
        logger.info(f"Feature validation warnings: {warnings}")

    return result


def fill_defaults(features: Dict[str, str]) -> Dict[str, str]:
    """
    Fill missing features with default values.
    Use with caution — defaults may mask data quality issues.
    """
    filled = dict(features)

    for name, definition in FEATURE_DEFINITIONS.items():
        if name not in filled:
            filled[name] = str(definition.default_value)
            logger.debug(f"Filled missing feature {name} with default {definition.default_value}")

    return filled


def compute_feature_hash(features: Dict[str, str]) -> str:
    """Compute a hash of the feature vector for change detection."""
    import hashlib
    sorted_features = json.dumps(dict(sorted(features.items())), sort_keys=True)
    return hashlib.sha256(sorted_features.encode()).hexdigest()[:16]
