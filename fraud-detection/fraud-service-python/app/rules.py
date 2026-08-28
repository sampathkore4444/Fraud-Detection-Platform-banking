"""
Rules engine — mirrors Go internal/rules/rules.go

Provides rule-based fraud detection as a fallback when the ML model
is unavailable or for hard business rules.
"""

import math
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

from app.scoring import Decision, ReasonCode

logger = logging.getLogger(__name__)


@dataclass
class RuleResult:
    violated: bool = False
    rule_name: str = ""
    severity: int = 0  # 0=none, 1=review, 2=decline
    message: str = ""


class RulesConfig:
    def __init__(
        self,
        enabled: bool = True,
        max_amount_per_day: float = 50000.0,
        max_tx_per_hour: int = 20,
        max_countries_per_day: int = 3,
        blocked_countries: List[str] = None,
    ):
        self.enabled = enabled
        self.max_amount_per_day = max_amount_per_day
        self.max_tx_per_hour = max_tx_per_hour
        self.max_countries_per_day = max_countries_per_day
        self.blocked_countries = blocked_countries or []


class RulesEngine:
    """
    Rule-based fraud detection engine.
    Mirrors Go rules.RulesEngine.
    """

    def __init__(self, config: RulesConfig):
        self.config = config

    def evaluate(self, features: Dict[str, str]) -> Tuple[Decision, str]:
        """
        Apply all rules to the feature vector.
        Returns the highest-severity decision and reason code.
        """
        if not self.config.enabled:
            return Decision.APPROVE, ""

        violations: List[RuleResult] = []

        # Rule 1: Blocked countries
        result = self._check_blocked_country(features)
        if result.violated:
            violations.append(result)

        # Rule 2: Velocity burst
        result = self._check_velocity_burst(features)
        if result.violated:
            violations.append(result)

        # Rule 3: Impossible travel
        result = self._check_impossible_travel(features)
        if result.violated:
            violations.append(result)

        # Rule 4: Amount anomaly
        result = self._check_amount_anomaly(features)
        if result.violated:
            violations.append(result)

        # Rule 5: Emulator/rooted device
        result = self._check_emulator(features)
        if result.violated:
            violations.append(result)

        # Rule 6: Device multi-account
        result = self._check_device_anomaly(features)
        if result.violated:
            violations.append(result)

        return self._pick_highest_severity(violations)

    def _check_blocked_country(self, features: Dict[str, str]) -> RuleResult:
        country = features.get("country_code", "")
        for blocked in self.config.blocked_countries:
            if country == blocked:
                return RuleResult(
                    violated=True,
                    rule_name="blocked_country",
                    severity=2,
                    message=f"Transaction from blocked country: {country}",
                )
        return RuleResult()

    def _check_velocity_burst(self, features: Dict[str, str]) -> RuleResult:
        tx_count = _parse_int(features, "velocity_tx_count_1h")
        if tx_count > self.config.max_tx_per_hour:
            return RuleResult(
                violated=True,
                rule_name="velocity_burst",
                severity=2,
                message="Transaction count in 1h exceeds limit",
            )
        if tx_count > self.config.max_tx_per_hour // 2:
            return RuleResult(
                violated=True,
                rule_name="velocity_elevated",
                severity=1,
                message="Elevated transaction velocity",
            )
        return RuleResult()

    def _check_impossible_travel(self, features: Dict[str, str]) -> RuleResult:
        countries = _parse_int(features, "velocity_unique_countries_1h")
        if countries > self.config.max_countries_per_day:
            return RuleResult(
                violated=True,
                rule_name="impossible_travel",
                severity=2,
                message="Too many distinct countries in 1 hour",
            )
        if countries > 1:
            return RuleResult(
                violated=True,
                rule_name="multi_country",
                severity=1,
                message="Multiple countries in short time window",
            )
        return RuleResult()

    def _check_amount_anomaly(self, features: Dict[str, str]) -> RuleResult:
        amount = _parse_float(features, "velocity_amount_sum_1h")
        if self.config.max_amount_per_day > 0 and amount > self.config.max_amount_per_day:
            return RuleResult(
                violated=True,
                rule_name="amount_anomaly",
                severity=2,
                message="Single transaction amount exceeds threshold",
            )

        zscore = _parse_float(features, "behavioral_amount_zscore")
        if abs(zscore) > 4.0:
            return RuleResult(
                violated=True,
                rule_name="amount_zscore",
                severity=1,
                message="Amount z-score exceeds threshold",
            )
        return RuleResult()

    def _check_emulator(self, features: Dict[str, str]) -> RuleResult:
        emulator = _parse_int(features, "device_is_emulator_detected")
        if emulator == 1:
            return RuleResult(
                violated=True,
                rule_name="emulator_detected",
                severity=2,
                message="Known emulator fingerprint detected",
            )
        rooted = _parse_int(features, "device_rooted_jailbroken")
        if rooted == 1:
            return RuleResult(
                violated=True,
                rule_name="rooted_device",
                severity=2,
                message="Rooted/jailbroken device detected",
            )
        return RuleResult()

    def _check_device_anomaly(self, features: Dict[str, str]) -> RuleResult:
        unique_accounts = _parse_int(features, "device_unique_accounts_24h")
        if unique_accounts > 5:
            return RuleResult(
                violated=True,
                rule_name="device_multi_account",
                severity=2,
                message="Too many accounts on same device in 24h",
            )
        if unique_accounts > 2:
            return RuleResult(
                violated=True,
                rule_name="device_elevated_accounts",
                severity=1,
                message="Multiple accounts on same device",
            )
        return RuleResult()

    @staticmethod
    def _pick_highest_severity(violations: List[RuleResult]) -> Tuple[Decision, str]:
        max_severity = 0
        reason = ""
        for v in violations:
            if v.severity > max_severity:
                max_severity = v.severity
                reason = v.rule_name

        if max_severity == 2:
            return Decision.DECLINE, reason
        if max_severity == 1:
            return Decision.REVIEW, reason
        return Decision.APPROVE, ""


# ─── Utilities ────────────────────────────────────────────────────────────────

def _parse_int(features: Dict[str, str], key: str) -> int:
    v = features.get(key, "0")
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return 0


def _parse_float(features: Dict[str, str], key: str) -> float:
    v = features.get(key, "0")
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0
