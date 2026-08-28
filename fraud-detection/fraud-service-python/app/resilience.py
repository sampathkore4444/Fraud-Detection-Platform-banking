"""
Resilience patterns — mirrors Go internal/resilience/ package

Implements:
- CircuitBreaker: CLOSED → OPEN → HALF_OPEN → CLOSED
- Retry with exponential backoff and jitter
- Fallback feature vectors for degraded mode
- Prometheus metrics for all resilience events
"""

import math
import random
import time
import threading
import logging
from enum import Enum
from typing import Callable, Any, Optional
from dataclasses import dataclass, field

from prometheus_client import Counter, Gauge

logger = logging.getLogger(__name__)


# ─── Prometheus Metrics ──────────────────────────────────────────────────────

circuit_breaker_state = Gauge(
    "fraud_service_circuit_breaker_state",
    "Current circuit breaker state (0=closed, 1=open, 2=half_open)",
    ["name"],
)

circuit_breaker_tripped = Counter(
    "fraud_service_circuit_breaker_tripped_total",
    "Total circuit breaker trips to OPEN",
    ["name"],
)

circuit_breaker_recovered = Counter(
    "fraud_service_circuit_breaker_recovered_total",
    "Total circuit breaker recoveries to CLOSED",
    ["name"],
)

circuit_breaker_requests = Counter(
    "fraud_service_circuit_breaker_requests_total",
    "Total requests through circuit breaker",
    ["name", "result"],
)

retry_attempts = Counter(
    "fraud_service_retry_attempts_total",
    "Total retry attempts",
    ["operation", "attempt"],
)

retry_successes = Counter(
    "fraud_service_retry_successes_total",
    "Total successful calls (including first attempt)",
    ["operation"],
)

retry_exhausted = Counter(
    "fraud_service_retry_exhausted_total",
    "Total calls that failed after all retries",
    ["operation"],
)

fallback_triggered = Counter(
    "fraud_service_fallback_triggered_total",
    "Total fallback triggers",
    ["component", "reason"],
)


# ─── Circuit Breaker ─────────────────────────────────────────────────────────

class CircuitState(Enum):
    CLOSED = "CLOSED"       # Normal — requests flow through
    OPEN = "OPEN"           # Tripped — fast-fail
    HALF_OPEN = "HALF_OPEN" # Probing — one request allowed


class CircuitBreaker:
    """
    Circuit breaker pattern — mirrors Go CircuitBreaker.

    States:
      CLOSED:   Normal operation. Failures counted.
      OPEN:     Tripped. All requests fast-fail.
      HALF_OPEN: After timeout, one probe allowed.
                 Success → CLOSED. Failure → OPEN.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout_seconds: float = 30.0,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout = timeout_seconds

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float = 0
        self._lock = threading.RLock()

        circuit_breaker_state.labels(name=name).set(0)

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def allow(self) -> bool:
        """Check if a request is allowed through."""
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                # Check if timeout elapsed → HALF_OPEN
                if time.time() - self._last_failure_time >= self.timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    circuit_breaker_state.labels(name=self.name).set(2)
                    logger.info(f"Circuit breaker [{self.name}] → HALF_OPEN (probing)")
                    return True
                return False

            if self._state == CircuitState.HALF_OPEN:
                return True

        return False

    def record_success(self):
        """Record a successful call."""
        with self._lock:
            circuit_breaker_requests.labels(name=self.name, result="success").inc()

            if self._state == CircuitState.CLOSED:
                self._failure_count = 0

            elif self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    circuit_breaker_state.labels(name=self.name).set(0)
                    circuit_breaker_recovered.labels(name=self.name).inc()
                    logger.info(f"Circuit breaker [{self.name}] → CLOSED (recovered)")

    def record_failure(self):
        """Record a failed call."""
        with self._lock:
            circuit_breaker_requests.labels(name=self.name, result="failure").inc()

            if self._state == CircuitState.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    self._last_failure_time = time.time()
                    circuit_breaker_state.labels(name=self.name).set(1)
                    circuit_breaker_tripped.labels(name=self.name).inc()
                    logger.warning(
                        f"Circuit breaker [{self.name}] → OPEN "
                        f"(failures={self._failure_count}, timeout={self.timeout}s)"
                    )

            elif self._state == CircuitState.HALF_OPEN:
                # Probe failed → back to OPEN
                self._state = CircuitState.OPEN
                self._last_failure_time = time.time()
                self._success_count = 0
                circuit_breaker_state.labels(name=self.name).set(1)
                circuit_breaker_tripped.labels(name=self.name).inc()
                logger.warning(f"Circuit breaker [{self.name}] → OPEN (probe failed)")

    def is_available(self) -> bool:
        return self._state in (CircuitState.CLOSED, CircuitState.HALF_OPEN)


# ─── Retry with Exponential Backoff ──────────────────────────────────────────

@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay_ms: float = 10
    max_delay_ms: float = 1000
    multiplier: float = 2.0
    jitter: float = 0.1


def default_redis_retry() -> RetryConfig:
    """Retry config tuned for Redis (fast, aggressive retries)."""
    return RetryConfig(max_attempts=3, base_delay_ms=5, max_delay_ms=100, multiplier=2.0, jitter=0.2)


def default_grpc_retry() -> RetryConfig:
    """Retry config tuned for gRPC (slower, fewer retries)."""
    return RetryConfig(max_attempts=2, base_delay_ms=20, max_delay_ms=200, multiplier=2.0, jitter=0.1)


def compute_backoff(cfg: RetryConfig, attempt: int) -> float:
    """
    Compute delay with exponential backoff + jitter.
    delay = min(base * multiplier^(attempt-1), max) + jitter
    """
    exp_delay = cfg.base_delay_ms * (cfg.multiplier ** (attempt - 1))
    capped = min(exp_delay, cfg.max_delay_ms)
    jitter = random.random() * cfg.jitter * capped
    return capped + jitter


def retry(
    operation: str,
    cfg: RetryConfig,
    fn: Callable[[], Any],
) -> Any:
    """
    Execute fn with retry and exponential backoff.
    Mirrors Go retry.Retry().
    """
    last_err = None

    for attempt in range(1, cfg.max_attempts + 1):
        try:
            result = fn()
            retry_successes.labels(operation=operation).inc()
            if attempt > 1:
                logger.info(f"Retry [{operation}] succeeded on attempt {attempt}")
            return result
        except Exception as e:
            last_err = e
            retry_attempts.labels(operation=operation, attempt=str(attempt)).inc()

            if attempt < cfg.max_attempts:
                delay_ms = compute_backoff(cfg, attempt)
                logger.debug(
                    f"Retry [{operation}] attempt {attempt} failed: {e}, "
                    f"backoff={delay_ms:.1f}ms"
                )
                time.sleep(delay_ms / 1000.0)

    retry_exhausted.labels(operation=operation).inc()
    logger.warning(
        f"Retry [{operation}] exhausted after {cfg.max_attempts} attempts: {last_err}"
    )
    raise last_err


# ─── Fallback Feature Vector ─────────────────────────────────────────────────

def default_feature_vector() -> dict:
    """
    Safe default feature vector when Redis is unavailable.
    Mirrors Go resilience.DefaultFeatureVector().

    Values represent "unknown/new customer" profile:
    - No velocity history (first transaction)
    - Unknown device
    - No behavioral baseline
    → Triggers REVIEW (conservative), not APPROVE or DECLINE.
    """
    return {
        # Velocity — first transaction
        "velocity_tx_count_1h": "1",
        "velocity_tx_count_24h": "1",
        "velocity_amount_sum_1h": "0",
        "velocity_amount_sum_24h": "0",
        "velocity_decline_count_24h": "0",
        "velocity_unique_countries_1h": "0",
        "velocity_unique_merchants_24h": "0",
        "velocity_avg_amount_7d": "0",
        "velocity_stddev_amount_7d": "0",
        "velocity_time_since_last_tx": "999999",
        # Behavioral — no baseline
        "behavioral_typical_amount_ratio": "1.0",
        "behavioral_typical_hour_score": "0.0",
        "behavioral_typical_day_score": "0.0",
        "behavioral_merchant_category_diversity": "0",
        "behavioral_amount_zscore": "0.0",
        "behavioral_is_recipient_new": "1",
        "behavioral_velocity_direction": "1.0",
        "behavioral_time_between_tx_stddev": "0",
        "behavioral_country_change_freq": "0",
        "behavioral_night_tx_ratio": "0",
        # Device — unknown (highest risk)
        "device_is_known": "0",
        "device_last_seen_hours_ago": "999999",
        "device_unique_accounts_24h": "0",
        "device_is_emulator_detected": "0",
        "device_rooted_jailbroken": "0",
        "device_ip_country_match": "0",
        "device_ip_is_vpn": "0",
        "device_browser_fingerprint_match": "0",
        "device_latency_anomaly": "0",
        "device_is_new_os_version": "0",
    }


# ─── Resilient Redis Client ──────────────────────────────────────────────────

class ResilientRedis:
    """
    Redis client with circuit breaker + retry + fallback.
    Mirrors Go resilience.ResilientRedis.
    """

    def __init__(self, redis_client, circuit_breaker: CircuitBreaker):
        self.client = redis_client
        self.cb = circuit_breaker
        self.retry_cfg = default_redis_retry()

    def get_feature_vector(self, key: str) -> dict:
        """
        Get feature vector from Redis with resilience.
        Falls back to default_feature_vector() on failure.
        """
        operation = "redis:get_feature_vector"

        # 1. Circuit breaker check
        if not self.cb.allow():
            fallback_triggered.labels(component="redis", reason="circuit_open").inc()
            logger.warning(f"Circuit breaker OPEN — returning default feature vector for {key}")
            return default_feature_vector()

        # 2. Retry with backoff
        try:
            def _get():
                val = self.client.get(key)
                if val is None:
                    raise KeyError(f"Key not found: {key}")
                import json
                result = json.loads(val) if isinstance(val, (str, bytes)) else val
                return result

            result = retry(operation, self.retry_cfg, _get)
            self.cb.record_success()
            return result

        except Exception as e:
            self.cb.record_failure()
            fallback_triggered.labels(component="redis", reason="retry_exhausted").inc()
            logger.warning(f"Redis unavailable after retries — returning default: {e}")
            return default_feature_vector()

    def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            return self.client.ping()
        except Exception:
            return False

    def setex(self, key: str, ttl_seconds: int, value: str):
        """Store value with TTL."""
        self.client.setex(key, ttl_seconds, value)
