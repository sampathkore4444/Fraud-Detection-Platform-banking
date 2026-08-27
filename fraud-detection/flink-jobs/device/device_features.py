"""
Device & Context Feature Computation — Flink Streaming Job

Computes device and context features for fraud detection per SPEC §3.2.4.
Features: device familiarity, emulator detection, IP risk, etc.

Runs with parallelism=8, RocksDB state backend, 30s checkpoint interval.
"""

from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.time import Time
from pyflink.table import StreamTableEnvironment
import json
import math
from datetime import datetime, timezone


KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "payments.raw.v1"
REDIS_ADDR = "redis:6379"
CHECKPOINT_INTERVAL_MS = 30000


def create_env():
    """Create and configure the Flink execution environment."""
    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # Parallelism per SPEC §3.2.1 — Device = 8
    env.set_parallelism(8)

    return env


class DeviceState:
    """Maintains device-related state for feature computation."""

    def __init__(self):
        # Device seen timestamps (device_id -> last seen ms)
        self.device_last_seen = {}

        # Device -> accounts mapping
        self.device_accounts = {}  # device_id -> set of account_ids

        # IP country lookup cache
        self.ip_country_cache = {}

        # VPN/proxy IP set
        self.vpn_ips = set()

        # Emulator fingerprints
        self.emulator_fingerprints = set()

        # Known browser fingerprints
        self.known_fingerprints = set()

        # OS version history per device
        self.device_os_versions = {}  # device_id -> list of (timestamp, os_version)

        # API latency tracking
        self.api_latencies = []

        # 90 day window for device lookups
        self.WINDOW_90D_MS = 90 * 24 * 3600 * 1000
        self.WINDOW_24H_MS = 24 * 3600 * 1000

    def update(self, event):
        """Update device state with new event."""
        device_id = event.get("device_id")
        account_id = event.get("account_id", "")
        event_time = event.get("timestamp_ms", 0)
        ip_address = event.get("ip_address")
        country_code = event.get("country_code", "")
        os_version = event.get("metadata", {}).get("os_version", "")
        fingerprint = event.get("metadata", {}).get("browser_fingerprint", "")
        is_emulator = event.get("metadata", {}).get("is_emulator", "false")
        is_rooted = event.get("metadata", {}).get("is_rooted", "false")
        api_latency = event.get("metadata", {}).get("api_latency_ms", "0")

        if device_id:
            # Update device last seen
            self.device_last_seen[device_id] = event_time

            # Update device -> accounts mapping
            if device_id not in self.device_accounts:
                self.device_accounts[device_id] = set()
            self.device_accounts[device_id].add(account_id)

            # Track OS versions
            if os_version:
                if device_id not in self.device_os_versions:
                    self.device_os_versions[device_id] = []
                self.device_os_versions[device_id].append((event_time, os_version))

        if ip_address:
            # Cache IP country mapping
            self.ip_country_cache[ip_address] = country_code

            # Track VPN IPs (from external feed — here simulated)
            # In production, this would be loaded from a threat intelligence feed

        if fingerprint:
            self.known_fingerprints.add(fingerprint)

        if is_emulator == "true":
            self.emulator_fingerprints.add(device_id)

        # Track API latency
        try:
            latency = float(api_latency)
            if latency > 0:
                self.api_latencies.append((event_time, latency))
        except (ValueError, TypeError):
            pass

    def compute_features(self, event):
        """Compute device features from current state."""
        device_id = event.get("device_id")
        ip_address = event.get("ip_address")
        country_code = event.get("country_code", "")
        event_time = event.get("timestamp_ms", 0)
        os_version = event.get("metadata", {}).get("os_version", "")
        fingerprint = event.get("metadata", {}).get("browser_fingerprint", "")

        features = {}

        if not device_id:
            # No device info — use safe defaults
            features.update({
                "device_is_known": 0,
                "device_last_seen_hours_ago": 999999.0,
                "device_unique_accounts_24h": 0,
                "device_is_emulator_detected": 0,
                "device_rooted_jailbroken": 0,
                "device_ip_country_match": 0,
                "device_ip_is_vpn": 0,
                "device_browser_fingerprint_match": 0,
                "device_latency_anomaly": 0,
                "device_is_new_os_version": 0,
            })
            return features

        # Feature 1: device_is_known (seen in last 90d)
        last_seen = self.device_last_seen.get(device_id, 0)
        is_known = 1 if (event_time - last_seen) < self.WINDOW_90D_MS else 0
        features["device_is_known"] = is_known

        # Feature 2: device_last_seen_hours_ago
        hours_ago = (event_time - last_seen) / (3600 * 1000) if last_seen > 0 else 999999.0
        features["device_last_seen_hours_ago"] = hours_ago

        # Feature 3: device_unique_accounts_24h
        accounts_24h = 0
        if device_id in self.device_accounts:
            # In production, filter by time window
            accounts_24h = len(self.device_accounts[device_id])
        features["device_unique_accounts_24h"] = accounts_24h

        # Feature 4: device_is_emulator_detected
        features["device_is_emulator_detected"] = (
            1 if device_id in self.emulator_fingerprints else 0
        )

        # Feature 5: device_rooted_jailbroken
        is_rooted = event.get("metadata", {}).get("is_rooted", "false")
        features["device_rooted_jailbroken"] = 1 if is_rooted == "true" else 0

        # Feature 6: device_ip_country_match
        if ip_address and ip_address in self.ip_country_cache:
            ip_country = self.ip_country_cache[ip_address]
            features["device_ip_country_match"] = 1 if ip_country == country_code else 0
        else:
            features["device_ip_country_match"] = 0

        # Feature 7: device_ip_is_vpn
        features["device_ip_is_vpn"] = 1 if ip_address in self.vpn_ips else 0

        # Feature 8: device_browser_fingerprint_match
        features["device_browser_fingerprint_match"] = (
            1 if fingerprint in self.known_fingerprints else 0
        )

        # Feature 9: device_latency_anomaly
        features["device_latency_anomaly"] = self._compute_latency_anomaly(event_time)

        # Feature 10: device_is_new_os_version
        features["device_is_new_os_version"] = self._check_new_os(
            device_id, os_version, event_time
        )

        return features

    def _compute_latency_anomaly(self, current_time):
        """Detect API latency anomalies using rolling statistics."""
        if len(self.api_latencies) < 10:
            return 0

        # Filter to recent latencies (1 hour)
        cutoff = current_time - 3600 * 1000
        recent = [lat for ts, lat in self.api_latencies if ts >= cutoff]

        if len(recent) < 5:
            return 0

        mean = sum(recent) / len(recent)
        variance = sum((x - mean) ** 2 for x in recent) / len(recent)
        stddev = math.sqrt(variance)

        current = recent[-1] if recent else 0
        if stddev > 0 and (current - mean) > 3 * stddev:
            return 1
        return 0

    def _check_new_os_version(self, device_id, current_os, event_time):
        """Check if OS version has changed recently."""
        if not device_id or not current_os:
            return 0

        history = self.device_os_versions.get(device_id, [])
        if not history:
            return 0

        # Check if any previous version differs from current
        for ts, os_ver in history:
            if ts < event_time and os_ver != current_os:
                return 1
        return 0


class DeviceFeatureProcessor(KeyedProcessFunction):
    """Stateful processor for device features."""

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = DeviceState()

    def process_element(self, value, ctx):
        """Process event and emit device features."""
        event = json.loads(value) if isinstance(value, str) else value

        # Update state
        self.state.update(event)

        # Compute features
        features = self.state.compute_features(event)

        # Enrich with metadata
        features["account_id"] = event.get("account_id", "")
        features["event_time_ms"] = str(event.get("timestamp_ms", 0))

        yield json.dumps(features)


# ── Redis Enrichment (Device Lookup) ──────────────────────────

class DeviceRedisEnricher:
    """
    Enriches events with device data from Redis before feature computation.
    Per SPEC §3.2.4: device_is_known, device_last_seen_hours_ago from Redis lookup.
    """

    def __init__(self):
        self.redis_client = None

    def open(self):
        import redis
        self.redis_client = redis.Redis(
            host=REDIS_ADDR.split(":")[0],
            port=int(REDIS_ADDR.split(":")[1]),
            decode_responses=True
        )

    def enrich(self, event):
        """Enrich event with Redis-stored device data."""
        device_id = event.get("device_id")
        if not device_id:
            return event

        # Lookup device accounts from Redis
        device_key = f"device:{device_id}:accounts"
        accounts = self.redis_client.smembers(device_key)
        if accounts:
            event["_redis_device_accounts"] = list(accounts)

        # Lookup device last seen
        last_seen_key = f"device:{device_id}:last_seen"
        last_seen = self.redis_client.get(last_seen_key)
        if last_seen:
            event["_redis_device_last_seen"] = int(last_seen)

        # Lookup IP risk
        ip_address = event.get("ip_address")
        if ip_address:
            ip_key = f"ip_risk:{ip_address}"
            ip_data = self.redis_client.hgetall(ip_key)
            if ip_data:
                event["_redis_ip_is_vpn"] = ip_data.get("is_vpn", "false")
                event["_redis_ip_country"] = ip_data.get("country", "")

        return event


# ── Redis Sink ────────────────────────────────────────────────

class DeviceRedisSink:
    """Writes device features to Redis."""

    def __init__(self):
        self.redis_client = None

    def open(self):
        import redis
        self.redis_client = redis.Redis(
            host=REDIS_ADDR.split(":")[0],
            port=int(REDIS_ADDR.split(":")[1]),
            decode_responses=True
        )

    def process(self, value):
        data = json.loads(value) if isinstance(value, str) else value
        device_id = data.get("device_id")
        account_id = data.get("account_id", "")

        if device_id:
            # Update device -> accounts mapping (90d TTL)
            self.redis_client.sadd(f"device:{device_id}:accounts", account_id)
            self.redis_client.expire(f"device:{device_id}:accounts", 90 * 86400)

            # Update device last seen
            self.redis_client.setex(
                f"device:{device_id}:last_seen",
                90 * 86400,
                str(data.get("event_time_ms", 0))
            )

        # Store feature vector for this transaction
        tx_id = data.get("transaction_id", "")
        if tx_id:
            self.redis_client.setex(
                f"device_features:{tx_id}",
                300,  # 5 min TTL per SPEC §3.2.5
                json.dumps(data)
            )

        return value


# ── Main Pipeline ─────────────────────────────────────────────

def main():
    env = create_env()

    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.0.0-1.18.jar")

    t_env = StreamTableEnvironment.create(env)

    t_env.execute_sql(f"""
        CREATE TABLE payment_events (
            event_id STRING,
            timestamp_ms BIGINT,
            account_id STRING,
            card_id STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            merchant_id STRING,
            merchant_category_code INT,
            channel STRING,
            country_code STRING,
            ip_address STRING,
            device_id STRING,
            geolocation STRING,
            metadata MAP<STRING, STRING>,
            event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'flink-device-features',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # Convert to DataStream for stateful processing
    ds = t_env.toDataStream(t_env.from("payment_events"))

    # Key by device_id (or account_id as fallback)
    keyed = ds.key_by(lambda e: e.get("device_id") or e["account_id"])

    # Process device features
    device_features = keyed.process(DeviceFeatureProcessor())

    # Write to Redis
    device_features.add_sink(DeviceRedisSink())

    env.execute("Device Feature Computation Job")


if __name__ == "__main__":
    main()
