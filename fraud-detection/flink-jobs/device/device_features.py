"""
Device & Context Feature Computation — Flink Streaming Job

Computes device and context features for fraud detection per SPEC §3.2.4.
Features: device familiarity, emulator detection, IP risk, etc.

Runs with parallelism=8, RocksDB state backend, 30s checkpoint interval.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.common import WatermarkStrategy
from pyflink.table import StreamTableEnvironment
import json
import math
from datetime import datetime, timezone
import redis


KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "payments.raw.v1"
REDIS_HOST = "redis"
REDIS_PORT = 6379
CHECKPOINT_INTERVAL_MS = 30000
REDIS_DEVICE_TTL = 90 * 86400   # 90 days
REDIS_FEATURE_TTL = 300          # 5 minutes


def create_env():
    """Create and configure the Flink execution environment."""
    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    env.set_parallelism(8)
    return env


# ── Device State ───────────────────────────────────────────────

class DeviceState:
    """Maintains device-related state for feature computation."""

    def __init__(self):
        self.device_last_seen = {}       # device_id -> last seen ms
        self.device_accounts = {}        # device_id -> set of account_ids
        self.ip_country_cache = {}       # ip -> country
        self.vpn_ips = set()
        self.emulator_fingerprints = set()
        self.known_fingerprints = set()
        self.device_os_versions = {}     # device_id -> [(timestamp, os_version)]
        self.api_latencies = []
        self.WINDOW_90D_MS = 90 * 24 * 3600 * 1000
        self.WINDOW_24H_MS = 24 * 3600 * 1000

    def update(self, event):
        """Update device state with new event."""
        device_id = event.get("device_id")
        account_id = event.get("account_id", "")
        event_time = event.get("timestamp_ms", 0)
        ip_address = event.get("ip_address")
        country_code = event.get("country_code", "")
        metadata = event.get("metadata", {}) or {}
        os_version = metadata.get("os_version", "")
        fingerprint = metadata.get("browser_fingerprint", "")
        is_emulator = metadata.get("is_emulator", "false")
        api_latency = metadata.get("api_latency_ms", "0")

        if device_id:
            self.device_last_seen[device_id] = event_time
            if device_id not in self.device_accounts:
                self.device_accounts[device_id] = set()
            self.device_accounts[device_id].add(account_id)

            if os_version:
                if device_id not in self.device_os_versions:
                    self.device_os_versions[device_id] = []
                self.device_os_versions[device_id].append((event_time, os_version))

        if ip_address:
            self.ip_country_cache[ip_address] = country_code

        if fingerprint:
            self.known_fingerprints.add(fingerprint)

        if is_emulator == "true":
            self.emulator_fingerprints.add(device_id)

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
        metadata = event.get("metadata", {}) or {}
        os_version = metadata.get("os_version", "")
        fingerprint = metadata.get("browser_fingerprint", "")
        is_rooted = metadata.get("is_rooted", "false")

        features = {}

        if not device_id:
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

        # device_is_known
        last_seen = self.device_last_seen.get(device_id, 0)
        features["device_is_known"] = 1 if (event_time - last_seen) < self.WINDOW_90D_MS else 0

        # device_last_seen_hours_ago
        features["device_last_seen_hours_ago"] = (
            (event_time - last_seen) / (3600 * 1000) if last_seen > 0 else 999999.0
        )

        # device_unique_accounts_24h
        features["device_unique_accounts_24h"] = len(self.device_accounts.get(device_id, set()))

        # device_is_emulator_detected
        features["device_is_emulator_detected"] = (
            1 if device_id in self.emulator_fingerprints else 0
        )

        # device_rooted_jailbroken
        features["device_rooted_jailbroken"] = 1 if is_rooted == "true" else 0

        # device_ip_country_match
        if ip_address and ip_address in self.ip_country_cache:
            features["device_ip_country_match"] = (
                1 if self.ip_country_cache[ip_address] == country_code else 0
            )
        else:
            features["device_ip_country_match"] = 0

        # device_ip_is_vpn
        features["device_ip_is_vpn"] = 1 if ip_address in self.vpn_ips else 0

        # device_browser_fingerprint_match
        features["device_browser_fingerprint_match"] = (
            1 if fingerprint in self.known_fingerprints else 0
        )

        # device_latency_anomaly
        features["device_latency_anomaly"] = self._compute_latency_anomaly(event_time)

        # device_is_new_os_version
        features["device_is_new_os_version"] = self._check_new_os(device_id, os_version, event_time)

        return features

    def _compute_latency_anomaly(self, current_time):
        if len(self.api_latencies) < 10:
            return 0
        cutoff = current_time - 3600 * 1000
        recent = [lat for ts, lat in self.api_latencies if ts >= cutoff]
        if len(recent) < 5:
            return 0
        mean = sum(recent) / len(recent)
        variance = sum((x - mean) ** 2 for x in recent) / len(recent)
        stddev = math.sqrt(variance)
        current = recent[-1]
        return 1 if stddev > 0 and (current - mean) > 3 * stddev else 0

    def _check_new_os(self, device_id, current_os, event_time):
        if not device_id or not current_os:
            return 0
        history = self.device_os_versions.get(device_id, [])
        for ts, os_ver in history:
            if ts < event_time and os_ver != current_os:
                return 1
        return 0


# ── Stateful Processor ─────────────────────────────────────────

class DeviceFeatureProcessor(KeyedProcessFunction):
    """Stateful processor for device features."""

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = DeviceState()

    def process_element(self, value, ctx):
        event = json.loads(value) if isinstance(value, str) else value
        self.state.update(event)
        features = self.state.compute_features(event)
        features["account_id"] = event.get("account_id", "")
        features["event_time_ms"] = str(event.get("timestamp_ms", 0))
        yield json.dumps(features)


# ── Redis Sink ────────────────────────────────────────────────

class DeviceRedisSink(MapFunction):
    """Writes device features to Redis."""

    def __init__(self):
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
        """Initialize Redis client — MUST accept runtime_context per PyFlink API."""
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

    def map(self, value):
        data = json.loads(value) if isinstance(value, str) else value
        device_id = data.get("device_id")
        account_id = data.get("account_id", "")
        tx_id = data.get("event_id", "")

        if device_id:
            # Update device -> accounts mapping (90d TTL)
            self.redis_client.sadd(f"device:{device_id}:accounts", account_id)
            self.redis_client.expire(f"device:{device_id}:accounts", REDIS_DEVICE_TTL)

            # Update device last seen
            self.redis_client.setex(
                f"device:{device_id}:last_seen",
                REDIS_DEVICE_TTL,
                str(data.get("event_time_ms", 0))
            )

        # Store feature vector for this transaction (5 min TTL)
        if tx_id:
            self.redis_client.setex(
                f"device_features:{tx_id}",
                REDIS_FEATURE_TTL,
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
    ds = t_env.toDataStream(t_env.from_path("payment_events"))

    # Key by device_id (fallback to account_id)
    keyed = ds.key_by(lambda e: e.get("device_id") or e["account_id"])

    # Process device features
    device_features = keyed.process(DeviceFeatureProcessor())

    # Write to Redis
    device_features.map(DeviceRedisSink())

    env.execute("Device Feature Computation Job")


if __name__ == "__main__":
    main()
