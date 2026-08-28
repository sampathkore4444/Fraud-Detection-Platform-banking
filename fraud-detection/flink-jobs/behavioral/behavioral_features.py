"""
Behavioral Feature Computation — Flink Streaming Job

Computes pattern-based behavioral features for fraud detection per SPEC §3.2.3.
Features: typical amounts, time patterns, merchant diversity, etc.

Runs with parallelism=16, RocksDB state backend, 30s checkpoint interval.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.common import WatermarkStrategy
from pyflink.table import StreamTableEnvironment
import json
import math
import statistics
from datetime import datetime, timezone
import redis


KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "payments.raw.v1"
REDIS_HOST = "redis"
REDIS_PORT = 6379
CHECKPOINT_INTERVAL_MS = 30000
REDIS_TTL_SECONDS = 86400


def create_env():
    """Create and configure the Flink execution environment."""
    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    env.set_parallelism(16)
    return env


# ── Behavioral State ──────────────────────────────────────────

class BehavioralState:
    """Maintains long-running behavioral state for an account."""

    def __init__(self):
        self.amount_history = []
        self.amount_sum = 0.0
        self.amount_sum_sq = 0.0
        self.amount_count = 0
        self.hour_counts = [0] * 24
        self.hour_total = 0
        self.day_counts = [0] * 7
        self.day_total = 0
        self.mcc_set = set()
        self.known_recipients = set()
        self.inflow_total = 0.0
        self.outflow_total = 0.0
        self.last_tx_time = None
        self.inter_tx_times = []
        self.country_history = []
        self.night_tx_count = 0
        self.total_tx_count = 0
        self.WINDOW_30D_MS = 30 * 24 * 3600 * 1000

    def update(self, event):
        """Update state with a new transaction event."""
        amount = float(event.get("amount", 0))
        event_time = event.get("timestamp_ms", 0)
        hour = event.get("hour_of_day", 0)
        day = event.get("day_of_week", 0)
        mcc = event.get("merchant_category_code", 0)
        recipient = event.get("merchant_id", "")
        country = event.get("country_code", "")
        channel = event.get("channel", "")

        # Amount stats
        self.amount_history.append((event_time, amount))
        self.amount_sum += amount
        self.amount_sum_sq += amount * amount
        self.amount_count += 1

        # Prune old amounts (30 day window)
        cutoff = event_time - self.WINDOW_30D_MS
        while self.amount_history and self.amount_history[0][0] < cutoff:
            _, old_amount = self.amount_history.pop(0)
            self.amount_sum -= old_amount
            self.amount_sum_sq -= old_amount * old_amount
            self.amount_count -= 1

        # Hour/day distribution
        self.hour_counts[hour] += 1
        self.hour_total += 1
        self.day_counts[day] += 1
        self.day_total += 1

        # Merchant/recipients
        self.mcc_set.add(mcc)
        self.known_recipients.add(recipient)

        # Inflow/outflow
        if channel in ("POS", "ATM", "CNP", "WEB"):
            self.outflow_total += amount
        else:
            self.inflow_total += amount

        # Inter-transaction time
        if self.last_tx_time is not None:
            delta = (event_time - self.last_tx_time) // 1000
            self.inter_tx_times.append(delta)
        self.last_tx_time = event_time

        # Country tracking
        self.country_history.append((event_time, country))
        country_cutoff = event_time - self.WINDOW_30D_MS
        self.country_history = [(t, c) for t, c in self.country_history if t >= country_cutoff]

        # Night transactions (23:00-05:00)
        if hour >= 23 or hour < 5:
            self.night_tx_count += 1
        self.total_tx_count += 1

    def compute_features(self, current_event):
        """Compute all behavioral features from current state."""
        current_amount = float(current_event.get("amount", 0))
        current_hour = current_event.get("hour_of_day", 0)
        current_day = current_event.get("day_of_week", 0)
        current_recipient = current_event.get("merchant_id", "")

        features = {}

        # behavioral_typical_amount_ratio
        avg_amount = self.amount_sum / max(self.amount_count, 1)
        features["behavioral_typical_amount_ratio"] = (
            current_amount / avg_amount if avg_amount > 0 else 1.0
        )

        # behavioral_typical_hour_score
        features["behavioral_typical_hour_score"] = (
            self.hour_counts[current_hour] / self.hour_total
            if self.hour_total > 0 else 1.0 / 24.0
        )

        # behavioral_typical_day_score
        features["behavioral_typical_day_score"] = (
            self.day_counts[current_day] / self.day_total
            if self.day_total > 0 else 1.0 / 7.0
        )

        # behavioral_merchant_category_diversity
        features["behavioral_merchant_category_diversity"] = len(self.mcc_set)

        # behavioral_amount_zscore
        if self.amount_count > 1:
            mean = self.amount_sum / self.amount_count
            variance = (self.amount_sum_sq / self.amount_count) - (mean * mean)
            stddev = math.sqrt(max(variance, 0))
            features["behavioral_amount_zscore"] = (
                (current_amount - mean) / stddev if stddev > 0 else 0.0
            )
        else:
            features["behavioral_amount_zscore"] = 0.0

        # behavioral_is_recipient_new
        features["behavioral_is_recipient_new"] = (
            0 if current_recipient in self.known_recipients else 1
        )

        # behavioral_velocity_direction
        features["behavioral_velocity_direction"] = (
            self.inflow_total / self.outflow_total
            if self.outflow_total > 0 else 1.0
        )

        # behavioral_time_between_tx_stddev
        if len(self.inter_tx_times) > 1:
            features["behavioral_time_between_tx_stddev"] = statistics.stdev(self.inter_tx_times)
        else:
            features["behavioral_time_between_tx_stddev"] = 0.0

        # behavioral_country_change_freq
        country_changes = sum(
            1 for i in range(1, len(self.country_history))
            if self.country_history[i][1] != self.country_history[i - 1][1]
        )
        features["behavioral_country_change_freq"] = country_changes / 30.0

        # behavioral_night_tx_ratio
        features["behavioral_night_tx_ratio"] = (
            self.night_tx_count / self.total_tx_count
            if self.total_tx_count > 0 else 0.0
        )

        return features


# ── Stateful Processor ─────────────────────────────────────────

class BehavioralFeatureProcessor(KeyedProcessFunction):
    """Stateful processor for behavioral features."""

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = BehavioralState()

    def process_element(self, value, ctx):
        event = json.loads(value) if isinstance(value, str) else value

        # Extract hour and day from timestamp
        ts_ms = event.get("timestamp_ms", 0)
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        event["hour_of_day"] = dt.hour
        event["day_of_week"] = dt.weekday()

        self.state.update(event)
        features = self.state.compute_features(event)
        features["account_id"] = event.get("account_id", "")
        features["event_time_ms"] = str(ts_ms)

        yield json.dumps(features)


# ── Redis Sink ────────────────────────────────────────────────

class BehavioralRedisSink(MapFunction):
    """Writes behavioral features to Redis with 24h TTL."""

    def __init__(self):
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
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
        account_id = data.get("account_id", "")

        # Store behavioral profile
        profile_key = f"account:{account_id}:profile"
        self.redis_client.setex(profile_key, REDIS_TTL_SECONDS, json.dumps(data))

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
            'properties.group.id' = 'flink-behavioral-features',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # Convert to DataStream for stateful processing
    ds = t_env.toDataStream(t_env.from_path("payment_events"))

    # Key by account and process behavioral features
    keyed = ds.key_by(lambda e: e["account_id"])
    behavioral = keyed.process(BehavioralFeatureProcessor())

    # Write to Redis
    behavioral.map(BehavioralRedisSink())

    env.execute("Behavioral Feature Computation Job")


if __name__ == "__main__":
    main()
