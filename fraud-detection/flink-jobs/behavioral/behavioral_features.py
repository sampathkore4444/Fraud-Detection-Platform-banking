"""
Behavioral Feature Computation — Flink Streaming Job

Computes pattern-based behavioral features for fraud detection per SPEC §3.2.3.
Features: typical amounts, time patterns, merchant diversity, etc.

Runs with parallelism=16, RocksDB state backend, 30s checkpoint interval.
"""

from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.time import Time
from pyflink.common import Types
from pyflink.table import StreamTableEnvironment
import json
import math
from datetime import datetime, timezone
from collections import defaultdict
import statistics


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

    # Parallelism per SPEC §3.2.1 — Behavioral = 16
    env.set_parallelism(16)

    return env


class BehavioralState:
    """Maintains long-running behavioral state for an account."""

    def __init__(self):
        # Amount history for z-score computation
        self.amount_history = []          # last 30 days
        self.amount_sum = 0.0
        self.amount_sum_sq = 0.0
        self.amount_count = 0

        # Hour distribution (0-23)
        self.hour_counts = [0] * 24
        self.hour_total = 0

        # Day distribution (0-6)
        self.day_counts = [0] * 7
        self.day_total = 0

        # Merchant category diversity
        self.mcc_set = set()

        # Recipient tracking
        self.known_recipients = set()

        # Inflow/outflow tracking
        self.inflow_total = 0.0
        self.outflow_total = 0.0

        # Inter-transaction time
        self.last_tx_time = None
        self.inter_tx_times = []

        # Country tracking
        self.country_history = []  # (timestamp, country)

        # Night transaction tracking
        self.night_tx_count = 0
        self.total_tx_count = 0

        # Window cutoffs (30 days in ms)
        self.WINDOW_30D_MS = 30 * 24 * 3600 * 1000
        self.WINDOW_7D_MS = 7 * 24 * 3600 * 1000

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

        # Update amount stats
        self.amount_history.append((event_time, amount))
        self.amount_sum += amount
        self.amount_sum_sq += amount * amount
        self.amount_count += 1

        # Prune old amounts (30 day window)
        cutoff = event_time - self.WINDOW_30D_MS
        while self.amount_history and self.amount_history[0][0] < cutoff:
            old_time, old_amount = self.amount_history.pop(0)
            self.amount_sum -= old_amount
            self.amount_sum_sq -= old_amount * old_amount
            self.amount_count -= 1

        # Hour distribution
        self.hour_counts[hour] += 1
        self.hour_total += 1

        # Day distribution
        self.day_counts[day] += 1
        self.day_total += 1

        # Merchant category diversity
        self.mcc_set.add(mcc)

        # Recipient tracking
        self.known_recipients.add(recipient)

        # Inflow/outflow (simplified — in production, track by channel)
        if channel in ("POS", "ATM", "CNP", "WEB"):
            self.outflow_total += amount
        else:
            self.inflow_total += amount

        # Inter-transaction time
        if self.last_tx_time is not None:
            delta = (event_time - self.last_tx_time) // 1000  # seconds
            self.inter_tx_times.append(delta)
        self.last_tx_time = event_time

        # Country tracking
        self.country_history.append((event_time, country))
        country_cutoff = event_time - self.WINDOW_30D_MS
        self.country_history = [(t, c) for t, c in self.country_history if t >= country_cutoff]

        # Night transaction tracking (23:00-05:00)
        if hour >= 23 or hour < 5:
            self.night_tx_count += 1
        self.total_tx_count += 1

    def compute_features(self, current_event):
        """Compute all behavioral features from current state."""
        current_amount = float(current_event.get("amount", 0))
        current_hour = current_event.get("hour_of_day", 0)
        current_day = current_event.get("day_of_week", 0)
        current_mcc = current_event.get("merchant_category_code", 0)
        current_recipient = current_event.get("merchant_id", "")
        current_country = current_event.get("country_code", "")

        features = {}

        # Feature 1: behavioral_typical_amount_ratio
        avg_amount = self.amount_sum / max(self.amount_count, 1)
        if avg_amount > 0:
            features["behavioral_typical_amount_ratio"] = current_amount / avg_amount
        else:
            features["behavioral_typical_amount_ratio"] = 1.0

        # Feature 2: behavioral_typical_hour_score
        if self.hour_total > 0:
            features["behavioral_typical_hour_score"] = (
                self.hour_counts[current_hour] / self.hour_total
            )
        else:
            features["behavioral_typical_hour_score"] = 1.0 / 24.0

        # Feature 3: behavioral_typical_day_score
        if self.day_total > 0:
            features["behavioral_typical_day_score"] = (
                self.day_counts[current_day] / self.day_total
            )
        else:
            features["behavioral_typical_day_score"] = 1.0 / 7.0

        # Feature 4: behavioral_merchant_category_diversity
        # Count distinct MCCs in last 30d
        features["behavioral_merchant_category_diversity"] = len(self.mcc_set)

        # Feature 5: behavioral_amount_zscore
        if self.amount_count > 1:
            mean = self.amount_sum / self.amount_count
            variance = (self.amount_sum_sq / self.amount_count) - (mean * mean)
            stddev = math.sqrt(max(variance, 0))
            if stddev > 0:
                features["behavioral_amount_zscore"] = (current_amount - mean) / stddev
            else:
                features["behavioral_amount_zscore"] = 0.0
        else:
            features["behavioral_amount_zscore"] = 0.0

        # Feature 6: behavioral_is_recipient_new
        features["behavioral_is_recipient_new"] = (
            0 if current_recipient in self.known_recipients else 1
        )

        # Feature 7: behavioral_velocity_direction
        if self.outflow_total > 0:
            features["behavioral_velocity_direction"] = (
                self.inflow_total / self.outflow_total
            )
        else:
            features["behavioral_velocity_direction"] = 1.0

        # Feature 8: behavioral_time_between_tx_stddev
        if len(self.inter_tx_times) > 1:
            features["behavioral_time_between_tx_stddev"] = statistics.stdev(
                self.inter_tx_times
            )
        else:
            features["behavioral_time_between_tx_stddev"] = 0.0

        # Feature 9: behavioral_country_change_freq
        country_changes = 0
        for i in range(1, len(self.country_history)):
            if self.country_history[i][1] != self.country_history[i - 1][1]:
                country_changes += 1
        days = 30.0
        features["behavioral_country_change_freq"] = country_changes / days

        # Feature 10: behavioral_night_tx_ratio
        if self.total_tx_count > 0:
            features["behavioral_night_tx_ratio"] = (
                self.night_tx_count / self.total_tx_count
            )
        else:
            features["behavioral_night_tx_ratio"] = 0.0

        return features


class BehavioralFeatureProcessor(KeyedProcessFunction):
    """Stateful processor for behavioral features."""

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = BehavioralState()

    def process_element(self, value, ctx):
        """Process event and emit behavioral features."""
        event = json.loads(value) if isinstance(value, str) else value

        # Extract hour and day from timestamp
        ts_ms = event.get("timestamp_ms", 0)
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        event["hour_of_day"] = dt.hour
        event["day_of_week"] = dt.weekday()

        # Update state
        self.state.update(event)

        # Compute features
        features = self.state.compute_features(event)

        # Enrich with event metadata
        features["account_id"] = event.get("account_id", "")
        features["event_time_ms"] = str(ts_ms)

        yield json.dumps(features)


# ── Redis Sink ────────────────────────────────────────────────

class BehavioralRedisSink:
    """Writes behavioral features to Redis with 24h TTL."""

    def __init__(self):
        self.redis_client = None

    def open(self, runtime_context):
        import redis
        self.redis_client = redis.Redis(
            host=REDIS_ADDR.split(":")[0],
            port=int(REDIS_ADDR.split(":")[1]),
            decode_responses=True
        )

    def process(self, value):
        data = json.loads(value) if isinstance(value, str) else value
        account_id = data.get("account_id", "")

        # Store behavioral profile
        profile_key = f"account:{account_id}:profile"
        self.redis_client.setex(profile_key, 86400, json.dumps(data))

        return value


# ── Main Pipeline ─────────────────────────────────────────────

def main():
    env = create_env()

    # Create Kafka source
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

    # Convert to DataStream for complex stateful processing
    ds = t_env.toDataStream(t_env.from("payment_events"))

    # Key by account and process behavioral features
    keyed = ds.key_by(lambda e: e["account_id"])
    behavioral = keyed.process(BehavioralFeatureProcessor())

    # Write to Redis
    # In production, use a custom Redis Sink
    behavioral.add_sink(BehavioralRedisSink())

    env.execute("Behavioral Feature Computation Job")


if __name__ == "__main__":
    main()
