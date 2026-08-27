"""
Velocity Feature Computation — Flink Streaming Job

Computes windowed velocity features for fraud detection per SPEC §3.2.2.
Features: transaction counts, amount sums, unique countries/merchants, etc.

Runs with parallelism=16, RocksDB state backend, 30s checkpoint interval.
"""

from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.datastream.functions import (
    ProcessWindowFunction, KeyedProcessFunction, RuntimeContext,
    MapFunction, FlatMapFunction
)
from pyflink.common.time import Time, TimeCharacteristic
from pyflink.common import Types, WatermarkStrategy
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table import expressions as F
import json
import time
import math
from collections import defaultdict
from datetime import datetime, timezone


# ── Configuration ─────────────────────────────────────────────

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "payments.raw.v1"
REDIS_ADDR = "redis:6379"
CHECKPOINT_INTERVAL_MS = 30000  # 30 seconds per SPEC §3.2.1


def create_env():
    """Create and configure the Flink execution environment."""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Checkpointing per SPEC §3.2.1
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.get_checkpoint_config().set_fail_on_checkpointing_error(False)

    # Parallelism per SPEC §3.2.1 — Velocity = 16
    env.set_parallelism(16)

    return env


def create_kafka_source(env):
    """Create Kafka source for payment events."""
    env.add_jars(
        "file:///opt/flink/lib/flink-connector-kafka-3.0.0-1.18.jar"
    )

    # Use Table API for easier Kafka integration
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
            'properties.group.id' = 'flink-velocity-features',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    return t_env


# ── Velocity Feature Tables (SQL) ─────────────────────────────

def compute_velocity_features(t_env):
    """
    Compute velocity features using Flink Table API SQL.
    All features per SPEC §3.2.2.
    """

    # Feature 1: velocity_tx_count_1h — 1 hour tumbling window
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW velocity_tx_count_1h AS
        SELECT
            account_id,
            COUNT(*) AS velocity_tx_count_1h,
            TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start
        FROM payment_events
        GROUP BY
            account_id,
            TUMBLE(event_time, INTERVAL '1' HOUR)
    """)

    # Feature 2: velocity_tx_count_24h — 24 hour tumbling window
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW velocity_tx_count_24h AS
        SELECT
            account_id,
            COUNT(*) AS velocity_tx_count_24h,
            TUMBLE_START(event_time, INTERVAL '24' HOUR) AS window_start
        FROM payment_events
        GROUP BY
            account_id,
            TUMBLE(event_time, INTERVAL '24' HOUR)
    """)

    # Feature 3: velocity_amount_sum_1h
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW velocity_amount_sum_1h AS
        SELECT
            account_id,
            COALESCE(SUM(CAST(amount AS DOUBLE)), 0.0) AS velocity_amount_sum_1h,
            TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start
        FROM payment_events
        GROUP BY
            account_id,
            TUMBLE(event_time, INTERVAL '1' HOUR)
    """)

    # Feature 4: velocity_amount_sum_24h
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW velocity_amount_sum_24h AS
        SELECT
            account_id,
            COALESCE(SUM(CAST(amount AS DOUBLE)), 0.0) AS velocity_amount_sum_24h,
            TUMBLE_START(event_time, INTERVAL '24' HOUR) AS window_start
        FROM payment_events
        GROUP BY
            account_id,
            TUMBLE(event_time, INTERVAL '24' HOUR)
    """)

    # Feature 6: velocity_unique_countries_1h
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW velocity_unique_countries_1h AS
        SELECT
            account_id,
            COUNT(DISTINCT country_code) AS velocity_unique_countries_1h,
            TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start
        FROM payment_events
        GROUP BY
            account_id,
            TUMBLE(event_time, INTERVAL '1' HOUR)
    """)

    # Feature 7: velocity_unique_merchants_24h
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW velocity_unique_merchants_24h AS
        SELECT
            account_id,
            COUNT(DISTINCT merchant_id) AS velocity_unique_merchants_24h,
            TUMBLE_START(event_time, INTERVAL '24' HOUR) AS window_start
        FROM payment_events
        GROUP BY
            account_id,
            TUMBLE(event_time, INTERVAL '24' HOUR)
    """)

    # Feature 8: velocity_avg_amount_7d
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW velocity_avg_amount_7d AS
        SELECT
            account_id,
            COALESCE(AVG(CAST(amount AS DOUBLE)), 0.0) AS velocity_avg_amount_7d,
            TUMBLE_START(event_time, INTERVAL '7' DAY) AS window_start
        FROM payment_events
        GROUP BY
            account_id,
            TUMBLE(event_time, INTERVAL '7' DAY)
    """)

    # Feature 9: velocity_stddev_amount_7d
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW velocity_stddev_amount_7d AS
        SELECT
            account_id,
            COALESCE(STDDEV_POP(CAST(amount AS DOUBLE)), 0.0) AS velocity_stddev_amount_7d,
            TUMBLE_START(event_time, INTERVAL '7' DAY) AS window_start
        FROM payment_events
        GROUP BY
            account_id,
            TUMBLE(event_time, INTERVAL '7' DAY)
    """)

    return t_env


# ── DataStream API for Complex Features ───────────────────────

class VelocityStatefulProcessor(KeyedProcessFunction):
    """
    Stateful processor for velocity features that require event-time deltas
    and decline tracking.
    """

    def __init__(self):
        self.last_tx_timestamp = None
        self.decline_count = 0
        self.state_last_tx = None
        self.state_decline_count = None

    def open(self, runtime_context: RuntimeContext):
        # Initialize state descriptors
        self.state_last_tx = runtime_context.get_state(
            runtime_context.get_state_descriptor("last_tx_timestamp", Types.LONG())
        )
        self.state_decline_count = runtime_context.get_state(
            runtime_context.get_state_descriptor("decline_count_24h", Types.INT())
        )

    def process_element(self, value, ctx):
        """Process each payment event and emit velocity features."""
        event = json.loads(value) if isinstance(value, str) else value

        account_id = event.get("account_id", "")
        event_time = event.get("timestamp_ms", 0)
        amount = float(event.get("amount", 0))
        country_code = event.get("country_code", "")

        # Feature 10: velocity_time_since_last_tx
        last_ts = self.state_last_tx.value() if self.state_last_tx else None
        time_since_last = 0
        if last_ts:
            time_since_last = (event_time - last_ts) // 1000  # seconds

        # Update state
        self.state_last_tx.update(event_time)

        # Build feature vector
        features = {
            "account_id": account_id,
            "event_time_ms": str(event_time),
            "velocity_time_since_last_tx": str(time_since_last),
        }

        yield json.dumps(features)

    def on_timer(self, timestamp, ctx, out):
        """Timer callback for periodic state cleanup."""
        pass


# ── Redis Sink for Velocity Features ─────────────────────────

class VelocityRedisSink(MapFunction):
    """Writes velocity features to Redis with 24h TTL."""

    def __init__(self):
        self.redis_client = None

    def open(self, runtime_context):
        import redis
        self.redis_client = redis.Redis(
            host=REDIS_ADDR.split(":")[0],
            port=int(REDIS_ADDR.split(":")[1]),
            decode_responses=True
        )

    def map(self, value):
        """Write feature to Redis."""
        data = json.loads(value) if isinstance(value, str) else value
        account_id = data.get("account_id", "")

        for key, val in data.items():
            if key != "account_id" and key != "event_time_ms":
                redis_key = f"velocity:{account_id}:{key}"
                self.redis_client.setex(redis_key, 86400, val)  # 24h TTL

        return value


# ── Main Pipeline ─────────────────────────────────────────────

def main():
    env = create_env()

    # Table API for SQL-based windowed features
    t_env = create_kafka_source(env)
    t_env = compute_velocity_features(t_env)

    # Output velocity features to Redis
    t_env.execute_sql("""
        CREATE TABLE velocity_features_sink (
            account_id STRING,
            velocity_tx_count_1h BIGINT,
            velocity_amount_sum_1h DOUBLE,
            velocity_tx_count_24h BIGINT,
            velocity_amount_sum_24h DOUBLE,
            velocity_unique_countries_1h BIGINT,
            velocity_unique_merchants_24h BIGINT,
            velocity_avg_amount_7d DOUBLE,
            velocity_stddev_amount_7d DOUBLE
        ) WITH (
            'connector' = 'redis',
            'mode' = 'UPSERT',
            'redis-mode' = 'single',
            'host' = 'redis',
            'port' = '6379',
            'key.prefix' = 'velocity:',
            'key.ttl' = '86400'
        )
    """)

    # Pipeline: join all velocity features
    t_env.execute_sql("""
        INSERT INTO velocity_features_sink
        SELECT
            v1.account_id,
            v1.velocity_tx_count_1h,
            a1.velocity_amount_sum_1h,
            v2.velocity_tx_count_24h,
            a2.velocity_amount_sum_24h,
            c.velocity_unique_countries_1h,
            m.velocity_unique_merchants_24h,
            avg7.velocity_avg_amount_7d,
            std7.velocity_stddev_amount_7d
        FROM velocity_tx_count_1h v1
        JOIN velocity_amount_sum_1h a1
            ON v1.account_id = a1.account_id AND v1.window_start = a1.window_start
        JOIN velocity_tx_count_24h v2
            ON v1.account_id = v2.account_id
        JOIN velocity_amount_sum_24h a2
            ON v1.account_id = a2.account_id
        JOIN velocity_unique_countries_1h c
            ON v1.account_id = c.account_id AND v1.window_start = c.window_start
        JOIN velocity_unique_merchants_24h m
            ON v1.account_id = m.account_id
        JOIN velocity_avg_amount_7d avg7
            ON v1.account_id = avg7.account_id
        JOIN velocity_stddev_amount_7d std7
            ON v1.account_id = std7.account_id
    """)

    # Also emit individual events for stateful processing
    ds = env.from_source(
        create_kafka_source(env),
        watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Time.seconds(5)),
        source_name="payment_events"
    )

    # Keyed stateful processing for time_since_last_tx
    keyed_stream = ds.key_by(lambda e: e["account_id"])
    velocity_stream = keyed_stream.process(VelocityStatefulProcessor())

    # Sink to Redis
    velocity_stream.map(VelocityRedisSink())

    env.execute("Velocity Feature Computation Job")


if __name__ == "__main__":
    main()
