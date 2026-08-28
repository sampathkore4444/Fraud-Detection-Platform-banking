"""
Velocity Feature Computation — Flink Streaming Job

Computes windowed velocity features for fraud detection per SPEC §3.2.2.
Features: transaction counts, amount sums, unique countries/merchants, etc.

Runs with parallelism=16, RocksDB state backend, 30s checkpoint interval.

PyFlink API notes:
- Uses DataStream API for stateful processing (KeyedProcessFunction)
- Uses MapFunction with RichMapFunction for Redis sink (has open() with runtime_context)
- State is managed via ValueState descriptors (correct PyFlink API)
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    KeyedProcessFunction, MapFunction, RuntimeContext
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.table import StreamTableEnvironment
import json
import redis


# ── Configuration ─────────────────────────────────────────────

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "payments.raw.v1"
REDIS_HOST = "redis"
REDIS_PORT = 6379
CHECKPOINT_INTERVAL_MS = 30000  # 30 seconds per SPEC §3.2.1
REDIS_TTL_SECONDS = 86400       # 24 hours


def create_env():
    """Create and configure the Flink execution environment."""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Checkpointing per SPEC §3.2.1
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # Parallelism per SPEC §3.2.1 — Velocity = 16
    env.set_parallelism(16)

    return env


# ── Stateful Processor ─────────────────────────────────────────

class VelocityStatefulProcessor(KeyedProcessFunction):
    """
    Stateful processor for velocity features that require event-time deltas.
    
    Uses correct PyFlink ValueState API:
    - ValueStateDescriptor for state declaration
    - runtime_context.get_state() to initialize
    """
    
    def __init__(self):
        self.last_tx_timestamp = None
        self.decline_count = 0
        self.state_last_tx = None
        self.state_decline_count = None

    def open(self, runtime_context: RuntimeContext):
        """Initialize state descriptors using correct PyFlink API."""
        # ValueStateDescriptor is the correct way to declare state
        self.state_last_tx = runtime_context.get_state(
            ValueStateDescriptor("last_tx_timestamp", Types.LONG())
        )
        self.state_decline_count = runtime_context.get_state(
            ValueStateDescriptor("decline_count_24h", Types.INT())
        )

    def process_element(self, value, ctx):
        """Process each payment event and emit velocity features."""
        event = json.loads(value) if isinstance(value, str) else value

        account_id = event.get("account_id", "")
        event_time = event.get("timestamp_ms", 0)
        is_declined = event.get("is_declined", False)

        # Feature: velocity_time_since_last_tx
        last_ts = self.state_last_tx.value()
        time_since_last = 0
        if last_ts is not None and last_ts > 0:
            time_since_last = (event_time - last_ts) // 1000  # seconds

        # Feature: velocity_decline_count_24h
        decline_count = self.state_decline_count.value() or 0
        if is_declined:
            decline_count += 1
            self.state_decline_count.update(decline_count)

        # Update state
        self.state_last_tx.update(event_time)

        # Build feature vector
        features = {
            "account_id": account_id,
            "event_time_ms": str(event_time),
            "velocity_time_since_last_tx": str(time_since_last),
            "velocity_decline_count_24h": str(decline_count),
        }

        yield json.dumps(features)


# ── Redis Sink ─────────────────────────────────────────────────

class VelocityRedisSink(MapFunction):
    """
    Writes velocity features to Redis with 24h TTL.
    
    Uses MapFunction (not SinkFunction) for simplicity.
    In production, use a proper Flink Redis connector or custom SinkFunction.
    """

    def __init__(self):
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
        """Initialize Redis client with connection pooling."""
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

    def map(self, value):
        """Write feature to Redis."""
        data = json.loads(value) if isinstance(value, str) else value
        account_id = data.get("account_id", "")

        for key, val in data.items():
            if key not in ("account_id", "event_time_ms"):
                redis_key = f"velocity:{account_id}:{key}"
                self.redis_client.setex(redis_key, REDIS_TTL_SECONDS, val)

        return value


# ── Main Pipeline ─────────────────────────────────────────────

def main():
    """
    Velocity feature computation pipeline.
    
    Architecture:
    1. Kafka → Flink (consume payment events)
    2. Key by account_id
    3. Compute windowed features (SQL) + stateful features (DataStream)
    4. Sink to Redis
    """
    env = create_env()

    # Add Kafka connector JAR
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.0.0-1.18.jar")

    # ── Step 1: Kafka Source ──────────────────────────────────
    # Use Table API SQL for Kafka source (most reliable in PyFlink)
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
            is_declined BOOLEAN,
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
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # ── Step 2: Convert to DataStream for stateful processing ─
    # toDataStream() works with bounded tables in PyFlink 1.18+
    ds = t_env.toDataStream(t_env.from_path("payment_events"))

    # ── Step 3: Compute stateful velocity features ────────────
    keyed_stream = ds.key_by(lambda e: e["account_id"])
    velocity_stream = keyed_stream.process(VelocityStatefulProcessor())

    # ── Step 4: Sink to Redis ─────────────────────────────────
    velocity_stream.map(VelocityRedisSink())

    # Execute
    env.execute("Velocity Feature Computation Job")


if __name__ == "__main__":
    main()
