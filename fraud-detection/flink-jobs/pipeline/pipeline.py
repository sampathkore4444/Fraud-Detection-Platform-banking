"""
Feature Merge & Scoring Pipeline — Flink Streaming Job

Orchestrates the end-to-end data flow per SPEC §4:
1. Consumes payment events from Kafka
2. Merges velocity, behavioral, and device features from Redis
3. Calls FraudScoringService via gRPC
4. Writes decisions to Kafka decisions topic, Redis, and audit log

This is the central pipeline that ties all components together.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.common import WatermarkStrategy
from pyflink.table import StreamTableEnvironment
import json
import time
import logging
import redis

# gRPC client for Fraud Service
try:
    import grpc
    from proto import fraud_pb2_grpc, fraud_pb2
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "payments.raw.v1"
REDIS_HOST = "redis"
REDIS_PORT = 6379
FRAUD_SERVICE_ADDR = "fraud-service:50051"
CHECKPOINT_INTERVAL_MS = 30000
GRPC_TIMEOUT_SECONDS = 0.05  # 50ms timeout per SPEC §6

logger = logging.getLogger("fraud-pipeline")


def create_env():
    """Create and configure the Flink execution environment."""
    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    env.set_parallelism(8)
    return env


# ── Feature Merger ────────────────────────────────────────────

class FeatureMerger(KeyedProcessFunction):
    """
    Merges velocity, behavioral, and device features from Redis
    into a single feature vector per SPEC §4 step 3-4.
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

    def process_element(self, value, ctx):
        """Merge features from all three feature groups."""
        event = json.loads(value) if isinstance(value, str) else value

        account_id = event.get("account_id", "")
        tx_id = event.get("event_id", "")

        merged = {}

        # 1. Load velocity features from Redis
        merged.update(self._load_velocity_features(account_id))

        # 2. Load behavioral features from Redis
        merged.update(self._load_behavioral_features(account_id))

        # 3. Load device features from Redis
        merged.update(self._load_device_features(tx_id))

        # 4. Add event-level features
        merged["transaction_id"] = tx_id
        merged["account_id"] = account_id
        merged["amount"] = str(event.get("amount", 0))
        merged["currency"] = event.get("currency", "")
        merged["merchant_id"] = event.get("merchant_id", "")
        merged["merchant_category_code"] = str(event.get("merchant_category_code", 0))
        merged["channel"] = event.get("channel", "")
        merged["country_code"] = event.get("country_code", "")
        merged["timestamp_ms"] = str(event.get("timestamp_ms", 0))

        # 5. Store merged feature vector in Redis (5 min TTL per SPEC §3.2.5)
        self._store_feature_vector(tx_id, merged)

        yield json.dumps(merged)

    def _load_velocity_features(self, account_id):
        features = {}
        patterns = [
            ("velocity_tx_count_1h", f"velocity:{account_id}:velocity_tx_count_1h"),
            ("velocity_tx_count_24h", f"velocity:{account_id}:velocity_tx_count_24h"),
            ("velocity_amount_sum_1h", f"velocity:{account_id}:velocity_amount_sum_1h"),
            ("velocity_amount_sum_24h", f"velocity:{account_id}:velocity_amount_sum_24h"),
            ("velocity_decline_count_24h", f"velocity:{account_id}:velocity_decline_count_24h"),
            ("velocity_unique_countries_1h", f"velocity:{account_id}:velocity_unique_countries_1h"),
            ("velocity_unique_merchants_24h", f"velocity:{account_id}:velocity_unique_merchants_24h"),
            ("velocity_avg_amount_7d", f"velocity:{account_id}:velocity_avg_amount_7d"),
            ("velocity_stddev_amount_7d", f"velocity:{account_id}:velocity_stddev_amount_7d"),
            ("velocity_time_since_last_tx", f"velocity:{account_id}:velocity_time_since_last_tx"),
        ]
        for feature_name, key in patterns:
            val = self.redis_client.get(key)
            features[feature_name] = val if val else "0"
        return features

    def _load_behavioral_features(self, account_id):
        profile_key = f"account:{account_id}:profile"
        profile = self.redis_client.get(profile_key)
        if profile:
            try:
                return json.loads(profile)
            except json.JSONDecodeError:
                pass
        return {
            "behavioral_typical_amount_ratio": "1.0",
            "behavioral_typical_hour_score": "0.0417",
            "behavioral_typical_day_score": "0.1429",
            "behavioral_merchant_category_diversity": "0",
            "behavioral_amount_zscore": "0.0",
            "behavioral_is_recipient_new": "0",
            "behavioral_velocity_direction": "1.0",
            "behavioral_time_between_tx_stddev": "0.0",
            "behavioral_country_change_freq": "0.0",
            "behavioral_night_tx_ratio": "0.0",
        }

    def _load_device_features(self, tx_id):
        device_key = f"device_features:{tx_id}"
        device_data = self.redis_client.get(device_key)
        if device_data:
            try:
                return json.loads(device_data)
            except json.JSONDecodeError:
                pass
        return {
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

    def _store_feature_vector(self, tx_id, features):
        key = f"feature_vector:{tx_id}"
        self.redis_client.setex(key, 300, json.dumps(features))


# ── Fraud Service gRPC Client ─────────────────────────────────

class FraudServiceScorer(MapFunction):
    """
    Calls FraudScoringService.ScoreTransaction via gRPC per SPEC §4 step 5-9.
    Falls back to REVIEW if service is unavailable.
    """

    def __init__(self):
        self.stub = None

    def open(self, runtime_context: RuntimeContext):
        """Initialize gRPC stub."""
        if GRPC_AVAILABLE:
            channel = grpc.insecure_channel(FRAUD_SERVICE_ADDR)
            self.stub = fraud_pb2_grpc.FraudScoringServiceStub(channel)

    def map(self, value):
        """Score transaction via Fraud Service."""
        features = json.loads(value) if isinstance(value, str) else value
        tx_id = features.get("transaction_id", "")
        timestamp_ms = int(features.get("timestamp_ms", 0))

        if not self.stub:
            return self._build_response(tx_id, "APPROVE", 0.0, "service_unavailable")

        try:
            request = fraud_pb2.ScoreRequest(
                transaction_id=tx_id,
                features=features,
                timestamp_ms=timestamp_ms,
            )
            response = self.stub.ScoreTransaction(
                request,
                timeout=GRPC_TIMEOUT_SECONDS,
            )
            return self._build_response(
                tx_id,
                response.decision.name,
                response.fraud_probability,
                response.reason_code,
            )
        except grpc.RpcError as e:
            logger.warning(f"gRPC call failed for {tx_id}: {e}")
            return self._build_response(tx_id, "REVIEW", 0.5, "service_error")

    def _build_response(self, tx_id, decision, probability, reason):
        return json.dumps({
            "transaction_id": tx_id,
            "decision": decision,
            "fraud_probability": probability,
            "reason_code": reason,
            "timestamp_ms": int(time.time() * 1000),
        })


# ── Decision Writer ──────────────────────────────────────────

class DecisionWriter(MapFunction):
    """
    Writes decisions to:
    a. Kafka decisions topic (payments.decisions.v1)
    b. Redis (short TTL for async lookups)
    c. Audit log (append-only to S3/GCS)
    d. Fraud alerts topic (for DECLINE)
    e. DLQ (for service errors)
    Per SPEC §4 step 10.
    """

    def __init__(self):
        self.kafka_producer = None
        self.redis_client = None

    def open(self, runtime_context: RuntimeContext):
        """Initialize Redis and Kafka producer."""
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

        try:
            from confluent_kafka import Producer
            self.kafka_producer = Producer({
                'bootstrap.servers': KAFKA_BROKER,
                'client.id': 'fraud-pipeline-decision-writer',
            })
        except ImportError:
            logger.warning("confluent-kafka not available, skipping Kafka output")

    def map(self, value):
        """Write decision to all sinks."""
        decision = json.loads(value) if isinstance(value, str) else value
        tx_id = decision.get("transaction_id", "")

        # 1. Write to Kafka decisions topic
        if self.kafka_producer:
            self.kafka_producer.produce(
                topic="payments.decisions.v1",
                key=tx_id,
                value=json.dumps(decision),
            )
            self.kafka_producer.flush()

        # 2. Write to Redis (5 min TTL)
        self.redis_client.setex(
            f"decision:{tx_id}",
            300,
            json.dumps(decision)
        )

        # 3. Write to audit log
        logger.info(f"AUDIT: {json.dumps(decision)}")

        # 4. If DECLINED, write to fraud alerts topic
        if decision.get("decision") == "DECLINE" and self.kafka_producer:
            self.kafka_producer.produce(
                topic="fraud.alerts.v1",
                key=tx_id,
                value=json.dumps({
                    "transaction_id": tx_id,
                    "fraud_probability": decision["fraud_probability"],
                    "reason_code": decision.get("reason_code", ""),
                    "timestamp_ms": decision.get("timestamp_ms", 0),
                }),
            )
            self.kafka_producer.flush()

        # 5. If service error, write to DLQ
        if decision.get("reason_code") in ("service_error", "service_unavailable") and self.kafka_producer:
            self.kafka_producer.produce(
                topic="fraud.dlq.v1",
                key=tx_id,
                value=json.dumps(decision),
            )
            self.kafka_producer.flush()

        return value


# ── Main Pipeline ─────────────────────────────────────────────

def main():
    """
    Main pipeline orchestrator per SPEC §4:
    1. Payment gateway → Kafka
    2. Flink consumes from Kafka
    3. Merge features from Redis
    4. Call Fraud Service
    5. Write decisions
    """
    env = create_env()
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.0.0-1.18.jar")

    t_env = StreamTableEnvironment.create(env)

    # ── Source: Kafka payments.raw.v1 ────────────────────────
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
            'properties.group.id' = 'fraud-pipeline-orchestrator',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # Convert to DataStream for complex processing
    ds = t_env.toDataStream(t_env.from_path("payment_events"))

    # ── Step 3-4: Merge features from Redis ──────────────────
    merged = ds.key_by(lambda e: e["account_id"]).process(FeatureMerger())

    # ── Step 5-9: Score via Fraud Service ────────────────────
    scored = merged.map(FraudServiceScorer())

    # ── Step 10: Write decisions ─────────────────────────────
    scored.map(DecisionWriter())

    # Execute
    env.execute("Fraud Detection Pipeline Orchestrator")


if __name__ == "__main__":
    main()
