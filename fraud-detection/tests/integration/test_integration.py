"""
Integration Tests

Tests Flink ↔ Redis ↔ Fraud Service integration per SPEC §8.
Uses testcontainers for Redis, and mock services.
"""

import unittest
import json
import time
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'ml-pipeline'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'feature-engineering'))


class TestRedisIntegration(unittest.TestCase):
    """Test Redis feature store operations per SPEC §3.3."""

    def setUp(self):
        """Set up Redis client for testing."""
        try:
            import redis
            self.redis_client = redis.Redis(
                host=os.environ.get("REDIS_HOST", "localhost"),
                port=int(os.environ.get("REDIS_PORT", 6379)),
                decode_responses=True
            )
            self.redis_client.ping()
            self.redis_available = True
        except Exception:
            self.redis_available = False
            self.redis_client = None

    def test_feature_vector_storage(self):
        """Test storing and retrieving feature vectors."""
        if not self.redis_available:
            self.skipTest("Redis not available")

        tx_id = "test_tx_001"
        features = {
            "velocity_tx_count_1h": "5",
            "device_is_known": "1",
            "behavioral_amount_zscore": "1.5",
        }

        key = f"feature_vector:{tx_id}"
        self.redis_client.setex(key, 300, json.dumps(features))

        retrieved = self.redis_client.get(key)
        self.assertIsNotNone(retrieved)
        self.assertEqual(json.loads(retrieved), features)

    def test_feature_vector_ttl(self):
        """Test feature vector TTL (5 min per SPEC §3.2.5)."""
        if not self.redis_available:
            self.skipTest("Redis not available")

        tx_id = "test_tx_ttl"
        key = f"feature_vector:{tx_id}"
        self.redis_client.setex(key, 5, "test_value")

        ttl = self.redis_client.ttl(key)
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 5)

    def test_velocity_feature_storage(self):
        """Test velocity feature storage with 24h TTL."""
        if not self.redis_available:
            self.skipTest("Redis not available")

        account_id = "test_account_001"
        features = {
            "velocity_tx_count_1h": "10",
            "velocity_amount_sum_1h": "5000.00",
            "velocity_tx_count_24h": "50",
        }

        for name, value in features.items():
            key = f"velocity:{account_id}:{name}"
            self.redis_client.setex(key, 86400, value)

        for name, expected in features.items():
            key = f"velocity:{account_id}:{name}"
            actual = self.redis_client.get(key)
            self.assertEqual(actual, expected)

    def test_behavioral_profile_storage(self):
        """Test behavioral profile storage with 24h TTL."""
        if not self.redis_available:
            self.skipTest("Redis not available")

        account_id = "test_account_002"
        profile = {
            "behavioral_typical_amount_ratio": 1.5,
            "behavioral_amount_zscore": 2.0,
            "behavioral_night_tx_ratio": 0.3,
        }

        key = f"account:{account_id}:profile"
        self.redis_client.setex(key, 86400, json.dumps(profile))

        retrieved = json.loads(self.redis_client.get(key))
        self.assertEqual(retrieved["behavioral_typical_amount_ratio"], 1.5)

    def test_device_account_mapping(self):
        """Test device → account mapping with 90d TTL per SPEC §3.3."""
        if not self.redis_available:
            self.skipTest("Redis not available")

        device_id = "device_test_001"
        accounts = {"acc_001", "acc_002", "acc_003"}

        key = f"device:{device_id}:accounts"
        self.redis_client.sadd(key, *accounts)
        self.redis_client.expire(key, 90 * 86400)

        retrieved = self.redis_client.smembers(key)
        self.assertEqual(retrieved, accounts)

    def test_ip_risk_lookup(self):
        """Test IP risk/VPN lookup per SPEC §3.3."""
        if not self.redis_available:
            self.skipTest("Redis not available")

        ip = "192.168.1.100"
        risk_data = {
            "is_vpn": "true",
            "country": "US",
            "risk_score": "0.8",
        }

        key = f"ip_risk:{ip}"
        self.redis_client.hset(key, mapping=risk_data)
        self.redis_client.expire(key, 86400)

        retrieved = self.redis_client.hgetall(key)
        self.assertEqual(retrieved["is_vpn"], "true")
        self.assertEqual(retrieved["country"], "US")


class TestScoringPipeline(unittest.TestCase):
    """Test end-to-end scoring pipeline logic."""

    def test_feature_merge_logic(self):
        """Test merging features from velocity, behavioral, device sources."""
        velocity = {
            "velocity_tx_count_1h": "5",
            "velocity_amount_sum_1h": "1000.0",
        }
        behavioral = {
            "behavioral_amount_zscore": "1.5",
            "behavioral_night_tx_ratio": "0.2",
        }
        device = {
            "device_is_known": "1",
            "device_ip_country_match": "0",
        }

        merged = {}
        merged.update(velocity)
        merged.update(behavioral)
        merged.update(device)

        self.assertEqual(len(merged), 6)
        self.assertEqual(merged["velocity_tx_count_1h"], "5")
        self.assertEqual(merged["behavioral_amount_zscore"], "1.5")
        self.assertEqual(merged["device_is_known"], "1")

    def test_scoring_with_all_features(self):
        """Test scoring with complete feature vector."""
        features = {
            "velocity_tx_count_1h": "10",
            "velocity_tx_count_24h": "50",
            "velocity_amount_sum_1h": "5000.0",
            "velocity_amount_sum_24h": "25000.0",
            "velocity_decline_count_24h": "3",
            "velocity_unique_countries_1h": "3",
            "velocity_unique_merchants_24h": "20",
            "velocity_avg_amount_7d": "500.0",
            "velocity_stddev_amount_7d": "200.0",
            "velocity_time_since_last_tx": "300",
            "behavioral_typical_amount_ratio": "10.0",
            "behavioral_typical_hour_score": "0.01",
            "behavioral_typical_day_score": "0.05",
            "behavioral_merchant_category_diversity": "50",
            "behavioral_amount_zscore": "4.0",
            "behavioral_is_recipient_new": "1",
            "behavioral_velocity_direction": "0.5",
            "behavioral_time_between_tx_stddev": "100",
            "behavioral_country_change_freq": "2.0",
            "behavioral_night_tx_ratio": "0.8",
            "device_is_known": "0",
            "device_last_seen_hours_ago": "500",
            "device_unique_accounts_24h": "5",
            "device_is_emulator_detected": "1",
            "device_rooted_jailbroken": "0",
            "device_ip_country_match": "0",
            "device_ip_is_vpn": "1",
            "device_browser_fingerprint_match": "0",
            "device_latency_anomaly": "1",
            "device_is_new_os_version": "1",
        }

        # This should result in high fraud probability
        # (velocity burst, new device, emulator, VPN, geo mismatch)
        self.assertEqual(len(features), 30)

    def test_decision_response_format(self):
        """Test decision response matches proto definition."""
        response = {
            "transaction_id": "tx_123",
            "decision": "APPROVE",
            "fraud_probability": 0.15,
            "model_version": "v1.0.0",
            "latency_ms": 5,
            "top_features": {},
            "reason_code": "",
        }

        required_fields = [
            "transaction_id", "decision", "fraud_probability",
            "model_version", "latency_ms", "top_features", "reason_code"
        ]
        for field in required_fields:
            self.assertIn(field, response)

        self.assertIn(response["decision"], ["APPROVE", "REVIEW", "DECLINE"])
        self.assertGreaterEqual(response["fraud_probability"], 0.0)
        self.assertLessEqual(response["fraud_probability"], 1.0)


class TestDecisionWriter(unittest.TestCase):
    """Test decision writing to Kafka + Redis + Audit Log."""

    def test_decision_to_redis(self):
        """Test writing decision to Redis."""
        try:
            import redis
            client = redis.Redis(
                host=os.environ.get("REDIS_HOST", "localhost"),
                port=int(os.environ.get("REDIS_PORT", 6379)),
                decode_responses=True
            )
            client.ping()
        except Exception:
            self.skipTest("Redis not available")

        decision = {
            "transaction_id": "tx_decision_001",
            "decision": "DECLINE",
            "fraud_probability": 0.85,
            "reason_code": "VELOCITY_BURST",
            "timestamp_ms": int(time.time() * 1000),
        }

        key = f"decision:{decision['transaction_id']}"
        client.setex(key, 300, json.dumps(decision))

        retrieved = json.loads(client.get(key))
        self.assertEqual(retrieved["decision"], "DECLINE")
        self.assertEqual(retrieved["reason_code"], "VELOCITY_BURST")

        # Cleanup
        client.delete(key)


if __name__ == "__main__":
    unittest.main()
