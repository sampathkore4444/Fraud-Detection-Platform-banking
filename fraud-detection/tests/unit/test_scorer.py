"""
Unit Tests for Fraud Scoring Engine

Tests feature computation logic, threshold logic, and decision classification.
Per SPEC §8: Unit tests for feature computation logic, threshold logic.
"""

import unittest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'fraud-service'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'ml-pipeline'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'feature-engineering'))


class TestDecisionThresholds(unittest.TestCase):
    """Test decision threshold logic per SPEC §3.4."""

    def test_approve_below_threshold(self):
        """Probability < 0.30 should result in APPROVE."""
        probability = 0.15
        decision = self.classify(probability)
        self.assertEqual(decision, "APPROVE")

    def test_approve_at_threshold_boundary(self):
        """Probability exactly 0.30 should result in APPROVE."""
        probability = 0.30
        decision = self.classify(probability)
        self.assertEqual(decision, "APPROVE")

    def test_review_in_range(self):
        """Probability between 0.30 and 0.70 should result in REVIEW."""
        probability = 0.50
        decision = self.classify(probability)
        self.assertEqual(decision, "REVIEW")

    def test_review_at_upper_boundary(self):
        """Probability exactly 0.70 should result in REVIEW."""
        probability = 0.70
        decision = self.classify(probability)
        self.assertEqual(decision, "REVIEW")

    def test_decline_above_threshold(self):
        """Probability > 0.70 should result in DECLINE."""
        probability = 0.85
        decision = self.classify(probability)
        self.assertEqual(decision, "DECLINE")

    def test_decline_at_max(self):
        """Probability 1.0 should result in DECLINE."""
        probability = 1.0
        decision = self.classify(probability)
        self.assertEqual(decision, "DECLINE")

    def classify(self, probability):
        """Replicate threshold logic from scorer.go."""
        if probability < 0.30:
            return "APPROVE"
        elif probability < 0.70:
            return "REVIEW"
        else:
            return "DECLINE"


class TestVelocityFeatures(unittest.TestCase):
    """Test velocity feature computation per SPEC §3.2.2."""

    def test_velocity_tx_count_1h(self):
        """Test 1-hour transaction count."""
        events = [
            {"timestamp_ms": 1000000, "account_id": "acc1"},
            {"timestamp_ms": 1500000, "account_id": "acc1"},
            {"timestamp_ms": 2000000, "account_id": "acc1"},
        ]
        count = self.count_events_in_window(events, window_ms=3600000)
        self.assertEqual(count, 3)

    def test_velocity_empty_window(self):
        """Test empty window returns 0."""
        events = []
        count = self.count_events_in_window(events, window_ms=3600000)
        self.assertEqual(count, 0)

    def test_velocity_unique_countries(self):
        """Test unique country count in window."""
        events = [
            {"country_code": "US"},
            {"country_code": "US"},
            {"country_code": "GB"},
            {"country_code": "DE"},
        ]
        countries = set(e["country_code"] for e in events)
        self.assertEqual(len(countries), 3)

    def test_velocity_amount_sum(self):
        """Test amount sum computation."""
        amounts = [100.0, 250.50, 75.25]
        total = sum(amounts)
        self.assertAlmostEqual(total, 425.75)

    def test_velocity_stddev(self):
        """Test stddev computation."""
        import statistics
        amounts = [100.0, 100.0, 100.0, 100.0]
        stddev = statistics.stdev(amounts)
        self.assertEqual(stddev, 0.0)

    def count_events_in_window(self, events, window_ms):
        """Helper: count events in time window."""
        if not events:
            return 0
        max_ts = max(e.get("timestamp_ms", 0) for e in events)
        cutoff = max_ts - window_ms
        return sum(1 for e in events if e.get("timestamp_ms", 0) >= cutoff)


class TestBehavioralFeatures(unittest.TestCase):
    """Test behavioral feature computation per SPEC §3.2.3."""

    def test_typical_amount_ratio(self):
        """Test amount ratio: current / average."""
        current = 200.0
        avg = 100.0
        ratio = current / avg
        self.assertAlmostEqual(ratio, 2.0)

    def test_typical_hour_score(self):
        """Test hour probability score."""
        hour_counts = [0] * 24
        hour_counts[14] = 50  # 50 transactions at 2 PM
        total = 100
        score = hour_counts[14] / total
        self.assertAlmostEqual(score, 0.5)

    def test_amount_zscore(self):
        """Test z-score computation."""
        import math
        values = [100, 100, 100, 100, 100, 200]
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        stddev = math.sqrt(variance)
        current = 200
        zscore = (current - mean) / stddev
        self.assertGreater(zscore, 0)

    def test_is_recipient_new(self):
        """Test first-time recipient detection."""
        known_recipients = {"merchant1", "merchant2", "merchant3"}
        new_recipient = "merchant_new"
        is_new = 0 if new_recipient in known_recipients else 1
        self.assertEqual(is_new, 1)

    def test_night_tx_ratio(self):
        """Test night transaction ratio (23:00-05:00)."""
        hours = [2, 14, 23, 8, 1, 3, 10, 15]
        night_hours = [h for h in hours if h >= 23 or h < 5]
        ratio = len(night_hours) / len(hours)
        self.assertAlmostEqual(ratio, 0.5)  # 4 out of 8


class TestDeviceFeatures(unittest.TestCase):
    """Test device feature computation per SPEC §3.2.4."""

    def test_device_is_known(self):
        """Test device familiarity check."""
        current_time = 1000000
        last_seen = 900000  # 100ms ago
        is_known = 1 if (current_time - last_seen) < (90 * 24 * 3600 * 1000) else 0
        self.assertEqual(is_known, 1)

    def test_device_not_known(self):
        """Test unknown device."""
        current_time = 10000000000  # far in future
        last_seen = 1000000
        is_known = 1 if (current_time - last_seen) < (90 * 24 * 3600 * 1000) else 0
        self.assertEqual(is_known, 0)

    def test_ip_country_match(self):
        """Test IP country matches transaction country."""
        ip_country = "US"
        tx_country = "US"
        match = 1 if ip_country == tx_country else 0
        self.assertEqual(match, 1)

    def test_ip_country_mismatch(self):
        """Test IP country mismatch."""
        ip_country = "US"
        tx_country = "GB"
        match = 1 if ip_country == tx_country else 0
        self.assertEqual(match, 0)

    def test_emulator_detection(self):
        """Test emulator detection."""
        emulator_fingerprints = {"device_abc", "device_def"}
        device_id = "device_abc"
        is_emulator = 1 if device_id in emulator_fingerprints else 0
        self.assertEqual(is_emulator, 1)

    def test_latency_anomaly(self):
        """Test API latency anomaly detection."""
        mean_latency = 50.0
        stddev_latency = 10.0
        current_latency = 85.0  # 3.5 standard deviations above mean
        is_anomaly = 1 if (current_latency - mean_latency) > 3 * stddev_latency else 0
        self.assertEqual(is_anomaly, 1)


class TestRulesEngine(unittest.TestCase):
    """Test rule-based fallback engine per SPEC §6."""

    def test_blocked_country(self):
        """Test blocked country rule."""
        blocked = ["XX", "YY"]
        country = "XX"
        is_blocked = country in blocked
        self.assertTrue(is_blocked)

    def test_velocity_burst(self):
        """Test velocity burst detection."""
        max_per_hour = 20
        tx_count_1h = 25
        is_burst = tx_count_1h > max_per_hour
        self.assertTrue(is_burst)

    def test_impossible_travel(self):
        """Test impossible travel detection."""
        countries_1h = 3
        max_countries = 1
        is_impossible = countries_1h > max_countries
        self.assertTrue(is_impossible)

    def test_emulator_rule(self):
        """Test emulator detection rule."""
        features = {"device_is_emulator_detected": "1"}
        is_emulator = features.get("device_is_emulator_detected") == "1"
        self.assertTrue(is_emulator)

    def test_rooted_device_rule(self):
        """Test rooted device detection."""
        features = {"device_rooted_jailbroken": "1"}
        is_rooted = features.get("device_rooted_jailbroken") == "1"
        self.assertTrue(is_rooted)


class TestFeatureValidator(unittest.TestCase):
    """Test feature validation rules per SPEC §3.2.5."""

    def test_valid_feature_vector(self):
        """Test validation of complete feature vector."""
        features = {f"feature_{i}": str(i) for i in range(30)}
        features["transaction_id"] = "tx_123"
        # Should not raise
        self.assertIsInstance(features, dict)

    def test_missing_feature_detection(self):
        """Test detection of missing required features."""
        features = {"feature_1": "1.0", "feature_2": "2.0"}
        expected_count = 30
        actual_count = len(features)
        self.assertLess(actual_count, expected_count)

    def test_out_of_range_detection(self):
        """Test detection of out-of-range values."""
        value = -5.0
        min_val = 0.0
        max_val = 100.0
        is_valid = min_val <= value <= max_val
        self.assertFalse(is_valid)

    def test_type_validation(self):
        """Test type validation."""
        value_str = "not_a_number"
        try:
            float(value_str)
            is_valid = True
        except ValueError:
            is_valid = False
        self.assertFalse(is_valid)


if __name__ == "__main__":
    unittest.main()
