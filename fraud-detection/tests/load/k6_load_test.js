/**
 * k6 Load Test Configuration
 *
 * Tests:
 * - 10K TPS sustained (SPEC §2: throughput target)
 * - Burst to 50K TPS
 * - Latency p99 < 100ms, p50 < 50ms (SPEC §2)
 *
 * Per SPEC §8: Load tests using k6
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// ── Custom Metrics ───────────────────────────────────────────

const scoringLatency = new Trend('scoring_latency', true);
const scoringSuccess = new Rate('scoring_success');
const scoringErrors = new Counter('scoring_errors');
const approveCount = new Counter('approve_count');
const reviewCount = new Counter('review_count');
const declineCount = new Counter('decline_count');

// ── Configuration ────────────────────────────────────────────

const FRAUD_SERVICE_URL = __ENV.FRAUD_SERVICE_URL || 'http://localhost:8080';
const API_KEY = __ENV.API_KEY || '';

// ── Scenarios ────────────────────────────────────────────────

export const options = {
  scenarios: {
    // Sustained load: 10K TPS for 5 minutes
    sustained: {
      executor: 'constant-arrival-rate',
      rate: 10000,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 100,
      maxVUs: 500,
      exec: 'scoreTransaction',
    },

    // Burst test: 50K TPS for 30 seconds
    burst: {
      executor: 'constant-arrival-rate',
      rate: 50000,
      timeUnit: '1s',
      duration: '30s',
      startTime: '6m',  // After sustained test
      preAllocatedVUs: 200,
      maxVUs: 1000,
      exec: 'scoreTransaction',
    },

    // Ramp-up test
    rampUp: {
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: [
        { target: 1000, duration: '2m' },
        { target: 5000, duration: '2m' },
        { target: 10000, duration: '2m' },
        { target: 0, duration: '1m' },
      ],
      startTime: '7m',
      exec: 'scoreTransaction',
    },
  },

  thresholds: {
    // Latency thresholds per SPEC §2
    scoring_latency: [
      'p(50)<50',    // p50 < 50ms
      'p(95)<80',    // p95 < 80ms
      'p(99)<100',   // p99 < 100ms
    ],

    // Success rate threshold
    scoring_success: ['rate>0.99'],  // >99% success

    // Error rate threshold
    scoring_errors: ['count<100'],   // <100 errors total
  },
};

// ── Test Data Generators ─────────────────────────────────────

function generatePaymentEvent() {
  const channels = ['POS', 'ATM', 'CNP', 'MOBILE', 'WEB'];
  const countries = ['US', 'GB', 'DE', 'FR', 'JP', 'AU', 'CA', 'BR'];
  const mccs = [5411, 5412, 5541, 5912, 5999, 6011, 7011, 7995];

  return {
    event_id: `k6_${Date.now()}_${randomIntBetween(1000, 9999)}`,
    timestamp_ms: Date.now(),
    account_id: `acc_${randomIntBetween(1, 10000)}`,
    card_id: `card_${randomIntBetween(1, 100000)}`,
    amount: (randomIntBetween(1, 10000) / 100).toFixed(2),
    currency: 'USD',
    merchant_id: `merch_${randomIntBetween(1, 5000)}`,
    merchant_category_code: mccs[randomIntBetween(0, mccs.length - 1)],
    channel: channels[randomIntBetween(0, channels.length - 1)],
    country_code: countries[randomIntBetween(0, countries.length - 1)],
    ip_address: `${randomIntBetween(1, 255)}.${randomIntBetween(0, 255)}.${randomIntBetween(0, 255)}.${randomIntBetween(1, 254)}`,
    device_id: `device_${randomIntBetween(1, 5000)}`,
    geolocation: `${(Math.random() * 180 - 90).toFixed(6)},${(Math.random() * 360 - 180).toFixed(6)}`,
    metadata: {
      os_version: 'iOS_17.0',
      browser_fingerprint: `fp_${randomIntBetween(1, 10000)}`,
    },
  };
}

function generateFeatures(event) {
  return {
    velocity_tx_count_1h: String(randomIntBetween(0, 20)),
    velocity_tx_count_24h: String(randomIntBetween(0, 200)),
    velocity_amount_sum_1h: String(randomIntBetween(0, 10000)),
    velocity_amount_sum_24h: String(randomIntBetween(0, 100000)),
    velocity_decline_count_24h: String(randomIntBetween(0, 5)),
    velocity_unique_countries_1h: String(randomIntBetween(0, 3)),
    velocity_unique_merchants_24h: String(randomIntBetween(0, 50)),
    velocity_avg_amount_7d: String(randomIntBetween(100, 5000)),
    velocity_stddev_amount_7d: String(randomIntBetween(50, 2000)),
    velocity_time_since_last_tx: String(randomIntBetween(0, 86400)),
    behavioral_typical_amount_ratio: String((Math.random() * 5).toFixed(2)),
    behavioral_typical_hour_score: String((Math.random() * 0.1).toFixed(4)),
    behavioral_typical_day_score: String((Math.random() * 0.2).toFixed(4)),
    behavioral_merchant_category_diversity: String(randomIntBetween(1, 100)),
    behavioral_amount_zscore: String((Math.random() * 6 - 3).toFixed(2)),
    behavioral_is_recipient_new: String(randomIntBetween(0, 1)),
    behavioral_velocity_direction: String((Math.random() * 2).toFixed(2)),
    behavioral_time_between_tx_stddev: String(randomIntBetween(0, 3600)),
    behavioral_country_change_freq: String((Math.random() * 2).toFixed(2)),
    behavioral_night_tx_ratio: String((Math.random() * 0.5).toFixed(2)),
    device_is_known: String(randomIntBetween(0, 1)),
    device_last_seen_hours_ago: String(randomIntBetween(0, 1000)),
    device_unique_accounts_24h: String(randomIntBetween(0, 5)),
    device_is_emulator_detected: String(randomIntBetween(0, 1)),
    device_rooted_jailbroken: String(randomIntBetween(0, 1)),
    device_ip_country_match: String(randomIntBetween(0, 1)),
    device_ip_is_vpn: String(randomIntBetween(0, 1)),
    device_browser_fingerprint_match: String(randomIntBetween(0, 1)),
    device_latency_anomaly: String(randomIntBetween(0, 1)),
    device_is_new_os_version: String(randomIntBetween(0, 1)),
  };
}

// ── Test Functions ───────────────────────────────────────────

export function scoreTransaction() {
  const event = generatePaymentEvent();
  const features = generateFeatures(event);

  const payload = JSON.stringify({
    transaction_id: event.event_id,
    features: features,
    timestamp_ms: event.timestamp_ms,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      ...(API_KEY ? { 'Authorization': `Bearer ${API_KEY}` } : {}),
    },
    timeout: '5s',
  };

  const startTime = Date.now();
  const response = http.post(`${FRAUD_SERVICE_URL}/api/v1/score`, payload, params);
  const latency = Date.now() - startTime;

  scoringLatency.add(latency);

  const success = check(response, {
    'status is 200': (r) => r.status === 200,
    'has decision': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.decision !== undefined;
      } catch {
        return false;
      }
    },
    'probability in range': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.fraud_probability >= 0 && body.fraud_probability <= 1;
      } catch {
        return false;
      }
    },
  });

  scoringSuccess.add(success);

  if (success) {
    try {
      const body = JSON.parse(response.body);
      if (body.decision === 'APPROVE') approveCount.add(1);
      else if (body.decision === 'REVIEW') reviewCount.add(1);
      else if (body.decision === 'DECLINE') declineCount.add(1);
    } catch {
      scoringErrors.add(1);
    }
  } else {
    scoringErrors.add(1);
  }
}

// ── Teardown ─────────────────────────────────────────────────

export function handleSummary(data) {
  console.log('\n📊 Load Test Summary');
  console.log('====================');
  console.log(`Scoring Latency p50: ${data.metrics.scoring_latency?.values?.['p(50)']?.toFixed(2) || 'N/A'}ms`);
  console.log(`Scoring Latency p99: ${data.metrics.scoring_latency?.values?.['p(99)']?.toFixed(2) || 'N/A'}ms`);
  console.log(`Success Rate: ${((data.metrics.scoring_success?.values?.rate || 0) * 100).toFixed(2)}%`);
  console.log(`Approvals: ${data.metrics.approve_count?.values?.count || 0}`);
  console.log(`Reviews: ${data.metrics.review_count?.values?.count || 0}`);
  console.log(`Declines: ${data.metrics.decline_count?.values?.count || 0}`);
  console.log(`Errors: ${data.metrics.scoring_errors?.values?.count || 0}`);

  return {
    'tests/load/load_test_report.json': JSON.stringify(data, null, 2),
    stdout: textSummary(data),
  };
}
