# Real-Time Fraud Detection System — Specification

## 1. Overview

A real-time fraud detection system for banking that ingests payment events via Kafka, processes feature engineering in parallel using Apache Flink, stores feature vectors in Redis, and scores transactions through an XGBoost model to produce an **APPROVE / REVIEW / DECLINE** decision in under 100ms p99 latency.

```
REAL-TIME
│
Payment Event
│
▼
Kafka
│
▼
Flink
│
┌──────────┼──────────┐
▼          ▼          ▼
Velocity   Behavioral   Device
Features    Features    Features
│          │          │
└──────────┼──────────┘
▼
Redis
│
▼
Fraud Service
│
▼
XGBoost
│
▼
APPROVE / REVIEW /
DECLINE
```

---

## 2. Goals & Constraints

| Dimension | Target |
|---|---|
| End-to-end latency (event → decision) | < 100ms p99, < 50ms p50 |
| Throughput | ≥ 10,000 transactions/second sustained |
| Availability | 99.95% (single-region), failover < 30s |
| False-positive rate | < 2% of legitimate transactions declined |
| Fraud detection rate | ≥ 95% of confirmed fraudulent transactions |
| Data retention (feature store) | 90 days hot in Redis, 1 year cold in object storage |
| Compliance | PCI-DSS Level 1, GDPR, SOC 2 Type II |

---

## 3. Component Specifications

### 3.1 Payment Event Ingestion

| Property | Value |
|---|---|
| Schema format | Avro with schema registry (Confluent or Redpanda) |
| Topic | `payments.raw.v1` |
| Partitioning key | `account_id` |
| Replication factor | 3 |
| Retention | 72 hours (raw), forever in DLQ |

**Event Schema (Avro)**

```json
{
  "namespace": "com.bank.fraud.events",
  "type": "record",
  "name": "PaymentEvent",
  "fields": [
    { "name": "event_id",      "type": "string", "doc": "UUID v4, unique idempotency key" },
    { "name": "timestamp_ms",  "type": "long",   "doc": "Unix epoch milliseconds" },
    { "name": "account_id",    "type": "string", "doc": "Bank account identifier" },
    { "name": "card_id",       "type": ["null","string"], "default": null },
    { "name": "amount",        "type": { "type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2 }, "doc": "Transaction amount" },
    { "name": "currency",      "type": "string", "doc": "ISO 4217 code" },
    { "name": "merchant_id",   "type": "string" },
    { "name": "merchant_category_code", "type": "int", "doc": "MCC code" },
    { "name": "channel",       "type": { "type": "enum", "name": "Channel", "symbols": ["POS","ATM","CNP","MOBILE","WEB"] } },
    { "name": "country_code",  "type": "string", "doc": "ISO 3166-1 alpha-2" },
    { "name": "ip_address",    "type": ["null","string"], "default": null },
    { "name": "device_id",     "type": ["null","string"], "default": null },
    { "name": "geolocation",   "type": ["null","string"], "default": null, "doc": "lat,lon or null" },
    { "name": "metadata",      "type": { "type": "map", "values": "string" }, "default": {} }
  ]
}
```

### 3.2 Apache Flink — Feature Engineering

Flink runs as a stateful streaming job (Flink on Kubernetes via Flink Operator, or managed service).

#### 3.2.1 Parallelism Model

Each feature group runs in its own operator chain with independent parallelism:

| Feature Group | Parallelism | State Backend | Checkpoint Interval |
|---|---|---|---|
| Velocity | 16 | RocksDB | 30s |
| Behavioral | 16 | RocksDB | 30s |
| Device | 8 | RocksDB | 30s |

#### 3.2.2 Velocity Features

Windowed counters over short time horizons.

| Feature | Window | Aggregation | Key |
|---|---|---|---|
| `velocity_tx_count_1h` | 1 hour tumbling | COUNT(*) | account_id |
| `velocity_tx_count_24h` | 24 hour tumbling | COUNT(*) | account_id |
| `velocity_amount_sum_1h` | 1 hour tumbling | SUM(amount) | account_id |
| `velocity_amount_sum_24h` | 24 hour tumbling | SUM(amount) | account_id |
| `velocity_decline_count_24h` | 24 hour tumbling | COUNT(*) WHERE declined | account_id |
| `velocity_unique_countries_1h` | 1 hour session | COUNT(DISTINCT country_code) | account_id |
| `velocity_unique_merchants_24h` | 24 hour tumbling | COUNT(DISTINCT merchant_id) | account_id |
| `velocity_avg_amount_7d` | 7 day tumbling | AVG(amount) | account_id |
| `velocity_stddev_amount_7d` | 7 day tumbling | STDDEV(amount) | account_id |
| `velocity_time_since_last_tx` | event time | LAST event time delta | account_id |

#### 3.2.3 Behavioral Features

Pattern-based features computed over longer horizons and user profiles.

| Feature | Description | State Source |
|---|---|---|
| `behavioral_typical_amount_ratio` | current_amount / avg_amount_7d | Flink state |
| `behavioral_typical_hour_score` | P(hour_of_day | account history) | Flink state |
| `behavioral_typical_day_score` | P(day_of_week | account history) | Flink state |
| `behavioral_merchant_category_diversity` | unique MCC count in last 30d | Flink state |
| `behavioral_amount_zscore` | z-score against account history | Flink state |
| `behavioral_is_recipient_new` | first-time recipient (Y/N) | Flink state |
| `behavioral_velocity_direction` | inflow vs outflow ratio 7d | Flink state |
| `behavioral_time_between_tx_stddev` | stddev of inter-tx intervals | Flink state |
| `behavioral_country_change_freq` | country changes per day 30d | Flink state |
| `behavioral_night_tx_ratio` | % transactions between 23:00–05:00 | Flink state |

#### 3.2.4 Device & Context Features

| Feature | Description | State Source |
|---|---|---|
| `device_is_known` | device_id seen in last 90d (Y/N) | Redis lookup |
| `device_last_seen_hours_ago` | hours since device last used | Redis lookup |
| `device_unique_accounts_24h` | distinct accounts on same device 24h | Flink state |
| `device_is_emulator_detected` | known emulator fingerprint (Y/N) | Redis lookup |
| `device_rooted_jailbroken` | rooted/jailbroken signal (Y/N) | Flink state |
| `device_ip_country_match` | IP geolocation country == tx country | Real-time geo |
| `device_ip_is_vpn` | VPN/proxy detection (Y/N) | Redis lookup (IP feed) |
| `device_browser_fingerprint_match` | fingerprint matches known (Y/N) | Redis lookup |
| `device_latency_anomaly` | API response time anomaly (Y/N) | Flink state |
| `device_is_new_os_version` | OS version changed (Y/N) | Flink state |

#### 3.2.5 Output Schema — Feature Vector

All feature groups merge into a single key-value pair:

```
Key:   feature_vector:{transaction_id}
Value: JSON blob with all 30 features + metadata
TTL:   5 minutes
```

### 3.3 Redis — Feature Store

| Property | Value |
|---|---|
| Deployment | Redis Cluster (6 nodes, 3 primary + 3 replica) |
| Persistence | RDB + AOF |
| Eviction | TTL-based only (features expire after 5 min) |
| Max memory | 64 GB per node |
| Serialization | JSON (upgradeable to MessagePack) |

**Key Patterns:**

| Key Pattern | TTL | Purpose |
|---|---|---|
| `feature_vector:{tx_id}` | 5 min | Assembled feature vector |
| `device:{device_id}:accounts` | 90 days | Device → account mapping |
| `account:{account_id}:profile` | 24 hours | Aggregated behavioral profile |
| `ip_risk:{ip_address}` | 24 hours | IP reputation / VPN flag |
| `velocity:{account_id}` | 24 hours | Rolling counters |

### 3.4 Fraud Service — Scoring API

gRPC service (with HTTP/JSON gateway for non-latency-critical callers).

**Proto Definition:**

```protobuf
syntax = "proto3";
package fraud.v1;

service FraudScoringService {
  rpc ScoreTransaction(ScoreRequest) returns (ScoreResponse);
  rpc GetDecision(StringRequest) returns (DecisionResponse);
  rpc HealthCheck(Empty) returns (HealthResponse);
}

message ScoreRequest {
  string transaction_id = 1;
  map<string, string> features = 2;   // key-value feature vector
  string model_version = 3;           // optional, for canary routing
}

message ScoreResponse {
  string transaction_id = 1;
  Decision decision = 2;
  double fraud_probability = 3;       // 0.0 – 1.0
  string model_version = 4;
  int64 latency_ms = 5;
  map<string, double> top_features = 6; // SHAP or feature importance
}

enum Decision {
  APPROVE  = 0;
  REVIEW   = 1;
  DECLINE  = 2;
}

message DecisionResponse {
  string transaction_id = 1;
  Decision decision = 2;
  string reason_code = 3;
  int64 timestamp_ms = 4;
}

message StringRequest {
  string value = 1;
}

message Empty {}

message HealthResponse {
  bool healthy = 1;
  string model_version = 2;
}
```

**Decision Thresholds:**

| Score Range | Decision | Action |
|---|---|---|
| 0.00 – 0.30 | APPROVE | Auto-approve, proceed with payment |
| 0.30 – 0.70 | REVIEW | Flag for manual review queue, hold payment 30 min |
| 0.70 – 1.00 | DECLINE | Auto-decline, send fraud alert to customer |

Thresholds are configurable per region/product and can be updated without redeployment.

### 3.5 XGBoost Model

| Property | Value |
|---|---|
| Framework | XGBoost (Python training, ONNX or native binary for serving) |
| Feature count | 30 (see sections 3.2.2–3.2.4) |
| Training data | 12 months of labeled transactions (fraud / not fraud) |
| Class imbalance handling | SMOTE + scale_pos_weight |
| Evaluation metrics | AUC-ROC ≥ 0.98, precision@5% FPR ≥ 0.90, recall ≥ 0.95 |
| Retraining cadence | Weekly (automated pipeline), with manual approval gate |
| Model serving | In-process XGBoost inference (< 1ms per prediction) |
| Explainability | Top-5 SHAP values returned with each decision |
| A/B testing | Canary model version support via `model_version` field |

**Feature Importance Hierarchy (expected):**

1. `velocity_tx_count_1h` — burst detection
2. `device_is_known` — first-time device risk
3. `behavioral_amount_zscore` — unusual amount
4. `device_ip_country_match` — geo anomaly
5. `velocity_unique_countries_1h` — impossible travel

---

## 4. Data Flow — End-to-End

```
1. Payment gateway emits PaymentEvent → Kafka topic payments.raw.v1
2. Flink job consumes from Kafka
3. Three parallel operator chains process:
   a. Velocity Features  → write to Redis velocity:{account_id}
   b. Behavioral Features → write to Redis account:{account_id}:profile
   c. Device Features    → lookup Redis device:{device_id}:*, ip_risk:{ip}
4. Flink merges all features into feature_vector:{tx_id} → Redis
5. Flink calls FraudScoringService.ScoreTransaction (gRPC)
6. Fraud Service loads feature vector from Redis
7. XGBoost scores the feature vector → fraud_probability
8. Threshold logic produces Decision (APPROVE/REVIEW/DECLINE)
9. Response streamed back to Flink
10. Flink writes decision to:
    a. decisions topic (Kafka) → downstream payment systems
    b. Redis (short TTL) → async lookups
    c. Audit log (append-only to S3/GCS)
```

---

## 5. Infrastructure & Deployment

### 5.1 Kafka

| Property | Value |
|---|---|
| Cluster | 5-broker minimum |
| Topics | `payments.raw.v1`, `payments.decisions.v1`, `fraud.alerts.v1`, `fraud.dlq.v1` |
| Monitoring | Consumer lag alerts at > 10,000 messages |

### 5.2 Flink

| Property | Value |
|---|---|
| Deployment | Kubernetes (Flink Operator) |
| Job manager | 2 replicas (HA) |
| Task managers | 8–16 (auto-scaled) |
| State backend | RocksDB with incremental checkpoints |
| Checkpoint storage | S3/GCS |
| Restart strategy | Fixed-delay (3 retries, 30s interval) |

### 5.3 Fraud Service

| Property | Value |
|---|---|
| Runtime | Go or Rust (for low latency) |
| Deployment | Kubernetes, 3–8 replicas (HPA on CPU/latency) |
| gRPC port | 50051 |
| HTTP gateway | Envoy sidecar or grpc-gateway |
| Model loading | Binary loaded at startup, hot-reload via SIGHUP |

### 5.4 Redis

| Property | Value |
|---|---|
| Deployment | Redis Cluster (6 nodes) |
| Monitoring | Memory usage, hit rate, latency p99 |
| Backup | Daily RDB snapshots to S3 |

### 5.5 Observability

| Layer | Tool | Metrics |
|---|---|---|
| Kafka | Prometheus + Grafana | Consumer lag, throughput, error rate |
| Flink | Flink metrics + Prometheus | Checkpoint duration, backpressure, throughput |
| Fraud Service | Prometheus + Grafana | Latency p50/p99, score distribution, error rate |
| Redis | Redis exporter + Prometheus | Hit rate, memory, latency, evictions |
| ML Model | Custom metrics | AUC drift, score distribution shift, feature drift |
| Tracing | OpenTelemetry → Jaeger/Tempo | End-to-end trace per transaction |
| Alerting | PagerDuty / OpsGenie | Latency breach, error rate > 1%, consumer lag |

---

## 6. Error Handling & Resilience

| Scenario | Handling |
|---|---|
| Kafka partition unavailable | Flink: automatic failover to replica partitions |
| Redis unavailable | Flink: use cached/default feature vector, flag for review |
| Fraud Service timeout (>50ms) | Flink: default to REVIEW decision, retry once |
| XGBoost inference failure | Fraud Service: fallback to rule-based engine, alert |
| Feature drift detected | Automated alert, manual model review gate |
| Model version mismatch | Fraud Service: reject and fall back to default model |
| DLQ message processing | Separate consumer, alert + manual inspection |

---

## 7. Security & Compliance

| Requirement | Implementation |
|---|---|
| PCI-DSS | No card numbers in feature vectors; tokenized card_id only |
| Encryption at rest | AES-256 for Redis, S3, Kafka (if not in transit) |
| Encryption in transit | TLS 1.2+ for all service-to-service communication |
| Access control | mTLS between services, RBAC on Kafka topics |
| Audit logging | Every decision logged with tx_id, score, model_version, timestamp |
| Data retention | 90 days hot (Redis), 1 year cold (S3), deletion policy |
| PII handling | IP addresses and geolocation hashed after feature extraction |
| Network isolation | VPC with private subnets, no public endpoints for internal services |

---

## 8. Testing Strategy

| Test Type | Scope | Tool |
|---|---|---|
| Unit tests | Feature computation logic, threshold logic | pytest / JUnit |
| Integration tests | Flink ↔ Redis ↔ Fraud Service | Testcontainers |
| Load tests | 10K TPS sustained, burst to 50K TPS | k6 / Gatling |
| Chaos tests | Redis node failure, Flink taskmanager crash | Litmus Chaos |
| Model tests | AUC regression, feature drift, edge cases | Great Expectations |
| Contract tests | gRPC proto compatibility | Buf / grpcurl |

---

## 9. CI/CD Pipeline

```
PR → Lint → Unit Tests → Type Check → Build Docker Images →
Integration Tests → Load Tests → Canary Deploy (5% traffic) →
Promote to 100% → Post-deploy monitoring
```

- **Model updates:** Separate pipeline: retrain → validate metrics → staging → canary → production
- **Feature additions:** Feature code change → add unit tests → integration tests → deploy
- **Rollback:** Automated rollback if error rate > 1% or latency p99 > 200ms within 10 min of deploy

---

## 10. Directory Structure (Proposed)

```
fraud-detection/
├── infra/
│   ├── kafka/          # topic configs, schema definitions
│   ├── flink/          # Flink job configs, deploy manifests
│   ├── redis/          # cluster configs
│   └── kubernetes/     # Helm charts, K8s manifests
├── flink-jobs/
│   ├── velocity/       # velocity feature computation
│   ├── behavioral/     # behavioral feature computation
│   ├── device/         # device feature computation
│   └── pipeline/       # merge + scoring orchestrator
├── fraud-service/
│   ├── cmd/            # service entrypoint
│   ├── internal/
│   │   ├── scoring/    # XGBoost inference
│   │   ├── rules/      # rule-based fallback engine
│   │   └── grpc/       # gRPC server handlers
│   ├── proto/          # protobuf definitions
│   └── models/         # trained model binaries
├── feature-engineering/
│   ├── schemas/        # Avro / protobuf schemas
│   ├── validation/     # feature validation rules
│   └── monitoring/     # drift detection scripts
├── ml-pipeline/
│   ├── training/       # XGBoost training scripts
│   ├── evaluation/     # AUC, precision, recall eval
│   └── data/           # labeled transaction datasets
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── load/
│   └── chaos/
└── docs/
    ├── architecture.md
    ├── runbook.md
    └── feature-catalog.md
```

---

## 11. Environment Matrix

| Environment | Kafka | Flink | Redis | Fraud Service | Purpose |
|---|---|---|---|---|---|
| Local dev | Docker Compose | Local Flink | 1 node | Local binary | Developer testing |
| CI | Testcontainers | Embedded | Testcontainers | Mock server | Automated tests |
| Staging | 3-broker cluster | 2 TM | 3-node cluster | 2 replicas | Integration & load tests |
| Production | 5-broker cluster | 8–16 TM | 6-node cluster | 3–8 replicas | Live traffic |

---

## 12. Success Metrics (Post-Launch)

| Metric | Target | Measurement |
|---|---|---|
| Fraud detection rate | ≥ 95% | Monthly review of confirmed frauds |
| False positive rate | < 2% | Monthly review of manual review queue |
| Mean time to decision | < 50ms | Prometheus latency histograms |
| System availability | 99.95% | Uptime monitoring |
| Model AUC-ROC | ≥ 0.98 | Weekly automated evaluation |
| Consumer lag | < 1,000 messages | Real-time alerting |
| Manual review backlog | < 500 cases/day | Queue monitoring |
