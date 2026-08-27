# Fraud Detection System — Architecture

## System Overview

A real-time fraud detection system for banking that processes payment events through Kafka, computes features using Apache Flink, stores them in Redis, and scores transactions via XGBoost to produce APPROVE/REVIEW/DECLINE decisions in under 100ms p99.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Payment Gateway                              │
│                     (Payment Event Source)                          │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ Avro / JSON
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Apache Kafka                                   │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────┐   │
│  │ payments.raw.v1 │ │payments.decisions│ │   fraud.alerts.v1   │   │
│  │  (12 partitions)│ │  .v1 (12 part.) │ │   (6 partitions)    │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────────┘   │
│  ┌─────────────────┐                                               │
│  │  fraud.dlq.v1   │                                               │
│  │  (6 partitions) │                                               │
│  └─────────────────┘                                               │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Apache Flink                                     │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                │
│  │   Velocity   │ │  Behavioral  │ │    Device    │                │
│  │   Features   │ │  Features    │ │   Features   │                │
│  │ Parallelism=16│ │ Parallelism=16│ │ Parallelism=8│                │
│  │ RocksDB      │ │ RocksDB      │ │ RocksDB      │                │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘                │
│         │                │                │                         │
│         └────────────────┼────────────────┘                         │
│                          ▼                                          │
│                ┌──────────────────┐                                 │
│                │ Feature Merger   │                                 │
│                │ (Pipeline Job)   │                                 │
│                │ Parallelism=8    │                                 │
│                └────────┬─────────┘                                 │
│                         │                                          │
└─────────────────────────┼──────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Redis Cluster                                  │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  feature_vector:{tx_id}  — TTL 5 min                        │    │
│  │  velocity:{account_id}   — TTL 24 hours                     │    │
│  │  account:{id}:profile    — TTL 24 hours                     │    │
│  │  device:{id}:accounts    — TTL 90 days                      │    │
│  │  ip_risk:{ip}            — TTL 24 hours                     │    │
│  └─────────────────────────────────────────────────────────────┘    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Fraud Service (Go)                               │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  gRPC Server (port 50051)                                   │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │    │
│  │  │   Scoring    │  │    Rules     │  │   Metrics    │      │    │
│  │  │   Engine     │  │   Engine     │  │  (Prometheus)│      │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────────────┘      │    │
│  │         │                 │                                 │    │
│  │         └────────┬────────┘                                 │    │
│  │                  ▼                                          │    │
│  │         ┌──────────────┐                                    │    │
│  │         │   XGBoost    │                                    │    │
│  │         │   Model      │                                    │    │
│  │         └──────┬───────┘                                    │    │
│  └────────────────┼────────────────────────────────────────────┘    │
│                   ▼                                                  │
│            ┌─────────────┐                                          │
│            │  APPROVE /  │                                          │
│            │  REVIEW /   │                                          │
│            │  DECLINE    │                                          │
│            └─────────────┘                                          │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow (End-to-End)

| Step | Component | Action |
|------|-----------|--------|
| 1 | Payment Gateway | Emits PaymentEvent → Kafka topic `payments.raw.v1` |
| 2 | Flink | Consumes events from Kafka |
| 3a | Flink (Velocity) | Computes windowed counters → Redis `velocity:{account_id}` |
| 3b | Flink (Behavioral) | Computes pattern features → Redis `account:{id}:profile` |
| 3c | Flink (Device) | Computes device features → Redis `device:{id}:*`, `ip_risk:{ip}` |
| 4 | Flink (Pipeline) | Merges all features → Redis `feature_vector:{tx_id}` |
| 5 | Flink (Pipeline) | Calls `FraudScoringService.ScoreTransaction` via gRPC |
| 6 | Fraud Service | Loads feature vector from Redis |
| 7 | Fraud Service | XGBoost scores feature vector → fraud_probability |
| 8 | Fraud Service | Threshold logic produces Decision (APPROVE/REVIEW/DECLINE) |
| 9 | Fraud Service | Returns ScoreResponse to Flink |
| 10a | Flink | Writes decision to Kafka `payments.decisions.v1` |
| 10b | Flink | Writes decision to Redis (short TTL) |
| 10c | Flink | Appends to audit log (S3/GCS) |

## Component Details

### Kafka
- **Topics:** `payments.raw.v1`, `payments.decisions.v1`, `fraud.alerts.v1`, `fraud.dlq.v1`
- **Partitioning:** By `account_id` for ordering guarantees
- **Retention:** 72 hours (raw), forever (DLQ)

### Flink
- **State Backend:** RocksDB with incremental checkpoints
- **Checkpoint Interval:** 30 seconds
- **Restart Strategy:** Fixed-delay (3 retries, 30s interval)
- **Parallelism:** Velocity=16, Behavioral=16, Device=8, Pipeline=8

### Redis
- **Deployment:** Cluster mode (6 nodes)
- **Persistence:** RDB + AOF
- **Eviction:** TTL-based only (noeviction policy)
- **Key Patterns:** feature_vector (5m), velocity (24h), device (90d)

### Fraud Service
- **Language:** Go
- **Protocol:** gRPC (primary), HTTP/JSON (gateway)
- **Port:** 50051 (gRPC), 9090 (metrics)
- **Model:** XGBoost with SHAP explainability
- **Fallback:** Rule-based engine when ML model unavailable

### Monitoring
- **Metrics:** Prometheus + Grafana
- **Tracing:** OpenTelemetry → Jaeger/Tempo
- **Alerting:** PagerDuty / OpsGenie

## Latency Budget

| Component | Target | Budget |
|-----------|--------|--------|
| Kafka consume | < 5ms | 5ms |
| Feature computation | < 20ms | 20ms |
| Redis read | < 5ms | 5ms |
| gRPC call to Fraud Service | < 10ms | 10ms |
| XGBoost inference | < 1ms | 1ms |
| Redis write (decision) | < 5ms | 5ms |
| Kafka produce (decision) | < 5ms | 5ms |
| **Total p99** | **< 100ms** | **51ms** |

## Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Kafka partition unavailable | Flink fails over to replica | Automatic |
| Redis unavailable | Default feature vector used | Flag for review |
| Fraud Service timeout | Default to REVIEW | Retry once |
| XGBoost inference failure | Rule-based fallback | Alert + manual |
| Flink TaskManager crash | Job restarts from checkpoint | 30s max |
| Feature drift detected | Automated alert | Manual review gate |
