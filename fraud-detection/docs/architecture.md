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
| 0 | Startup | Go service loads XGBoost model JSON + scaler JSON (μ, σ per feature) |
| 1 | Payment Gateway | Emits PaymentEvent → Kafka topic `payments.raw.v1` |
| 2 | Flink | Consumes events from Kafka |
| 3a | Flink (Velocity) | Computes windowed counters → Redis `velocity:{account_id}` |
| 3b | Flink (Behavioral) | Computes pattern features → Redis `account:{id}:profile` |
| 3c | Flink (Device) | Computes device features → Redis `device:{id}:*`, `ip_risk:{ip}` |
| 4 | Flink (Pipeline) | Merges all features → Redis `feature_vector:{tx_id}` |
| 5 | Flink (Pipeline) | Calls `FraudScoringService.ScoreTransaction` via gRPC |
| 6 | Fraud Service | Loads feature vector from Redis |
| 7 | Fraud Service | **StandardScaler normalizes features: (x - μ) / σ** |
| 8 | Fraud Service | XGBoost scores **scaled** feature vector → fraud_probability |
| 9 | Fraud Service | Threshold logic produces Decision (APPROVE/REVIEW/DECLINE) |
| 10 | Fraud Service | Returns ScoreResponse to Flink |
| 11a | Flink | Writes decision to Kafka `payments.decisions.v1` |
| 11b | Flink | Writes decision to Redis (short TTL) |
| 11c | Flink | Appends to audit log (S3/GCS) |

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

### Cross-Language Model Serving (Python → Go)

The ML model is trained in Python but served in Go. This introduces two critical synchronization problems:
1. **Feature scaling** must be identical on both sides
2. **Model format** — XGBoost's native JSON uses parallel arrays, not nested objects

#### The XGBoost JSON Format (What Go Actually Reads)

XGBoost exports models as nested JSON with **parallel arrays** per tree:

```json
{
  "learner": {
    "learner_model_param": { "base_score": "0.5", "num_feature": "30" },
    "gradient_booster": {
      "model": {
        "trees": [
          {
            "split_indices":    [1, 5, 18, 10, ...],    // feature index per node
            "split_conditions": [0.21, 0.94, -0.09, ...], // threshold per node
            "left_children":    [1, 3, 5, 7, -1, ...],    // left child (-1 = leaf)
            "right_children":   [2, 4, 6, 8, -1, ...],    // right child (-1 = leaf)
            "base_weights":     [0.0, -1.97, 1.96, ...],  // leaf values
            "tree_param":       { "num_nodes": "19" }
          }
        ]
      }
    }
  }
}
```

Go parses this into `compiledTree` with `compiledNode` structs, then walks each tree at inference time.

#### End-to-End Inference Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                   TRAINING (Python)                              │
│                                                                  │
│  train.py                                                        │
│    ├─ StandardScaler.fit(X_train)  → learns μ, σ per feature    │
│    ├─ XGBClassifier.fit(X_scaled, y)  → 500 trees on SCALED    │
│    └─ export_model()                                            │
│         ├─ fraud_xgboost_v1.0.0.json  (native XGBoost format)  │
│         ├─ scaler_v1.0.0.json         (mean/std arrays)         │
│         ├─ scaler_v1.0.0.pkl          (pickle for Python only)  │
│         └─ model_metadata_v1.0.0.json                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                    artifacts copied to
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   STARTUP (Go)                                   │
│                                                                  │
│  main.go                                                        │
│    ├─ LoadModel(fraud_xgboost_v1.0.0.json)                      │
│    │    └─ parseXGBoostJSON()                                    │
│    │         ├─ read learner.gradient_booster.model.trees[]      │
│    │         ├─ compile parallel arrays → compiledTree{Nodes[]}  │
│    │         └─ extract base_score (0.5)                         │
│    ├─ LoadScaler(scaler_v1.0.0.json) → mean[], std[]            │
│    └─ model.SetScaler(scaler)                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   INFERENCE (Go) — per request                   │
│                                                                  │
│  Scorer.Score(tx_id, features)                                   │
│    │                                                              │
│    ├─ 1. extractFeatures()                                       │
│    │      map[string]string → [30]float64 (raw values)          │
│    │                                                              │
│    ├─ 2. scaleFeatures()                                         │
│    │      (x - μ) / σ  for each of 30 features                  │
│    │      μ, σ loaded from scaler_v1.0.0.json                   │
│    │                                                              │
│    ├─ 3. Sum tree predictions                                    │
│    │      logit = base_score (0.5)                               │
│    │      for each of 500 trees:                                 │
│    │        logit += tree.predict(scaled_features)               │
│    │          └─ walk root→leaf: if feat[i] ≤ threshold → left  │
│    │                              else → right                   │
│    │          └─ return leaf_value                               │
│    │                                                              │
│    ├─ 4. sigmoid(logit) → probability [0, 1]                    │
│    │                                                              │
│    ├─ 5. Threshold classification                                │
│    │      prob < 0.30 → APPROVE                                  │
│    │      prob < 0.70 → REVIEW                                   │
│    │      prob ≥ 0.70 → DECLINE                                  │
│    │                                                              │
│    └─ 6. Rules engine override (can escalate, never downgrade)   │
└─────────────────────────────────────────────────────────────────┘
```

#### Why scaler_v1.0.0.pkl exists but Go uses scaler_v1.0.0.json

| File | Format | Consumer | Purpose |
|------|--------|----------|--------|
| `scaler_v1.0.0.pkl` | Python pickle | Python services | Batch scoring, drift monitor, retraining |
| `scaler_v1.0.0.json` | JSON (mean/std arrays) | **Go Fraud Service** | Real-time inference — no Python runtime needed |

The `.pkl` is a Python-serialized `StandardScaler` object. Go cannot read pickle natively. Instead, the training pipeline exports the same scaler parameters as plain JSON arrays (`mean[]` and `std[]`), which Go loads and applies as `(x - mean[i]) / std[i]` per feature.

#### What happens if the scaler is missing?

The Go service logs a warning and serves with raw (unscaled) features. This causes **training-serving skew** — the model receives values on different scales than it was trained on, producing unreliable fraud probabilities. The drift monitor detects this by comparing prediction distributions against the expected baseline.

#### How Go Calls the Trained Model (No Python at Serving Time)

Go loads the model artifacts directly and runs inference natively — it does **not** call Python. Here is the exact flow:

**Startup (once)**

```
main.go
  ├─ LoadModel("fraud_xgboost_v1.0.0.json")
  │    └─ Parses XGBoost's native JSON format
  │         (parallel arrays: split_indices[], split_conditions[], etc.)
  │         → compiles into 500 trees with 2,518 total nodes
  │         → extracts base_score = 0.5
  │
  ├─ LoadScaler("scaler_v1.0.0.json")
  │    └─ Loads mean[30] and std[30] arrays
  │
  └─ model.SetScaler(scaler)
```

**Per Request (~1ms)**

```
Scorer.Score(tx_id, features)
  │
  ├─ 1. extractFeatures()  → 30 raw float64 values
  │
  ├─ 2. scaleFeatures()    → (x - μ) / σ  per feature
  │                          (same normalization as Python training)
  │
  ├─ 3. logit = 0.5  (base_score)
  │     for each of 500 trees:
  │       logit += tree.predict(scaled_features)
  │         └─ walk: root → if feat[i] ≤ threshold → left
  │                             else → right
  │                  → return leaf_value
  │
  ├─ 4. probability = sigmoid(logit)
  │
  └─ 5. prob < 0.30 → APPROVE
        prob < 0.70 → REVIEW
        prob ≥ 0.70 → DECLINE
```

**Verified Working**

```
🟢 Legit transaction:  probability = 0.000050  → APPROVE  ✅
🔴 Fraud transaction:  probability = 0.999987  → DECLINE  ✅
```

**The Two Formats**

| File | What Go Does With It |
|------|---------------------|
| `fraud_xgboost_v1.0.0.json` | Parses XGBoost's parallel-array tree format → walks 500 trees at inference |
| `scaler_v1.0.0.json` | Loads mean/std → normalizes raw features before scoring |
| `scaler_v1.0.0.pkl` | **Go never touches this** — only Python services (batch scoring, drift monitor) |

The key insight: **XGBoost's JSON is self-contained**. Each tree stores split rules as parallel arrays (`split_indices[i]`, `split_conditions[i]`, `left_children[i]`, `right_children[i]`, `base_weights[i]`). Go compiles these into fast pointer-walking trees — no Python runtime needed at serving time.

### Model Artifact Lifecycle

```
Python ML Pipeline                    Go Fraud Service
─────────────────                    ─────────────────
                                       startup:
train.py                              │
  ├─ fit StandardScaler (μ, σ)        ├─ LoadModel(fraud_xgboost_v1.0.0.json)
  ├─ fit XGBClassifier (on scaled)    ├─ LoadScaler(scaler_v1.0.0.json)
  ├─ evaluate metrics                 └─ serve requests
  ├─ approval gate                         │
  └─ export:                              scoring:
       ├─ model JSON ───────────────────►  extractFeatures()
       ├─ scaler JSON ──────────────────►  scaleFeatures()  ← (x-μ)/σ
       ├─ scaler .pkl (for Python only)     Predict(scaled)
       └─ metadata JSON                    ─► fraud probability
```

### Artifact Versions & Formats

| Artifact | Format | Size | Consumer | Updated |
|----------|--------|------|----------|--------|
| `fraud_xgboost_v1.0.0.json` | XGBoost native JSON | ~300 KB | Go service | Weekly |
| `scaler_v1.0.0.json` | JSON (mean/std arrays) | ~3 KB | Go service | Weekly |
| `scaler_v1.0.0.pkl` | Python pickle | ~2 KB | Python (batch, drift) | Weekly |
| `model_metadata_v1.0.0.json` | JSON (feature names, importance) | ~3 KB | Go service, dashboards | Weekly |
| `metrics_v1.0.0.json` | JSON (evaluation results) | ~1 KB | Approval gate, dashboards | Weekly |

## Monitoring
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
