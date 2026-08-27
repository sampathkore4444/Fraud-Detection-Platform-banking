# Feature Store Data Model — Redis

## Overview

This document defines the complete Redis data model for the fraud detection feature store. It covers all key patterns, data structures, TTLs, access patterns, read/write throughput, and memory sizing estimates.

---

## 1. Redis Cluster Topology

| Property | Value |
|---|---|
| Deployment | Redis Cluster |
| Nodes | 6 (3 primary + 3 replica) |
| Max memory per node | 64 GB |
| Total cluster capacity | 192 GB (primary only) |
| Persistence | RDB + AOF |
| Eviction policy | `noeviction` (TTL-based only) |
| Serialization | JSON (upgradeable to MessagePack) |
| Hash slots | 16,384 distributed across 3 primaries |

### Slot Allocation

| Primary Node | Hash Slot Range | Key Prefixes |
|---|---|---|
| Primary 1 | 0 – 5460 | `feature_vector:*`, `decision:*` |
| Primary 2 | 5461 – 10922 | `velocity:*`, `account:*` |
| Primary 3 | 10923 – 16383 | `device:*`, `ip_risk:*` |

---

## 2. Key Patterns

### 2.1 Feature Vector (Assembled)

```
Key:    feature_vector:{transaction_id}
Type:   STRING (JSON)
TTL:    300 seconds (5 minutes)
Written by:  Flink Pipeline Orchestrator (step 4)
Read by:     Fraud Service (step 6)
```

**Purpose:** Stores the complete assembled feature vector for a transaction, combining velocity, behavioral, and device features into a single object for the Fraud Service to consume.

**Value Structure:**
```json
{
  "transaction_id": "txn_abc123def456",
  "account_id": "acc_9876543210",
  "computed_at_ms": 1693123456789,

  "velocity_tx_count_1h": "12",
  "velocity_tx_count_24h": "87",
  "velocity_amount_sum_1h": "4500.75",
  "velocity_amount_sum_24h": "32150.00",
  "velocity_decline_count_24h": "2",
  "velocity_unique_countries_1h": "1",
  "velocity_unique_merchants_24h": "15",
  "velocity_avg_amount_7d": "350.20",
  "velocity_stddev_amount_7d": "125.50",
  "velocity_time_since_last_tx": "3420",

  "behavioral_typical_amount_ratio": "2.45",
  "behavioral_typical_hour_score": "0.08",
  "behavioral_typical_day_score": "0.14",
  "behavioral_merchant_category_diversity": "23",
  "behavioral_amount_zscore": "1.85",
  "behavioral_is_recipient_new": "1",
  "behavioral_velocity_direction": "0.75",
  "behavioral_time_between_tx_stddev": "1800.50",
  "behavioral_country_change_freq": "0.10",
  "behavioral_night_tx_ratio": "0.15",

  "device_is_known": "0",
  "device_last_seen_hours_ago": "720.50",
  "device_unique_accounts_24h": "1",
  "device_is_emulator_detected": "0",
  "device_rooted_jailbroken": "0",
  "device_ip_country_match": "1",
  "device_ip_is_vpn": "0",
  "device_browser_fingerprint_match": "1",
  "device_latency_anomaly": "0",
  "device_is_new_os_version": "0"
}
```

**Access Pattern:**
- **Write:** Once per transaction (Flink pipeline, ~10K writes/sec)
- **Read:** Once per transaction (Fraud Service, ~10K reads/sec)
- **Key Space:** ~10K keys per second × 300s TTL = ~3M active keys
- **Size per key:** ~1.5 KB (JSON) + ~100 bytes (key) = ~1.6 KB
- **Memory estimate:** 3M × 1.6 KB = **4.8 GB**

---

### 2.2 Velocity Features (Per Account)

```
Key:    velocity:{account_id}:{feature_name}
Type:   STRING
TTL:    86400 seconds (24 hours)
Written by:  Flink Velocity Features job
Read by:     Flink Pipeline Orchestrator (step 3a)
```

**Purpose:** Stores windowed velocity counters per account. Each feature is a separate key to allow independent TTLs and atomic updates.

**Key Examples:**
```
velocity:acc_9876543210:velocity_tx_count_1h        = "12"
velocity:acc_9876543210:velocity_tx_count_24h       = "87"
velocity:acc_9876543210:velocity_amount_sum_1h      = "4500.75"
velocity:acc_9876543210:velocity_amount_sum_24h     = "32150.00"
velocity:acc_9876543210:velocity_decline_count_24h  = "2"
velocity:acc_9876543210:velocity_unique_countries_1h = "1"
velocity:acc_9876543210:velocity_unique_merchants_24h = "15"
velocity:acc_9876543210:velocity_avg_amount_7d      = "350.20"
velocity:acc_9876543210:velocity_stddev_amount_7d   = "125.50"
velocity:acc_9876543210:velocity_time_since_last_tx = "3420"
```

**Access Pattern:**
- **Write:** ~10K writes/sec (all accounts with transactions)
- **Read:** ~10K reads/sec (pipeline merge step)
- **Key Space:** ~10 features × ~1M active accounts = ~10M active keys
- **Size per key:** ~60 bytes (value) + ~50 bytes (key) = ~110 bytes
- **Memory estimate:** 10M × 110 bytes = **1.1 GB**

---

### 2.3 Behavioral Profile (Per Account)

```
Key:    account:{account_id}:profile
Type:   STRING (JSON)
TTL:    86400 seconds (24 hours)
Written by:  Flink Behavioral Features job
Read by:     Flink Pipeline Orchestrator (step 3b)
```

**Purpose:** Stores aggregated behavioral profile for an account, computed over longer horizons (7–30 days). Stored as a single JSON object for atomic reads.

**Value Structure:**
```json
{
  "behavioral_typical_amount_ratio": 1.5,
  "behavioral_typical_hour_score": 0.08,
  "behavioral_typical_day_score": 0.14,
  "behavioral_merchant_category_diversity": 23,
  "behavioral_amount_zscore": 1.85,
  "behavioral_is_recipient_new": 0,
  "behavioral_velocity_direction": 0.75,
  "behavioral_time_between_tx_stddev": 1800.50,
  "behavioral_country_change_freq": 0.10,
  "behavioral_night_tx_ratio": 0.15,
  "updated_at_ms": 1693123456789
}
```

**Access Pattern:**
- **Write:** ~10K writes/sec (update on each transaction)
- **Read:** ~10K reads/sec (pipeline merge step)
- **Key Space:** ~1M active accounts = ~1M active keys
- **Size per key:** ~500 bytes (JSON) + ~40 bytes (key) = ~540 bytes
- **Memory estimate:** 1M × 540 bytes = **540 MB**

---

### 2.4 Device → Account Mapping

```
Key:    device:{device_id}:accounts
Type:   SET
TTL:    7776000 seconds (90 days)
Written by:  Flink Device Features job
Read by:     Flink Device Features job (enrichment)
```

**Purpose:** Maps a device fingerprint to the set of bank accounts that have used it. Used to detect device farming (one device, many accounts).

**Value Example:**
```
SADD device:dev_abc123:accounts acc_9876543210 acc_1234567890 acc_5555555555
```

**Access Pattern:**
- **Write:** ~10K writes/sec (SADD, idempotent)
- **Read:** ~10K reads/sec (SMEMBERS for device anomaly detection)
- **Key Space:** ~500K active devices = ~500K active keys
- **Size per key:** ~200 bytes (set) + ~40 bytes (key) = ~240 bytes
- **Memory estimate:** 500K × 240 bytes = **120 MB**

---

### 2.5 Device Last Seen

```
Key:    device:{device_id}:last_seen
Type:   STRING (timestamp_ms)
TTL:    7776000 seconds (90 days)
Written by:  Flink Device Features job
Read by:     Flink Device Features job, Fraud Service
```

**Purpose:** Tracks when a device was last used. Used to compute `device_last_seen_hours_ago` and `device_is_known`.

**Value Example:**
```
SET device:dev_abc123:last_seen "1693123456789"
```

**Access Pattern:**
- **Write:** ~10K writes/sec
- **Read:** ~10K reads/sec
- **Key Space:** ~500K active devices = ~500K active keys
- **Size per key:** ~20 bytes (value) + ~40 bytes (key) = ~60 bytes
- **Memory estimate:** 500K × 60 bytes = **30 MB**

---

### 2.6 IP Risk / Reputation

```
Key:    ip_risk:{ip_address}
Type:   HASH
TTL:    86400 seconds (24 hours)
Written by:  External threat intelligence feed, Flink Device Features
Read by:     Flink Device Features job, Fraud Service
```

**Purpose:** Stores IP reputation data including VPN/proxy detection, country mapping, and risk score. Updated from external threat intelligence feeds.

**Value Structure:**
```
HSET ip_risk:203.0.113.42
  is_vpn        "true"
  is_proxy      "false"
  country       "US"
  risk_score    "0.85"
  threat_type   "vpn"
  last_updated  "1693123456789"
  source        "threat_intel_feed_v2"
```

**Access Pattern:**
- **Write:** ~1K writes/sec (external feed updates)
- **Read:** ~10K reads/sec (device feature enrichment)
- **Key Space:** ~2M tracked IPs = ~2M active keys
- **Size per key:** ~200 bytes (hash) + ~30 bytes (key) = ~230 bytes
- **Memory estimate:** 2M × 230 bytes = **460 MB**

---

### 2.7 Decision Cache

```
Key:    decision:{transaction_id}
Type:   STRING (JSON)
TTL:    300 seconds (5 minutes)
Written by:  Flink Pipeline Orchestrator (step 10b)
Read by:     Fraud Service GetDecision RPC, async lookups
```

**Purpose:** Stores the fraud decision for a transaction, enabling async lookups by downstream systems.

**Value Structure:**
```json
{
  "transaction_id": "txn_abc123def456",
  "decision": "APPROVE",
  "fraud_probability": 0.15,
  "model_version": "v1.0.0",
  "reason_code": "",
  "latency_ms": 5,
  "timestamp_ms": 1693123456789,
  "top_features": {
    "velocity_tx_count_1h": 0.12,
    "device_is_known": -0.08
  }
}
```

**Access Pattern:**
- **Write:** ~10K writes/sec (one per scored transaction)
- **Read:** ~1K reads/sec (async lookups, not on hot path)
- **Key Space:** ~10K/sec × 300s = ~3M active keys
- **Size per key:** ~400 bytes (JSON) + ~50 bytes (key) = ~450 bytes
- **Memory estimate:** 3M × 450 bytes = **1.35 GB**

---

### 2.8 Fraud Alerts

```
Key:    fraud_alert:{transaction_id}
Type:   STRING (JSON)
TTL:    2592000 seconds (30 days)
Written by:  Flink Pipeline Orchestrator (step 10a, DECLINE only)
Read by:     Fraud alert consumers, manual review queue
```

**Purpose:** Stores fraud alerts for declined transactions. Longer TTL for compliance and investigation purposes.

**Value Structure:**
```json
{
  "transaction_id": "txn_abc123def456",
  "account_id": "acc_9876543210",
  "decision": "DECLINE",
  "fraud_probability": 0.92,
  "reason_code": "VELOCITY_BURST",
  "model_version": "v1.0.0",
  "timestamp_ms": 1693123456789,
  "investigation_status": "pending",
  "assigned_to": null
}
```

**Access Pattern:**
- **Write:** ~100 writes/sec (declined transactions only, ~1% of total)
- **Read:** ~10 reads/sec (manual review queue polling)
- **Key Space:** ~100/sec × 2.6M seconds (30d) = ~260M active keys
- **Size per key:** ~500 bytes (JSON) + ~50 bytes (key) = ~550 bytes
- **Memory estimate:** 260M × 550 bytes = **143 GB** ⚠️ (requires archival)

> ⚠️ **Note:** This key pattern exceeds single-node capacity. In production, use:
> - Redis Cluster (distributed across nodes)
> - Move older alerts to S3/GCS cold storage after 7 days
> - Use Redis Streams or Kafka for alert delivery instead of Redis storage

---

## 3. Complete Key Pattern Summary

| # | Key Pattern | Type | TTL | Write/sec | Read/sec | Active Keys | Size/Key | Memory |
|---|---|---|---|---|---|---|---|---|
| 1 | `feature_vector:{tx_id}` | STRING | 5 min | 10K | 10K | 3M | 1.6 KB | 4.8 GB |
| 2 | `velocity:{acct_id}:{feat}` | STRING | 24 hr | 100K | 100K | 10M | 110 B | 1.1 GB |
| 3 | `account:{acct_id}:profile` | STRING | 24 hr | 10K | 10K | 1M | 540 B | 540 MB |
| 4 | `device:{dev_id}:accounts` | SET | 90 days | 10K | 10K | 500K | 240 B | 120 MB |
| 5 | `device:{dev_id}:last_seen` | STRING | 90 days | 10K | 10K | 500K | 60 B | 30 MB |
| 6 | `ip_risk:{ip}` | HASH | 24 hr | 1K | 10K | 2M | 230 B | 460 MB |
| 7 | `decision:{tx_id}` | STRING | 5 min | 10K | 1K | 3M | 450 B | 1.35 GB |
| 8 | `fraud_alert:{tx_id}` | STRING | 30 days | 100 | 10 | 260M | 550 B | 143 GB* |

*Requires archival strategy — see Section 5

**Total Hot Memory (without alerts):** ~8.4 GB
**Total with alerts (before archival):** ~151 GB → distributed across 3 primary nodes (~50 GB each)

---

## 4. Data Lifecycle

### 4.1 Write Path

```
Payment Event
    │
    ▼
Kafka (payments.raw.v1)
    │
    ▼
Flink Velocity Features ──► SET velocity:{acct_id}:{feat}  (TTL 24h)
Flink Behavioral Features ──► SET account:{acct_id}:profile  (TTL 24h)
Flink Device Features ──► SADD device:{dev_id}:accounts  (TTL 90d)
                        ──► SET device:{dev_id}:last_seen  (TTL 90d)
                        ──► HSET ip_risk:{ip}  (TTL 24h)
    │
    ▼
Flink Pipeline Merge ──► SET feature_vector:{tx_id}  (TTL 5min)
    │
    ▼
Fraud Service ──► GET feature_vector:{tx_id}
    │
    ▼
Flink Pipeline Decision ──► SET decision:{tx_id}  (TTL 5min)
                        ──► SET fraud_alert:{tx_id}  (TTL 30d, DECLINE only)
```

### 4.2 Read Path

```
Fraud Service
    │
    ├──► GET feature_vector:{tx_id}        (1 read, hot path)
    │
    ├──► GET decision:{tx_id}              (async lookup, not hot)
    │
    └──► GET fraud_alert:{tx_id}           (manual review, not hot)
```

### 4.3 Expiration Strategy

| Key Pattern | TTL | Expiration Action |
|---|---|---|
| `feature_vector:*` | 5 min | Auto-expire, no action needed |
| `velocity:*` | 24 hr | Auto-expire, recomputed on next transaction |
| `account:*:profile` | 24 hr | Auto-expire, recomputed on next transaction |
| `device:*:accounts` | 90 days | Auto-expire, device becomes "unknown" |
| `device:*:last_seen` | 90 days | Auto-expire, same as above |
| `ip_risk:*` | 24 hr | Auto-expire, refreshed by threat feed |
| `decision:*` | 5 min | Auto-expire, decision also in Kafka |
| `fraud_alert:*` | 30 days | Archive to S3/GCS, then delete |

---

## 5. Archival Strategy

### 5.1 Fraud Alerts Archival

The `fraud_alert:*` keys accumulate to ~143 GB over 30 days. To manage this:

```
┌──────────────────┐     ┌──────────────┐     ┌──────────────┐
│  Redis (hot)     │────►│  S3/GCS      │────►│  Glacier     │
│  Last 7 days     │     │  7-90 days   │     │  90+ days    │
│  ~34 GB          │     │  ~109 GB     │     │  Archive     │
└──────────────────┘     └──────────────┘     └──────────────┘
```

**Archive Job (Daily):**
```python
# Pseudocode for daily archival
def archive_fraud_alerts():
    cutoff = now() - 7 days
    for key in redis.scan("fraud_alert:*"):
        if redis.ttl(key) < (30 days - 7 days):
            data = redis.get(key)
            s3.put_object(
                Bucket="fraud-alerts-archive",
                Key=f"{year}/{month}/{day}/{transaction_id}.json",
                Body=data
            )
            redis.delete(key)  # Free Redis memory
```

### 5.2 Feature Vector Archival

Feature vectors expire after 5 minutes and don't need archival since:
- The decision is captured in `decision:*` and Kafka `payments.decisions.v1`
- Full audit trail is written to S3/GCS append-only log

### 5.3 Velocity Profile Archival

Velocity features expire after 24 hours and are recomputed from Flink state. The Flink RocksDB state backend provides durability via checkpoints to S3/GCS.

---

## 6. Access Pattern Analysis

### 6.1 Hot Path (Must be < 5ms)

These keys are on the critical scoring path:

| Key Pattern | Operation | Latency Target |
|---|---|---|
| `feature_vector:{tx_id}` | GET | < 2ms |
| `decision:{tx_id}` | SET | < 2ms |

### 6.2 Warm Path (Must be < 20ms)

These keys are read during feature computation:

| Key Pattern | Operation | Latency Target |
|---|---|---|
| `velocity:{acct_id}:*` | MGET (10 keys) | < 5ms |
| `account:{acct_id}:profile` | GET | < 5ms |
| `device:{dev_id}:accounts` | SMEMBERS | < 10ms |
| `device:{dev_id}:last_seen` | GET | < 5ms |
| `ip_risk:{ip}` | HGETALL | < 5ms |

### 6.3 Cold Path (Must be < 100ms)

These keys are read asynchronously:

| Key Pattern | Operation | Latency Target |
|---|---|---|
| `decision:{tx_id}` | GET | < 50ms |
| `fraud_alert:{tx_id}` | GET | < 100ms |

---

## 7. Sizing Calculations

### 7.1 Key Space Growth

```
At 10K TPS sustained:

feature_vector:*    = 10K/sec × 300s  =     3,000,000 keys
velocity:*          = 10K/sec × 10 feat × 86400s / 86400s TTL = 10,000,000 keys
account:*:profile   = 10K/sec × 86400s / 86400s TTL           =  1,000,000 keys
device:*:accounts   = 10K/sec × 90 days                       =    500,000 keys
device:*:last_seen  = 10K/sec × 90 days                       =    500,000 keys
ip_risk:*           = External feed                            =  2,000,000 keys
decision:*          = 10K/sec × 300s  =     3,000,000 keys
fraud_alert:*       = 100/sec × 30 days = 260,000,000 keys (archived after 7d)

Total (before archival): ~280,000,000 keys
Total (with archival):   ~20,000,000 keys
```

### 7.2 Memory Calculation

```
Without fraud alerts (archived):
  feature_vector:    3M × 1.6KB   =  4.8 GB
  velocity:         10M × 110B    =  1.1 GB
  account profile:   1M × 540B    =  0.5 GB
  device accounts:   0.5M × 240B  =  0.1 GB
  device last_seen:  0.5M × 60B   =  0.03 GB
  ip_risk:           2M × 230B    =  0.5 GB
  decision:          3M × 450B    =  1.4 GB
  ─────────────────────────────────────────
  Total hot memory:               ≈  8.4 GB

With fraud alerts (7 days):
  fraud_alert:       600K × 550B  =  0.3 GB
  ─────────────────────────────────────────
  Total with alerts:              ≈  8.7 GB

Cluster sizing (3 primary nodes):
  Per node: 8.7 GB / 3 = ~2.9 GB primary data
  With replica factor: 2.9 GB × 2 = ~5.8 GB per node
  With headroom (50%): 5.8 GB / 0.5 = ~11.6 GB recommended per node

  ✅ Well within 64 GB/node limit
```

### 7.3 Throughput Calculation

```
At 10K TPS sustained:

Write operations/sec:
  feature_vector SET:     10,000/sec
  velocity SET:          100,000/sec (10 features per tx)
  account profile SET:    10,000/sec
  device SADD:            10,000/sec
  device last_seen SET:   10,000/sec
  decision SET:           10,000/sec
  fraud alert SET:          100/sec (1% decline rate)
  ─────────────────────────────────
  Total writes:          ~150,000/sec

Read operations/sec:
  feature_vector GET:     10,000/sec
  velocity MGET:          10,000/sec (10 keys per MGET = 100,000 key ops)
  account profile GET:    10,000/sec
  device SMEMBERS:        10,000/sec
  device last_seen GET:   10,000/sec
  ip_risk HGETALL:        10,000/sec
  ─────────────────────────────────
  Total reads:           ~150,000/sec

Redis Cluster capacity (per node):
  Single-node Redis: ~100K ops/sec
  Cluster (3 primaries): ~300K ops/sec total
  With replicas: Read replicas can absorb read load

  ✅ Within Redis Cluster capacity
```

---

## 8. Monitoring & Alerting

### 8.1 Key Metrics

| Metric | Warning | Critical | Action |
|---|---|---|---|
| Memory usage per node | > 40 GB (62%) | > 50 GB (78%) | Scale nodes or archive |
| Keyspace hits/sec | < 90% of total ops | < 80% | Check key patterns |
| Evictions/sec | > 0 | > 100/sec | Check TTLs, add memory |
| Latency p99 | > 5ms | > 10ms | Check network, scale |
| Connected clients | > 500 | > 1000 | Check connection pooling |
| Expired keys/sec | > 200K | > 500K | Normal for velocity keys |

### 8.2 Prometheus Queries

```promql
# Memory usage percentage
redis_memory_used_bytes / redis_memory_max_bytes * 100

# Operations per second
rate(redis_commands_processed_total[1m])

# Hit rate
redis_keyspace_hits_total / (redis_keyspace_hits_total + redis_keyspace_misses_total)

# Key count per pattern
redis_keys{pattern="feature_vector:*"}
redis_keys{pattern="velocity:*"}
redis_keys{pattern="decision:*"}
redis_keys{pattern="fraud_alert:*"}

# Latency
redis_commands_duration_seconds_total / redis_commands_processed_total
```

### 8.3 Grafana Dashboard Panels

| Panel | Query | Visualization |
|---|---|---|
| Memory Usage | `redis_memory_used_bytes` | Gauge |
| Operations/sec | `rate(redis_commands_processed_total[1m])` | Time series |
| Hit Rate | `redis_keyspace_hits_total / ...` | Gauge |
| Key Count by Pattern | `redis_keys{pattern="..."}` | Stacked bar |
| Latency p50/p99 | Histogram | Time series |
| Expired Keys | `rate(redis_expired_keys_total[1m])` | Time series |
| Evictions | `rate(redis_evicted_keys_total[1m])` | Time series |

---

## 9. Disaster Recovery

### 9.1 Backup Strategy

| Backup Type | Frequency | Retention | Storage |
|---|---|---|---|
| RDB snapshot | Every 6 hours | 7 days | S3/GCS |
| AOF rewrite | Daily | 30 days | S3/GCS |
| Cross-region replica | Real-time | Rolling | DR region |

### 9.2 Recovery Procedures

| Scenario | RTO | RPO | Procedure |
|---|---|---|---|
| Single node failure | < 30s | 0 (replica promotion) | Automatic failover |
| Primary node failure | < 30s | 0 (replica promotion) | Automatic failover |
| Full cluster failure | < 5 min | < 6 hours | Restore from RDB + AOF |
| Region failure | < 30 min | < 1 hour | Cross-region failover |

### 9.3 Data Recovery

```bash
# Restore from RDB snapshot
redis-cli --rdb /backup/dump.rdb

# Restore from AOF
redis-cli --appendonly yes --appendfilename appendonly.aof

# Verify data integrity
redis-cli DBSIZE
redis-cli INFO keyspace
```

---

## 10. Migration & Upgrade Path

### 10.1 JSON → MessagePack Migration

When JSON serialization becomes a bottleneck:

1. Deploy dual-write (JSON + MessagePack)
2. Migrate reads to MessagePack
3. Stop JSON writes
4. Clean up JSON keys

### 10.2 Single Node → Cluster Migration

When outgrowing a single Redis instance:

1. Deploy Redis Cluster
2. Use `redis-cli --cluster create` to initialize
3. Migrate keys with `redis-cli --cluster reshard`
4. Update application connection strings

### 10.3 Redis → Redis + Object Storage

When fraud_alert keys exceed cluster capacity:

1. Deploy S3/GCS bucket for archival
2. Run archival job to move old alerts
3. Update reads to check Redis first, fallback to S3
4. Reduce Redis TTL for fraud_alert to 7 days
