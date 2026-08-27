# Fraud Detection System — Runbook

## Quick Reference

| Issue | Severity | Section |
|-------|----------|---------|
| High latency | P1 | [High Latency](#high-latency) |
| Consumer lag | P1 | [Consumer Lag](#consumer-lag) |
| Redis memory pressure | P1 | [Redis Memory](#redis-memory) |
| Fraud Service errors | P1 | [Service Errors](#service-errors) |
| Model drift detected | P2 | [Model Drift](#model-drift) |
| Flink job failure | P1 | [Flink Job Failure](#flink-job-failure) |
| Kafka topic unavailable | P1 | [Kafka Topic Issues](#kafka-topic-issues) |

---

## High Latency

**Symptoms:** Fraud scoring latency p99 > 100ms

**Diagnosis:**
```bash
# Check Fraud Service latency
curl http://fraud-service:9090/metrics | grep fraud_scoring_latency

# Check Redis latency
redis-cli --latency-history -i 5

# Check Flink backpressure
kubectl logs -l app=flink-taskmanager | grep -i backpressure
```

**Resolution:**
1. Check if Redis is under memory pressure → [Redis Memory](#redis-memory)
2. Check if Flink is backpressured → scale task managers
3. Check if Fraud Service CPU is saturated → scale HPA
4. Check network latency between components

---

## Consumer Lag

**Symptoms:** Kafka consumer lag > 10,000 messages on `payments.raw.v1`

**Diagnosis:**
```bash
# Check consumer group lag
kafka-consumer-groups --bootstrap-server kafka:29092 \
  --group flink-velocity-features \
  --describe

# Check Flink metrics
curl http://flink-jobmanager:9249/metrics | grep flink_consumer_lag
```

**Resolution:**
1. Scale Flink task managers (increase parallelism)
2. Check for Kafka partition rebalancing
3. Check for Flink checkpoint failures
4. If persistent, consider increasing Flink resources

---

## Redis Memory

**Symptoms:** Redis memory usage > 80%, evictions occurring

**Diagnosis:**
```bash
# Check Redis memory
redis-cli info memory | grep used_memory_human
redis-cli info memory | grep maxmemory_human

# Check key distribution
redis-cli --bigkeys

# Check TTL distribution
redis-cli --scan --pattern "feature_vector:*" | wc -l
```

**Resolution:**
1. Check if TTLs are being applied correctly
2. Look for keys without TTL
3. Scale Redis vertically (increase maxmemory)
4. Check for key pattern bloat

---

## Service Errors

**Symptoms:** Fraud Service error rate > 1%

**Diagnosis:**
```bash
# Check Fraud Service logs
kubectl logs -l app=fraud-service --tail=100

# Check health endpoint
curl http://fraud-service:50051/health

# Check Redis connectivity from Fraud Service
kubectl exec -it fraud-service-xxx -- redis-cli -h redis ping
```

**Resolution:**
1. Check Redis connectivity
2. Check model file availability
3. Check gRPC connection pool
4. Restart Fraud Service if needed

---

## Model Drift

**Symptoms:** Automated alert for feature drift, AUC-ROC regression

**Diagnosis:**
```bash
# Check drift monitoring dashboard in Grafana
# Alert: fraud_feature_drift_pvalue < 0.05

# Check model metrics
curl http://fraud-service:9090/metrics | grep fraud_model_auc
```

**Resolution:**
1. Review drift monitoring report
2. Check if drift is expected (seasonality, event)
3. If sustained, trigger model retraining
4. Review feature importance changes

---

## Flink Job Failure

**Symptoms:** Flink job in FAILED state, no new features being computed

**Diagnosis:**
```bash
# Check Flink job status
kubectl logs -l app=flink-jobmanager | grep -i "job.*failed"

# Check checkpoint failures
kubectl logs -l app=flink-taskmanager | grep -i "checkpoint.*failed"

# Check Flink web UI
kubectl port-forward svc/flink-jobmanager 8082:8081
```

**Resolution:**
1. Check if Kafka topic is available
2. Check Redis connectivity from Flink
3. Check for out-of-memory errors
4. Restart job from last checkpoint
5. If persistent, check for code bugs

---

## Kafka Topic Issues

**Symptoms:** Topic not found, partition unavailable

**Diagnosis:**
```bash
# List topics
kafka-topics --list --bootstrap-server kafka:29092

# Check topic config
kafka-topics --describe --bootstrap-server kafka:29092 --topic payments.raw.v1

# Check broker status
kafka-broker-api-versions --bootstrap-server kafka:29092
```

**Resolution:**
1. Recreate topic if missing
2. Check broker replication
3. Check for disk space issues
4. Scale brokers if needed

---

## Emergency Procedures

### Kill Switch
To stop all fraud scoring (emergency only):
```bash
kubectl scale deployment fraud-service --replicas=0 -n fraud-detection
```

### Rollback
To rollback to previous model version:
```bash
# Update model version in ConfigMap
kubectl edit configmap fraud-service-config -n fraud-detection
# Restart Fraud Service
kubectl rollout restart deployment fraud-service -n fraud-detection
```

### Force Checkpoint
To force Flink checkpoint:
```bash
# Via Flink REST API
curl -X POST http://flink-jobmanager:8081/jobs/<job-id>/checkpoints
```

---

## Monitoring Dashboards

| Dashboard | URL | Purpose |
|-----------|-----|---------|
| Fraud Service | Grafana → Fraud Service | Latency, errors, score distribution |
| Kafka | Grafana → Kafka | Consumer lag, throughput |
| Flink | Grafana → Flink | Checkpoints, backpressure, throughput |
| Redis | Grafana → Redis | Memory, hit rate, latency |
| ML Model | Grafana → Model | AUC, drift, feature importance |

## Alerting Rules

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| `fraud_high_latency` | p99 > 100ms for 5min | P1 | Page on-call |
| `fraud_high_error_rate` | error_rate > 1% for 2min | P1 | Page on-call |
| `fraud_consumer_lag` | lag > 10000 for 5min | P1 | Page on-call |
| `fraud_redis_memory` | usage > 80% | P2 | Alert team |
| `fraud_model_drift` | p_value < 0.05 | P2 | Alert ML team |
| `fraud_checkpoint_failed` | 3 consecutive failures | P1 | Page on-call |
