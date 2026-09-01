# Fraud Detection Platform

Real-time fraud detection system for banking that processes payment events through Kafka, computes features using Apache Flink, stores them in Redis, and scores transactions via XGBoost to produce APPROVE/REVIEW/DECLINE decisions in under 100ms p99.

## Architecture

```
Payment Event → Kafka → Flink → Redis → Fraud Service → XGBoost → DECISION
                    │
              ┌─────┼─────┐
              ▼     ▼     ▼
          Velocity Behavioral Device
          Features  Features  Features
```

See [docs/architecture.md](docs/architecture.md) for full architecture details.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Go 1.21+
- kubectl (for Kubernetes)

### Local Development

```bash
# Start infrastructure (Kafka, Redis, Flink)
docker-compose up -d

# Run Fraud Service (Go) locally
cd fraud-service
go run cmd/server/main.go

# OR Run Fraud Service (Python) locally
cd fraud-service-python
python -m app.main

# Run ML training pipeline
cd ml-pipeline
python training/train.py --data data/training_data.csv --version v1.0.0

# Run Admin Portal
cd admin-portal/api && uvicorn app:app --reload --port 8000
cd admin-portal/frontend && npm start

# Run feature validation
python feature-engineering/validation/feature_validator.py
```

### Running Tests

```bash
# Unit tests
python -m pytest tests/unit/ -v

# Integration tests (requires Redis)
REDIS_HOST=localhost python -m pytest tests/integration/ -v

# Load tests (requires running service)
k6 run tests/load/k6_load_test.js
```

## Project Structure

```
fraud-detection/
├── infra/                    # Infrastructure configs
│   ├── kafka/               # Topic configs, schema definitions
│   ├── flink/               # Flink job configs
│   ├── redis/               # Redis configs
│   └── kubernetes/          # K8s manifests, Helm charts
├── flink-jobs/              # Flink streaming jobs
│   ├── velocity/            # Velocity features (SPEC §3.2.2)
│   ├── behavioral/          # Behavioral features (SPEC §3.2.3)
│   ├── device/              # Device features (SPEC §3.2.4)
│   └── pipeline/            # Feature merge + scoring (SPEC §4)
├── fraud-service/           # Go gRPC scoring service
│   ├── cmd/server/          # Service entrypoint
│   ├── internal/            # Internal packages
│   │   ├── scoring/         # XGBoost inference (SPEC §3.5)
│   │   ├── rules/           # Rule-based fallback (SPEC §6)
│   │   ├── resilience/      # Circuit breaker, retry, fallback
│   │   ├── grpc/            # gRPC handlers
│   │   └── config/          # Configuration
│   ├── proto/               # Protobuf definitions (SPEC §3.4)
│   └── models/              # Trained model binaries
├── fraud-service-python/    # Python gRPC scoring service (alternative)
│   ├── app/
│   │   ├── scoring.py       # XGBoost inference (same model as Go)
│   │   ├── rules.py         # Rule-based fallback (6 rules)
│   │   ├── resilience.py    # Circuit breaker, retry, fallback
│   │   ├── server.py        # gRPC server
│   │   └── config.py        # YAML config + env overrides
│   ├── Dockerfile
│   └── requirements.txt
├── admin-portal/            # Fraud operations dashboard
│   ├── api/                 # FastAPI backend
│   │   ├── app.py           # REST API (decisions, models, rules, audit)
│   │   └── requirements.txt
│   └── frontend/            # React admin UI
│       ├── src/App.js       # Dashboard, review queue, models, rules, audit
│       └── package.json
├── feature-engineering/     # Feature schemas & validation
│   ├── schemas/             # Avro schemas (SPEC §3.1)
│   ├── validation/          # Feature validation (SPEC §3.2.5)
│   └── monitoring/          # Drift detection (SPEC §3.5)
├── ml-pipeline/             # ML training & evaluation
│   ├── training/            # XGBoost training (SPEC §3.5)
│   ├── evaluation/          # Model evaluation
│   └── data/                # Training datasets
├── tests/                   # Test suites
│   ├── unit/                # Unit tests (SPEC §8)
│   ├── integration/         # Integration tests
│   ├── load/                # Load tests (k6)
│   └── chaos/               # Chaos test scenarios
├── docs/                    # Documentation
│   ├── architecture.md      # Architecture overview
│   ├── runbook.md           # Operations runbook
│   ├── feature-catalog.md   # Feature documentation
│   └── feature-store-data-model.md  # Redis key patterns & sizing
└── docker-compose.yml       # Local development environment
```

## Configuration

### Fraud Service

| Env Variable | Default | Description |
|---|---|---|
| `CONFIG_PATH` | — | Path to YAML config |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `GRPC_PORT` | `50051` | gRPC port |
| `MODEL_VERSION` | `v1.0.0` | Model version |

### Decision Thresholds

| Score Range | Decision | Action |
|---|---|---|
| 0.00 – 0.30 | APPROVE | Auto-approve |
| 0.30 – 0.70 | REVIEW | Manual review queue |
| 0.70 – 1.00 | DECLINE | Auto-decline + fraud alert |

## Deployment

### Kubernetes

```bash
# Install Helm chart
helm upgrade --install fraud-detection \
  infra/kubernetes/charts/fraud-detection \
  --namespace fraud-detection \
  --create-namespace
```

### Docker

```bash
# Build Fraud Service (Go)
docker build -f Dockerfile.fraud-service -t fraud-service:latest fraud-service/

# Build Fraud Service (Python)
docker build -f fraud-service-python/Dockerfile -t fraud-service-python:latest fraud-service-python/

# Build Flink Jobs
docker build -f Dockerfile.flink-jobs -t flink-jobs:latest .
```

## CI/CD

Automated pipeline per SPEC §9:
1. Lint → Unit Tests → Build Docker Images
2. Integration Tests → Load Tests
3. Canary Deploy (5% traffic) → Promote to 100%

## Monitoring

- **Metrics:** Prometheus + Grafana
- **Tracing:** OpenTelemetry → Jaeger
- **Alerting:** PagerDuty integration

## Compliance

- PCI-DSS Level 1
- GDPR compliant (PII hashing)
- SOC 2 Type II
- Audit logging for all decisions

## Admin Portal

A React + FastAPI admin dashboard for fraud operations:

| Feature | Description |
|---------|-------------|
| **Real-time Dashboard** | Fraud rate, latency, approval/decline ratio, queue size |
| **Decision Review Queue** | Analyst interface to approve/decline REVIEW transactions |
| **Model Management** | View model versions, metrics, activate/deactivate |
| **Rules Editor** | Create/edit/disable fraud rules without code deploy |
| **Audit Trail** | Searchable log of all decisions and analyst actions |
| **Case Management** | Investigate fraud alerts, assign analysts |

### Admin Portal Quick Start

```bash
# Backend (FastAPI)
cd admin-portal/api
pip install -r requirements.txt
uvicorn app:app --reload --port 8000

# Frontend (React)
cd admin-portal/frontend
npm install
npm start
```

See [docs/architecture.md](docs/architecture.md) for the full admin portal architecture.

## Documentation

- [Architecture](docs/architecture.md)
- [Runbook](docs/runbook.md)
- [Feature Catalog](docs/feature-catalog.md)
- [Feature Store Data Model](docs/feature-store-data-model.md)
- [API Spec](fraud-service/api/swagger.json)
