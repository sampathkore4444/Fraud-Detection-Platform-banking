"""
Main entrypoint — mirrors Go cmd/server/main.go

Handles:
- Configuration loading
- Model + scaler loading
- Redis connection with resilience
- gRPC server startup
- HTTP metrics/health endpoint
- Graceful shutdown on SIGINT/SIGTERM
"""

import os
import sys
import signal
import time
import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis
from prometheus_client import (
    start_http_server,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from app.config import load_config
from app.scoring import Model, ScalerParams, Scorer, load_model, load_scaler
from app.rules import RulesEngine, RulesConfig
from app.resilience import CircuitBreaker, ResilientRedis
from app.server import FraudServicer, ScoreRequest, ScoreResponse

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fraud-service")


# ─── Prometheus Metrics ──────────────────────────────────────────────────────

# Decision counters
decisions_total = Counter(
    "fraud_service_decisions_total",
    "Total fraud decisions",
    ["decision", "reason"],
)

# Latency histogram
scoring_latency = Histogram(
    "fraud_service_scoring_latency_ms",
    "Scoring latency in milliseconds",
    buckets=[1, 2, 5, 10, 20, 50, 100, 200, 500],
)

# Model info
model_version = Gauge(
    "fraud_service_model_version_info",
    "Current model version",
    ["version"],
)

# Redis status
redis_healthy = Gauge(
    "fraud_service_redis_healthy",
    "Redis connectivity (1=healthy, 0=unhealthy)",
)

# Uptime
uptime_seconds = Gauge(
    "fraud_service_uptime_seconds",
    "Service uptime in seconds",
)


# ─── HTTP Handler for /metrics and /health ────────────────────────────────────

class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for Prometheus metrics and health check."""

    fraud_server = None  # Set at startup

    def do_GET(self):
        if self.path == "/metrics":
            metrics = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(metrics)

        elif self.path == "/health":
            health = {
                "status": "ok",
                "model": MetricsHandler.fraud_server.scorer.model_version if MetricsHandler.fraud_server else "unknown",
                "uptime": int(time.time() - (MetricsHandler.fraud_server.start_time if MetricsHandler.fraud_server else time.time())),
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(health).encode())

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default access logs


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    """Main entrypoint — mirrors Go main()."""
    logger.info("Starting Fraud Detection Service (Python)")

    # ── Load Configuration ──────────────────────────────────
    config_path = os.environ.get("CONFIG_PATH", "")
    cfg = load_config(config_path)

    logger.info(
        f"Config loaded: model={cfg.model.path}, version={cfg.model.version}, "
        f"approve_threshold={cfg.scoring.approve_threshold}, "
        f"review_threshold={cfg.scoring.review_threshold}"
    )

    # ── Load Model + Scaler ─────────────────────────────────
    model = Model()

    if os.path.exists(cfg.model.path):
        try:
            model = load_model(cfg.model.path)
            model.version = cfg.model.version
            model.thresholds.approve = cfg.scoring.approve_threshold
            model.thresholds.review = cfg.scoring.review_threshold
        except Exception as e:
            logger.warning(f"Failed to load model: {e} — using default")
            model = Model()
    else:
        logger.info("No model file found — using default model")
        model = Model()

    # Load scaler
    if os.path.exists(cfg.model.scaler_path):
        try:
            scaler = load_scaler(cfg.model.scaler_path)
            model.scaler = scaler
            logger.info(f"Scaler loaded from {cfg.model.scaler_path}")
        except Exception as e:
            logger.warning(f"Failed to load scaler: {e} — serving with raw features")
    else:
        logger.warning(f"No scaler file found at {cfg.model.scaler_path}")

    # Set model info metrics
    model_version.labels(version=model.version).set(1)

    # ── Initialize Rules Engine ─────────────────────────────
    rules_config = RulesConfig(
        enabled=cfg.rules.enabled,
        max_amount_per_day=cfg.rules.max_amount_per_day,
        max_tx_per_hour=cfg.rules.max_tx_per_hour,
        max_countries_per_day=cfg.rules.max_countries_per_day,
        blocked_countries=cfg.rules.blocked_countries,
    )
    rules_engine = RulesEngine(rules_config)

    # ── Initialize Scorer ───────────────────────────────────
    scorer = Scorer(model, rules_engine)

    # ── Connect to Redis (with resilience) ──────────────────
    raw_redis = redis.Redis(
        host=cfg.redis.addr.split(":")[0],
        port=int(cfg.redis.addr.split(":")[1]),
        password=cfg.redis.password or None,
        db=cfg.redis.db,
        socket_connect_timeout=5,
        socket_timeout=cfg.redis.read_timeout_ms / 1000.0,
        retry_on_timeout=True,
    )

    try:
        if raw_redis.ping():
            logger.info(f"Connected to Redis at {cfg.redis.addr}")
            redis_healthy.set(1)
        else:
            logger.warning("Redis ping returned False — running degraded")
            redis_healthy.set(0)
    except Exception as e:
        logger.warning(f"Failed to connect to Redis: {e} — running degraded")
        redis_healthy.set(0)

    # Wrap with resilience
    redis_cb = CircuitBreaker(
        name="redis",
        failure_threshold=5,
        success_threshold=3,
        timeout_seconds=30.0,
    )
    resilient_redis = ResilientRedis(raw_redis, redis_cb)

    # ── Create Fraud Server ─────────────────────────────────
    fraud_server = FraudServicer(scorer, resilient_redis)
    MetricsHandler.fraud_server = fraud_server

    # ── Start Metrics HTTP Server ───────────────────────────
    metrics_port = cfg.metrics.port
    try:
        metrics_server = HTTPServer(("0.0.0.0", metrics_port), MetricsHandler)
        metrics_thread = threading.Thread(target=metrics_server.serve_forever, daemon=True)
        metrics_thread.start()
        logger.info(f"Metrics server started on :{metrics_port}")
    except Exception as e:
        logger.warning(f"Failed to start metrics server: {e}")

    # ── Start gRPC Server (simplified — using HTTP for demo) ─
    grpc_port = cfg.server.grpc_port
    logger.info(f"gRPC server would start on :{grpc_port}")
    logger.info("For demo, scoring is available via the FraudServicer directly")

    # ── Graceful Shutdown ───────────────────────────────────
    shutdown_event = threading.Event()

    def handle_signal(signum, frame):
        logger.info(f"Received signal {signum} — shutting down gracefully")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(
        f"Fraud Service ready — model={model.version}, "
        f"trees={model.num_trees}, features={len(model.features)}"
    )

    # Wait for shutdown
    shutdown_event.wait()

    # Cleanup
    logger.info("Shutting down...")
    try:
        metrics_server.shutdown()
    except Exception:
        pass
    raw_redis.close()
    logger.info("Fraud Service stopped")


if __name__ == "__main__":
    main()
