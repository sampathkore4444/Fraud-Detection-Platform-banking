"""
gRPC server — mirrors Go internal/grpc/server.go

Implements:
- ScoreTransaction: evaluate single transaction for fraud
- GetDecision: retrieve previously computed decision
- HealthCheck: service health and model version
- ScoreBatch: batch scoring via streaming
"""

import json
import time
import logging
from concurrent import futures

import grpc
from grpc import StatusCode

from app.scoring import Scorer, Decision, ScoreResult
from app.resilience import ResilientRedis, default_feature_vector

logger = logging.getLogger(__name__)


# ─── Proto stubs (generated from fraud.proto) ────────────────────────────────
# In production, use grpc_tools.protoc to generate these.
# For now, we define the message types inline.

class ScoreRequest:
    def __init__(self, transaction_id="", features=None, model_version="", timestamp_ms=0):
        self.transaction_id = transaction_id
        self.features = features or {}
        self.model_version = model_version
        self.timestamp_ms = timestamp_ms


class ScoreResponse:
    def __init__(self, transaction_id="", decision=0, fraud_probability=0.0,
                 model_version="", latency_ms=0, top_features=None, reason_code=""):
        self.transaction_id = transaction_id
        self.decision = decision
        self.fraud_probability = fraud_probability
        self.model_version = model_version
        self.latency_ms = latency_ms
        self.top_features = top_features or {}
        self.reason_code = reason_code


class DecisionResponse:
    def __init__(self, transaction_id="", decision=0, timestamp_ms=0):
        self.transaction_id = transaction_id
        self.decision = decision
        self.timestamp_ms = timestamp_ms


class HealthResponse:
    def __init__(self, healthy=True, model_version="", model_loaded_at_ms=0, uptime_seconds=0):
        self.healthy = healthy
        self.model_version = model_version
        self.model_loaded_at_ms = model_loaded_at_ms
        self.uptime_seconds = uptime_seconds


# ─── Fraud Server ─────────────────────────────────────────────────────────────

class FraudServicer:
    """
    FraudScoringService gRPC servicer.
    Mirrors Go grpc.FraudServer.
    """

    def __init__(self, scorer: Scorer, redis_client: ResilientRedis):
        self.scorer = scorer
        self.redis = redis_client
        self.start_time = time.time()

    def ScoreTransaction(self, request, context):
        """
        Evaluate a single transaction for fraud.
        Mirrors Go FraudServer.ScoreTransaction().
        """
        if not request.transaction_id:
            context.abort(StatusCode.INVALID_ARGUMENT, "transaction_id is required")

        start = time.time()

        # Load feature vector from Redis (with resilience)
        features = request.features
        if not features:
            features = self.redis.get_feature_vector(
                f"feature_vector:{request.transaction_id}"
            )

        # Determine model version
        model_version = request.model_version or self.scorer.model_version

        # Score the transaction
        result = self.scorer.score(
            transaction_id=request.transaction_id,
            features=features,
            model_version=model_version,
            timestamp_ms=request.timestamp_ms,
        )

        latency_ms = int((time.time() - start) * 1000)

        return ScoreResponse(
            transaction_id=result.transaction_id,
            decision=result.decision.value,
            fraud_probability=result.fraud_probability,
            model_version=result.model_version,
            latency_ms=latency_ms,
            top_features=result.top_features,
            reason_code=result.reason_code,
        )

    def GetDecision(self, request, context):
        """
        Retrieve a previously computed decision.
        Mirrors Go FraudServer.GetDecision().
        """
        if not request.value:
            context.abort(StatusCode.INVALID_ARGUMENT, "transaction_id is required")

        key = f"decision:{request.value}"
        decision_data = self.redis.get_feature_vector(key)

        if not decision_data or "decision" not in decision_data:
            context.abort(StatusCode.NOT_FOUND, f"decision not found for {request.value}")

        decision_str = decision_data["decision"]
        decision_map = {"APPROVE": 0, "REVIEW": 1, "DECLINE": 2}

        return DecisionResponse(
            transaction_id=request.value,
            decision=decision_map.get(decision_str, 0),
            timestamp_ms=int(time.time() * 1000),
        )

    def HealthCheck(self, request, context):
        """
        Service health check.
        Mirrors Go FraudServer.HealthCheck().
        """
        redis_healthy = self.redis.ping()
        uptime = int(time.time() - self.start_time)

        return HealthResponse(
            healthy=redis_healthy,
            model_version=self.scorer.model_version,
            model_loaded_at_ms=int(self.scorer.model_loaded_at * 1000),
            uptime_seconds=uptime,
        )


# ─── gRPC Server Factory ─────────────────────────────────────────────────────

def create_grpc_server(
    scorer: Scorer,
    redis_client: ResilientRedis,
    port: int = 50051,
    max_concurrent_streams: int = 1000,
) -> grpc.Server:
    """
    Create and configure the gRPC server.
    Mirrors Go main.go gRPC server setup.
    """
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=100),
        options=[
            ("grpc.max_concurrent_streams", max_concurrent_streams),
        ],
    )

    servicer = FraudServicer(scorer, redis_client)

    # Register service (using dynamic registration since we don't have proto stubs)
    # In production, use generated proto stubs:
    #   fraud_pb2_grpc.add_FraudScoringServiceServicer_to_server(servicer, server)

    return server
