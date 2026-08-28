"""
Configuration module — mirrors Go internal/config/config.go

Loads YAML config with environment variable overrides.
Default values match the Go service defaults exactly.
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ServerConfig:
    grpc_port: int = 50051
    http_port: int = 8080
    read_timeout_seconds: float = 5.0


@dataclass
class ScoringConfig:
    approve_threshold: float = 0.30
    review_threshold: float = 0.70
    default_model: str = "xgboost-v1"
    timeout_ms: int = 50


@dataclass
class RedisConfig:
    addr: str = "localhost:6379"
    password: str = ""
    db: int = 0
    pool_size: int = 20
    read_timeout_ms: int = 10
    write_timeout_ms: int = 10
    key_prefix: str = "feature_vector:"
    vector_ttl_seconds: int = 300  # 5 minutes


@dataclass
class ModelConfig:
    path: str = "models/fraud_xgboost.json"
    scaler_path: str = "models/scaler_v1.0.0.json"
    version: str = "v1.0.0"
    hot_reload: bool = True
    check_interval_seconds: int = 30
    max_features: int = 30


@dataclass
class RulesConfig:
    enabled: bool = True
    max_amount_per_day: float = 50000.0
    max_tx_per_hour: int = 20
    max_countries_per_day: int = 3
    blocked_countries: List[str] = field(default_factory=list)


@dataclass
class MetricsConfig:
    enabled: bool = True
    port: int = 9090
    prefix: str = "fraud_service"


@dataclass
class Config:
    server: ServerConfig = field(default_factory=ServerConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    rules: RulesConfig = field(default_factory=RulesConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)


def load_config(path: Optional[str] = None) -> Config:
    """
    Load configuration from YAML file with environment variable overrides.
    Mirrors Go config.Load() behavior.
    """
    cfg = Config()

    # Load from YAML file if provided
    if path and os.path.exists(path):
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        if data:
            if "server" in data:
                cfg.server = ServerConfig(**{
                    k: v for k, v in data["server"].items()
                    if k in ServerConfig.__dataclass_fields__
                })
            if "scoring" in data:
                cfg.scoring = ScoringConfig(**{
                    k: v for k, v in data["scoring"].items()
                    if k in ScoringConfig.__dataclass_fields__
                })
            if "redis" in data:
                cfg.redis = RedisConfig(**{
                    k: v for k, v in data["redis"].items()
                    if k in RedisConfig.__dataclass_fields__
                })
            if "model" in data:
                cfg.model = ModelConfig(**{
                    k: v for k, v in data["model"].items()
                    if k in ModelConfig.__dataclass_fields__
                })
            if "rules" in data:
                cfg.rules = RulesConfig(**{
                    k: v for k, v in data["rules"].items()
                    if k in RulesConfig.__dataclass_fields__
                })
            if "metrics" in data:
                cfg.metrics = MetricsConfig(**{
                    k: v for k, v in data["metrics"].items()
                    if k in MetricsConfig.__dataclass_fields__
                })

    # Environment variable overrides (same as Go)
    if v := os.environ.get("REDIS_ADDR"):
        cfg.redis.addr = v
    if v := os.environ.get("GRPC_PORT"):
        try:
            cfg.server.grpc_port = int(v)
        except ValueError:
            pass
    if v := os.environ.get("MODEL_VERSION"):
        cfg.model.version = v
    if v := os.environ.get("MODEL_PATH"):
        cfg.model.path = v
    if v := os.environ.get("SCALER_PATH"):
        cfg.model.scaler_path = v
    if v := os.environ.get("HTTP_PORT"):
        try:
            cfg.metrics.port = int(v)
        except ValueError:
            pass

    return cfg
