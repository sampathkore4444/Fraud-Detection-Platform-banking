"""
Fraud Detection Admin Portal — FastAPI Backend

Endpoints:
- GET  /api/stats              — Real-time dashboard stats
- GET  /api/decisions          — List decisions (paginated, filterable)
- GET  /api/decisions/{id}     — Get decision detail
- PUT  /api/decisions/{id}     — Update decision (approve/decline/assign)
- GET  /api/review-queue       — Get REVIEW queue for analysts
- PUT  /api/review-queue/{id}  — Analyst action (approve/decline with notes)
- GET  /api/models             — List model versions
- GET  /api/models/{version}   — Get model detail + metrics
- POST /api/models/activate    — Activate a model version
- GET  /api/rules              — List fraud rules
- POST /api/rules              — Create new rule
- PUT  /api/rules/{id}         — Update rule
- PUT  /api/rules/{id}/toggle  — Enable/disable rule
- GET  /api/audit              — Search audit trail
- GET  /api/cases              — List investigation cases
- POST /api/cases              — Create case from decision
- PUT  /api/cases/{id}         — Update case status/notes
"""

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, List
from enum import Enum

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import redis

# ─── App Setup ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Fraud Detection Admin Portal",
    version="1.0.0",
    description="Admin interface for fraud detection platform",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis connection
redis_client = redis.Redis(
    host="localhost", port=6379, decode_responses=True,
    socket_connect_timeout=5, socket_timeout=5,
)

# ─── Models ───────────────────────────────────────────────────────────────────

class DecisionAction(str, Enum):
    APPROVE = "APPROVE"
    DECLINE = "DECLINE"
    ESCALATE = "ESCALATE"

class ReviewAction(str, Enum):
    APPROVE = "APPROVE"
    DECLINE = "DECLINE"
    ESCALATE = "ESCALATE"

class CaseStatus(str, Enum):
    OPEN = "OPEN"
    INVESTIGATING = "INVESTIGATING"
    RESOLVED_FRAUD = "RESOLVED_FRAUD"
    RESOLVED_LEGITIMATE = "RESOLVED_LEGITIMATE"
    CLOSED = "CLOSED"

# ─── Request/Response Schemas ────────────────────────────────────────────────

class DecisionUpdate(BaseModel):
    action: DecisionAction
    analyst_id: Optional[str] = None
    notes: Optional[str] = None

class ReviewAction(BaseModel):
    action: ReviewAction
    analyst_id: str
    notes: str = ""

class RuleCreate(BaseModel):
    name: str
    description: str = ""
    rule_type: str  # velocity, geo, amount, device, custom
    condition: dict  # {"field": "velocity_tx_count_1h", "operator": ">", "value": 10}
    severity: int = 1  # 1=review, 2=decline
    enabled: bool = True

class RuleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    condition: Optional[dict] = None
    severity: Optional[int] = None
    enabled: Optional[bool] = None

class CaseCreate(BaseModel):
    decision_id: str
    analyst_id: str
    notes: str = ""

class CaseUpdate(BaseModel):
    status: Optional[CaseStatus] = None
    notes: Optional[str] = None
    analyst_id: Optional[str] = None

class ModelActivate(BaseModel):
    version: str

# ─── Dashboard Stats ─────────────────────────────────────────────────────────

@app.get("/api/stats")
def get_stats():
    """Real-time dashboard statistics."""
    now = time.time()
    hour_ago = now - 3600

    # Count decisions from Redis
    decisions = _get_all_decisions()
    recent = [d for d in decisions if d.get("timestamp_ms", 0) / 1000 > hour_ago]

    total = len(recent)
    approved = sum(1 for d in recent if d.get("decision") == "APPROVE")
    reviewed = sum(1 for d in recent if d.get("decision") == "REVIEW")
    declined = sum(1 for d in recent if d.get("decision") == "DECLINE")

    # Latency stats
    latencies = [d.get("latency_ms", 0) for d in recent if d.get("latency_ms")]
    avg_latency = sum(latencies) / max(len(latencies), 1)
    p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0

    # Fraud rate
    fraud_rate = declined / max(total, 1)

    # Review queue size
    review_queue = [d for d in decisions if d.get("decision") == "REVIEW"]

    # Active model
    active_model = redis_client.get("active_model_version") or "v1.0.0"

    # Open cases
    cases = _get_all_cases()
    open_cases = sum(1 for c in cases if c.get("status") in ("OPEN", "INVESTIGATING"))

    return {
        "period": "1h",
        "total_transactions": total,
        "approved": approved,
        "reviewed": reviewed,
        "declined": declined,
        "fraud_rate": round(fraud_rate, 4),
        "avg_latency_ms": round(avg_latency, 2),
        "p95_latency_ms": p95_latency,
        "review_queue_size": len(review_queue),
        "active_model": active_model,
        "open_cases": open_cases,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ─── Decisions ────────────────────────────────────────────────────────────────

@app.get("/api/decisions")
def list_decisions(
    decision: Optional[str] = Query(None, description="Filter by decision (APPROVE/REVIEW/DECLINE)"),
    account_id: Optional[str] = Query(None, description="Filter by account ID"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List decisions with pagination and filters."""
    decisions = _get_all_decisions()

    if decision:
        decisions = [d for d in decisions if d.get("decision") == decision]
    if account_id:
        decisions = [d for d in decisions if d.get("account_id") == account_id]

    # Sort by timestamp descending
    decisions.sort(key=lambda d: d.get("timestamp_ms", 0), reverse=True)

    total = len(decisions)
    page = decisions[offset:offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "decisions": page,
    }

@app.get("/api/decisions/{tx_id}")
def get_decision(tx_id: str):
    """Get decision detail by transaction ID."""
    key = f"decision:{tx_id}"
    data = redis_client.get(key)
    if not data:
        raise HTTPException(status_code=404, detail=f"Decision not found: {tx_id}")
    return json.loads(data)

@app.put("/api/decisions/{tx_id}")
def update_decision(tx_id: str, update: DecisionUpdate):
    """Update a decision (analyst override)."""
    key = f"decision:{tx_id}"
    data = redis_client.get(key)
    if not data:
        raise HTTPException(status_code=404, detail=f"Decision not found: {tx_id}")

    decision = json.loads(data)
    decision["original_decision"] = decision.get("decision")
    decision["decision"] = update.action.value
    decision["analyst_override"] = True
    decision["analyst_id"] = update.analyst_id
    decision["analyst_notes"] = update.notes
    decision["override_timestamp_ms"] = int(time.time() * 1000)

    redis_client.setex(key, 86400, json.dumps(decision))

    # Log to audit trail
    _log_audit("decision_override", {
        "tx_id": tx_id,
        "original": decision.get("original_decision"),
        "new": update.action.value,
        "analyst_id": update.analyst_id,
        "notes": update.notes,
    })

    return decision

# ─── Review Queue ─────────────────────────────────────────────────────────────

@app.get("/api/review-queue")
def get_review_queue(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Get REVIEW queue for analysts."""
    decisions = _get_all_decisions()
    review_items = [d for d in decisions if d.get("decision") == "REVIEW"]

    # Sort by fraud probability descending (highest risk first)
    review_items.sort(key=lambda d: d.get("fraud_probability", 0), reverse=True)

    total = len(review_items)
    page = review_items[offset:offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "queue": page,
    }

@app.put("/api/review-queue/{tx_id}")
def review_decision(tx_id: str, action: ReviewAction):
    """Analyst action on a REVIEW decision."""
    key = f"decision:{tx_id}"
    data = redis_client.get(key)
    if not data:
        raise HTTPException(status_code=404, detail=f"Decision not found: {tx_id}")

    decision = json.loads(data)
    if decision.get("decision") != "REVIEW":
        raise HTTPException(status_code=400, detail=f"Decision is not REVIEW: {decision.get('decision')}")

    decision["decision"] = action.action.value
    decision["reviewed_by"] = action.analyst_id
    decision["review_notes"] = action.notes
    decision["reviewed_at_ms"] = int(time.time() * 1000)

    redis_client.setex(key, 86400, json.dumps(decision))

    _log_audit("review_action", {
        "tx_id": tx_id,
        "action": action.action.value,
        "analyst_id": action.analyst_id,
        "notes": action.notes,
    })

    return decision

# ─── Models ───────────────────────────────────────────────────────────────────

@app.get("/api/models")
def list_models():
    """List all model versions."""
    active = redis_client.get("active_model_version") or "v1.0.0"

    # Get model metadata from Redis or filesystem
    models = []
    model_keys = redis_client.keys("model:*")
    for key in model_keys:
        data = redis_client.get(key)
        if data:
            models.append(json.loads(data))

    # If no models in Redis, return defaults
    if not models:
        models = [
            {
                "version": "v1.0.0",
                "status": "active" if active == "v1.0.0" else "inactive",
                "trained_at": "2026-08-28T00:00:00Z",
                "metrics": {"auc_roc": 1.0, "precision": 1.0, "recall": 1.0, "f1": 1.0},
                "num_trees": 500,
                "num_features": 30,
            },
        ]

    return {"active_version": active, "models": models}

@app.get("/api/models/{version}")
def get_model(version: str):
    """Get model detail."""
    key = f"model:{version}"
    data = redis_client.get(key)
    if data:
        return json.loads(data)

    if version == "v1.0.0":
        return {
            "version": "v1.0.0",
            "status": "active",
            "trained_at": "2026-08-28T00:00:00Z",
            "metrics": {"auc_roc": 1.0, "precision": 1.0, "recall": 1.0, "f1": 1.0},
            "num_trees": 500,
            "num_features": 30,
            "feature_importance": {
                "velocity_tx_count_1h": 0.79,
                "behavioral_country_change_freq": 0.14,
                "behavioral_amount_zscore": 0.02,
            },
        }

    raise HTTPException(status_code=404, detail=f"Model not found: {version}")

@app.post("/api/models/activate")
def activate_model(req: ModelActivate):
    """Activate a model version."""
    redis_client.set("active_model_version", req.version)
    _log_audit("model_activated", {"version": req.version})
    return {"status": "ok", "active_version": req.version}

# ─── Rules ────────────────────────────────────────────────────────────────────

@app.get("/api/rules")
def list_rules():
    """List all fraud rules."""
    rules = _get_all_rules()
    if not rules:
        # Return default rules
        rules = _default_rules()
        # Store them
        redis_client.set("rules", json.dumps(rules))

    return {"rules": rules}

@app.post("/api/rules")
def create_rule(rule: RuleCreate):
    """Create a new fraud rule."""
    rules = _get_all_rules()
    new_rule = {
        "id": str(uuid.uuid4())[:8],
        "name": rule.name,
        "description": rule.description,
        "rule_type": rule.rule_type,
        "condition": rule.condition,
        "severity": rule.severity,
        "enabled": rule.enabled,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": "admin",
    }
    rules.append(new_rule)
    redis_client.set("rules", json.dumps(rules))

    _log_audit("rule_created", {"rule_id": new_rule["id"], "name": rule.name})
    return new_rule

@app.put("/api/rules/{rule_id}")
def update_rule(rule_id: str, update: RuleUpdate):
    """Update a fraud rule."""
    rules = _get_all_rules()
    for rule in rules:
        if rule["id"] == rule_id:
            if update.name is not None:
                rule["name"] = update.name
            if update.description is not None:
                rule["description"] = update.description
            if update.condition is not None:
                rule["condition"] = update.condition
            if update.severity is not None:
                rule["severity"] = update.severity
            if update.enabled is not None:
                rule["enabled"] = update.enabled
            rule["updated_at"] = datetime.now(timezone.utc).isoformat()

            redis_client.set("rules", json.dumps(rules))
            _log_audit("rule_updated", {"rule_id": rule_id})
            return rule

    raise HTTPException(status_code=404, detail=f"Rule not found: {rule_id}")

@app.put("/api/rules/{rule_id}/toggle")
def toggle_rule(rule_id: str):
    """Toggle rule enabled/disabled."""
    rules = _get_all_rules()
    for rule in rules:
        if rule["id"] == rule_id:
            rule["enabled"] = not rule["enabled"]
            rule["updated_at"] = datetime.now(timezone.utc).isoformat()
            redis_client.set("rules", json.dumps(rules))
            _log_audit("rule_toggled", {"rule_id": rule_id, "enabled": rule["enabled"]})
            return rule

    raise HTTPException(status_code=404, detail=f"Rule not found: {rule_id}")

# ─── Audit Trail ─────────────────────────────────────────────────────────────

@app.get("/api/audit")
def list_audit(
    action: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Search audit trail."""
    audits = _get_all_audits()

    if action:
        audits = [a for a in audits if a.get("action") == action]

    # Sort by timestamp descending
    audits.sort(key=lambda a: a.get("timestamp_ms", 0), reverse=True)

    total = len(audits)
    page = audits[offset:offset + limit]

    return {"total": total, "offset": offset, "limit": limit, "audit_trail": page}

# ─── Cases ────────────────────────────────────────────────────────────────────

@app.get("/api/cases")
def list_cases(
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List investigation cases."""
    cases = _get_all_cases()

    if status:
        cases = [c for c in cases if c.get("status") == status]

    cases.sort(key=lambda c: c.get("created_at", ""), reverse=True)

    total = len(cases)
    page = cases[offset:offset + limit]

    return {"total": total, "offset": offset, "limit": limit, "cases": page}

@app.post("/api/cases")
def create_case(case: CaseCreate):
    """Create investigation case from a decision."""
    # Verify decision exists
    key = f"decision:{case.decision_id}"
    data = redis_client.get(key)
    if not data:
        raise HTTPException(status_code=404, detail=f"Decision not found: {case.decision_id}")

    decision = json.loads(data)
    case_id = f"CASE-{str(uuid.uuid4())[:8]}"

    new_case = {
        "case_id": case_id,
        "decision_id": case.decision_id,
        "account_id": decision.get("account_id", ""),
        "fraud_probability": decision.get("fraud_probability", 0),
        "reason_code": decision.get("reason_code", ""),
        "status": "OPEN",
        "analyst_id": case.analyst_id,
        "notes": [case.notes] if case.notes else [],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Store case
    cases = _get_all_cases()
    cases.append(new_case)
    redis_client.set("cases", json.dumps(cases))

    _log_audit("case_created", {"case_id": case_id, "tx_id": case.decision_id})
    return new_case

@app.put("/api/cases/{case_id}")
def update_case(case_id: str, update: CaseUpdate):
    """Update case status/notes."""
    cases = _get_all_cases()
    for case in cases:
        if case["case_id"] == case_id:
            if update.status is not None:
                case["status"] = update.status.value
            if update.notes is not None:
                case["notes"].append(update.notes)
            if update.analyst_id is not None:
                case["analyst_id"] = update.analyst_id
            case["updated_at"] = datetime.now(timezone.utc).isoformat()

            redis_client.set("cases", json.dumps(cases))
            _log_audit("case_updated", {"case_id": case_id, "status": case.get("status")})
            return case

    raise HTTPException(status_code=404, detail=f"Case not found: {case_id}")

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_all_decisions() -> list:
    """Get all decisions from Redis."""
    decisions = []
    keys = redis_client.keys("decision:*")
    for key in keys:
        if key.startswith("decision:feature_vector:") or key.startswith("decision:case"):
            continue
        data = redis_client.get(key)
        if data:
            try:
                decisions.append(json.loads(data))
            except json.JSONDecodeError:
                pass
    return decisions

def _get_all_rules() -> list:
    """Get all rules from Redis."""
    data = redis_client.get("rules")
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            pass
    return []

def _get_all_audits() -> list:
    """Get all audit entries from Redis."""
    data = redis_client.get("audit_trail")
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            pass
    return []

def _get_all_cases() -> list:
    """Get all cases from Redis."""
    data = redis_client.get("cases")
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            pass
    return []

def _log_audit(action: str, details: dict):
    """Append to audit trail."""
    audits = _get_all_audits()
    entry = {
        "id": str(uuid.uuid4())[:8],
        "action": action,
        "details": details,
        "timestamp_ms": int(time.time() * 1000),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    audits.append(entry)
    # Keep last 10000 entries
    if len(audits) > 10000:
        audits = audits[-10000:]
    redis_client.set("audit_trail", json.dumps(audits))

def _default_rules() -> list:
    """Default fraud rules matching Go rules engine."""
    return [
        {
            "id": "rule-001",
            "name": "blocked_country",
            "description": "Block transactions from sanctioned countries",
            "rule_type": "geo",
            "condition": {"field": "country_code", "operator": "in", "value": ["XX", "YY"]},
            "severity": 2,
            "enabled": True,
        },
        {
            "id": "rule-002",
            "name": "velocity_burst",
            "description": "Flag excessive transactions in 1 hour",
            "rule_type": "velocity",
            "condition": {"field": "velocity_tx_count_1h", "operator": ">", "value": 20},
            "severity": 2,
            "enabled": True,
        },
        {
            "id": "rule-003",
            "name": "impossible_travel",
            "description": "Multiple countries in 1 hour",
            "rule_type": "geo",
            "condition": {"field": "velocity_unique_countries_1h", "operator": ">", "value": 3},
            "severity": 2,
            "enabled": True,
        },
        {
            "id": "rule-004",
            "name": "amount_anomaly",
            "description": "Transaction amount exceeds threshold",
            "rule_type": "amount",
            "condition": {"field": "velocity_amount_sum_1h", "operator": ">", "value": 50000},
            "severity": 2,
            "enabled": True,
        },
        {
            "id": "rule-005",
            "name": "emulator_detected",
            "description": "Known emulator fingerprint",
            "rule_type": "device",
            "condition": {"field": "device_is_emulator_detected", "operator": "==", "value": 1},
            "severity": 2,
            "enabled": True,
        },
        {
            "id": "rule-006",
            "name": "device_multi_account",
            "description": "Too many accounts on same device",
            "rule_type": "device",
            "condition": {"field": "device_unique_accounts_24h", "operator": ">", "value": 5},
            "severity": 2,
            "enabled": True,
        },
    ]

# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    """Health check."""
    redis_ok = False
    try:
        redis_ok = redis_client.ping()
    except Exception:
        pass

    return {
        "status": "healthy" if redis_ok else "degraded",
        "redis": "connected" if redis_ok else "disconnected",
        "version": "1.0.0",
    }

# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
