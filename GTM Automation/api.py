"""
Layer 5 — api
FastAPI application that exposes all 5 layers of the GTM engine
as HTTP endpoints.

Run locally:
    uvicorn layer5.api:app --reload --port 8000

Swagger UI auto-generated at:
    http://localhost:8000/docs
"""

import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

import duckdb
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from .dispatcher import dispatch
from .lead_router import assign_lead
from .crm_updater import update_crm
from .slack_alert import send_alert

logger = logging.getLogger(__name__)

DB_PATH = Path(r"C:\Users\Pranav\GTM AI Engine\gtm_engine.duckdb")

app = FastAPI(
    title="GTM Engine API",
    description="Autonomous GTM revenue engine — 5-layer architecture",
    version="1.0.0",
)


# ── Pydantic models ────────────────────────────────────────────────────────────

class RoutingDecision(BaseModel):
    lead_id:     str                      = Field(...,  example="lead_0042")
    score:       int                      = Field(...,  ge=0, le=100, example=91)
    action_type: Union[str, list[str]]    = Field(...,  example=["assign", "update_crm", "alert"])
    rep:         Optional[str]            = Field(None, example="sarah.jones")
    priority:    Optional[str]            = Field(None, example="high")
    reason:      Optional[str]            = Field(None, example="Champion identified")
    company:     Optional[str]            = Field(None, example="Acme Corp")
    contact:     Optional[str]            = Field(None, example="John Smith")


class ScoreRequest(BaseModel):
    lead_id: str = Field(..., example="lead_0042")


class WebhookPayload(BaseModel):
    event: str              = Field(...,  example="lead.scored")
    data:  dict[str, Any]   = Field(...,  example={
        "lead_id": "lead_0042",
        "score": 91,
        "action_type": ["assign", "update_crm", "alert"],
        "rep": "sarah.jones",
        "priority": "high",
        "reason": "Champion identified",
        "company": "Acme Corp",
        "contact": "John Smith",
    })


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["system"])
def health():
    """Liveness check."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ── Layer 5: GTM automation ────────────────────────────────────────────────────

@app.post("/dispatch", tags=["layer5"])
def dispatch_action(decision: RoutingDecision):
    """
    Dispatch a routing decision to all relevant action handlers.
    This is the main Layer 5 entry point.
    """
    try:
        result = dispatch(decision.model_dump())
        return result
    except Exception as e:
        logger.error(f"[api] dispatch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/assign", tags=["layer5"])
def assign(decision: RoutingDecision):
    """Assign a lead to a rep and write to rep_assignments."""
    try:
        return assign_lead(decision.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/crm-update", tags=["layer5"])
def crm_update(decision: RoutingDecision):
    """Write a CRM stage-update event to crm_events."""
    try:
        return update_crm(decision.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/alert", tags=["layer5"])
def alert(decision: RoutingDecision):
    """Fire a Slack alert for a high-scoring lead (mock or live)."""
    try:
        return send_alert(decision.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── n8n webhook ────────────────────────────────────────────────────────────────

@app.post("/webhook", tags=["layer5"])
async def n8n_webhook(
    payload: WebhookPayload,
    request: Request,
    x_n8n_signature: Optional[str] = Header(None, alias="X-N8N-Signature"),
):
    """
    n8n-compatible webhook endpoint.

    Expects a JSON body of the form:
        { "event": "<event_name>", "data": { ...RoutingDecision fields... } }

    HMAC verification:
        If the WEBHOOK_SECRET environment variable is set, the request MUST
        include an X-N8N-Signature header of the form:
            sha256=<hex_digest>
        computed as HMAC-SHA256(secret, raw_request_body).
        Requests that fail verification are rejected with HTTP 401.
    """
    # ── HMAC signature verification ────────────────────────────────────────────
    secret = os.getenv("WEBHOOK_SECRET")
    if secret:
        if not x_n8n_signature:
            raise HTTPException(
                status_code=401,
                detail="Missing X-N8N-Signature header.",
            )

        raw_body = await request.body()
        expected_digest = hmac.new(
            secret.encode(),
            raw_body,
            hashlib.sha256,
        ).hexdigest()
        expected_header = f"sha256={expected_digest}"

        if not hmac.compare_digest(expected_header, x_n8n_signature):
            raise HTTPException(
                status_code=401,
                detail="Invalid X-N8N-Signature — HMAC verification failed.",
            )

    # ── Map payload.data onto RoutingDecision ──────────────────────────────────
    try:
        decision = RoutingDecision(**payload.data)
    except Exception as e:
        raise HTTPException(
            status_code=422,
            detail=f"payload.data could not be mapped to a RoutingDecision: {e}",
        )

    # ── Dispatch (identical to POST /dispatch) ─────────────────────────────────
    try:
        result = dispatch(decision.model_dump())
        return {
            "event":    payload.event,
            "lead_id":  decision.lead_id,
            "dispatch": result,
        }
    except Exception as e:
        logger.error(f"[api] webhook dispatch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Layer 4: agentic decisions ─────────────────────────────────────────────────

@app.get("/leads/decisions", tags=["layer4"])
def get_decisions(limit: int = 50):
    """
    Fetch the latest agentic routing decisions from Layer 4.
    Reads from the agent_decisions table in gtm_engine.duckdb.
    """
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        rows = conn.execute(f"""
            SELECT * FROM agent_decisions
            ORDER BY created_at DESC
            LIMIT {limit}
        """).fetchdf()
        conn.close()
        return rows.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Layer 3: scores ────────────────────────────────────────────────────────────

@app.get("/leads/scores", tags=["layer3"])
def get_scores(limit: int = 50, min_score: int = 0):
    """
    Fetch lead scores from Layer 3.
    Reads from the lead_scores table in gtm_engine.duckdb.
    """
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        rows = conn.execute(f"""
            SELECT * FROM lead_scores
            WHERE score >= {min_score}
            ORDER BY score DESC
            LIMIT {limit}
        """).fetchdf()
        conn.close()
        return rows.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/leads/{lead_id}/score", tags=["layer3"])
def get_lead_score(lead_id: str):
    """Fetch the score for a specific lead."""
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        row = conn.execute("""
            SELECT * FROM lead_scores WHERE lead_id = ? LIMIT 1
        """, [lead_id]).fetchdf()
        conn.close()
        if row.empty:
            raise HTTPException(status_code=404, detail=f"Lead {lead_id} not found")
        return row.to_dict(orient="records")[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Layer 1: raw leads ─────────────────────────────────────────────────────────

@app.get("/leads", tags=["layer1"])
def get_leads(limit: int = 50):
    """
    Fetch cleaned leads from Layer 1.
    Reads from leads_final in gtm_engine.duckdb.
    """
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        rows = conn.execute(f"""
            SELECT * FROM leads_final
            ORDER BY created_at DESC
            LIMIT {limit}
        """).fetchdf()
        conn.close()
        return rows.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/leads/{lead_id}", tags=["layer1"])
def get_lead(lead_id: str):
    """Fetch a single lead by ID."""
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        row = conn.execute("""
            SELECT * FROM leads_final WHERE lead_id = ? LIMIT 1
        """, [lead_id]).fetchdf()
        conn.close()
        if row.empty:
            raise HTTPException(status_code=404, detail=f"Lead {lead_id} not found")
        return row.to_dict(orient="records")[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Layer 5: results ───────────────────────────────────────────────────────────

@app.get("/assignments", tags=["layer5"])
def get_assignments(limit: int = 50):
    """Fetch recent rep assignments from Layer 5."""
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        rows = conn.execute(f"""
            SELECT * FROM rep_assignments
            ORDER BY assigned_at DESC
            LIMIT {limit}
        """).fetchdf()
        conn.close()
        return rows.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/crm-events", tags=["layer5"])
def get_crm_events(limit: int = 50):
    """Fetch recent CRM events from Layer 5."""
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        rows = conn.execute(f"""
            SELECT * FROM crm_events
            ORDER BY created_at DESC
            LIMIT {limit}
        """).fetchdf()
        conn.close()
        return rows.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Run directly ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("layer5.api:app", host="0.0.0.0", port=8000, reload=True)