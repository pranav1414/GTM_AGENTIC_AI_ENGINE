"""
Layer 5 — crm_updater
Simulates a CRM update by writing a structured event record
to the crm_events table in gtm_engine.duckdb.

In production, replace _write_to_duckdb() with a POST to
your CRM's API (Salesforce, HubSpot, Pipedrive, etc.).
The payload shape is already CRM-compatible.
"""

import logging
import duckdb
import json
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(r"C:\Users\Pranav\GTM AI Engine\gtm_engine.duckdb")

# CRM stage mapping based on score bands
STAGE_MAP = [
    (80, "SQL — Sales Qualified Lead"),
    (60, "MQL — Marketing Qualified Lead"),
    (40, "Prospect"),
    (0,  "Lead"),
]


def _score_to_stage(score: int) -> str:
    for threshold, stage in STAGE_MAP:
        if score >= threshold:
            return stage
    return "Lead"


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_events (
            event_id     VARCHAR PRIMARY KEY,
            lead_id      VARCHAR NOT NULL,
            event_type   VARCHAR NOT NULL,
            stage        VARCHAR,
            score        INTEGER,
            rep          VARCHAR,
            company      VARCHAR,
            contact      VARCHAR,
            payload      JSON,
            created_at   TIMESTAMP NOT NULL
        )
    """)


def _build_payload(routing_decision: dict, stage: str) -> dict:
    """Build a CRM-compatible payload (mirrors HubSpot/Salesforce field names)."""
    return {
        "dealstage":          stage,
        "hs_lead_status":     "IN_PROGRESS",
        "lead_score":         routing_decision.get("score", 0),
        "hubspot_owner_id":   routing_decision.get("rep", "unassigned"),
        "company":            routing_decision.get("company", ""),
        "firstname":          routing_decision.get("contact", "").split()[0] if routing_decision.get("contact") else "",
        "lastname":           " ".join(routing_decision.get("contact", "").split()[1:]),
        "gtm_engine_reason":  routing_decision.get("reason", ""),
        "gtm_engine_version": "1.0",
    }


def update_crm(routing_decision: dict) -> dict:
    """
    Write a CRM event record for a scored lead.

    Args:
        routing_decision: dict from Layer 4 dispatcher

    Returns:
        dict with status, event_id, stage, and payload preview
    """
    lead_id    = routing_decision.get("lead_id", "unknown")
    score      = routing_decision.get("score", 0)
    rep        = routing_decision.get("rep", "unassigned")
    company    = routing_decision.get("company", "")
    contact    = routing_decision.get("contact", "")
    stage      = _score_to_stage(score)
    created_at = datetime.now(timezone.utc)
    event_id   = f"crm_{lead_id}_{int(created_at.timestamp())}"
    payload    = _build_payload(routing_decision, stage)

    conn = duckdb.connect(str(DB_PATH))
    try:
        _ensure_table(conn)
        conn.execute("""
            INSERT OR REPLACE INTO crm_events
                (event_id, lead_id, event_type, stage, score, rep,
                 company, contact, payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            event_id, lead_id, "stage_update", stage, score, rep,
            company, contact, json.dumps(payload), created_at,
        ])
        conn.commit()
        logger.info(f"[crm_updater] Lead {lead_id} → stage='{stage}' (score={score})")
    finally:
        conn.close()

    return {
        "status":   "ok",
        "event_id": event_id,
        "lead_id":  lead_id,
        "stage":    stage,
        "score":    score,
        "payload":  payload,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = update_crm({
        "lead_id":  "lead_0042",
        "score":    91,
        "rep":      "sarah.jones",
        "company":  "Acme Corp",
        "contact":  "John Smith",
        "reason":   "Champion identified, budget confirmed",
    })
    print(json.dumps(result, indent=2))
