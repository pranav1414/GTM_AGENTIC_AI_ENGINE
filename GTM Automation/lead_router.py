"""
Layer 5 — lead_router
Assigns a scored lead to a sales rep and writes the assignment
to the rep_assignments table in gtm_engine.duckdb.
"""

import logging
import duckdb
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(r"C:\Users\Pranav\GTM AI Engine\gtm_engine.duckdb")

# Simple round-robin / rules-based rep pool
# In production this would query a capacity/territory table
REP_POOL = {
    "high":   ["sarah.jones", "mike.chen"],
    "medium": ["lisa.patel", "tom.garcia"],
    "low":    ["inbound-queue"],
}


def _get_rep(routing_decision: dict) -> str:
    """
    Determine the rep to assign.
    Uses the rep field from Layer 4 if present, otherwise falls back
    to the pool based on priority.
    """
    if routing_decision.get("rep"):
        return routing_decision["rep"]

    priority = routing_decision.get("priority", "low")
    pool = REP_POOL.get(priority, REP_POOL["low"])
    # Deterministic assignment: hash lead_id against pool size
    lead_id = routing_decision.get("lead_id", "")
    idx = abs(hash(lead_id)) % len(pool)
    return pool[idx]


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rep_assignments (
            id            VARCHAR PRIMARY KEY,
            lead_id       VARCHAR NOT NULL,
            rep           VARCHAR NOT NULL,
            score         INTEGER,
            priority      VARCHAR,
            reason        TEXT,
            assigned_at   TIMESTAMP NOT NULL
        )
    """)


def assign_lead(routing_decision: dict) -> dict:
    """
    Assign a lead to a rep and persist the assignment.

    Args:
        routing_decision: dict from Layer 4 dispatcher

    Returns:
        dict with status, lead_id, rep, and assignment_id
    """
    lead_id  = routing_decision.get("lead_id", "unknown")
    score    = routing_decision.get("score", 0)
    priority = routing_decision.get("priority", "low")
    reason   = routing_decision.get("reason", "")
    rep      = _get_rep(routing_decision)

    assigned_at   = datetime.now(timezone.utc)
    assignment_id = f"asgn_{lead_id}_{int(assigned_at.timestamp())}"

    conn = duckdb.connect(str(DB_PATH))
    try:
        _ensure_table(conn)
        conn.execute("""
            INSERT OR REPLACE INTO rep_assignments
                (id, lead_id, rep, score, priority, reason, assigned_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [assignment_id, lead_id, rep, score, priority, reason, assigned_at])
        conn.commit()
        logger.info(f"[lead_router] Lead {lead_id} → {rep} (score={score}, priority={priority})")
    finally:
        conn.close()

    return {
        "status":        "ok",
        "assignment_id": assignment_id,
        "lead_id":       lead_id,
        "rep":           rep,
        "score":         score,
        "priority":      priority,
        "assigned_at":   assigned_at.isoformat(),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = assign_lead({
        "lead_id":  "lead_0042",
        "score":    91,
        "priority": "high",
        "reason":   "Champion identified, budget confirmed",
    })
    import json; print(json.dumps(result, indent=2))
