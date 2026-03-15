"""
Layer 3 — DuckDB Writer
Persists final lead scores into DuckDB so Layer 4 can query them with SQL.

JD alignment: "unified data layer that merges structured and unstructured signals"
All layers read/write from the same DuckDB file — no scattered JSON outputs.
"""

import json
from datetime import datetime
from typing import Any

import duckdb


# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH    = "gtm_engine.duckdb"   # shared DB used across all layers
TABLE_NAME = "lead_scores"


# ── Schema ────────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    lead_id          VARCHAR PRIMARY KEY,
    company_name     VARCHAR,
    job_title        VARCHAR,
    deal_stage       VARCHAR,

    llm_score        INTEGER,
    final_score      INTEGER,
    tier             VARCHAR,

    reasoning        VARCHAR,
    top_signals      VARCHAR,   -- JSON array stored as string
    rules_fired      VARCHAR,   -- JSON array stored as string
    rule_reasons     VARCHAR,   -- JSON array stored as string
    score_delta      INTEGER,

    scored_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_llm_response VARCHAR
)
"""

UPSERT_SQL = f"""
INSERT OR REPLACE INTO {TABLE_NAME} (
    lead_id, company_name, job_title, deal_stage,
    llm_score, final_score, tier,
    reasoning, top_signals, rules_fired, rule_reasons, score_delta,
    scored_at, raw_llm_response
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# ── Connection helper ─────────────────────────────────────────────────────────

def get_connection(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection and ensure the scores table exists."""
    conn = duckdb.connect(db_path)
    conn.execute(CREATE_TABLE_SQL)
    return conn


# ── Write functions ───────────────────────────────────────────────────────────

def write_score(scored_lead: dict[str, Any], db_path: str = DB_PATH) -> None:
    """
    Upsert a single scored lead into DuckDB.

    Args:
        scored_lead: A lead dict with both CRM fields and scoring results merged in.
                     Expected keys: lead_id, final_score, tier, reasoning, etc.
        db_path:     Path to the DuckDB file (default: gtm_engine.duckdb).
    """
    conn = get_connection(db_path)
    try:
        conn.execute(UPSERT_SQL, _to_row(scored_lead))
    finally:
        conn.close()


def write_scores_batch(scored_leads: list[dict[str, Any]], db_path: str = DB_PATH) -> int:
    """
    Upsert a batch of scored leads into DuckDB in a single transaction.

    Args:
        scored_leads: List of scored lead dicts.
        db_path:      Path to the DuckDB file.

    Returns:
        Number of rows successfully written.
    """
    conn = get_connection(db_path)
    written = 0
    try:
        conn.execute("BEGIN TRANSACTION")
        for lead in scored_leads:
            try:
                conn.execute(UPSERT_SQL, _to_row(lead))
                written += 1
            except Exception as e:
                lead_id = lead.get("lead_id", "unknown")
                print(f"  Warning: failed to write lead {lead_id} — {e}")
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        raise e
    finally:
        conn.close()

    return written


# ── Read helpers (for Layer 4 to use) ────────────────────────────────────────

def read_hot_leads(min_score: int = 70, db_path: str = DB_PATH) -> list[dict]:
    """Return all leads with final_score >= min_score, sorted descending."""
    conn = get_connection(db_path)
    try:
        rows = conn.execute(f"""
            SELECT * FROM {TABLE_NAME}
            WHERE final_score >= ?
            ORDER BY final_score DESC
        """, [min_score]).fetchdf()
        return rows.to_dict(orient="records")
    finally:
        conn.close()


def read_scores_summary(db_path: str = DB_PATH) -> dict:
    """Return a quick summary of the scores table — useful for Layer 4 agents."""
    conn = get_connection(db_path)
    try:
        summary = conn.execute(f"""
            SELECT
                COUNT(*)                                    AS total_leads,
                ROUND(AVG(final_score), 1)                  AS avg_score,
                COUNT(CASE WHEN tier = 'Hot'  THEN 1 END)  AS hot_count,
                COUNT(CASE WHEN tier = 'Warm' THEN 1 END)  AS warm_count,
                COUNT(CASE WHEN tier = 'Cold' THEN 1 END)  AS cold_count,
                MAX(scored_at)                              AS last_scored_at
            FROM {TABLE_NAME}
        """).fetchone()

        return {
            "total_leads":   summary[0],
            "avg_score":     summary[1],
            "hot_count":     summary[2],
            "warm_count":    summary[3],
            "cold_count":    summary[4],
            "last_scored_at": str(summary[5]),
        }
    finally:
        conn.close()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _to_row(lead: dict[str, Any]) -> tuple:
    """Convert a scored lead dict to a tuple matching UPSERT_SQL parameter order."""
    return (
        str(lead.get("lead_id", "")),
        str(lead.get("company_name", "")),
        str(lead.get("job_title", "")),
        str(lead.get("deal_stage", "")),

        int(lead.get("score", 0) or 0),           # llm_score (raw from LLM)
        int(lead.get("final_score", 0) or 0),      # final_score (after rules)
        str(lead.get("tier", "Cold")),

        str(lead.get("reasoning", "")),
        json.dumps(lead.get("top_signals", [])),
        json.dumps(lead.get("rules_fired", [])),
        json.dumps(lead.get("rule_reasons", [])),
        int(lead.get("score_delta", 0) or 0),

        datetime.now(),
        str(lead.get("raw_llm_response", "")),
    )


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import tempfile, os

    # Use a temp DB for the test so we don't pollute the real one
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        test_db = f.name

    sample_leads = [
        {
            "lead_id": "L001", "company_name": "Acme Corp", "job_title": "VP Sales",
            "deal_stage": "Negotiation", "score": 72, "final_score": 72, "tier": "Hot",
            "reasoning": "Strong buying signal, exec sponsor confirmed.",
            "top_signals": ["budget confirmed", "demo completed"],
            "rules_fired": [], "rule_reasons": [], "score_delta": 0,
        },
        {
            "lead_id": "L002", "company_name": "Beta Inc", "job_title": "Manager",
            "deal_stage": "Closed Lost", "score": 68, "final_score": 10, "tier": "Cold",
            "reasoning": "Lost deal — competitor chosen.",
            "top_signals": ["pricing objection"],
            "rules_fired": ["closed_lost"], "rule_reasons": ["Deal is Closed Lost — score capped at 10"],
            "score_delta": -58,
        },
    ]

    written = write_scores_batch(sample_leads, db_path=test_db)
    print(f"Written: {written} rows")

    summary = read_scores_summary(db_path=test_db)
    print(f"Summary: {summary}")

    hot = read_hot_leads(min_score=50, db_path=test_db)
    print(f"Hot leads (score >= 50): {[l['lead_id'] for l in hot]}")

    os.unlink(test_db)
    print("Test passed.")
