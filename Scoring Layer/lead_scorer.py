"""
Layer 3 — Lead Scorer
Orchestrates per-lead scoring by combining structured CRM fields
from leads_final with semantic context chunks from Layer 2.
Uses Gemini 2.5 Flash as the LLM judge.
"""

import json
import os
from typing import Any
from google import genai

# Layer 2 retriever
from layer2 import get_retriever

# ── Config ────────────────────────────────────────────────────────────────────

TOP_K_CHUNKS = 5
SCORE_MODEL  = "gemini-2.5-flash"
OUTPUT_PATH  = "scored_leads.json"

SCORING_PROMPT = """You are an expert B2B sales analyst. Your job is to score a lead's readiness to buy on a scale of 0 to 100.

Use the structured CRM data and the call/transcript context below to make your assessment.

## Lead CRM Data
{crm_data}

## Relevant Context from Calls & Transcripts
{context_chunks}

## Instructions
Analyse the lead carefully. Consider:
- Buying signals (urgency, budget mentions, decision-maker involvement)
- Objections or blockers (pricing concerns, timing, competition)
- Engagement quality (number of touches, recency, tone)
- Fit signals (company size, industry, role match)

Respond ONLY in this exact JSON format — no preamble, no markdown:
{{
  "score": <integer 0-100>,
  "tier": "<Hot | Warm | Cold>",
  "reasoning": "<2-3 sentence plain English explanation>",
  "top_signals": ["<signal 1>", "<signal 2>", "<signal 3>"]
}}
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_crm(lead: dict[str, Any]) -> str:
    fields = [
        ("Lead ID",       lead.get("lead_id", "unknown")),
        ("Company",       lead.get("company_name", "—")),
        ("Industry",      lead.get("industry", "—")),
        ("Company size",  lead.get("company_size", "—")),
        ("Contact role",  lead.get("job_title", "—")),
        ("Deal stage",    lead.get("deal_stage", "—")),
        ("Last activity", lead.get("last_activity_date", "—")),
        ("Lead source",   lead.get("lead_source", "—")),
        ("Notes",         lead.get("notes", "None")),
    ]
    return "\n".join(f"  {k}: {v}" for k, v in fields)


def _format_chunks(chunks: list[str]) -> str:
    if not chunks:
        return "  No transcript or context available for this lead."
    return "\n\n".join(f"  [{i+1}] {chunk.strip()}" for i, chunk in enumerate(chunks))


# ── Core scoring function ─────────────────────────────────────────────────────

def score_lead(lead: dict[str, Any], retriever=None) -> dict[str, Any]:
    if retriever is None:
        retriever = get_retriever()

    lead_id = str(lead.get("lead_id", ""))

    # 1. Pull semantic context from Layer 2
    query  = f"{lead.get('company_name', '')} {lead.get('job_title', '')} {lead.get('notes', '')}"
    chunks = retriever.get_relevant_chunks(query=query, lead_id=lead_id, k=TOP_K_CHUNKS)

    # 2. Build the prompt
    prompt = SCORING_PROMPT.format(
        crm_data       = _format_crm(lead),
        context_chunks = _format_chunks(chunks),
    )

    # 3. Call Gemini 2.5 Flash
    gemini_client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    response      = gemini_client.models.generate_content(
        model    = SCORE_MODEL,
        contents = prompt,
    )
    raw_text = response.text.strip()

    # 4. Parse LLM response
    from score_parser import parse_score
    parsed = parse_score(raw_text)

    # 5. Apply deterministic rules on top of LLM score
    from rules_engine import apply_rules
    rules_result = apply_rules(lead, llm_score=parsed["score"])

    return {
        **lead,
        **parsed,
        **rules_result,
        "raw_llm_response": raw_text,
    }


# ── Batch runner ──────────────────────────────────────────────────────────────

def score_all_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from duckdb_writer import write_scores_batch

    retriever = get_retriever()
    scored    = []

    for i, lead in enumerate(leads):
        lead_id = lead.get("lead_id", f"index_{i}")
        print(f"  Scoring lead {i+1}/{len(leads)} — {lead_id} ...", end=" ")
        try:
            result = score_lead(lead, retriever=retriever)
            scored.append(result)
            print(f"llm={result.get('score','?')}  final={result.get('final_score','?')}  tier={result.get('tier','?')}")
        except Exception as e:
            print(f"ERROR: {e}")
            scored.append({**lead, "score": 0, "final_score": 0, "tier": "Error", "reasoning": str(e)})

    scored.sort(key=lambda x: x.get("final_score") or 0, reverse=True)

    written = write_scores_batch(scored)
    print(f"\nScored {len(scored)} leads → DuckDB ({written} rows written)")
    return scored


# ── CLI entrypoint ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    input_path = sys.argv[1] if len(sys.argv) > 1 else "leads_final.json"

    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found. Run Layer 1 first.")
        sys.exit(1)

    with open(input_path) as f:
        leads = json.load(f)

    print(f"Loaded {len(leads)} leads from {input_path}")
    score_all_leads(leads)