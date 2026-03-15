"""
Layer 3 — Rules Engine
Deterministic guardrails that run AFTER the LLM judge.
These hard rules override or cap the probabilistic score where
business logic must always win, regardless of what the LLM decided.

JD alignment: "blending probabilistic AI reasoning with deterministic logic"
"""

from datetime import datetime, timezone
from typing import Any


# ── Rule definitions ──────────────────────────────────────────────────────────
#
# Each rule is a dict with:
#   condition : callable(lead) → bool   — when to apply this rule
#   action    : "cap" | "floor" | "set" — what to do to the score
#   value     : int                     — the target value
#   reason    : str                     — logged alongside the override
#
# Rules are evaluated in order. Multiple rules can fire on the same lead.
# "cap"   = score cannot exceed this value
# "floor" = score cannot go below this value
# "set"   = score is forced to exactly this value (strongest override)

RULES = [
    {
        "name":      "closed_lost",
        "condition": lambda lead: str(lead.get("deal_stage", "")).lower() == "closed lost",
        "action":    "cap",
        "value":     10,
        "reason":    "Deal is Closed Lost — score capped at 10",
    },
    {
        "name":      "closed_won",
        "condition": lambda lead: str(lead.get("deal_stage", "")).lower() == "closed won",
        "action":    "set",
        "value":     100,
        "reason":    "Deal is Closed Won — score set to 100",
    },
    {
        "name":      "disqualified",
        "condition": lambda lead: str(lead.get("deal_stage", "")).lower() in ("disqualified", "churned", "unqualified"),
        "action":    "set",
        "value":     0,
        "reason":    "Lead is disqualified — score set to 0",
    },
    {
        "name":      "stale_lead",
        "condition": lambda lead: _days_since(lead.get("last_activity_date")) > 180,
        "action":    "cap",
        "value":     20,
        "reason":    "No activity in 180+ days — score capped at 20",
    },
    {
        "name":      "very_stale_lead",
        "condition": lambda lead: _days_since(lead.get("last_activity_date")) > 365,
        "action":    "cap",
        "value":     5,
        "reason":    "No activity in 365+ days — score capped at 5",
    },
    {
        "name":      "no_contact_info",
        "condition": lambda lead: not lead.get("email") and not lead.get("phone"),
        "action":    "cap",
        "value":     30,
        "reason":    "No email or phone on record — score capped at 30",
    },
    {
        "name":      "executive_sponsor",
        "condition": lambda lead: any(
            title in str(lead.get("job_title", "")).lower()
            for title in ("ceo", "cto", "cro", "chief", "president", "vp", "vice president")
        ),
        "action":    "floor",
        "value":     40,
        "reason":    "Executive-level contact — score floored at 40",
    },
    {
        "name":      "contract_sent",
        "condition": lambda lead: str(lead.get("deal_stage", "")).lower() in ("contract sent", "proposal sent", "negotiation"),
        "action":    "floor",
        "value":     70,
        "reason":    "Contract or proposal sent — score floored at 70",
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _days_since(date_value: Any) -> int:
    """Return number of days since a date value. Returns 0 if unparseable."""
    if not date_value:
        return 0
    try:
        if isinstance(date_value, datetime):
            dt = date_value
        else:
            # Handle common date string formats
            for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%d/%m/%Y"):
                try:
                    dt = datetime.strptime(str(date_value)[:10], fmt[:len(str(date_value)[:10])])
                    break
                except ValueError:
                    continue
            else:
                return 0

        now = datetime.now()
        return max(0, (now - dt.replace(tzinfo=None)).days)
    except Exception:
        return 0


# ── Core apply function ───────────────────────────────────────────────────────

def apply_rules(lead: dict[str, Any], llm_score: int) -> dict[str, Any]:
    """
    Run all deterministic rules against a lead and return the final score.

    Args:
        lead:      The lead dict (CRM fields).
        llm_score: The raw score from the LLM judge (0–100).

    Returns:
        Dict with keys:
            final_score   (int)       — score after all rules applied
            rules_fired   (list[str]) — names of rules that triggered
            rule_reasons  (list[str]) — human-readable explanation per rule
            score_delta   (int)       — how much rules moved the score from LLM
    """
    score = llm_score
    rules_fired = []
    rule_reasons = []

    for rule in RULES:
        try:
            if rule["condition"](lead):
                original = score
                action   = rule["action"]
                value    = rule["value"]

                if action == "cap":
                    score = min(score, value)
                elif action == "floor":
                    score = max(score, value)
                elif action == "set":
                    score = value

                rules_fired.append(rule["name"])
                rule_reasons.append(rule["reason"])

        except Exception as e:
            # Never let a broken rule kill the whole pipeline
            rule_reasons.append(f"Rule '{rule['name']}' errored: {e}")

    # Clamp final score to valid range
    score = max(0, min(100, score))

    return {
        "final_score":  score,
        "rules_fired":  rules_fired,
        "rule_reasons": rule_reasons,
        "score_delta":  score - llm_score,
    }


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_leads = [
        {"lead_id": "L001", "deal_stage": "Closed Lost",   "last_activity_date": "2024-01-01", "job_title": "VP Sales",  "email": "a@b.com"},
        {"lead_id": "L002", "deal_stage": "Contract Sent", "last_activity_date": "2026-03-01", "job_title": "Manager",   "email": "b@b.com"},
        {"lead_id": "L003", "deal_stage": "Prospecting",   "last_activity_date": "2024-06-01", "job_title": "CEO",       "email": ""},
        {"lead_id": "L004", "deal_stage": "Closed Won",    "last_activity_date": "2026-03-10", "job_title": "Engineer",  "email": "c@b.com"},
        {"lead_id": "L005", "deal_stage": "Qualified",     "last_activity_date": "2026-02-01", "job_title": "Director",  "email": "d@b.com"},
    ]

    llm_scores = [72, 55, 45, 80, 63]

    print(f"{'Lead':<8} {'LLM':>5} {'Final':>6}  Rules fired")
    print("-" * 60)
    for lead, llm in zip(test_leads, llm_scores):
        result = apply_rules(lead, llm)
        fired  = ", ".join(result["rules_fired"]) or "none"
        print(f"{lead['lead_id']:<8} {llm:>5} {result['final_score']:>6}  {fired}")
