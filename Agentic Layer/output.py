import json
from pathlib import Path

REQUIRED_FIELDS = {"lead_id", "company_name", "score", "priority", "urgency", "rep_tier", "reason"}
VALID_PRIORITIES = {"high", "medium", "low"}
VALID_URGENCIES = {"immediate", "within_48h", "nurture"}
VALID_REP_TIERS = {"senior_rep", "mid_rep", "sdr"}


def validate_decision(decision: dict) -> tuple[bool, str]:
    """Validate a single routing decision dict. Returns (is_valid, error_message)."""
    missing = REQUIRED_FIELDS - set(decision.keys())
    if missing:
        return False, f"Missing fields: {missing}"

    if decision["priority"] not in VALID_PRIORITIES:
        return False, f"Invalid priority: {decision['priority']}"

    if decision["urgency"] not in VALID_URGENCIES:
        return False, f"Invalid urgency: {decision['urgency']}"

    if decision["rep_tier"] not in VALID_REP_TIERS:
        return False, f"Invalid rep_tier: {decision['rep_tier']}"

    if not isinstance(decision["score"], (int, float)):
        return False, f"Score must be numeric, got: {type(decision['score'])}"

    return True, ""


def save_routing_decisions(decisions: list[dict], output_path: str):
    """Validate all decisions and write to JSON file."""
    valid = []
    invalid = []

    for d in decisions:
        is_valid, error = validate_decision(d)
        if is_valid:
            valid.append(d)
        else:
            print(f"  [INVALID] lead_id={d.get('lead_id', '?')} — {error}")
            invalid.append({"raw": d, "error": error})

    output = {
        "total": len(decisions),
        "valid": len(valid),
        "invalid": len(invalid),
        "decisions": valid,
    }

    Path(output_path).write_text(json.dumps(output, indent=2))

    if invalid:
        invalid_path = output_path.replace(".json", "_invalid.json")
        Path(invalid_path).write_text(json.dumps(invalid, indent=2))
        print(f"  {len(invalid)} invalid decisions written to {invalid_path}")

    print(f"  {len(valid)} valid decisions written to {output_path}")
