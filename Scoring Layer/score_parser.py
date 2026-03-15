"""
Layer 3 — Score Parser
Safely extracts the structured score payload from the LLM judge's response.
Handles malformed JSON and missing fields gracefully so the batch runner
never dies on a single bad response.
"""

import json
import re


# ── Tier thresholds (must match the prompt's definition) ──────────────────────

def _derive_tier(score: int) -> str:
    if score >= 70:
        return "Hot"
    elif score >= 40:
        return "Warm"
    return "Cold"


# ── Main parser ───────────────────────────────────────────────────────────────

def parse_score(raw_text: str) -> dict:
    """
    Parse the LLM judge's raw text response into a clean score dict.

    Expected LLM output format:
        {
          "score": 82,
          "tier": "Hot",
          "reasoning": "...",
          "top_signals": ["...", "...", "..."]
        }

    Args:
        raw_text: The raw string returned by the Claude API.

    Returns:
        A dict with keys: score (int), tier (str), reasoning (str),
        top_signals (list[str]). Falls back to safe defaults on any error.
    """
    # Strip markdown code fences if the LLM wrapped the JSON
    cleaned = re.sub(r"```(?:json)?|```", "", raw_text).strip()

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        # Last resort: try to extract just the JSON object with a regex
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group())
            except json.JSONDecodeError:
                return _fallback(raw_text, reason="JSON parse failed after regex extraction")
        else:
            return _fallback(raw_text, reason="No JSON object found in response")

    # Validate and coerce fields
    score = data.get("score")
    if not isinstance(score, (int, float)):
        # Try to parse a numeric string
        try:
            score = int(str(score).strip())
        except (ValueError, TypeError):
            return _fallback(raw_text, reason=f"Invalid score value: {score!r}")

    score = max(0, min(100, int(score)))   # clamp to 0–100

    tier = data.get("tier", _derive_tier(score))
    if tier not in ("Hot", "Warm", "Cold"):
        tier = _derive_tier(score)

    reasoning = str(data.get("reasoning", "")).strip() or "No reasoning provided."

    top_signals = data.get("top_signals", [])
    if not isinstance(top_signals, list):
        top_signals = []
    top_signals = [str(s).strip() for s in top_signals if s][:5]  # cap at 5

    return {
        "score":       score,
        "tier":        tier,
        "reasoning":   reasoning,
        "top_signals": top_signals,
        "parse_error": None,
    }


# ── Fallback ──────────────────────────────────────────────────────────────────

def _fallback(raw_text: str, reason: str = "Unknown parse error") -> dict:
    """Return a safe default when parsing fails, preserving the raw text for debugging."""
    return {
        "score":       0,
        "tier":        "Cold",
        "reasoning":   f"Score could not be parsed: {reason}",
        "top_signals": [],
        "parse_error": reason,
    }


# ── Quick test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    samples = [
        # Clean response
        '{"score": 85, "tier": "Hot", "reasoning": "Strong buying signal.", "top_signals": ["budget confirmed", "demo booked"]}',
        # With markdown fences
        '```json\n{"score": 52, "tier": "Warm", "reasoning": "Moderate fit.", "top_signals": ["replied to email"]}\n```',
        # Missing tier — should derive it
        '{"score": 30, "reasoning": "Low engagement.", "top_signals": []}',
        # Broken JSON
        'score is 75 because they mentioned budget approval next quarter',
    ]

    for s in samples:
        result = parse_score(s)
        print(result)
