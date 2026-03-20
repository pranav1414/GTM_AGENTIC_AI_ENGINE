import duckdb
import time
import json
import re
from crewai import Crew, Task, Process
from layer4.agents import build_agents
from layer4.output import save_routing_decisions

DB_PATH = r"C:\Users\Pranav\GTM AI Engine\gtm_engine.duckdb"
OUTPUT_PATH = "./routing_decisions.json"


def load_scored_leads(min_score: int = 0) -> list[dict]:
    """Load scored leads from DuckDB. Optionally filter by minimum score."""
    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute(
        f"""
        SELECT lead_id, company_name, score, rep_notes
        FROM leads_scored
        WHERE score >= {min_score}
        ORDER BY score DESC
        """
    ).fetchall()
    con.close()

    return [
        {
            "lead_id": row[0],
            "company_name": row[1],
            "score": row[2],
            "rep_notes": row[3] or "",
        }
        for row in rows
    ]


def sanitize_text(text: str) -> str:
    """
    Remove characters that break Groq function-call JSON parsing.
    Strips double quotes, curly braces, and angle brackets from free text.
    """
    if not text:
        return ""
    # Replace double quotes with single quotes
    text = text.replace('"', "'")
    # Remove curly braces — they confuse JSON parsers
    text = text.replace("{", "(").replace("}", ")")
    # Remove angle brackets
    text = text.replace("<", "").replace(">", "")
    # Collapse multiple spaces/newlines
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_tasks(lead: dict, lead_analyst, decision_agent, formatter) -> list[Task]:
    """Build the three sequential tasks for a single lead."""

    # Sanitize rep_notes before embedding in task description
    safe_notes = sanitize_text(lead.get("rep_notes", ""))
    safe_company = sanitize_text(lead.get("company_name", ""))

    task1 = Task(
    description=(
        f"You are analysing lead {lead['lead_id']} from {safe_company} with a score of {lead['score']}. "
        f"Background: {safe_notes}. "
        f"Use the ChromaDB retriever tool with lead_id='{lead['lead_id']}' to check for transcript context. "
        f"Then write ONE paragraph of 3-5 sentences summarising what matters about this lead. "
        f"STRICT RULES: Write only plain sentences. No bullet points. No labels like Score or Rep notes. "
        f"No colons followed by values. No double quotes. No curly braces. Just plain flowing sentences."
    ),
    expected_output=(
        "One plain paragraph of 3-5 sentences with no labels, no colons, no quotes, "
        "no special characters. Just flowing plain text sentences."
    ),
    agent=lead_analyst,
)

    task2 = Task(
        description=(
            f"Using the analyst summary from Task 1, apply the GTM rules engine "
            f"with score={lead['score']}. "
            f"Pass the summary as context_summary to the rules engine tool. "
            f"IMPORTANT: The context_summary must be plain text only — no double quotes, "
            f"no curly braces, no special characters. "
            f"Then state clearly: priority level, urgency, rep tier, and a one-sentence reason."
        ),
        expected_output=(
            "A plain-text routing decision with four fields clearly stated: "
            "priority (high/medium/low), urgency (immediate/within_48h/nurture), "
            "rep_tier (senior_rep/mid_rep/sdr), and reason (one plain sentence, no quotes)."
        ),
        agent=decision_agent,
        context=[task1],
    )

    task3 = Task(
        description=(
            f"Convert the routing decision from Task 2 into a valid JSON object. "
            f"Include exactly these fields: lead_id, company_name, score, "
            f"priority, urgency, rep_tier, reason. "
            f"Use these exact values: "
            f"lead_id='{lead['lead_id']}', "
            f"company_name='{safe_company}', "
            f"score={lead['score']}. "
            f"Fill priority, urgency, rep_tier, reason from Task 2 output. "
            f"Output ONLY the JSON object. No markdown. No explanation. No extra text."
        ),
        expected_output=(
            "A single valid JSON object with exactly these keys: "
            "lead_id, company_name, score, priority, urgency, rep_tier, reason. "
            "No markdown fences. No extra text before or after the JSON."
        ),
        agent=formatter,
        context=[task2],
    )

    return [task1, task2, task3]


def parse_decision(raw: str, lead: dict) -> dict | None:
    """
    Safely parse the formatter's output into a dict.
    Falls back to extracting fields from plain text if JSON parse fails.
    """
    if not raw:
        return None

    # Strip markdown fences
    clean = raw.strip().strip("```json").strip("```").strip()

    # Try direct JSON parse first
    try:
        decision = json.loads(clean)
        # Validate required fields are present and non-null
        required = ["priority", "urgency", "rep_tier", "reason"]
        if all(decision.get(f) for f in required):
            return decision
    except json.JSONDecodeError:
        pass

    # Fallback: try to extract priority/urgency/rep_tier from plain text
    priority_map = {"high": "high", "medium": "medium", "low": "low"}
    urgency_map  = {"immediate": "immediate", "within_48h": "within_48h", "nurture": "nurture"}
    tier_map     = {"senior_rep": "senior_rep", "mid_rep": "mid_rep", "sdr": "sdr"}

    text_lower = clean.lower()

    priority  = next((v for k, v in priority_map.items() if k in text_lower), None)
    urgency   = next((v for k, v in urgency_map.items()  if k in text_lower), None)
    rep_tier  = next((v for k, v in tier_map.items()     if k in text_lower), None)

    if priority and urgency and rep_tier:
        # Extract reason — look for "reason:" line
        reason_match = re.search(r"reason[:\s]+(.+?)(?:\n|$)", clean, re.IGNORECASE)
        reason = reason_match.group(1).strip() if reason_match else "Extracted from plain-text fallback"
        return {
            "lead_id":      str(lead["lead_id"]),
            "company_name": lead["company_name"],
            "score":        lead["score"],
            "priority":     priority,
            "urgency":      urgency,
            "rep_tier":     rep_tier,
            "reason":       reason,
        }

    return None


def run_layer4(min_score: int = 0):
    """Main entry point. Processes all scored leads and writes routing_decisions.json."""
    print("Loading scored leads from DuckDB...")
    leads = load_scored_leads(min_score=min_score)
    leads = leads[:2]
    print(f"Found {len(leads)} leads to process.\n")

    lead_analyst, decision_agent, formatter = build_agents()
    all_decisions = []

    for i, lead in enumerate(leads):
        print(f"[{i+1}/{len(leads)}] Processing lead: {lead['lead_id']} — {lead['company_name']} (score: {lead['score']})")

        tasks = build_tasks(lead, lead_analyst, decision_agent, formatter)

        crew = Crew(
            agents=[lead_analyst, decision_agent, formatter],
            tasks=tasks,
            process=Process.sequential,
            verbose=False,
        )

        try:
            result = crew.kickoff()
            raw = result.raw if hasattr(result, "raw") else str(result)

            decision = parse_decision(raw, lead)

            if decision:
                all_decisions.append(decision)
                print(f"  -> {decision.get('priority', 'unknown').upper()} | {decision.get('urgency', 'unknown')} | {decision.get('rep_tier', 'unknown')}")
            else:
                print(f"  -> Could not parse decision for {lead['lead_id']} — skipping")
                print(f"     Raw output: {raw[:200] if raw else 'empty'}")

        except Exception as e:
            print(f"  -> Crew failed for lead {lead['lead_id']}: {type(e).__name__}: {str(e)[:200]}")
            print(f"     Skipping this lead and continuing...")

        # Rate limit sleep between leads
        if i < len(leads) - 1:
            print(f"  [rate limit] sleeping 60s before next lead...")
            time.sleep(60)

    save_routing_decisions(all_decisions, OUTPUT_PATH)
    print(f"\nDone. {len(all_decisions)} routing decisions written to {OUTPUT_PATH}")
    return all_decisions


if __name__ == "__main__":
    run_layer4(min_score=0)