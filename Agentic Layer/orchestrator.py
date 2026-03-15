import duckdb
import time
import json
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


def build_tasks(lead: dict, lead_analyst, decision_agent, formatter) -> list[Task]:
    """Build the three sequential tasks for a single lead."""

    task1 = Task(
        description=(
            f"Analyse the following lead and summarise the key signals.\n\n"
            f"Lead ID: {lead['lead_id']}\n"
            f"Company: {lead['company_name']}\n"
            f"Score: {lead['score']}\n"
            f"Rep notes: {lead['rep_notes']}\n\n"
            f"Use the ChromaDB retriever tool with lead_id='{lead['lead_id']}' "
            f"to pull transcript context. Then write a 3-5 sentence summary of "
            f"what matters about this lead."
        ),
        expected_output=(
            "A concise plain-text summary (3-5 sentences) covering: "
            "score interpretation, notable transcript signals, and any red flags or "
            "strong positive indicators."
        ),
        agent=lead_analyst,
    )

    task2 = Task(
        description=(
            f"Using the analyst's summary, apply the GTM rules engine with "
            f"score={lead['score']} and the context summary from Task 1. "
            f"Then make a final routing decision. "
            f"State clearly: priority level, urgency, rep tier, and a one-sentence reason."
        ),
        expected_output=(
            "A plain-text routing decision with four fields clearly stated: "
            "priority (high/medium/low), urgency (immediate/within_48h/nurture), "
            "rep_tier (senior_rep/mid_rep/sdr), and reason (one sentence)."
        ),
        agent=decision_agent,
        context=[task1],
    )

    task3 = Task(
        description=(
            f"Convert the routing decision from Task 2 into a valid JSON object. "
            f"Include exactly these fields: lead_id, company_name, score, "
            f"priority, urgency, rep_tier, reason. "
            f"lead_id='{lead['lead_id']}', company_name='{lead['company_name']}', "
            f"score={lead['score']}. "
            f"Output ONLY the JSON object, no markdown, no explanation."
        ),
        expected_output=(
            "A single valid JSON object with keys: lead_id, company_name, score, "
            "priority, urgency, rep_tier, reason. No markdown fences."
        ),
        agent=formatter,
        context=[task2],
    )

    return [task1, task2, task3]


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

        result = crew.kickoff()
        time.sleep(60)

        try:
            # Strip markdown fences if the formatter added them anyway
            raw = result.raw.strip().strip("```json").strip("```").strip()
            decision = json.loads(raw)
            all_decisions.append(decision)
            print(f"  -> {str(decision.get('priority', 'unknown')).upper()} | {decision.get('urgency', 'unknown')} | {decision.get('rep_tier', 'unknown')}")
        except json.JSONDecodeError as e:
            print(f"  -> JSON parse error for {lead['lead_id']}: {e}")
            print(f"     Raw output: {result.raw[:200]}")

    save_routing_decisions(all_decisions, OUTPUT_PATH)
    print(f"\nDone. {len(all_decisions)} routing decisions written to {OUTPUT_PATH}")
    return all_decisions


if __name__ == "__main__":
    run_layer4(min_score=0)
