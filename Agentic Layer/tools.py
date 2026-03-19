from crewai.tools import tool
import chromadb
import re

CHROMA_PATH = "./Data/chroma"
COLLECTION_NAME = "gtm_context"


def _clean(text: str) -> str:
    """Strip characters that break Groq function-call JSON serialization."""
    if not text:
        return ""
    text = text.replace('"', "'")
    text = text.replace("{", "(").replace("}", ")")
    text = text.replace("<", "").replace(">", "")
    text = text.replace("\\", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


@tool("chroma_db_lead_context_retriever")
def chroma_retriever_tool(lead_id: str) -> str:
    """
    Retrieves the top 3 most relevant transcript chunks for a given lead_id
    from ChromaDB. Returns them as a plain text string.
    Input: lead_id as a string.
    """
    try:
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collection = client.get_collection(COLLECTION_NAME)

        results = collection.query(
            query_texts=[f"lead context for {lead_id}"],
            n_results=3,
            where={"lead_id": str(lead_id)},
        )

        chunks = results.get("documents", [[]])[0]
        if not chunks:
            return f"No transcript context found for lead_id: {lead_id}"

        formatted = "\n---\n".join(
            f"Chunk {i+1}: {_clean(chunk)}" for i, chunk in enumerate(chunks)
        )
        return formatted

    except Exception as e:
        return f"No transcript context found for lead_id: {lead_id}"


@tool("gtm_rules_engine")
def rules_engine_tool(score: int, context_summary: str = "") -> str:
    """
    Applies GTM business rules to a lead score and optional context summary.
    Returns priority, rep_tier, urgency, and override status as plain text.
    Input: score as integer, context_summary as plain text string (no quotes or special characters).
    """
    try:
        score = int(float(str(score).strip()))
    except Exception:
        score = 0

    # Sanitize incoming context_summary — remove anything that could break JSON
    safe_summary = _clean(str(context_summary))

    # ── Score-based rules ───────────────────────────────────────────────────
    if score >= 80:
        priority = "high"
        rep_tier = "senior_rep"
        urgency  = "immediate"
    elif score >= 55:
        priority = "medium"
        rep_tier = "mid_rep"
        urgency  = "within_48h"
    else:
        priority = "low"
        rep_tier = "sdr"
        urgency  = "nurture"

    # ── Context override — bump priority if urgency signals detected ────────
    urgency_keywords = ["deadline", "q1", "q2", "urgent", "budget approved", "ready to buy"]
    override = any(kw in safe_summary.lower() for kw in urgency_keywords)

    if override and priority != "high":
        priority  = "high"
        urgency   = "immediate"
        rep_tier  = "senior_rep"
        override_note = "Priority bumped due to urgency signals in context"
    else:
        override_note = "No override applied"

    return (
        f"priority: {priority}\n"
        f"rep_tier: {rep_tier}\n"
        f"urgency: {urgency}\n"
        f"override_applied: {override}\n"
        f"note: {override_note}"
    )