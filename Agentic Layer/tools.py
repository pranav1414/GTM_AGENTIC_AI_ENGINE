from crewai.tools import tool
import chromadb
import json

CHROMA_PATH = "./Data/chroma"
COLLECTION_NAME = "gtm_context"

@tool("ChromaDB Lead Context Retriever")
def chroma_retriever_tool(lead_id: str) -> str:
    """
    Retrieves the top-3 most relevant transcript chunks for a given lead_id
    from ChromaDB. Returns them as a formatted string for the analyst to use.
    """
    try:
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collection = client.get_collection(COLLECTION_NAME)

        results = collection.query(
            query_texts=[f"lead context for {lead_id}"],
            n_results=3,
            where={"lead_id": lead_id},
        )

        chunks = results.get("documents", [[]])[0]
        if not chunks:
            return f"No transcript context found for lead_id: {lead_id}"

        formatted = "\n---\n".join(
            f"Chunk {i+1}:\n{chunk}" for i, chunk in enumerate(chunks)
        )
        return formatted

    except Exception as e:
        return f"ChromaDB retrieval error: {str(e)}"


@tool("GTM Rules Engine")
def rules_engine_tool(score: int, context_summary: str) -> str:
    """
    Applies deterministic GTM business rules to a lead score and context summary.
    Returns a structured plain-text recommendation that the decision agent can use.
    """
    score = int(score)

    if score >= 80:
        priority = "high"
        rep_tier = "senior_rep"
        urgency = "immediate"
    elif score >= 55:
        priority = "medium"
        rep_tier = "mid_rep"
        urgency = "within_48h"
    else:
        priority = "low"
        rep_tier = "sdr"
        urgency = "nurture"

    # Context overrides — bump urgency if transcript signals are strong
    urgency_keywords = ["deadline", "q1", "q2", "urgent", "budget approved", "ready to buy"]
    context_lower = context_summary.lower()
    override = any(kw in context_lower for kw in urgency_keywords)

    if override and priority != "high":
        priority = "high"
        urgency = "immediate"
        rep_tier = "senior_rep"
        override_note = "Priority bumped due to urgency signals in transcript."
    else:
        override_note = ""

    result = (
        f"Rules output:\n"
        f"  priority: {priority}\n"
        f"  rep_tier: {rep_tier}\n"
        f"  urgency: {urgency}\n"
        f"  override_applied: {bool(override_note)}\n"
        f"  note: {override_note}"
    )
    return result
