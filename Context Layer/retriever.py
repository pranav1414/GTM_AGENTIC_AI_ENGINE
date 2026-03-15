"""
Layer 2 — Retriever
LangChain retriever chain wrapping ChromaDB.
Called by Layer 3 (scoring) and Layer 4 (agentic) to fetch
semantically relevant context for any lead.
"""

import chromadb
from sentence_transformers import SentenceTransformer
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings  import HuggingFaceEmbeddings
from langchain.schema                import Document

# ── config (must match embedder.py) ───────────────────────────────────────────
CHROMA_DIR      = "data/chroma"
COLLECTION_NAME = "gtm_context"
MODEL_NAME      = "all-MiniLM-L6-v2"
TOP_K           = 5     # how many chunks to return per query


# ── singleton — load model + collection once ──────────────────────────────────
_retriever = None

def get_retriever():
    """
    Returns a cached LangChain retriever.
    Lazy-loaded on first call — subsequent calls reuse the same instance.
    """
    global _retriever
    if _retriever is not None:
        return _retriever

    print("[retriever] loading embedding model + ChromaDB...")

    embeddings = HuggingFaceEmbeddings(
        model_name=MODEL_NAME,
        model_kwargs={"device": "cpu"},
        encode_kwargs={"normalize_embeddings": True},
    )

    vectorstore = Chroma(
        collection_name=COLLECTION_NAME,
        embedding_function=embeddings,
        persist_directory=CHROMA_DIR,
    )

    _retriever = vectorstore.as_retriever(
        search_type="similarity",
        search_kwargs={"k": TOP_K},
    )

    print("[retriever] ready")
    return _retriever


# ── public API ─────────────────────────────────────────────────────────────────
def query_context(query: str, lead_id: str = None) -> list[Document]:
    """
    Semantic search over all indexed chunks.

    Args:
        query:   natural language question, e.g. "pricing objections mentioned"
        lead_id: optional — filter results to a specific lead only

    Returns:
        list of LangChain Document objects with .page_content and .metadata
    """
    retriever = get_retriever()

    if lead_id:
        # filter to a single lead's chunks
        docs = retriever.vectorstore.similarity_search(
            query,
            k=TOP_K,
            filter={"lead_id": str(lead_id)},
        )
    else:
        docs = retriever.invoke(query)

    return docs


def get_context_for_lead(lead_id: str) -> str:
    """
    Convenience wrapper used by Layer 3.
    Returns all context for a lead as a single joined string,
    ready to be passed into the scoring model as a feature.
    """
    docs = query_context(
        query=f"lead context signals buying intent objections sentiment",
        lead_id=lead_id,
    )
    if not docs:
        return ""

    return "\n---\n".join(d.page_content for d in docs)


def query_leads_by_signal(signal: str, n: int = 10) -> list[dict]:
    """
    Convenience wrapper used by Layer 4 (agentic layer).
    Returns top-n leads matching a buying signal description.

    Example:
        query_leads_by_signal("urgent timeline, decision maker engaged", n=5)
    """
    docs = query_context(query=signal)
    seen = {}
    for doc in docs:
        lid = doc.metadata.get("lead_id")
        if lid and lid not in seen:
            seen[lid] = {
                "lead_id": lid,
                "source":  doc.metadata.get("source"),
                "excerpt": doc.page_content[:200],
            }
    return list(seen.values())[:n]


if __name__ == "__main__":
    # quick smoke test
    results = query_leads_by_signal("pricing concern budget tight")
    for r in results:
        print(f"\nlead_id: {r['lead_id']}")
        print(f"excerpt: {r['excerpt']}")
