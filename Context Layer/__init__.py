"""
Layer 2 — Context Layer
Semantic search over unstructured GTM signals.

Public API (used by Layer 3 + Layer 4):
    from layer2 import build_index, get_context_for_lead, query_leads_by_signal
"""

from .embedder  import build_index
from .retriever import get_context_for_lead, query_leads_by_signal, query_context

__all__ = [
    "build_index",           # run once to index all transcripts + lead notes
    "get_context_for_lead",  # Layer 3: get full context string for one lead
    "query_leads_by_signal", # Layer 4: find leads matching a buying signal
    "query_context",         # raw semantic search — returns LangChain Documents
]
