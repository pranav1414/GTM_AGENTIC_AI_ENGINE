"""
Layer 3 — Scoring Layer
Public interface for Layer 4 (and any other consumer).

Usage:
    from layer3 import score_lead, score_all_leads
    from layer3 import read_hot_leads, read_scores_summary
"""

from .lead_scorer import score_lead, score_all_leads
from .score_parser import parse_score
from .rules_engine import apply_rules
from .duckdb_writer import write_score, write_scores_batch, read_hot_leads, read_scores_summary

__all__ = [
    "score_lead",
    "score_all_leads",
    "parse_score",
    "apply_rules",
    "write_score",
    "write_scores_batch",
    "read_hot_leads",
    "read_scores_summary",
]
