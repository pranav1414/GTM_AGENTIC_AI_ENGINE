"""
Layer 5 — dispatcher
Reads the routing decision JSON from Layer 4 and fans out
to the appropriate action modules.
"""

import json
import logging
from typing import Union

from .lead_router import assign_lead
from .crm_updater import update_crm
from .slack_alert import send_alert

logger = logging.getLogger(__name__)

# Maps action_type values to handler functions
ACTION_REGISTRY = {
    "assign": assign_lead,
    "update_crm": update_crm,
    "alert": send_alert,
}


def dispatch(routing_decision: Union[dict, str]) -> dict:
    """
    Entry point for Layer 5.

    Accepts a routing decision dict (or JSON string) from Layer 4 and
    dispatches to the correct action handler(s).

    Expected routing_decision shape:
    {
        "lead_id": "abc123",
        "score": 87,
        "action_type": "assign",          # or "update_crm", "alert", or list
        "rep": "sarah.jones",
        "reason": "High intent + budget confirmed",
        "priority": "high"
    }

    action_type can be a single string OR a list of actions to fan out:
        "action_type": ["assign", "update_crm", "alert"]

    Returns a results dict keyed by action_type.
    """
    if isinstance(routing_decision, str):
        routing_decision = json.loads(routing_decision)

    lead_id = routing_decision.get("lead_id", "unknown")
    action_type = routing_decision.get("action_type")

    if not action_type:
        logger.warning(f"[dispatcher] No action_type in routing decision for lead {lead_id}")
        return {"status": "skipped", "reason": "no action_type provided"}

    # Normalise to list so single and multi-action leads share one path
    actions = action_type if isinstance(action_type, list) else [action_type]

    results = {}
    for action in actions:
        handler = ACTION_REGISTRY.get(action)
        if not handler:
            logger.warning(f"[dispatcher] Unknown action_type '{action}' for lead {lead_id} — skipping")
            results[action] = {"status": "error", "reason": f"unknown action type: {action}"}
            continue

        logger.info(f"[dispatcher] Dispatching '{action}' for lead {lead_id}")
        try:
            result = handler(routing_decision)
            results[action] = result
        except Exception as e:
            logger.error(f"[dispatcher] Handler '{action}' failed for lead {lead_id}: {e}")
            results[action] = {"status": "error", "reason": str(e)}

    return {"lead_id": lead_id, "results": results}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Example routing decision from Layer 4
    sample = {
        "lead_id": "lead_0042",
        "score": 91,
        "action_type": ["assign", "update_crm", "alert"],
        "rep": "sarah.jones",
        "reason": "High intent, budget confirmed, champion identified",
        "priority": "high",
        "company": "Acme Corp",
        "contact": "John Smith",
    }

    output = dispatch(sample)
    print(json.dumps(output, indent=2))
