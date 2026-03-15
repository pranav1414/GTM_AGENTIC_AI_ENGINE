"""
Layer 5 — slack_alert
Formats and "sends" a Slack notification for high-priority leads.

Mock mode (default): logs the payload as structured JSON to stdout.
Live mode: set SLACK_WEBHOOK_URL env var and it will POST for real.

The payload is built in Slack Block Kit format so it renders
correctly when you flip to a real webhook — no reformatting needed.
"""

import json
import logging
import os
from datetime import datetime, timezone
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# Only fire Slack alerts for leads above this score threshold
ALERT_SCORE_THRESHOLD = 70

PRIORITY_EMOJI = {
    "high":   ":fire:",
    "medium": ":large_yellow_circle:",
    "low":    ":white_circle:",
}


def _build_blocks(routing_decision: dict) -> list:
    """Build a Slack Block Kit message."""
    lead_id  = routing_decision.get("lead_id", "unknown")
    score    = routing_decision.get("score", 0)
    priority = routing_decision.get("priority", "low")
    rep      = routing_decision.get("rep", "unassigned")
    company  = routing_decision.get("company", lead_id)
    contact  = routing_decision.get("contact", "")
    reason   = routing_decision.get("reason", "")
    emoji    = PRIORITY_EMOJI.get(priority, ":white_circle:")

    header = f"{emoji} New {priority.upper()} priority lead — {company}"
    if contact:
        header += f" ({contact})"

    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header, "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Lead ID:*\n`{lead_id}`"},
                {"type": "mrkdwn", "text": f"*Score:*\n{score}/100"},
                {"type": "mrkdwn", "text": f"*Assigned to:*\n{rep}"},
                {"type": "mrkdwn", "text": f"*Priority:*\n{priority.capitalize()}"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Why:*\n{reason}"},
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"GTM Engine · {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
                }
            ],
        },
    ]


def _post_to_slack(payload: dict) -> bool:
    """POST payload to real Slack webhook. Returns True on success."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except urllib.error.URLError as e:
        logger.error(f"[slack_alert] Webhook POST failed: {e}")
        return False


def send_alert(routing_decision: dict) -> dict:
    """
    Send (or mock-send) a Slack alert for a high-scoring lead.

    Args:
        routing_decision: dict from Layer 4 dispatcher

    Returns:
        dict with status, mode (mock|live), and payload
    """
    lead_id = routing_decision.get("lead_id", "unknown")
    score   = routing_decision.get("score", 0)

    if score < ALERT_SCORE_THRESHOLD:
        logger.info(f"[slack_alert] Lead {lead_id} score={score} below threshold — skipping alert")
        return {
            "status":  "skipped",
            "lead_id": lead_id,
            "reason":  f"score {score} < threshold {ALERT_SCORE_THRESHOLD}",
        }

    blocks  = _build_blocks(routing_decision)
    payload = {"blocks": blocks}

    if SLACK_WEBHOOK_URL:
        # Live mode
        success = _post_to_slack(payload)
        mode    = "live"
        status  = "ok" if success else "error"
        logger.info(f"[slack_alert] Live POST for lead {lead_id} — {status}")
    else:
        # Mock mode — pretty-print to stdout so it's inspectable
        mode   = "mock"
        status = "ok"
        logger.info(f"[slack_alert] MOCK alert for lead {lead_id} (score={score})")
        print("\n── Slack alert payload ──────────────────────────────────────")
        print(json.dumps(payload, indent=2))
        print("─────────────────────────────────────────────────────────────\n")

    return {
        "status":  status,
        "mode":    mode,
        "lead_id": lead_id,
        "score":   score,
        "payload": payload,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = send_alert({
        "lead_id":  "lead_0042",
        "score":    91,
        "priority": "high",
        "rep":      "sarah.jones",
        "company":  "Acme Corp",
        "contact":  "John Smith",
        "reason":   "Champion identified, budget confirmed, high urgency",
    })
    # Print everything except the full payload (already printed above)
    result_summary = {k: v for k, v in result.items() if k != "payload"}
    print(json.dumps(result_summary, indent=2))
