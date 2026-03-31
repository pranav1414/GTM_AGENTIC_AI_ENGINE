# 🚀 GTM Agentic AI Engine
Autonomous revenue engine that scores, routes, and acts on CRM leads. 
Built on a 5-layer architecture that takes raw CRM data and transcripts, scores every lead using an LLM judge, runs autonomous AI agents to make routing decisions, and dispatches the results to reps via a production-ready REST API.

---

## ⚡ What It Does
Drop in a CSV of leads. The engine:

1. Ingests raw CRM data and call transcripts into a local data warehouse
2. Embeds all unstructured text into a semantic vector index for intelligent retrieval
3. Scores every lead 0–100 using an LLM judge + deterministic business rules
4. Routes each lead through 3 autonomous AI agents that reason, decide, and format
5. Dispatches rep assignments, CRM stage updates, and Slack alerts — all via REST API
6. Accepts inbound intent signals from any external platform via a secure webhook

---

## 🏗️ Architecture
GTM Agentic AI Engine

![GTM Agentic AI Engine](GTM%20AI%20AGENTIC%20ENGINE.png)

---

## 🧠 The Agentic Layer (Layer 4)
Three CrewAI agents run sequentially for each lead, each one builds on the previous agent's output:

| Agent | Role | Tool | Output |
|---|---|---|---|
| Senior Lead Analyst | Reads CRM data + pulls transcript context | ChromaDB retriever | 3–5 sentence signal summary |
| GTM Decision Strategist | Applies rules to the summary | GTM rules engine | priority · urgency · rep_tier |
| Structured Output Formatter | Converts decision to JSON | None | Clean JSON routing decision |

The blend of probabilistic LLM reasoning (Layer 3 scoring + Agent analysis) with deterministic logic (rules engine caps/floors/overrides) ensures no lead is misdirected due to a hallucination.

---

## 🛠️ Tech Stack
Python · DuckDB · ChromaDB · CrewAI · Groq · FastAPI · Pandas · sentence-transformers

---

## 📁 Project Structure
```
GTM_AGENTIC_AI_ENGINE/
│
├── 01_data_layer.py          # Layer 1 entry point
├── run_pipeline.py           # Master runner — executes all 5 layers
│
├── Data/
│   ├── leads.csv             # 10 CRM leads
│   ├── lead_001.txt          # Gong-style call transcript
│   ├── lead_002.json         # Gmail API email thread
│   └── lead_003.txt          # Gong-style call transcript
│
├── Context Layer/
│   ├── embedder.py           # Chunks + embeds text into ChromaDB
│   └── retriever.py          # Semantic search interface
│
├── Scoring Layer/
│   ├── lead_scorer.py        # LLM judge orchestration
│   ├── score_parser.py       # Safe JSON extraction from LLM
│   ├── rules_engine.py       # Deterministic score overrides
│   └── duckdb_writer.py      # Persists scores to DuckDB
│
├── Agentic Layer/
│   ├── agents.py             # 3 CrewAI agent definitions
│   ├── orchestrator.py       # Crew kickoff + JSON parsing
│   ├── tools.py              # ChromaDB + rules engine tools
│   └── output.py             # Decision validation + file write
│
├── GTM Automation/
│   ├── dispatcher.py         # Fan-out to all handlers
│   ├── lead_router.py        # Rep assignment → DuckDB
│   ├── crm_updater.py        # CRM stage update → DuckDB (HubSpot/Salesforce-compatible payload)
│   ├── slack_alert.py        # Slack Block Kit alerts
│   └── api.py                # FastAPI — all 5 layers + webhook endpoint
│
└── dbt_project/              # dbt SQL transformations
    ├── stg_leads.sql
    ├── leads_enriched.sql
    └── leads_final.sql
```

---

## 📊 Key Numbers

| Metric | Value |
|---|---|
| Leads processed | 10 (extensible to any size) |
| ChromaDB chunks | 34 |
| Embedding dimensions | 384 |
| Score range | 0 – 100 |
| Hot tier | score ≥ 70 |
| Warm tier | 40 ≤ score < 70 |
| Cold tier | score < 40 |
| LLM model | llama-3.1-8b-instant |
| Total cost | $0 |

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | /health | Liveness check |
| POST | /dispatch | Main Layer 5 entry — fan out to all handlers |
| POST | /assign | Assign a lead to a rep |
| POST | /crm-update | Write a CRM stage update |
| POST | /alert | Fire a Slack alert |
| POST | /webhook | n8n-compatible webhook — accepts intent signals from any platform |
| GET | /assignments | All rep assignments |
| GET | /crm-events | All CRM events |
| GET | /leads/scores | All scored leads |
| GET | /leads/{lead_id}/score | Score for a specific lead |
| GET | /leads | All cleaned leads |

---

## 🔗 Webhook Endpoint
`POST /webhook` accepts inbound intent signals from any platform that supports outbound HTTP — HubSpot, Salesforce, n8n, Zapier, Slack, or any custom service.

**Request format:**
```json
{
  "event": "lead.scored",
  "data": {
    "lead_id": "lead_0042",
    "score": 91,
    "action_type": ["assign", "update_crm", "alert"],
    "rep": "sarah.jones",
    "priority": "high",
    "reason": "Champion identified",
    "company": "Acme Corp",
    "contact": "John Smith"
  }
}
```

**Security:** Set the `WEBHOOK_SECRET` environment variable to enable HMAC-SHA256 signature verification via the `X-N8N-Signature` header. Requests with an invalid or missing signature are rejected with HTTP 401. If `WEBHOOK_SECRET` is not set, the endpoint accepts unsigned requests (suitable for local development).

**How it works:** The `data` object is mapped directly onto the internal `RoutingDecision` model and passed to the same `dispatch()` function used by `POST /dispatch` — so any external platform can trigger the full GTM automation layer with a single HTTP POST.

---

## 🔄 Production Upgrade Path
This project is architected for production. The only changes needed to go live:

| Mock (current) | Production swap |
|---|---|
| DuckDB CRM writes | HubSpot / Salesforce API POST |
| Logged Slack payload | Set `SLACK_WEBHOOK_URL` env var |
| Local CSV | Fivetran / Airbyte pulling from Salesforce |
| Groq free tier | Groq Dev Tier or any OpenAI-compatible API |
| Unsigned webhook | Set `WEBHOOK_SECRET` env var for HMAC verification |

---

## 💡 Example Output
```
LAYER 3 — Lead Scoring
  Scoring lead 1/10 — ScaleUp AI ... llm=60  final=60  tier=Warm

LAYER 4 — Agentic Routing
  [1/2] Processing lead: ScaleUp AI (score: 60)
  Agent 1: CEO role is positive signal, no transcript context available
  Agent 2: priority=medium | urgency=within_48h | rep_tier=mid_rep
  Agent 3: {"lead_id": "6", "priority": "medium", ...}
  -> MEDIUM | within_48h | mid_rep

LAYER 5 — GTM Automation
  → 6 | MEDIUM | lisa.patel
  → 1 | LOW    | inbound-queue
  ✅ Layer 5 complete — 2 leads dispatched
```

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---
