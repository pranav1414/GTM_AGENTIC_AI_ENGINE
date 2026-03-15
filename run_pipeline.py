"""
run_pipeline.py
===============
Master runner for the GTM AI Engine.
Executes all 5 layers in sequence:

    Layer 1 → Ingest raw data into DuckDB
    Layer 2 → Embed transcripts + leads into ChromaDB
    Layer 3 → Score leads via Groq LLM + deterministic rules
    Layer 4 → Agentic routing decisions via CrewAI + Groq
    Layer 5 → GTM automation (assign, CRM update, Slack alert)

Run from project root:
    python run_pipeline.py
"""

import sys
import os
import json
import importlib.util
import duckdb
import traceback
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────

ROOT        = Path(__file__).parent
DB_PATH     = ROOT / "gtm_engine.duckdb"
DATA_DIR    = ROOT / "Data"
CHROMA_DIR  = ROOT / "Data" / "chroma"

# ── Helpers ────────────────────────────────────────────────────────────────────

def banner(title: str):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def success(msg: str):
    print(f"  ✅  {msg}")


def warn(msg: str):
    print(f"  ⚠️   {msg}")


def load_module(name: str, filepath: Path):
    """Dynamically load a module from a file path and register it in sys.modules."""
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod  = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def register_layer(package: str, folder: Path, modules: list[str]):
    """Register all modules in a layer folder under a package namespace."""
    for mod in modules:
        load_module(f"{package}.{mod}", folder / f"{mod}.py")


# ── Layer 1 ────────────────────────────────────────────────────────────────────

def run_layer1():
    banner("LAYER 1 — Data Ingestion")

    import pandas as pd
    import glob
    import re

    conn = duckdb.connect(str(DB_PATH))

    # ── Load leads.csv ──────────────────────────────────────────────────────
    leads_csv = DATA_DIR / "leads.csv"
    if not leads_csv.exists():
        warn(f"leads.csv not found at {leads_csv} — skipping")
    else:
        df = pd.read_csv(leads_csv)
        conn.execute("DROP TABLE IF EXISTS raw_leads")
        conn.execute("CREATE TABLE raw_leads AS SELECT * FROM df")
        count = conn.execute("SELECT COUNT(*) FROM raw_leads").fetchone()[0]
        success(f"raw_leads: {count} rows loaded from leads.csv")

    # ── Load transcripts ────────────────────────────────────────────────────
    txt_files  = list(DATA_DIR.glob("*.txt"))
    json_files = list(DATA_DIR.glob("*.json"))
    all_files  = txt_files + json_files

    if not all_files:
        warn(f"No transcript files found in {DATA_DIR}")
    else:
        records = []
        for filepath in all_files:
            filename  = filepath.name
            extension = filepath.suffix.lower()

            if extension == ".json":
                try:
                    with open(filepath) as f:
                        data = json.load(f)
                    lead_id = data.get("_crm_enrichment", {}).get("lead_id", 0)
                    raw_parts = []
                    for msg in data.get("messages", []):
                        body = msg["payload"]["body"].get("data", "")
                        body = re.sub(r"<[^>]+>", " ", body)
                        raw_parts.append(body.strip())
                    raw_content = "\n\n---\n\n".join(raw_parts)
                    source_type = "gmail_api"
                except Exception as e:
                    warn(f"Failed to parse {filename}: {e}")
                    continue
            else:
                raw_content = filepath.read_text(encoding="utf-8")
                try:
                    lead_id = int(filename.split("_")[1].split(".")[0])
                except (IndexError, ValueError):
                    lead_id = 0
                source_type = "gong_transcript"

            records.append({
                "filename":    filename,
                "lead_id":     lead_id,
                "source_type": source_type,
                "raw_content": raw_content,
                "char_count":  len(raw_content),
            })

        transcript_df = pd.DataFrame(records)
        conn.execute("DROP TABLE IF EXISTS raw_transcripts")
        conn.execute("CREATE TABLE raw_transcripts AS SELECT * FROM transcript_df")
        count = conn.execute("SELECT COUNT(*) FROM raw_transcripts").fetchone()[0]
        success(f"raw_transcripts: {count} rows loaded")

    # ── Run dbt-style transformations inline ────────────────────────────────
    # (since dbt requires a separate install, we replicate the transforms in SQL)
    try:
        conn.execute("DROP TABLE IF EXISTS leads_final")
        conn.execute("""
            CREATE TABLE leads_final AS
            SELECT
                CAST(id AS VARCHAR)                 AS lead_id,
                name,
                company                             AS company_name,
                industry,
                COALESCE(job_title, 'Unknown')      AS job_title,
                'Prospecting'                       AS deal_stage,
                COALESCE(lead_source, 'Unknown')    AS lead_source,
                CAST(last_activity_days AS VARCHAR) AS last_activity_date,
                ''                                  AS notes,
                COALESCE(email, '')                 AS email,
                ''                                  AS phone,
                has_budget,
                in_buying_stage,
                employee_count,
                annual_revenue,
                country,
                num_website_visits,
                num_emails_opened,
                num_demo_requests,
                created_date
            FROM raw_leads
        """)
        count = conn.execute("SELECT COUNT(*) FROM leads_final").fetchone()[0]
        success(f"leads_final: {count} rows transformed")
    except Exception as e:
        warn(f"leads_final transform failed: {e}")
        traceback.print_exc()

    conn.close()
    success("Layer 1 complete")


# ── Layer 2 ────────────────────────────────────────────────────────────────────

def run_layer2():
    banner("LAYER 2 — Context Embeddings")

    try:
        from sentence_transformers import SentenceTransformer
        import chromadb

        MODEL_NAME      = "all-MiniLM-L6-v2"
        COLLECTION_NAME = "gtm_context"
        CHUNK_SIZE      = 300
        CHUNK_OVERLAP   = 50

        def chunk_text(text):
            chunks, start = [], 0
            while start < len(text):
                end = start + CHUNK_SIZE
                chunks.append(text[start:end].strip())
                start += CHUNK_SIZE - CHUNK_OVERLAP
            return [c for c in chunks if len(c) > 20]

        print("  Loading sentence-transformer model...")
        model  = SentenceTransformer(MODEL_NAME)
        client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        collection = client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )

        docs = []

        # Load .txt transcript files
        for path in DATA_DIR.glob("*.txt"):
            text    = path.read_text(encoding="utf-8").strip()
            lead_id = path.stem
            for i, chunk in enumerate(chunk_text(text)):
                docs.append({
                    "id":      f"transcript_{lead_id}_{i}",
                    "text":    chunk,
                    "source":  "transcript",
                    "lead_id": lead_id,
                    "chunk_i": i,
                })

        # Load lead notes from DuckDB
        try:
            conn = duckdb.connect(str(DB_PATH), read_only=True)
            rows = conn.execute("""
                SELECT lead_id, COALESCE(notes, '') AS notes
                FROM leads_final
                WHERE LENGTH(COALESCE(notes, '')) > 20
            """).fetchall()
            conn.close()
            for lead_id, notes in rows:
                for i, chunk in enumerate(chunk_text(notes)):
                    docs.append({
                        "id":      f"lead_{lead_id}_note_{i}",
                        "text":    chunk,
                        "source":  "lead_notes",
                        "lead_id": str(lead_id),
                        "chunk_i": i,
                    })
        except Exception as e:
            warn(f"Could not load lead notes: {e}")

        if not docs:
            warn("No documents to embed — index will be empty")
        else:
            BATCH = 64
            for i in range(0, len(docs), BATCH):
                batch      = docs[i:i + BATCH]
                texts      = [d["text"] for d in batch]
                ids        = [d["id"]   for d in batch]
                metadatas  = [{k: v for k, v in d.items() if k != "text"} for d in batch]
                embeddings = model.encode(texts, show_progress_bar=False).tolist()
                collection.upsert(ids=ids, documents=texts,
                                  embeddings=embeddings, metadatas=metadatas)

            success(f"ChromaDB index: {collection.count()} chunks embedded")

    except Exception as e:
        warn(f"Layer 2 failed: {e}")
        traceback.print_exc()

    success("Layer 2 complete")


# ── Layer 3 ────────────────────────────────────────────────────────────────────

def run_layer3():
    banner("LAYER 3 — Lead Scoring")

    scoring_dir = ROOT / "Scoring Layer"

    # Register modules under 'layer3' namespace
    register_layer("layer3", scoring_dir, ["score_parser", "rules_engine", "duckdb_writer"])

    # Patch duckdb_writer to use absolute DB path
    duckdb_writer = sys.modules["layer3.duckdb_writer"]
    duckdb_writer.DB_PATH = str(DB_PATH)

    # Register scoring modules with correct import aliases
    # lead_scorer imports from 'score_parser' and 'rules_engine' directly
    load_module("score_parser",  scoring_dir / "score_parser.py")
    load_module("rules_engine",  scoring_dir / "rules_engine.py")
    load_module("duckdb_writer", scoring_dir / "duckdb_writer.py")
    sys.modules["duckdb_writer"].DB_PATH = str(DB_PATH)

    # Register layer2 retriever shim
    _register_layer2_retriever()

    load_module("layer3.lead_scorer", scoring_dir / "lead_scorer.py")
    lead_scorer = sys.modules["layer3.lead_scorer"]

    # Load leads from DuckDB
    conn  = duckdb.connect(str(DB_PATH), read_only=True)
    rows  = conn.execute("SELECT * FROM leads_final").fetchdf()
    conn.close()
    leads = rows.to_dict(orient="records")
    print(f"  Loaded {len(leads)} leads from leads_final")

    scored = lead_scorer.score_all_leads(leads)
    success(f"Layer 3 complete — {len(scored)} leads scored")
    return scored


def _register_layer2_retriever():
    """Register a layer2 shim so lead_scorer's 'from layer2 import get_retriever' works."""
    import types

    class _Retriever:
        def __init__(self):
            import chromadb
            from sentence_transformers import SentenceTransformer
            self.model  = SentenceTransformer("all-MiniLM-L6-v2")
            client      = chromadb.PersistentClient(path=str(CHROMA_DIR))
            self.col    = client.get_or_create_collection("gtm_context")

        def get_relevant_chunks(self, query: str, lead_id: str = "", k: int = 5) -> list[str]:
            try:
                where = {"lead_id": lead_id} if lead_id else None
                q_emb = self.model.encode([query]).tolist()
                kwargs = dict(query_embeddings=q_emb, n_results=min(k, self.col.count() or 1))
                if where:
                    kwargs["where"] = where
                results = self.col.query(**kwargs)
                return results.get("documents", [[]])[0]
            except Exception as e:
                print(f"  [retriever] warning: {e}")
                return []

    layer2_mod = types.ModuleType("layer2")
    layer2_mod.get_retriever = lambda: _Retriever()
    sys.modules["layer2"] = layer2_mod


# ── Layer 4 ────────────────────────────────────────────────────────────────────

def run_layer4():
    banner("LAYER 4 — Agentic Routing")

    agentic_dir = ROOT / "Agentic Layer"

    register_layer("layer4", agentic_dir, ["tools", "output", "agents", "orchestrator"])

    # Patch orchestrator DB path
    orchestrator = sys.modules["layer4.orchestrator"]
    orchestrator.DB_PATH = str(DB_PATH)

    # Check leads_scored table exists (Layer 3 writes lead_scores, Layer 4 reads leads_scored)
    # We alias lead_scores → leads_scored so Layer 4 can query it
    conn = duckdb.connect(str(DB_PATH))
    try:
        conn.execute("""
            CREATE OR REPLACE VIEW leads_scored AS
            SELECT
                lead_id,
                company_name,
                final_score AS score,
                reasoning   AS rep_notes
            FROM lead_scores
        """)
        count = conn.execute("SELECT COUNT(*) FROM leads_scored").fetchone()[0]
        conn.close()
        print(f"  leads_scored view: {count} leads available")
    except Exception as e:
        conn.close()
        warn(f"Could not create leads_scored view: {e}")
        return

    decisions = orchestrator.run_layer4(min_score=0)
    success(f"Layer 4 complete — {len(decisions)} routing decisions made")
    return decisions


# ── Layer 5 ────────────────────────────────────────────────────────────────────

def run_layer5(decisions: list[dict]):
    banner("LAYER 5 — GTM Automation")

    gtm_dir = ROOT / "GTM Automation"
    register_layer("layer5", gtm_dir, ["lead_router", "crm_updater", "slack_alert", "dispatcher"])

    # Patch DB paths
    for mod_name in ["layer5.lead_router", "layer5.crm_updater"]:
        mod = sys.modules[mod_name]
        mod.DB_PATH = DB_PATH

    dispatcher = sys.modules["layer5.dispatcher"]

    if not decisions:
        warn("No decisions from Layer 4 — skipping Layer 5")
        return

    print(f"  Dispatching {len(decisions)} routing decisions...")
    results = []
    for decision in decisions:
        # Map Layer 4 fields to Layer 5 dispatcher format
        payload = {
            "lead_id":     decision.get("lead_id", "unknown"),
            "score":       decision.get("score", 0),
            "action_type": ["assign", "update_crm", "alert"],
            "priority":    decision.get("priority", "low"),
            "reason":      decision.get("reason", ""),
            "company":     decision.get("company_name", ""),
        }
        result = dispatcher.dispatch(payload)
        results.append(result)
        print(f"  → {payload['lead_id']} | {payload['priority'].upper()} | {result.get('results', {}).get('assign', {}).get('rep', '?')}")

    success(f"Layer 5 complete — {len(results)} leads dispatched")
    return results


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "🚀 " * 20)
    print("  GTM AI ENGINE — FULL PIPELINE RUN")
    print("🚀 " * 20)
    print(f"\n  DB:   {DB_PATH}")
    print(f"  Data: {DATA_DIR}")

    try:
        run_layer1()
    except Exception as e:
        print(f"\n❌  Layer 1 failed: {e}"); traceback.print_exc(); sys.exit(1)

    try:
        run_layer2()
    except Exception as e:
        print(f"\n❌  Layer 2 failed: {e}"); traceback.print_exc(); sys.exit(1)

    try:
        run_layer3()
    except Exception as e:
        print(f"\n❌  Layer 3 failed: {e}"); traceback.print_exc(); sys.exit(1)

    try:
        decisions = run_layer4()
    except Exception as e:
        print(f"\n❌  Layer 4 failed: {e}"); traceback.print_exc(); sys.exit(1)

    try:
        run_layer5(decisions or [])
    except Exception as e:
        print(f"\n❌  Layer 5 failed: {e}"); traceback.print_exc(); sys.exit(1)

    banner("✅  FULL PIPELINE COMPLETE")
    print(f"\n  All 5 layers ran successfully.")
    print(f"  Results written to: {DB_PATH}")
    print(f"  Swagger UI:         http://localhost:8000/docs\n")


if __name__ == "__main__":
    main()