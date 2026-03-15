"""
========================================================
LAYER 1 — DATA LAYER
========================================================
PURPOSE:
    Ingest raw structured (CSV) and unstructured data
    (Gong transcripts, Gmail JSON) into DuckDB — our
    local data warehouse.

HOW TO RUN:
    python 01_data_layer.py

WHAT IT DOES:
    Step 1 → Load leads.csv into DuckDB as raw_leads table
    Step 2 → Load transcript files into raw_transcripts table
    Step 3 → Validate everything loaded correctly

REAL WORLD EQUIVALENT:
    In production this script would be replaced by:
    - Fivetran / Airbyte pulling from Salesforce API  (structured)
    - Gong API + Gmail API pulling transcripts        (unstructured)
    But the DuckDB destination and table structure is identical.

JD SKILL THIS COVERS:
    "Design and manage a unified data layer that merges
     structured (DataLake/APIs) and unstructured signals
     (Calls/Emails/Chats)"
========================================================
"""

import duckdb
import pandas as pd
import os
import glob
import json
import re

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH          = "data/gtm_engine.duckdb"   # our local "data warehouse" file
LEADS_CSV        = "data/leads.csv"
TRANSCRIPTS_DIR  = "data/transcripts/"


# ── STEP 1: INGEST STRUCTURED DATA (CRM leads) ───────────────────────────────

def ingest_leads(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Load leads.csv into DuckDB as raw_leads table.

    WHY PANDAS FIRST?
        pandas reads the CSV and gives us a DataFrame.
        DuckDB can read a DataFrame directly — no SQL INSERT needed.
        In production: replace pd.read_csv() with a Salesforce API call.

    WHAT IS raw_leads?
        The "raw" prefix is a naming convention from dbt.
        raw = untouched, exactly as it came from the source.
        We never modify raw tables — dbt transforms them later.
    """
    print("\n📥  [STEP 1] Ingesting structured CRM data (leads.csv)...")

    # Read CSV into a pandas DataFrame
    df = pd.read_csv(LEADS_CSV)
    print(f"    CSV loaded: {len(df)} rows, {len(df.columns)} columns")
    print(f"    Columns: {list(df.columns)}")

    # Drop table if it already exists so we can re-run this script safely
    # This is called "idempotency" — running twice gives same result
    conn.execute("DROP TABLE IF EXISTS raw_leads")

    # Write DataFrame directly into DuckDB — one line, no SQL DDL needed
    # DuckDB automatically infers column types from the DataFrame
    conn.execute("CREATE TABLE raw_leads AS SELECT * FROM df")

    count = conn.execute("SELECT COUNT(*) FROM raw_leads").fetchone()[0]
    print(f"    ✅  raw_leads table created: {count} rows loaded into DuckDB")
    return count


# ── STEP 2: INGEST UNSTRUCTURED DATA (transcripts) ───────────────────────────

def parse_transcript_file(filepath: str) -> dict:
    """
    Parse a single transcript file regardless of its format.

    WHY DO WE NEED THIS?
        Our transcripts come in 2 different formats:
        - .txt  → Gong-style raw text with timestamps
        - .json → Gmail API raw JSON with HTML bodies

        A real pipeline handles many source formats.
        This function normalizes them into one consistent structure
        before loading into DuckDB.

    RETURNS: dict with keys: lead_id, source_type, raw_content, char_count
    """
    filename  = os.path.basename(filepath)
    extension = os.path.splitext(filename)[1].lower()

    # ── Handle Gmail API JSON format ──────────────────────
    if extension == ".json":
        with open(filepath, "r") as f:
            data = json.load(f)

        # Extract lead_id from the CRM enrichment block
        lead_id = data.get("_crm_enrichment", {}).get("lead_id", 0)

        # Concatenate all email message bodies into one string
        # Strip HTML tags so the LLM gets clean text to reason about
        raw_parts = []
        for msg in data.get("messages", []):
            headers    = {h["name"]: h["value"]
                          for h in msg["payload"].get("headers", [])}
            body       = msg["payload"]["body"].get("data", "")

            # Remove HTML tags
            body_clean = re.sub(r"<[^>]+>", " ", body)

            # Decode HTML entities
            body_clean = (body_clean
                          .replace("&#39;", "'")
                          .replace("&lt;",  "<")
                          .replace("&gt;",  ">")
                          .replace("&amp;", "&")
                          .replace("&nbsp;"," "))

            from_addr  = headers.get("From", "unknown")
            date_str   = headers.get("Date", "unknown")
            raw_parts.append(
                f"[{date_str}] FROM: {from_addr}\n{body_clean.strip()}"
            )

        raw_content = "\n\n---\n\n".join(raw_parts)
        source_type = "gmail_api"

    # ── Handle Gong .txt transcript format ────────────────
    else:
        with open(filepath, "r") as f:
            raw_content = f.read()

        # Extract lead_id from filename: lead_002.txt → 2
        try:
            lead_id = int(filename.split("_")[1].split(".")[0])
        except (IndexError, ValueError):
            lead_id = 0

        source_type = "gong_transcript"

    return {
        "filename"   : filename,
        "lead_id"    : lead_id,
        "source_type": source_type,   # where this data came from
        "raw_content": raw_content,   # full text, unmodified
        "char_count" : len(raw_content),
    }


def ingest_transcripts(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Load all transcript files from the transcripts directory into DuckDB.

    WHY STORE RAW CONTENT IN DUCKDB?
        We want a single landing zone for all data before processing.
        Layer 2 (Context Layer) will read from this table and embed it.
        Storing raw means we can always re-process without re-fetching.
    """
    print("\n📥  [STEP 2] Ingesting unstructured transcripts...")

    # Find all .txt and .json files in transcripts directory
    txt_files  = glob.glob(os.path.join(TRANSCRIPTS_DIR, "*.txt"))
    json_files = glob.glob(os.path.join(TRANSCRIPTS_DIR, "*.json"))
    all_files  = txt_files + json_files

    print(f"    Found {len(all_files)} transcript files: "
          f"{len(txt_files)} .txt (Gong), {len(json_files)} .json (Gmail)")

    # Parse each file into a normalized dict
    records = []
    for filepath in all_files:
        record = parse_transcript_file(filepath)
        records.append(record)
        print(f"    Parsed: {record['filename']} "
              f"(lead_id={record['lead_id']}, "
              f"source={record['source_type']}, "
              f"chars={record['char_count']})")

    # Convert list of dicts → DataFrame → DuckDB table
    transcript_df = pd.DataFrame(records)

    conn.execute("DROP TABLE IF EXISTS raw_transcripts")
    conn.execute("CREATE TABLE raw_transcripts AS SELECT * FROM transcript_df")

    count = conn.execute("SELECT COUNT(*) FROM raw_transcripts").fetchone()[0]
    print(f"    ✅  raw_transcripts table created: {count} rows loaded")
    return count


# ── STEP 3: VALIDATE ─────────────────────────────────────────────────────────

def validate(conn: duckdb.DuckDBPyConnection):
    """
    Run basic checks to confirm data loaded correctly.
    Print a preview so you can see what's in the warehouse.

    IN PRODUCTION: these would be automated dbt tests or
    Great Expectations checks that fail the pipeline if broken.
    """
    print("\n" + "=" * 55)
    print("📊  DATA WAREHOUSE PREVIEW")
    print("=" * 55)

    # Preview raw_leads
    print("\n🗂️  raw_leads — sample rows:")
    preview = conn.execute("""
        SELECT id, name, company, industry,
               last_activity_days, has_budget, in_buying_stage
        FROM raw_leads
        LIMIT 5
    """).df()
    print(preview.to_string(index=False))

    # Stats on leads
    print("\n📈  Lead stats:")
    stats = conn.execute("""
        SELECT
            COUNT(*)                                                   AS total_leads,
            SUM(CASE WHEN has_budget      = 'true' THEN 1 ELSE 0 END) AS has_budget,
            SUM(CASE WHEN in_buying_stage = 'true' THEN 1 ELSE 0 END) AS in_buying_stage,
            ROUND(AVG(last_activity_days), 1)                         AS avg_days_inactive
        FROM raw_leads
    """).df()
    print(stats.to_string(index=False))

    # Preview transcripts
    print("\n📝  raw_transcripts — what we ingested:")
    t_preview = conn.execute("""
        SELECT lead_id,
               filename,
               source_type,
               char_count,
               substr(raw_content, 1, 100) || '...' AS content_preview
        FROM raw_transcripts
    """).df()
    print(t_preview.to_string(index=False))

    # List all tables now in DuckDB
    print("\n🗄️  Tables now in DuckDB warehouse:")
    tables = conn.execute("SHOW TABLES").df()
    print(tables.to_string(index=False))


# ── MAIN ─────────────────────────────────────────────────────────────────────

def run():
    print("=" * 55)
    print("🏗️   LAYER 1 — DATA INGESTION STARTING")
    print("=" * 55)
    print(f"Target warehouse: {DB_PATH}")

    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)

    # Connect to DuckDB (creates the .duckdb file if it doesn't exist)
    conn = duckdb.connect(DB_PATH)

    # Run each ingestion step
    leads_count       = ingest_leads(conn)
    transcripts_count = ingest_transcripts(conn)

    # Validate everything looks right
    validate(conn)

    conn.close()

    print("\n" + "=" * 55)
    print("✅  LAYER 1 COMPLETE")
    print(f"   Warehouse file:  {DB_PATH}")
    print(f"   raw_leads:       {leads_count} rows")
    print(f"   raw_transcripts: {transcripts_count} rows")
    print("\n   Next step → run dbt to transform raw tables")
    print("   Then       → python 02_context_layer.py")
    print("=" * 55)


if __name__ == "__main__":
    run()
