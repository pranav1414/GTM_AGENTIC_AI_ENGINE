"""
Layer 2 — Embedder
Loads transcripts + leads_final from DuckDB, chunks text,
embeds via sentence-transformers, persists to ChromaDB.
"""

import os
import uuid
import duckdb
import chromadb
from pathlib import Path
from sentence_transformers import SentenceTransformer

# ── config ────────────────────────────────────────────────────────────────────
TRANSCRIPTS_DIR = Path("Data")               # folder of .txt files from Layer 1
DUCKDB_PATH     = "Data/leads.duckdb"        # Layer 1 DuckDB output
CHROMA_DIR      = "data/chroma"              # where ChromaDB persists to disk
COLLECTION_NAME = "gtm_context"
MODEL_NAME      = "all-MiniLM-L6-v2"        # 384-dim, fast, local, free
CHUNK_SIZE      = 300                        # characters per chunk
CHUNK_OVERLAP   = 50                         # overlap between chunks


# ── helpers ───────────────────────────────────────────────────────────────────
def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping chunks so no context is lost at boundaries."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + size
        chunks.append(text[start:end].strip())
        start += size - overlap
    return [c for c in chunks if len(c) > 20]   # drop tiny tail chunks


def load_transcripts() -> list[dict]:
    """Load every .txt file from the transcripts directory."""
    docs = []
    if not TRANSCRIPTS_DIR.exists():
        print(f"[embedder] WARNING: {TRANSCRIPTS_DIR} not found — skipping transcripts")
        return docs

    for path in TRANSCRIPTS_DIR.glob("*.txt"):
        text = path.read_text(encoding="utf-8").strip()
        lead_id = path.stem   # filename without .txt = lead_id convention
        for i, chunk in enumerate(chunk_text(text)):
            docs.append({
                "id":      f"transcript_{lead_id}_{i}",
                "text":    chunk,
                "source":  "transcript",
                "lead_id": lead_id,
                "chunk_i": i,
            })

    print(f"[embedder] loaded {len(docs)} transcript chunks from {TRANSCRIPTS_DIR}")
    return docs


def load_leads_text() -> list[dict]:
    """
    Pull free-text fields from leads_final in DuckDB.
    Embeds notes/descriptions — NOT numeric fields (those stay as SQL).
    """
    docs = []
    try:
        con = duckdb.connect(DUCKDB_PATH, read_only=True)

        # adjust column names to match your actual leads_final schema
        rows = con.execute("""
            SELECT
                lead_id,
                COALESCE(notes, '')        AS notes,
                COALESCE(description, '')  AS description
            FROM leads_final
            WHERE LENGTH(COALESCE(notes, '') || COALESCE(description, '')) > 20
        """).fetchall()
        con.close()

        for lead_id, notes, description in rows:
            combined = f"{notes} {description}".strip()
            for i, chunk in enumerate(chunk_text(combined)):
                docs.append({
                    "id":      f"lead_{lead_id}_note_{i}",
                    "text":    chunk,
                    "source":  "lead_notes",
                    "lead_id": str(lead_id),
                    "chunk_i": i,
                })

        print(f"[embedder] loaded {len(docs)} lead note chunks from DuckDB")
    except Exception as e:
        print(f"[embedder] WARNING: could not load leads from DuckDB — {e}")

    return docs


# ── main indexing function ─────────────────────────────────────────────────────
def build_index(force_rebuild: bool = False) -> chromadb.Collection:
    """
    Full indexing pipeline:
      1. Load text from transcripts + DuckDB
      2. Embed via sentence-transformers (local, no API calls)
      3. Upsert into ChromaDB (idempotent — safe to re-run)
    """
    print(f"[embedder] initialising model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)

    print(f"[embedder] connecting to ChromaDB at {CHROMA_DIR}")
    client = chromadb.PersistentClient(path=CHROMA_DIR)

    if force_rebuild:
        try:
            client.delete_collection(COLLECTION_NAME)
            print(f"[embedder] deleted existing collection for rebuild")
        except Exception:
            pass

    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},   # cosine similarity for semantic search
    )

    # gather all documents
    docs = load_transcripts() + load_leads_text()

    if not docs:
        print("[embedder] WARNING: no documents found — index is empty")
        return collection

    # embed in batches (memory-efficient)
    BATCH = 64
    for i in range(0, len(docs), BATCH):
        batch = docs[i : i + BATCH]
        texts     = [d["text"]    for d in batch]
        ids       = [d["id"]      for d in batch]
        metadatas = [{k: v for k, v in d.items() if k != "text"} for d in batch]

        embeddings = model.encode(texts, show_progress_bar=False).tolist()

        collection.upsert(
            ids=ids,
            documents=texts,
            embeddings=embeddings,
            metadatas=metadatas,
        )
        print(f"[embedder] upserted batch {i // BATCH + 1} ({len(batch)} chunks)")

    print(f"[embedder] index complete — {collection.count()} chunks in ChromaDB")
    return collection


if __name__ == "__main__":
    build_index()
