#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tbsg_embed_v3.py - FIXED VERSION

Generate OpenAI embeddings for rows in a Postgres/Supabase table where the embedding column is NULL.

✅ FIXES:
- Fixed incomplete conn.close() bug
- Added retry logic with exponential backoff
- Added checkpoint/resume capability
- Better error handling and logging
- Increased default batch size to 200
- Added progress file for resume capability

Environment variables required:
- SUPABASE_DB_URL (preferred) OR PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD
- OPENAI_API_KEY

Optional env vars:
- OPENAI_EMBED_MODEL (default: text-embedding-3-small)
- EMBED_BATCH_SIZE (default: 200)
- EMBED_SLEEP (default: 0.01)
- EMBED_FETCH_SIZE (default: 1000)
- MAX_RETRIES (default: 3)

Usage:
  python tbsg_embed_v3.py --target product_master
  python tbsg_embed_v3.py --target product_master --resume
"""

import os
import sys
import time
import json
import argparse
from typing import List, Dict, Tuple, Optional
from pathlib import Path

import psycopg2
import psycopg2.extras
from psycopg2 import sql

# OpenAI SDK compatibility (new + legacy)
_OPENAI_STYLE = "unknown"
try:
    from openai import OpenAI
    _OPENAI_STYLE = "client"
except Exception:
    import openai
    _OPENAI_STYLE = "module"

# Default text columns per table
DEFAULTS: Dict[str, List[str]] = {
    "product_master": ["description", "code", "range", "group", "subgroup", "manufacturer"],
    "products_master": ["description", "code", "range", "group", "subgroup", "manufacturer"],
    "products": ["Description", "Product Code", "Category", "Brand", "Long Description", "Details"],
    "kb_chunks": ["content", "text", "title", "source", "url"],
    "faq": ["question", "answer"],
    "contracts": ["searchable_text"],
    "customer_rules": ["searchable_text"],
}

def env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default

def connect_db() -> psycopg2.extensions.connection:
    """
    Preferred: SUPABASE_DB_URL or DATABASE_URL
    Fallback: PG* env vars
    """
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)

    host = os.environ["PGHOST"]
    port = int(os.environ.get("PGPORT", "5432"))
    dbname = os.environ["PGDATABASE"]
    user = os.environ["PGUSER"]
    password = os.environ["PGPASSWORD"]
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)

def table_columns(conn, schema: str, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]

def all_text_columns(conn, schema: str, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
              AND data_type IN ('text','character varying')
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]

def existing_subset(all_cols: List[str], preferred: List[str]) -> List[str]:
    lower_map = {c.lower(): c for c in all_cols}
    picked: List[str] = []
    for p in preferred:
        c = lower_map.get(p.lower())
        if c:
            picked.append(c)
    return picked

def detect_text_columns(conn, schema: str, table: str) -> List[str]:
    cols = table_columns(conn, schema, table)

    if table in DEFAULTS:
        picked = existing_subset(cols, DEFAULTS[table])
        if picked:
            return picked

    picked = all_text_columns(conn, schema, table)
    if picked:
        return picked

    raise RuntimeError(f"No text/varchar columns found for {schema}.{table}")

def find_pk(conn, schema: str, table: str) -> str:
    """Find the primary key column."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            """,
            (schema, table),
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0]

    cols = table_columns(conn, schema, table)
    if any(c.lower() == "id" for c in cols):
        return next(c for c in cols if c.lower() == "id")

    raise RuntimeError(f"Could not detect primary key for {schema}.{table}")

def detect_embedding_col(conn, schema: str, table: str) -> str:
    """Find embedding column."""
    cols = table_columns(conn, schema, table)
    lower = {c.lower(): c for c in cols}
    for cand in ["embedding", "embeddings", "vector"]:
        if cand in lower:
            return lower[cand]
    raise RuntimeError(
        f"No embedding column found for {schema}.{table}. Expected: embedding/embeddings/vector"
    )

def count_remaining_rows(conn, schema: str, table: str, embedding_col: str) -> int:
    with conn.cursor() as cur:
        q = sql.SQL("SELECT COUNT(*) FROM {tbl} WHERE {emb} IS NULL").format(
            tbl=sql.Identifier(schema, table),
            emb=sql.Identifier(embedding_col),
        )
        cur.execute(q)
        return int(cur.fetchone()[0])

def stream_rows(
    conn,
    schema: str,
    table: str,
    pk: str,
    text_cols: List[str],
    embedding_col: str,
    max_rows: int,
    fetch_size: int,
    skip_processed: set = None,
):
    """
    Yield (pk_value, concatenated_text) for rows where embedding is NULL.
    Skip rows in skip_processed set if provided (for resume).
    """
    if skip_processed is None:
        skip_processed = set()

    q = sql.SQL("SELECT {pk}, {cols} FROM {tbl} WHERE {emb} IS NULL ORDER BY {pk}").format(
        pk=sql.Identifier(pk),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in text_cols),
        tbl=sql.Identifier(schema, table),
        emb=sql.Identifier(embedding_col),
    )
    if max_rows > 0:
        q = sql.SQL("{} LIMIT {}").format(q, sql.Literal(max_rows))

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(q)

    while True:
        rows = cur.fetchmany(fetch_size)
        if not rows:
            break

        for row in rows:
            rid = row[pk]
            
            # Skip if already processed
            if rid in skip_processed:
                continue

            parts: List[str] = []
            for c in text_cols:
                val = row[c]
                if val is None:
                    continue
                s = str(val).strip()
                if s:
                    parts.append(s)
            yield rid, " | ".join(parts)

    cur.close()

def embed_batch_with_retry(client, model: str, texts: List[str], max_retries: int = 3) -> List[List[float]]:
    """
    Call OpenAI embedding API with exponential backoff retry logic.
    """
    for attempt in range(max_retries):
        try:
            if _OPENAI_STYLE == "client":
                res = client.embeddings.create(model=model, input=texts)
                return [d.embedding for d in res.data]
            else:
                res = openai.Embedding.create(model=model, input=texts)
                return [d["embedding"] for d in res["data"]]
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"⚠️  API error (attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait}s...", flush=True)
                time.sleep(wait)
            else:
                raise  # Final attempt failed

def print_progress(processed: int, total: int, start_time: float, failed: int = 0):
    elapsed = time.time() - start_time
    rate = processed / elapsed if elapsed > 0 else 0.0
    remaining = max(total - processed, 0)
    eta = remaining / rate if rate > 0 else float("inf")
    eta_mins = eta / 60 if eta != float("inf") else float("inf")

    if total > 0:
        pct = 100 * processed / total
        print(
            f"✅ Processed: {processed:,}/{total:,} ({pct:.1f}%) | ❌ Failed: {failed:,} | "
            f"⚡ Rate: {rate:.2f} rows/s | ⏱️  ETA: {eta_mins:.1f} min",
            flush=True,
        )
    else:
        print(f"✅ Processed: {processed:,} | ❌ Failed: {failed:,} | ⚡ Rate: {rate:.2f} rows/s", flush=True)

def update_embeddings(
    conn,
    schema: str,
    table: str,
    pk: str,
    embedding_col: str,
    ids: List,
    vectors: List[List[float]],
):
    """Update embeddings using executemany for speed."""
    with conn.cursor() as cur:
        q = sql.SQL("UPDATE {tbl} SET {emb} = %s WHERE {pk} = %s").format(
            tbl=sql.Identifier(schema, table),
            emb=sql.Identifier(embedding_col),
            pk=sql.Identifier(pk),
        )
        params = [(vec, rid) for rid, vec in zip(ids, vectors)]
        psycopg2.extras.execute_batch(cur, q.as_string(conn), params, page_size=200)
    conn.commit()

class CheckpointManager:
    """Manage checkpoint file for resume capability."""
    
    def __init__(self, table: str):
        self.checkpoint_file = Path(f".embedding_checkpoint_{table}.json")
        self.data = self._load()
    
    def _load(self) -> dict:
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file) as f:
                    return json.load(f)
            except Exception:
                return {"processed": [], "failed": []}
        return {"processed": [], "failed": []}
    
    def save(self, processed_ids: List, failed_ids: List = None):
        """Save checkpoint to disk."""
        self.data["processed"].extend(processed_ids)
        if failed_ids:
            self.data["failed"].extend(failed_ids)
        
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f)
    
    def get_processed_ids(self) -> set:
        """Return set of already processed IDs."""
        return set(self.data.get("processed", []))
    
    def clear(self):
        """Delete checkpoint file."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

def main():
    parser = argparse.ArgumentParser(description="Generate embeddings with retry logic and checkpointing.")
    parser.add_argument("--target", required=True, help="Table name (e.g., product_master)")
    parser.add_argument("--schema", default="public", help="Schema name (default: public)")
    parser.add_argument("--model", default=os.environ.get("OPENAI_EMBED_MODEL", "text-embedding-3-small"))
    parser.add_argument("--batch-size", type=int, default=env_int("EMBED_BATCH_SIZE", 200))
    parser.add_argument("--sleep", type=float, default=env_float("EMBED_SLEEP", 0.01))
    parser.add_argument("--max-rows", type=int, default=0, help="Limit rows (0 = no limit)")
    parser.add_argument("--fetch-size", type=int, default=env_int("EMBED_FETCH_SIZE", 1000))
    parser.add_argument("--progress-interval", type=int, default=10, help="Print progress every N batches")
    parser.add_argument("--max-retries", type=int, default=env_int("MAX_RETRIES", 3))
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--clear-checkpoint", action="store_true", help="Clear checkpoint and start fresh")

    args = parser.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("Missing OPENAI_API_KEY")

    if _OPENAI_STYLE == "client":
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    else:
        openai.api_key = os.environ["OPENAI_API_KEY"]
        client = None

    table = args.target
    schema = args.schema

    # Checkpoint management
    checkpoint = CheckpointManager(table)
    if args.clear_checkpoint:
        checkpoint.clear()
        print("🗑️  Checkpoint cleared.")
        return
    
    skip_processed = checkpoint.get_processed_ids() if args.resume else set()
    if skip_processed:
        print(f"📂 Resuming: skipping {len(skip_processed):,} already processed rows")

    conn = connect_db()
    try:
        pk = find_pk(conn, schema, table)
        embedding_col = detect_embedding_col(conn, schema, table)
        text_cols = detect_text_columns(conn, schema, table)

        total = count_remaining_rows(conn, schema, table, embedding_col)
        if args.max_rows > 0:
            total = min(total, args.max_rows)

        print(f"🎯 Target: {schema}.{table}")
        print(f"🔑 PK: {pk} | Embedding column: {embedding_col}")
        print(f"📝 Text columns: {text_cols}")
        print(f"📊 Rows remaining: {total:,}")
        print(f"🤖 Model: {args.model} | Batch: {args.batch_size} | Sleep: {args.sleep}s | Retries: {args.max_retries}")
        print("─" * 80, flush=True)

        start = time.time()
        processed = 0
        failed = 0
        batch_num = 0

        batch_ids: List = []
        batch_texts: List[str] = []

        for rid, text in stream_rows(
            conn,
            schema,
            table,
            pk,
            text_cols,
            embedding_col,
            args.max_rows,
            args.fetch_size,
            skip_processed,
        ):
            batch_ids.append(rid)
            batch_texts.append(text)

            if len(batch_ids) >= args.batch_size:
                batch_num += 1
                try:
                    vectors = embed_batch_with_retry(client, args.model, batch_texts, args.max_retries)
                    update_embeddings(conn, schema, table, pk, embedding_col, batch_ids, vectors)
                    processed += len(batch_ids)
                    
                    # Save checkpoint every 10 batches
                    if batch_num % 10 == 0:
                        checkpoint.save(batch_ids)
                    
                except Exception as e:
                    failed += len(batch_ids)
                    print(f"❌ Batch {batch_num} failed after {args.max_retries} retries: {e}", flush=True)
                    checkpoint.save([], batch_ids)  # Save failed IDs

                batch_ids, batch_texts = [], []

                if batch_num % args.progress_interval == 0:
                    print_progress(processed, total, start, failed)

                if args.sleep > 0:
                    time.sleep(args.sleep)

        # Flush remainder
        if batch_ids:
            batch_num += 1
            try:
                vectors = embed_batch_with_retry(client, args.model, batch_texts, args.max_retries)
                update_embeddings(conn, schema, table, pk, embedding_col, batch_ids, vectors)
                processed += len(batch_ids)
                checkpoint.save(batch_ids)
            except Exception as e:
                failed += len(batch_ids)
                print(f"❌ Final batch failed: {e}", flush=True)
                checkpoint.save([], batch_ids)

        print_progress(processed, total, start, failed)
        
        if failed == 0:
            print("✅ All done! Clearing checkpoint.", flush=True)
            checkpoint.clear()
        else:
            print(f"⚠️  {failed:,} rows failed. Run with --resume to retry.", flush=True)

    except Exception as e:
        print(f"❌ Fatal error: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()  # ✅ FIXED: Was incomplete "conn.c"

if __name__ == "__main__":
    main()
