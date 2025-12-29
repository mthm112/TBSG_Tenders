#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tbsg_embed_v3.py

Generate OpenAI embeddings for rows in a Postgres/Supabase table where the embedding column is NULL.

✅ Designed for TBSG product master:
- public.product_master
  - pk: typically "id"
  - text columns (defaults): description, code, range, group, subgroup, manufacturer

Also supports the same generic targets as your Fenns script (kb_chunks, faq, contracts, customer_rules),
but with updated defaults to include product_master.

Environment variables required:
- SUPABASE_DB_URL (preferred)  OR  PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD
- OPENAI_API_KEY

Optional env vars:
- OPENAI_EMBED_MODEL (default: text-embedding-3-small)
- EMBED_BATCH_SIZE (default: 100)
- EMBED_SLEEP (default: 0.1)
- EMBED_FETCH_SIZE (default: 1000)

Usage:
  python tbsg_embed_v3.py --target product_master
  python tbsg_embed_v3.py --target product_master --schema public --batch-size 100 --sleep 0.05
"""

import os
import time
import argparse
from typing import List, Dict, Tuple, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import sql

# OpenAI SDK compatibility (new + legacy)
_OPENAI_STYLE = "unknown"
try:
    from openai import OpenAI  # type: ignore
    _OPENAI_STYLE = "client"
except Exception:
    import openai  # type: ignore
    _OPENAI_STYLE = "module"

# Default text columns per table
DEFAULTS: Dict[str, List[str]] = {
    # ✅ TBSG
    "product_master": ["description", "code", "range", "group", "subgroup", "manufacturer"],
    # Some teams name it "products_master"
    "products_master": ["description", "code", "range", "group", "subgroup", "manufacturer"],

    # Generic (kept for parity with your Fenns workflow)
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
    Preferred: SUPABASE_DB_URL (a full postgres connection string)
    Fallback: PG* env vars
    """
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)

    # Fallback to PG* vars
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

def column_exists(conn, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None

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

    # Prefer table-specific defaults if present
    if table in DEFAULTS:
        picked = existing_subset(cols, DEFAULTS[table])
        if picked:
            return picked

    # Otherwise embed all text columns
    picked = all_text_columns(conn, schema, table)
    if picked:
        return picked

    raise RuntimeError(f"No text/varchar columns found for {schema}.{table}")

def find_pk(conn, schema: str, table: str) -> str:
    """
    Find the primary key column. If none, fall back to 'id' if present, else error.
    """
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

    raise RuntimeError(f"Could not detect primary key for {schema}.{table} (no PK and no id column).")

def detect_embedding_col(conn, schema: str, table: str) -> str:
    """
    Prefer 'embedding' if present; otherwise allow 'embeddings' or 'vector' if present.
    """
    cols = table_columns(conn, schema, table)
    lower = {c.lower(): c for c in cols}
    for cand in ["embedding", "embeddings", "vector"]:
        if cand in lower:
            return lower[cand]
    raise RuntimeError(
        f"No embedding column found for {schema}.{table}. Expected a column named embedding/embeddings/vector."
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
):
    """
    Yield (pk_value, concatenated_text) for rows where embedding is NULL.
    Uses fetchmany() to avoid server cursor timeouts.
    """
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

def embed_batch(client, model: str, texts: List[str]) -> List[List[float]]:
    if _OPENAI_STYLE == "client":
        res = client.embeddings.create(model=model, input=texts)
        return [d.embedding for d in res.data]
    else:
        res = openai.Embedding.create(model=model, input=texts)  # type: ignore
        return [d["embedding"] for d in res["data"]]

def print_progress(processed: int, total: int, start_time: float, failed: int = 0):
    elapsed = time.time() - start_time
    rate = processed / elapsed if elapsed > 0 else 0.0
    remaining = max(total - processed, 0)
    eta = remaining / rate if rate > 0 else float("inf")
    eta_mins = eta / 60 if eta != float("inf") else float("inf")

    if total > 0:
        pct = 100 * processed / total
        print(
            f"Processed: {processed:,}/{total:,} ({pct:.1f}%) | Failed: {failed:,} | "
            f"Rate: {rate:.2f} rows/s | ETA: {eta_mins:.1f} min",
            flush=True,
        )
    else:
        print(f"Processed: {processed:,} | Failed: {failed:,} | Rate: {rate:.2f} rows/s", flush=True)

def update_embeddings(
    conn,
    schema: str,
    table: str,
    pk: str,
    embedding_col: str,
    ids: List,
    vectors: List[List[float]],
):
    """
    Update embeddings using executemany for speed.
    """
    with conn.cursor() as cur:
        q = sql.SQL("UPDATE {tbl} SET {emb} = %s WHERE {pk} = %s").format(
            tbl=sql.Identifier(schema, table),
            emb=sql.Identifier(embedding_col),
            pk=sql.Identifier(pk),
        )
        params = [(vec, rid) for rid, vec in zip(ids, vectors)]
        psycopg2.extras.execute_batch(cur, q.as_string(conn), params, page_size=200)
    conn.commit()

def main():
    parser = argparse.ArgumentParser(description="Generate embeddings for rows where embedding is NULL.")
    parser.add_argument("--target", required=True, help="Table name (e.g., product_master)")
    parser.add_argument("--schema", default="public", help="Schema name (default: public)")
    parser.add_argument("--model", default=os.environ.get("OPENAI_EMBED_MODEL", "text-embedding-3-small"))
    parser.add_argument("--batch-size", type=int, default=env_int("EMBED_BATCH_SIZE", 100))
    parser.add_argument("--sleep", type=float, default=env_float("EMBED_SLEEP", 0.1), help="Seconds to sleep between batches")
    parser.add_argument("--max-rows", type=int, default=0, help="Limit rows (0 = no limit)")
    parser.add_argument("--fetch-size", type=int, default=env_int("EMBED_FETCH_SIZE", 1000), help="DB fetchmany size")
    parser.add_argument("--progress-interval", type=int, default=20, help="Print progress every N batches")

    args = parser.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("Missing OPENAI_API_KEY")

    if _OPENAI_STYLE == "client":
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    else:
        openai.api_key = os.environ["OPENAI_API_KEY"]  # type: ignore
        client = None  # type: ignore

    table = args.target
    schema = args.schema

    conn = connect_db()
    try:
        pk = find_pk(conn, schema, table)
        embedding_col = detect_embedding_col(conn, schema, table)
        text_cols = detect_text_columns(conn, schema, table)

        total = count_remaining_rows(conn, schema, table, embedding_col)
        if args.max_rows > 0:
            total = min(total, args.max_rows)

        print(f"Target: {schema}.{table}")
        print(f"PK: {pk} | Embedding column: {embedding_col}")
        print(f"Text columns used: {text_cols}")
        print(f"Rows remaining: {total:,}")
        print(f"Model: {args.model} | Batch size: {args.batch_size} | Sleep: {args.sleep}s")
        print("----", flush=True)

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
        ):
            batch_ids.append(rid)
            batch_texts.append(text)

            if len(batch_ids) >= args.batch_size:
                batch_num += 1
                try:
                    vectors = embed_batch(client, args.model, batch_texts)
                    update_embeddings(conn, schema, table, pk, embedding_col, batch_ids, vectors)
                    processed += len(batch_ids)
                except Exception as e:
                    failed += len(batch_ids)
                    # keep going, but print the error
                    print(f"⚠️ Batch {batch_num} failed: {e}", flush=True)

                batch_ids, batch_texts = [], []

                if batch_num % args.progress_interval == 0:
                    print_progress(processed, total, start, failed)

                if args.sleep > 0:
                    time.sleep(args.sleep)

        # Flush remainder
        if batch_ids:
            batch_num += 1
            try:
                vectors = embed_batch(client, args.model, batch_texts)
                update_embeddings(conn, schema, table, pk, embedding_col, batch_ids, vectors)
                processed += len(batch_ids)
            except Exception as e:
                failed += len(batch_ids)
                print(f"⚠️ Final batch failed: {e}", flush=True)

        print_progress(processed, total, start, failed)
        print("✅ Done.", flush=True)

    finally:
        conn.c
