import os
import time
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import Json

DSN = os.getenv("DATABASE_URL", "postgresql://dfs:dfs@postgres:5432/dfs")

SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    bucket TEXT, key TEXT, version INT, size BIGINT, chunking TEXT,
    chunks JSONB, created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (bucket, key, version)
);
CREATE TABLE IF NOT EXISTS chunk_refs (
    hash TEXT PRIMARY KEY, size INT, refcount INT NOT NULL, nodes JSONB
);
"""


def _connect():
    for _ in range(30):  # wait for postgres to start
        try:
            return psycopg2.connect(DSN)
        except psycopg2.OperationalError:
            time.sleep(1)
    raise RuntimeError("cannot reach postgres")


@contextmanager
def cursor():
    conn = _connect()
    try:
        with conn:
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def init():
    with cursor() as cur:
        cur.execute(SCHEMA)


def add_ref(h, size, nodes):
    """Insert chunk with refcount 1, or increment. Returns the new refcount."""
    with cursor() as cur:
        cur.execute(
            """INSERT INTO chunk_refs (hash, size, refcount, nodes) VALUES (%s, %s, 1, %s)
               ON CONFLICT (hash) DO UPDATE SET refcount = chunk_refs.refcount + 1
               RETURNING refcount""", (h, size, Json(nodes)))
        return cur.fetchone()[0]


def release(h):
    """Decrement refcount. Row is removed at 0. Returns (refcount, nodes) or None."""
    with cursor() as cur:
        cur.execute("UPDATE chunk_refs SET refcount = refcount - 1 WHERE hash = %s "
                    "RETURNING refcount, nodes", (h,))
        row = cur.fetchone()
        if row and row[0] <= 0:
            cur.execute("DELETE FROM chunk_refs WHERE hash = %s", (h,))
        return row


def chunk_nodes(h):
    with cursor() as cur:
        cur.execute("SELECT nodes FROM chunk_refs WHERE hash = %s", (h,))
        row = cur.fetchone()
        return row[0] if row else []


def add_object(bucket, key, size, mode, chunks):
    with cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(version), 0) + 1 FROM objects "
                    "WHERE bucket = %s AND key = %s", (bucket, key))
        version = cur.fetchone()[0]
        cur.execute("INSERT INTO objects (bucket, key, version, size, chunking, chunks) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (bucket, key, version, size, mode, Json(chunks)))
        return version


def get_object(bucket, key, version=None):
    with cursor() as cur:
        if version is None:
            cur.execute("SELECT version, size, chunks FROM objects WHERE bucket = %s AND key = %s "
                        "ORDER BY version DESC LIMIT 1", (bucket, key))
        else:
            cur.execute("SELECT version, size, chunks FROM objects WHERE bucket = %s AND key = %s "
                        "AND version = %s", (bucket, key, version))
        return cur.fetchone()


def delete_object(bucket, key):
    """Delete all versions of the object. Returns the chunk hashes they referenced."""
    with cursor() as cur:
        cur.execute("DELETE FROM objects WHERE bucket = %s AND key = %s RETURNING chunks",
                    (bucket, key))
        return [h for row in cur.fetchall() for h in row[0]]


def stats():
    with cursor() as cur:
        cur.execute("SELECT count(*), COALESCE(sum(size), 0), "
                    "COALESCE(sum(size::bigint * refcount), 0) FROM chunk_refs")
        unique, unique_bytes, logical_bytes = cur.fetchone()
        return {"unique_chunks": unique, "unique_bytes": int(unique_bytes),
                "logical_bytes": int(logical_bytes),
                "dedup_ratio": round(logical_bytes / unique_bytes, 2) if unique_bytes else None}