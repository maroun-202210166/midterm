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
ALTER TABLE chunk_refs ADD COLUMN IF NOT EXISTS shards JSONB;
ALTER TABLE objects ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '{}';
CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, mode TEXT NOT NULL);
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
    """Decrement refcount. Row is removed at 0. Returns (refcount, nodes, shards) or None."""
    with cursor() as cur:
        cur.execute("UPDATE chunk_refs SET refcount = refcount - 1 WHERE hash = %s "
                    "RETURNING refcount, nodes, shards", (h,))
        row = cur.fetchone()
        if row and row[0] <= 0:
            cur.execute("DELETE FROM chunk_refs WHERE hash = %s", (h,))
        return row


def chunk_nodes(h):
    with cursor() as cur:
        cur.execute("SELECT nodes FROM chunk_refs WHERE hash = %s", (h,))
        row = cur.fetchone()
        return row[0] if row else []


def chunk_info(h):
    """Returns (size, nodes, shards) or None. shards is None for replicated chunks."""
    with cursor() as cur:
        cur.execute("SELECT size, nodes, shards FROM chunk_refs WHERE hash = %s", (h,))
        return cur.fetchone()


def set_shards(h, shards):
    with cursor() as cur:
        cur.execute("UPDATE chunk_refs SET shards = %s WHERE hash = %s", (Json(shards), h))


def set_nodes(h, nodes):
    with cursor() as cur:
        cur.execute("UPDATE chunk_refs SET nodes = %s WHERE hash = %s", (Json(nodes), h))


def chunks_on(node):
    """Hashes of all chunks that have a copy or shard on this node."""
    with cursor() as cur:
        cur.execute("SELECT hash FROM chunk_refs WHERE nodes @> %s", (Json([node]),))
        return [r[0] for r in cur.fetchall()]


def all_chunks():
    with cursor() as cur:
        cur.execute("SELECT hash FROM chunk_refs")
        return [r[0] for r in cur.fetchall()]


def create_bucket(name, mode):
    with cursor() as cur:
        cur.execute("INSERT INTO buckets (name, mode) VALUES (%s, %s) "
                    "ON CONFLICT (name) DO UPDATE SET mode = %s", (name, mode, mode))


def bucket_mode(name):
    """'replication' or 'ec'. Unknown buckets are replicated."""
    with cursor() as cur:
        cur.execute("SELECT mode FROM buckets WHERE name = %s", (name,))
        row = cur.fetchone()
        return row[0] if row else "replication"


def add_object(bucket, key, size, mode, chunks, tags=None):
    with cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(version), 0) + 1 FROM objects "
                    "WHERE bucket = %s AND key = %s", (bucket, key))
        version = cur.fetchone()[0]
        cur.execute("INSERT INTO objects (bucket, key, version, size, chunking, chunks, tags) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (bucket, key, version, size, mode, Json(chunks), Json(tags or {})))
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


# ---------- Task 5: listing, search, metadata, cluster status ----------
def list_buckets():
    with cursor() as cur:
        cur.execute("SELECT name, mode FROM buckets UNION "
                    "SELECT DISTINCT bucket, 'replication' FROM objects "
                    "WHERE bucket NOT IN (SELECT name FROM buckets) ORDER BY 1")
        return [{"bucket": n, "mode": m} for n, m in cur.fetchall()]


def search_objects(bucket, prefix="", min_size=None, max_size=None, after=None, before=None, tags=None):
    """Latest version of every object in the bucket that matches all the given filters."""
    sql = ("SELECT key, version, size, created_at, tags FROM ("
           " SELECT DISTINCT ON (key) key, version, size, created_at, tags FROM objects"
           " WHERE bucket = %s AND key LIKE %s ORDER BY key, version DESC) latest WHERE true")
    like = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    args = [bucket, like]
    if min_size is not None:
        sql += " AND size >= %s"
        args.append(min_size)
    if max_size is not None:
        sql += " AND size <= %s"
        args.append(max_size)
    if after:
        sql += " AND created_at >= %s"
        args.append(after)
    if before:
        sql += " AND created_at <= %s"
        args.append(before)
    if tags:
        sql += " AND tags @> %s"
        args.append(Json(tags))
    with cursor() as cur:
        cur.execute(sql + " ORDER BY key", args)
        return [{"key": k, "version": v, "size": sz, "created_at": c.isoformat(), "tags": t}
                for k, v, sz, c, t in cur.fetchall()]


def object_meta(bucket, key, version=None):
    q = "SELECT version, size, chunking, chunks, created_at, tags FROM objects WHERE bucket = %s AND key = %s"
    with cursor() as cur:
        if version is None:
            cur.execute(q + " ORDER BY version DESC LIMIT 1", (bucket, key))
        else:
            cur.execute(q + " AND version = %s", (bucket, key, version))
        row = cur.fetchone()
    if not row:
        return None
    v, size, chunking, chunks, created, tags = row
    return {"bucket": bucket, "key": key, "version": v, "size": size, "chunking": chunking,
            "created_at": created.isoformat(), "tags": tags, "chunk_count": len(chunks), "chunks": chunks}


def chunk_sizes(hashes):
    with cursor() as cur:
        cur.execute("SELECT hash, size FROM chunk_refs WHERE hash = ANY(%s)", (list(hashes),))
        return dict(cur.fetchall())


def degraded(dead_nodes):
    """(chunks with a copy/shard on a dead node, objects that use such chunks)."""
    with cursor() as cur:
        cur.execute("SELECT array_agg(hash) FROM chunk_refs WHERE nodes ?| %s", (list(dead_nodes),))
        hashes = cur.fetchone()[0] or []
        if not hashes:
            return 0, 0
        cur.execute("SELECT count(*) FROM objects WHERE chunks ?| %s", (hashes,))
        return len(hashes), cur.fetchone()[0]


def all_latest_objects():
    with cursor() as cur:
        cur.execute("SELECT DISTINCT ON (bucket, key) bucket, key, version, size, created_at, tags "
                    "FROM objects ORDER BY bucket, key, version DESC")
        return [{"bucket": b, "key": k, "version": v, "size": sz, "created_at": c.isoformat(), "tags": t}
                for b, k, v, sz, c, t in cur.fetchall()]


# ---------- Task 7: garbage collection ----------
def _placed_on(rows, node):
    """Hashes placed on `node` by these chunk_refs rows: whole chunks (replication) or shards (EC)."""
    return {h if shards is None else shards[i]
            for h, node_list, shards in rows for i, t in enumerate(node_list) if t == node}


def locations_on(node):
    """Hashes the metadata places on `node`."""
    with cursor() as cur:
        cur.execute("SELECT hash, nodes, shards FROM chunk_refs WHERE nodes @> %s", (Json([node]),))
        return _placed_on(cur.fetchall(), node)


def is_expected(lh, node):
    """True if the chunk or shard `lh` is (now) placed on `node`."""
    with cursor() as cur:
        cur.execute("SELECT hash, nodes, shards FROM chunk_refs WHERE nodes @> %s AND (hash = %s OR shards ? %s)",
                    (Json([node]), lh, lh))
        return lh in _placed_on(cur.fetchall(), node)
