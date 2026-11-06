import hashlib
import os
import threading
import time

import requests
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from metadata_service import db, ec
from metadata_service.chunking import split
from metadata_service.ring import Ring

app = FastAPI(title="Metadata service")
nodes = {}
DEAD_AFTER = 15
FIXED_SIZE = int(os.getenv("FIXED_CHUNK_SIZE", str(4 * 1024 * 1024)))

# Replication: N copies, write quorum W, read quorum R (W + R > N)
N = int(os.getenv("REPLICAS", "3"))
W = int(os.getenv("WRITE_QUORUM", "2"))
R = int(os.getenv("READ_QUORUM", "2"))
# Erasure coding: K data shards + M parity shards (K+M must be <= number of nodes)
K = int(os.getenv("EC_K", "2"))
M = int(os.getenv("EC_M", "2"))

CHECK_EVERY = 5     # seconds between failure checks
SCRUB_EVERY = int(os.getenv("SCRUB_EVERY", "60"))
traffic = {"read": 0, "written": 0}            # bytes moved by the repair job only (not scrub, not users)
ctx = threading.local()                        # ctx.counting is True only inside the repair thread
repair_stats = {"repaired": 0, "scrub_checked": 0, "scrub_fixed": 0}


@app.on_event("startup")
def startup():
    db.init()
    threading.Thread(target=repair_loop, daemon=True).start()
    threading.Thread(target=scrub_loop, daemon=True).start()


# ---------- heartbeats ----------
class Heartbeat(BaseModel):
    node_id: str
    address: str
    free_space: int
    chunk_count: int
    used_bytes: int = 0


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    nodes[hb.node_id] = {**hb.model_dump(), "last_seen": time.time()}
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    now = time.time()
    return [{**n, "seconds_since_heartbeat": round(now - n["last_seen"], 1),
             "alive": now - n["last_seen"] < DEAD_AFTER} for n in nodes.values()]


# ---------- small helpers ----------
def sha(data):
    return hashlib.sha256(data).hexdigest()


def is_alive(node_id):
    n = nodes.get(node_id)
    return bool(n) and time.time() - n["last_seen"] < DEAD_AFTER


def alive_nodes():
    return [n for n in nodes if is_alive(n)]


def put_to(node, h, data):
    try:
        r = requests.put(f"{nodes[node]['address']}/chunks/{h}", data=data, timeout=10)
    except (requests.RequestException, KeyError):
        return False
    if r.status_code in (200, 201):
        if getattr(ctx, "counting", False):
            traffic["written"] += len(data)
        return True
    return False


def get_from(node, h):
    """Returns the verified bytes stored on `node` under hash h, or None."""
    if not is_alive(node):
        return None
    try:
        r = requests.get(f"{nodes[node]['address']}/chunks/{h}", timeout=10)
    except requests.RequestException:
        return None
    if r.status_code == 200 and sha(r.content) == h:
        if getattr(ctx, "counting", False):
            traffic["read"] += len(r.content)
        return r.content
    return None


def delete_from(node, h):
    if node in nodes:
        try:
            requests.delete(f"{nodes[node]['address']}/chunks/{h}", timeout=5)
        except requests.RequestException:
            pass


def loc_hash(h, shards, i):
    """Hash stored at location i: the chunk itself (replication) or shard i (erasure coding)."""
    return h if shards is None else shards[i]


# ---------- writing chunks ----------
def store_replicas(h, piece, targets):
    ok = sum(put_to(t, h, piece) for t in targets)
    return ok >= W                                     # write quorum


def store_shards(h, piece, targets):
    shards = ec.encode(piece, K, M)
    hashes = [sha(s) for s in shards]
    db.set_shards(h, hashes)
    ok = sum(put_to(t, hashes[i], shards[i]) for i, t in enumerate(targets))
    return ok >= K + 1                                 # data stays safe against 1 more loss


def release_chunks(hashes):
    """Drop one reference per hash. Delete from nodes only when refcount hits 0."""
    for h in hashes:
        row = db.release(h)
        if row and row[0] <= 0:
            _, node_list, shards = row
            for i, t in enumerate(node_list):
                delete_from(t, loc_hash(h, shards, i))


# ---------- reading chunks ----------
def read_chunk(h):
    """Returns the chunk bytes, or None if it cannot be read (not enough good copies/shards)."""
    info = db.chunk_info(h)
    if not info:
        return None
    size, node_list, shards = info
    data = None
    if shards is None:                                 # replication: need R valid copies
        good = 0
        for t in node_list:
            d = get_from(t, h)
            if d is not None:
                data, good = d, good + 1
                if good >= R:
                    break
        if good < R:
            return None
    else:                                              # erasure coding: need K valid shards
        got = {}
        for i, t in enumerate(node_list):
            d = get_from(t, shards[i])
            if d is not None:
                got[i] = d
                if len(got) == K:
                    break
        if len(got) < K:
            return None
        data = ec.decode(got, K, M, size)
    return data if sha(data) == h else None


# ---------- objects ----------
@app.put("/buckets/{bucket}")
def create_bucket(bucket: str, mode: str = "replication"):
    if mode not in ("replication", "ec"):
        raise HTTPException(400, "mode must be 'replication' or 'ec'")
    db.create_bucket(bucket, mode)
    return {"bucket": bucket, "mode": mode}


def store_object(bucket, key, data, mode):
    ec_bucket = db.bucket_mode(bucket) == "ec"
    ring = Ring(alive_nodes())
    if not ring.nodes():
        raise HTTPException(503, "no storage nodes alive")

    chunk_list, new, dup = [], 0, 0
    for piece in split(data, mode, FIXED_SIZE):
        h = sha(piece)
        targets = ring.get_nodes(h, K + M if ec_bucket else N)
        if ec_bucket and len(targets) < K + M:
            release_chunks(chunk_list)
            raise HTTPException(503, f"erasure coding {K}+{M} needs {K + M} alive nodes")
        chunk_list.append(h)
        if db.add_ref(h, len(piece), targets) == 1:    # first time we see this chunk: store it
            store = store_shards if ec_bucket else store_replicas
            if not store(h, piece, targets):
                release_chunks(chunk_list)             # roll back references
                raise HTTPException(503, "write quorum not reached")
            new += 1
        else:                                          # already stored: just a new reference
            dup += 1
    version = db.add_object(bucket, key, len(data), mode, chunk_list)
    return {"bucket": bucket, "key": key, "version": version, "size": len(data),
            "redundancy": "ec" if ec_bucket else "replication", "chunking": mode,
            "chunks": len(chunk_list), "new_chunks": new, "deduplicated_chunks": dup}


@app.put("/objects/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, request: Request, mode: str = "fixed"):
    data = await request.body()
    return await run_in_threadpool(store_object, bucket, key, data, mode)


@app.get("/objects/{bucket}/{key:path}")
def get_object(bucket: str, key: str, version: int | None = None):
    row = db.get_object(bucket, key, version)
    if not row:
        raise HTTPException(404, "object not found")
    out = bytearray()
    for h in row[2]:
        piece = read_chunk(h)
        if piece is None:
            raise HTTPException(500, f"chunk {h[:12]} unavailable")
        out += piece
    return Response(content=bytes(out), media_type="application/octet-stream")


@app.delete("/objects/{bucket}/{key:path}")
def delete_object(bucket: str, key: str):
    hashes = db.delete_object(bucket, key)
    if not hashes:
        raise HTTPException(404, "object not found")
    release_chunks(hashes)
    return {"deleted": key, "chunk_refs_released": len(hashes)}


@app.get("/stats")
def stats():
    return db.stats()


@app.get("/traffic")
def get_traffic():
    return traffic


@app.get("/repair")
def get_repair():
    return repair_stats


# ---------- repair of a failed node ----------
def repair_node(dead):
    """Rebuild every chunk/shard that lived on `dead` onto the remaining nodes."""
    for h in db.chunks_on(dead):
        info = db.chunk_info(h)
        if not info:
            continue
        size, node_list, shards = info
        data = read_chunk(h)
        if data is None:
            continue                                   # not enough good copies right now
        alive = alive_nodes()
        for i, t in enumerate(node_list):
            if t != dead:
                continue
            if shards is None:                         # replication: a node without a copy
                order = Ring(alive).get_nodes(h, len(alive))
                cands = [n for n in order if n not in node_list]
            else:                                      # EC: the node with the fewest shards
                cands = sorted(alive, key=node_list.count)[:1]
            if not cands:
                continue
            piece = data if shards is None else ec.encode(data, K, M)[i]
            if put_to(cands[0], loc_hash(h, shards, i), piece):
                node_list[i] = cands[0]
                repair_stats["repaired"] += 1
        db.set_nodes(h, node_list)


def repair_loop():
    ctx.counting = True                            # only this thread's traffic counts as repair traffic
    while True:
        time.sleep(CHECK_EVERY)
        try:
            for n in list(nodes):
                if not is_alive(n):
                    repair_node(n)
        except Exception as e:
            print("repair error:", e, flush=True)


# ---------- scrubber (bit rot / missing chunks) ----------
def scrub_once():
    for h in db.all_chunks():
        info = db.chunk_info(h)
        if not info:
            continue
        size, node_list, shards = info
        for i, t in enumerate(node_list):
            if not is_alive(t):
                continue
            lh = loc_hash(h, shards, i)
            repair_stats["scrub_checked"] += 1
            if get_from(t, lh) is not None:
                continue                               # healthy
            data = read_chunk(h)                       # rebuild from the healthy copies
            if data is None:
                continue
            piece = data if shards is None else ec.encode(data, K, M)[i]
            delete_from(t, lh)                         # remove the corrupted file
            if put_to(t, lh, piece):
                repair_stats["scrub_fixed"] += 1


@app.post("/scrub")
def scrub_now():
    scrub_once()
    return repair_stats


def scrub_loop():
    while True:
        time.sleep(SCRUB_EVERY)
        try:
            scrub_once()
        except Exception as e:
            print("scrub error:", e, flush=True)