import hashlib
import os
import re
import threading
import time
import urllib.parse
from datetime import datetime, timezone

import requests
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from metadata_service import db, ec, jobs
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

ELASTIC_URL = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
CHECK_EVERY = 5     # seconds between failure checks
SCRUB_EVERY = int(os.getenv("SCRUB_EVERY", "60"))
GC_EVERY = int(os.getenv("GC_EVERY", "120"))
traffic = {"read": 0, "written": 0}            # bytes moved by the repair job only (not scrub, not users)
ctx = threading.local()                        # ctx.counting is True only inside the repair thread
repair_stats = {"repaired": 0, "scrub_checked": 0, "scrub_fixed": 0, "rebalanced": 0, "gc_deleted": 0}


@app.on_event("startup")
def startup():
    db.init()
    if jobs.ENABLED:                           # Task 7: the workers do the jobs, this service only plans them
        threading.Thread(target=monitor_loop, daemon=True).start()
    else:                                      # Tasks 4-6: this service does the jobs itself
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


def parse_tags(tag_list):
    """['env:prod', 'team:a'] -> {'env': 'prod', 'team': 'a'}"""
    tags = {}
    for t in tag_list:
        if ":" not in t:
            raise HTTPException(400, f"tag '{t}' must look like key:value")
        k, v = t.split(":", 1)
        tags[k] = v
    return tags


# uploads are handled piece by piece, so a large file never has to fit in memory
def new_upload(bucket):
    ring = Ring(alive_nodes())
    if not ring.nodes():
        raise HTTPException(503, "no storage nodes alive")
    return {"ec": db.bucket_mode(bucket) == "ec", "ring": ring, "chunks": [], "size": 0, "new": 0, "dup": 0}


def store_piece(up, piece):
    h = sha(piece)
    targets = up["ring"].get_nodes(h, K + M if up["ec"] else N)
    if up["ec"] and len(targets) < K + M:
        release_chunks(up["chunks"])
        raise HTTPException(503, f"erasure coding {K}+{M} needs {K + M} alive nodes")
    up["chunks"].append(h)
    up["size"] += len(piece)
    if db.add_ref(h, len(piece), targets) == 1:        # first time we see this chunk: store it
        store = store_shards if up["ec"] else store_replicas
        if not store(h, piece, targets):
            release_chunks(up["chunks"])               # roll back references
            raise HTTPException(503, "write quorum not reached")
        up["new"] += 1
    else:                                              # already stored: just a new reference
        up["dup"] += 1


def finish_upload(up, bucket, key, mode, tags):
    version = db.add_object(bucket, key, up["size"], mode, up["chunks"], tags)
    es_index(bucket, key, version, up["size"], datetime.now(timezone.utc).isoformat(), tags)
    return {"bucket": bucket, "key": key, "version": version, "size": up["size"],
            "redundancy": "ec" if up["ec"] else "replication", "chunking": mode, "tags": tags,
            "chunks": len(up["chunks"]), "new_chunks": up["new"], "deduplicated_chunks": up["dup"]}


@app.put("/objects/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, request: Request, mode: str = "fixed",
                     tag: list[str] = Query([])):
    tags = parse_tags(tag)
    up = await run_in_threadpool(new_upload, bucket)
    buf = bytearray()
    async for part in request.stream():                # data arrives in small parts
        buf += part
        while mode != "cdc" and len(buf) >= FIXED_SIZE:   # fixed-size: store each full chunk now
            piece = bytes(buf[:FIXED_SIZE])
            del buf[:FIXED_SIZE]
            await run_in_threadpool(store_piece, up, piece)
    for piece in split(bytes(buf), mode, FIXED_SIZE):  # the rest (everything, for cdc)
        await run_in_threadpool(store_piece, up, piece)
    return await run_in_threadpool(finish_upload, up, bucket, key, mode, tags)


@app.get("/objects/{bucket}/{key:path}")
def get_object(bucket: str, key: str, request: Request, version: int | None = None):
    row = db.get_object(bucket, key, version)
    if not row:
        raise HTTPException(404, "object not found")
    size, hashes = row[1], row[2]
    start, end = 0, size - 1
    header = request.headers.get("range")
    if header:                                         # byte-range request: bytes=a-b, a- or -n
        m = re.fullmatch(r"bytes=(\d*)-(\d*)", header.strip())
        if not m or not (m[1] or m[2]):
            raise HTTPException(400, "invalid Range header")
        if not m[1]:
            start = max(size - int(m[2]), 0)
        else:
            start = int(m[1])
            end = min(int(m[2]), size - 1) if m[2] else size - 1
        if start > end:
            raise HTTPException(416, "range not satisfiable", headers={"Content-Range": f"bytes */{size}"})

    sizes = db.chunk_sizes(hashes)
    out, pos = bytearray(), 0
    for h in hashes:                                   # only read the chunks that overlap the range
        if pos > end:
            break
        if pos + sizes[h] > start:
            piece = read_chunk(h)
            if piece is None:
                raise HTTPException(500, f"chunk {h[:12]} unavailable")
            out += piece[max(start - pos, 0):end - pos + 1]
        pos += sizes[h]
    headers = {"Accept-Ranges": "bytes"}
    if header:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return Response(content=bytes(out), status_code=206 if header else 200,
                    media_type="application/octet-stream", headers=headers)


@app.delete("/objects/{bucket}/{key:path}")
def delete_object(bucket: str, key: str):
    hashes = db.delete_object(bucket, key)
    if not hashes:
        raise HTTPException(404, "object not found")
    es_delete(bucket, key)
    release_chunks(hashes)
    return {"deleted": key, "chunk_refs_released": len(hashes)}


@app.get("/buckets")
def list_buckets():
    return db.list_buckets()


@app.get("/buckets/{bucket}/objects")
def search_objects(bucket: str, prefix: str = "", min_size: int | None = None, max_size: int | None = None,
                   after: str | None = None, before: str | None = None, tag: list[str] = Query([])):
    found = db.search_objects(bucket, prefix, min_size, max_size, after, before, parse_tags(tag))
    return {"bucket": bucket, "count": len(found), "objects": found}


@app.get("/metadata/{bucket}/{key:path}")
def object_metadata(bucket: str, key: str, version: int | None = None):
    meta = db.object_meta(bucket, key, version)
    if not meta:
        raise HTTPException(404, "object not found")
    return meta


@app.get("/cluster")
def cluster():
    dead = [n for n in nodes if not is_alive(n)]
    chunks, objects = db.degraded(dead) if dead else (0, 0)
    return {"nodes": [{"node_id": n["node_id"], "alive": is_alive(n["node_id"]), "free_space": n["free_space"],
                       "chunk_count": n["chunk_count"], "used_bytes": n["used_bytes"]} for n in nodes.values()],
            "alive_nodes": len(nodes) - len(dead), "dead_nodes": len(dead),
            "degraded_chunks": chunks, "degraded_objects": objects, "repair": repair_stats}


# ---------- Elasticsearch: search over the object metadata (best effort) ----------
def es_id(bucket, key):
    return urllib.parse.quote(f"{bucket}/{key}", safe="")


def es_index(bucket, key, version, size, created_at, tags, refresh=True):
    doc = {"bucket": bucket, "key": key, "version": version, "size": size, "created_at": created_at,
           "tags": tags, "tags_text": " ".join(f"{k}:{v}" for k, v in tags.items())}
    try:
        requests.put(f"{ELASTIC_URL}/objects/_doc/{es_id(bucket, key)}",
                     params={"refresh": "true"} if refresh else {}, json=doc, timeout=3)
    except requests.RequestException:
        pass                                           # search is optional: never fail an upload because of it


def es_delete(bucket, key):
    try:
        requests.delete(f"{ELASTIC_URL}/objects/_doc/{es_id(bucket, key)}", params={"refresh": "true"}, timeout=3)
    except requests.RequestException:
        pass


@app.get("/search")
def search(q: str, size: int = 50):
    """Text search on bucket name, object key and tags (key:value), done by Elasticsearch."""
    value = {"value": f"*{q}*", "case_insensitive": True}
    body = {"size": size, "sort": [{"key.keyword": "asc"}],
            "query": {"bool": {"minimum_should_match": 1, "should": [
                {"wildcard": {"key.keyword": value}},
                {"wildcard": {"bucket.keyword": value}},
                {"wildcard": {"tags_text.keyword": value}}]}}}
    try:
        r = requests.post(f"{ELASTIC_URL}/objects/_search", json=body, timeout=5)
    except requests.RequestException:
        raise HTTPException(503, "Elasticsearch unavailable")
    if r.status_code == 404:                           # nothing has been indexed yet
        return {"query": q, "count": 0, "objects": []}
    if r.status_code != 200:
        raise HTTPException(502, f"Elasticsearch error {r.status_code}")
    found = [h["_source"] for h in r.json()["hits"]["hits"]]
    fields = ("bucket", "key", "version", "size", "created_at", "tags")
    return {"query": q, "count": len(found), "objects": [{f: o.get(f) for f in fields} for o in found]}


@app.post("/search/reindex")
def reindex():
    """Index again every object already stored (for objects uploaded before Elasticsearch was running)."""
    objects = db.all_latest_objects()
    for o in objects:
        es_index(o["bucket"], o["key"], o["version"], o["size"], o["created_at"], o["tags"], refresh=False)
    try:
        requests.post(f"{ELASTIC_URL}/objects/_refresh", timeout=5)
    except requests.RequestException:
        raise HTTPException(503, "Elasticsearch unavailable")
    return {"indexed": len(objects)}


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
def repair_chunk(h, dead):
    """Rebuild the copy/shard of chunk h that lived on `dead` onto another node. Returns how many were rebuilt."""
    info = db.chunk_info(h)
    if not info or dead not in info[1]:
        return 0                                       # chunk deleted, or already repaired
    size, node_list, shards = info
    data = read_chunk(h)
    if data is None:
        return 0                                       # not enough good copies right now
    alive = alive_nodes()
    rebuilt = 0
    for i, t in enumerate(node_list):
        if t != dead:
            continue
        order = Ring(alive).get_nodes(h, len(alive))   # alive nodes, in ring order for this chunk
        if shards is None:                             # replication: a node without a copy
            cands = [n for n in order if n not in node_list]
        else:                                          # EC: the node with the fewest shards (ring order on ties)
            cands = sorted(order, key=node_list.count)[:1]
        if not cands:
            continue
        piece = data if shards is None else ec.encode(data, K, M)[i]
        if put_to(cands[0], loc_hash(h, shards, i), piece):
            node_list[i] = cands[0]
            rebuilt += 1
    db.set_nodes(h, node_list)
    return rebuilt


def repair_node(dead):
    """Rebuild every chunk/shard that lived on `dead` onto the remaining nodes."""
    for h in db.chunks_on(dead):
        repair_stats["repaired"] += repair_chunk(h, dead)


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
def scrub_chunk(h):
    """Re-read every copy/shard of chunk h and rewrite the bad ones. Returns (checked, fixed)."""
    info = db.chunk_info(h)
    if not info:
        return 0, 0
    size, node_list, shards = info
    checked = fixed = 0
    for i, t in enumerate(node_list):
        if not is_alive(t):
            continue
        lh = loc_hash(h, shards, i)
        checked += 1
        if get_from(t, lh) is not None:
            continue                                   # healthy
        data = read_chunk(h)                           # rebuild from the healthy copies
        if data is None:
            continue
        piece = data if shards is None else ec.encode(data, K, M)[i]
        delete_from(t, lh)                             # remove the corrupted file
        if put_to(t, lh, piece):
            fixed += 1
    return checked, fixed


def scrub_once():
    for h in db.all_chunks():
        checked, fixed = scrub_chunk(h)
        repair_stats["scrub_checked"] += checked
        repair_stats["scrub_fixed"] += fixed


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


# ---------- Task 7: rebalancing and garbage collection ----------
def rebalance_chunk(h, new):
    """If the ring now places chunk h on the new node, move one copy/shard there. Returns 1 if moved."""
    info = db.chunk_info(h)
    if not info:
        return 0
    size, node_list, shards = info
    alive = alive_nodes()
    if new in node_list or new not in alive or any(t not in alive for t in node_list):
        return 0                                       # already there, or a node is down (that is the repair's job)
    want = Ring(alive).get_nodes(h, len(node_list))
    if new not in want:
        return 0                                       # the ring does not place this chunk on the new node
    for i, t in enumerate(node_list):
        if t in want and node_list.index(t) == i:
            continue                                   # this node keeps its piece
        lh = loc_hash(h, shards, i)
        piece = get_from(t, lh)
        if piece is None or not put_to(new, lh, piece):
            return 0
        node_list[i] = new
        db.set_nodes(h, node_list)
        delete_from(t, lh)                             # the old node no longer needs it
        return 1
    return 0


def gc_node(node):
    """Delete the files on `node` that the metadata does not place there
    (old copies kept by a node that came back, or chunks of deleted objects). Returns how many."""
    if not is_alive(node):
        return 0
    try:
        stored = requests.get(f"{nodes[node]['address']}/chunks", timeout=30).json()
    except (requests.RequestException, KeyError, ValueError):
        return 0
    expected = db.locations_on(node)
    deleted = 0
    for lh in stored:
        if lh not in expected and not db.is_expected(lh, node):   # checked again just before deleting
            delete_from(node, lh)
            deleted += 1
    return deleted


# ---------- Task 7: jobs for the workers, through RabbitMQ ----------
node_state = {}            # node id -> True (alive) or False (dead), as last seen by the monitor
queue_status = {"waiting": 0}
jobs_done = {}             # (worker, job type) -> number of jobs done


def monitor_loop():
    """Watch the nodes and put jobs in the queue. The workers take them and do the work."""
    last_scrub = last_gc = time.time()
    while True:
        time.sleep(CHECK_EVERY)
        try:
            todo = {}                                  # dict, so the same job is never sent twice in one round

            def add(kind, h=None, node=None):
                todo[(kind, h, node)] = {"type": kind, "hash": h, "node": node}

            for n in list(nodes):
                alive, before = is_alive(n), node_state.get(n)
                if before is True and not alive:       # node just failed: rebuild its chunks
                    for h in db.chunks_on(n):
                        add("repair", h, n)
                if before is not True and alive and not db.chunks_on(n):
                    for h in db.all_chunks():          # new (or emptied) node joined: move its share to it
                        add("rebalance", h, n)
                node_state[n] = alive
            if jobs.waiting() == 0:                    # queue empty: retries and periodic jobs
                for n in list(nodes):
                    if not is_alive(n):
                        for h in db.chunks_on(n):
                            add("repair", h, n)
                if time.time() - last_gc > GC_EVERY:
                    for n in alive_nodes():
                        add("gc", node=n)
                    last_gc = time.time()
                if time.time() - last_scrub > SCRUB_EVERY:
                    for h in db.all_chunks():
                        add("scrub", h)
                    last_scrub = time.time()
            jobs.publish(list(todo.values()))
            queue_status["waiting"] = jobs.waiting()
        except Exception as e:
            print("monitor error:", e, flush=True)


class JobReport(BaseModel):
    worker: str
    counts: dict[str, int] = {}                        # e.g. {"repaired": 3}
    jobs: dict[str, int] = {}                          # e.g. {"repair": 3}


@app.post("/jobs/report")
def job_report(r: JobReport):
    """The workers send their results here, so the UI and Grafana show them."""
    for k, v in r.counts.items():
        repair_stats[k] = repair_stats.get(k, 0) + v
    for kind, count in r.jobs.items():
        jobs_done[(r.worker, kind)] = jobs_done.get((r.worker, kind), 0) + count
    return {"ok": True}


@app.get("/jobs")
def jobs_info():
    return {"waiting": queue_status["waiting"],
            "done": [{"worker": w, "type": t, "count": c} for (w, t), c in sorted(jobs_done.items())]}


# ---------- Task 7: metrics for Prometheus ----------
@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    lines = []
    for n in sorted(nodes.values(), key=lambda n: n["node_id"]):
        label = f'{{node="{n["node_id"]}"}}'
        lines += [f"storage_node_up{label} {int(is_alive(n['node_id']))}",
                  f"storage_node_used_bytes{label} {n['used_bytes']}",
                  f"storage_node_free_bytes{label} {n['free_space']}",
                  f"storage_node_chunks{label} {n['chunk_count']}"]
    dead = [n for n in nodes if not is_alive(n)]
    chunks, objects = db.degraded(dead) if dead else (0, 0)
    lines += [f"degraded_chunks {chunks}", f"degraded_objects {objects}",
              f"chunks_repaired_total {repair_stats['repaired']}",
              f"chunks_rebalanced_total {repair_stats['rebalanced']}",
              f"gc_deleted_total {repair_stats['gc_deleted']}",
              f"scrub_fixed_total {repair_stats['scrub_fixed']}",
              f"jobs_waiting {queue_status['waiting']}"]
    for (w, t), c in sorted(jobs_done.items()):
        lines.append(f'jobs_done_total{{worker="{w}",type="{t}"}} {c}')
    return "\n".join(lines) + "\n"
