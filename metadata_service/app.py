import hashlib
import os
import time

import requests
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

from metadata_service import db
from metadata_service.chunking import split
from metadata_service.ring import Ring

app = FastAPI(title="Metadata service")
nodes = {}
DEAD_AFTER = 15
REPLICAS = int(os.getenv("REPLICAS", "2"))
FIXED_SIZE = int(os.getenv("FIXED_CHUNK_SIZE", str(4 * 1024 * 1024)))


@app.on_event("startup")
def startup():
    db.init()


# ---------- heartbeats (from Task 2) ----------
class Heartbeat(BaseModel):
    node_id: str
    address: str
    free_space: int
    chunk_count: int


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    nodes[hb.node_id] = {**hb.model_dump(), "last_seen": time.time()}
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    now = time.time()
    return [{**n, "seconds_since_heartbeat": round(now - n["last_seen"], 1),
             "alive": now - n["last_seen"] < DEAD_AFTER} for n in nodes.values()]


# ---------- helpers ----------
def addr(node_id):
    return nodes.get(node_id, {}).get("address")


def upload(h, data, targets):
    ok = 0
    for t in targets:
        try:
            r = requests.put(f"{addr(t)}/chunks/{h}", data=data, timeout=10)
            if r.status_code in (200, 201):
                ok += 1
        except (requests.RequestException, TypeError):
            pass
    return ok > 0


def release_chunks(hashes):
    """Drop one reference per hash. Delete from nodes only when refcount hits 0."""
    for h in hashes:
        row = db.release(h)
        if row and row[0] <= 0:
            for t in row[1]:
                if addr(t):
                    try:
                        requests.delete(f"{addr(t)}/chunks/{h}", timeout=5)
                    except requests.RequestException:
                        pass


# ---------- objects ----------
@app.put("/objects/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, request: Request, mode: str = "fixed"):
    data = await request.body()
    ring = Ring(nodes.keys())
    if not ring.nodes():
        raise HTTPException(503, "no storage nodes registered yet")

    chunk_list, new, dup = [], 0, 0
    for piece in split(data, mode, FIXED_SIZE):
        h = hashlib.sha256(piece).hexdigest()
        targets = ring.get_nodes(h, REPLICAS)
        chunk_list.append(h)
        refcount = db.add_ref(h, len(piece), targets)
        if refcount == 1:                      # first time we see this chunk: store it
            if not upload(h, piece, targets):
                release_chunks(chunk_list)     # roll back references
                raise HTTPException(503, "could not store chunk on any target node")
            new += 1
        else:                                  # already stored: just a new reference
            dup += 1
    version = db.add_object(bucket, key, len(data), mode, chunk_list)
    return {"bucket": bucket, "key": key, "version": version, "size": len(data),
            "chunking": mode, "chunks": len(chunk_list),
            "new_chunks": new, "deduplicated_chunks": dup}


@app.get("/objects/{bucket}/{key:path}")
def get_object(bucket: str, key: str, version: int | None = None):
    row = db.get_object(bucket, key, version)
    if not row:
        raise HTTPException(404, "object not found")
    out = bytearray()
    for h in row[2]:
        for t in db.chunk_nodes(h):
            if not addr(t):
                continue
            try:
                r = requests.get(f"{addr(t)}/chunks/{h}", timeout=10)
            except requests.RequestException:
                continue
            if r.status_code == 200 and hashlib.sha256(r.content).hexdigest() == h:
                out += r.content
                break
        else:
            raise HTTPException(500, f"chunk {h[:12]} unavailable")
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