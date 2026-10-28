import hashlib, os, re, shutil, threading, time
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException, Request, Response

NODE_ID = os.getenv("NODE_ID", "node1")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8000")
ADDRESS = os.getenv("NODE_ADDRESS", f"http://{NODE_ID}:8000")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))

app = FastAPI(title=f"Storage node {NODE_ID}")
DATA_DIR.mkdir(parents=True, exist_ok=True)
HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def chunk_path(h: str) -> Path:
    if not HASH_RE.match(h):
        raise HTTPException(400, "chunk id must be a 64-char lowercase SHA-256 hex")
    return DATA_DIR / h[:2] / h


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@app.put("/chunks/{h}")
async def put_chunk(h: str, request: Request):
    path = chunk_path(h)
    data = await request.body()
    if sha256(data) != h:
        raise HTTPException(400, "content does not match the SHA-256 in the URL")
    if path.exists():
        return {"status": "exists", "hash": h}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)  # atomic: no half-written chunks
    return Response(status_code=201, content=f'{{"status":"stored","hash":"{h}"}}',
                    media_type="application/json")


@app.get("/chunks/{h}")
def get_chunk(h: str):
    path = chunk_path(h)
    if not path.exists():
        raise HTTPException(404, "chunk not found")
    data = path.read_bytes()
    if sha256(data) != h:  # verify on every read
        raise HTTPException(500, "chunk corrupted")
    return Response(content=data, media_type="application/octet-stream")


@app.head("/chunks/{h}")
def head_chunk(h: str):
    path = chunk_path(h)
    if not path.exists():
        return Response(status_code=404)
    return Response(status_code=200, headers={"X-Chunk-Size": str(path.stat().st_size)})


@app.delete("/chunks/{h}")
def delete_chunk(h: str):
    path = chunk_path(h)
    if not path.exists():
        raise HTTPException(404, "chunk not found")
    path.unlink()
    return {"status": "deleted", "hash": h}


@app.get("/health")
def health():
    return {"node_id": NODE_ID, **stats()}


def stats():
    count = sum(1 for p in DATA_DIR.rglob("*") if p.is_file() and p.suffix != ".tmp")
    return {"free_space": shutil.disk_usage(DATA_DIR).free, "chunk_count": count}


def heartbeat_loop():
    while True:
        try:
            requests.post(f"{METADATA_URL}/heartbeat", timeout=3,
                          json={"node_id": NODE_ID, "address": ADDRESS, **stats()})
        except requests.RequestException:
            pass  # metadata service not reachable, try again next round
        time.sleep(HEARTBEAT_INTERVAL)


@app.on_event("startup")
def start_heartbeat():
    threading.Thread(target=heartbeat_loop, daemon=True).start()