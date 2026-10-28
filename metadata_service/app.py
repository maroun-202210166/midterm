import time
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Metadata service")
nodes = {}
DEAD_AFTER = 15  # seconds without a heartbeat


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