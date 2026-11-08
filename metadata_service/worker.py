"""Task 7 worker: takes jobs from RabbitMQ and runs them (repair, rebalance, garbage collection, scrub).
Several workers run at the same time, and RabbitMQ gives each job to only one of them."""
import os
import threading
import time

import requests

from metadata_service import app as m           # reuse the chunk functions of the metadata service
from metadata_service import jobs

WORKER_ID = os.getenv("WORKER_ID", "worker")
METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8000")
counts, done = {}, {}                           # results not yet sent to the metadata service
lock = threading.Lock()
nodes_time = 0


def refresh_nodes():
    """Copy the node list (addresses, alive or not) from the metadata service, at most every 2 seconds."""
    global nodes_time
    if time.time() - nodes_time < 2:
        return
    now = time.time()
    m.nodes = {n["node_id"]: {**n, "last_seen": now if n["alive"] else 0}
               for n in requests.get(f"{METADATA_URL}/nodes", timeout=5).json()}
    nodes_time = now


def run(job):
    refresh_nodes()
    kind, h, node = job["type"], job.get("hash"), job.get("node")
    if kind == "repair":
        result = {"repaired": m.repair_chunk(h, node)}
    elif kind == "rebalance":
        result = {"rebalanced": m.rebalance_chunk(h, node)}
    elif kind == "gc":
        result = {"gc_deleted": m.gc_node(node)}
    elif kind == "scrub":
        checked, fixed = m.scrub_chunk(h)
        result = {"scrub_checked": checked, "scrub_fixed": fixed}
    else:
        return
    with lock:
        for k, v in result.items():
            counts[k] = counts.get(k, 0) + v
        done[kind] = done.get(kind, 0) + 1
    if kind != "scrub" or result["scrub_fixed"]:
        print(f"{WORKER_ID}: {kind} {(h or node)[:12]} on {node or '-'} -> {result}", flush=True)


def report_loop():
    """Every 2 seconds, send the results to the metadata service (shown in the UI and Grafana)."""
    while True:
        time.sleep(2)
        with lock:
            if not done:
                continue
            body = {"worker": WORKER_ID, "counts": dict(counts), "jobs": dict(done)}
            counts.clear()
            done.clear()
        try:
            requests.post(f"{METADATA_URL}/jobs/report", json=body, timeout=5)
        except requests.RequestException:
            pass                                # only the counters are affected, not the data


if __name__ == "__main__":
    print(f"{WORKER_ID} started", flush=True)
    threading.Thread(target=report_loop, daemon=True).start()
    while True:
        try:
            jobs.consume(run)
        except Exception as e:                  # connection to RabbitMQ lost: connect again
            print("queue error:", e, flush=True)
            time.sleep(3)
