"""Compare replication (3 copies) with erasure coding (2+2) on both workloads.
Usage: python bench.py small | large | all
For each (workload, bucket mode): upload -> measure storage -> stop node3 -> measure repair -> clean up."""
import csv
import os
import random
import subprocess
import sys
import time

import requests

API = "http://localhost:8000"
SMALL_FILES = int(os.getenv("SMALL_FILES", "5000"))     # 1-50 KB each
LARGE_FILES = int(os.getenv("LARGE_FILES", "10"))
LARGE_MB = int(os.getenv("LARGE_MB", "100"))
COMPOSE = ["docker", "compose", "-f", "docker-compose.storage.yml"]
http = requests.Session()


# ---------- datasets (generated, same for both modes) ----------
def small_files():
    for i in range(SMALL_FILES):
        rnd = random.Random(i)
        yield f"file{i}.json", rnd.randbytes(rnd.randint(1, 50) * 1024)


def large_files():
    for i in range(LARGE_FILES):
        yield f"big{i}.bin", random.Random(1000 + i).randbytes(LARGE_MB * 1024 * 1024)


# ---------- helpers ----------
def nodes():
    return {n["node_id"]: n for n in http.get(f"{API}/nodes").json()}


def stored_bytes():
    time.sleep(6)                                  # wait for fresh heartbeats
    return sum(n["used_bytes"] for n in nodes().values())


def stop_node():
    subprocess.run(COMPOSE + ["stop", "node3"], check=True)


def start_node():
    subprocess.run(COMPOSE + ["start", "node3"], check=True)


def wait_until(alive):
    while nodes()["node3"]["alive"] != alive:
        time.sleep(2)


def wait_repair_done():
    last, quiet = -1, 0
    while quiet < 15:                              # no new repairs for 15 s -> finished
        done = http.get(f"{API}/repair").json()["repaired"]
        quiet = quiet + 2 if done == last else 0
        last = done
        time.sleep(2)


# ---------- one run ----------
def run(workload, make_files, bucket_mode):
    bucket = f"{workload}-{bucket_mode}"
    http.put(f"{API}/buckets/{bucket}", params={"mode": bucket_mode})
    before = stored_bytes()

    logical, names, t0 = 0, [], time.time()
    for name, data in make_files():
        r = http.put(f"{API}/objects/{bucket}/{name}", data=data)
        r.raise_for_status()
        logical += len(data)
        names.append(name)
        if len(names) % 500 == 0:
            print(f"  uploaded {len(names)} files", flush=True)
    upload_s = time.time() - t0
    stored = stored_bytes() - before

    # failure + repair
    t1 = http.get(f"{API}/traffic").json()
    stop_node()
    wait_until(False)
    t0 = time.time()
    wait_repair_done()
    repair_s = time.time() - t0 - 15
    t2 = http.get(f"{API}/traffic").json()
    repair_mb = (t2["read"] - t1["read"] + t2["written"] - t1["written"]) / 1e6

    # clean up and bring the node back
    for name in names:
        http.delete(f"{API}/objects/{bucket}/{name}")
    start_node()
    wait_until(True)
    print("  done", flush=True)

    return {"workload": workload, "mode": bucket_mode, "files": len(names),
            "logical_MB": round(logical / 1e6, 1), "stored_MB": round(stored / 1e6, 1),
            "overhead_x": round(stored / logical, 2), "upload_s": round(upload_s, 1),
            "repair_traffic_MB": round(repair_mb, 1), "repair_s": round(repair_s, 1)}


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if stored_bytes() > 100_000:
        sys.exit("The cluster already holds data (old copies would falsify the results).\n"
                 "Reset it first:  docker compose -f docker-compose.storage.yml down -v\n"
                 "then:            docker compose -f docker-compose.storage.yml up -d")
    workloads = {"small": small_files, "large": large_files}
    rows = []
    for w in (workloads if which == "all" else [which]):
        for mode in ("replication", "ec"):
            print(f"running {w} / {mode} ...", flush=True)
            rows.append(run(w, workloads[w], mode))
            print(rows[-1], flush=True)
    with open("benchmark_results.csv", "a", newline="") as f:
        out = csv.DictWriter(f, fieldnames=rows[0].keys())
        if f.tell() == 0:
            out.writeheader()
        out.writerows(rows)
    print("\nsaved to benchmark_results.csv")