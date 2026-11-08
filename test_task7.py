"""Task 7 test helper.
  python test_task7.py lb       20 requests through the load balancer: which gateway answered each
  python test_task7.py upload   upload 10 objects to a replicated bucket and 10 to an erasure-coded bucket
  python test_task7.py check    download all 20 and compare them with the originals
  python test_task7.py status   nodes, counters, and the jobs done by each worker
"""
import os
import random
import sys
from collections import Counter

import requests

API = os.getenv("API", "http://localhost:8080")        # the load balancer
META = os.getenv("META", "http://localhost:8000")      # the metadata service
KEY = {"X-API-Key": "secret-midterm-key"}
BUCKETS = {"t7-rep": "replication", "t7-ec": "ec"}
FILES = 10
SIZE = 5 * 1024 * 1024                                 # 5 MB -> 2 chunks each


def data(bucket, i):
    return random.Random(f"{bucket}-{i}").randbytes(SIZE)   # same bytes every time, so we can compare


def lb():
    seen = Counter()
    for _ in range(20):
        r = requests.get(f"{API}/buckets", headers=KEY)
        seen[f"{r.headers.get('X-Gateway')} (HTTP {r.status_code})"] += 1
    for gateway, count in sorted(seen.items()):
        print(f"{gateway}: {count} requests")


def upload():
    for bucket, mode in BUCKETS.items():
        requests.post(f"{API}/buckets/{bucket}", params={"mode": mode}, headers=KEY).raise_for_status()
        for i in range(FILES):
            r = requests.put(f"{API}/buckets/{bucket}/objects/file{i}.bin", data=data(bucket, i), headers=KEY)
            print(f"PUT {bucket}/file{i}.bin -> {r.status_code}, via {r.headers.get('X-Gateway')}")
    status()


def check():
    good = 0
    for bucket in BUCKETS:
        for i in range(FILES):
            r = requests.get(f"{API}/buckets/{bucket}/objects/file{i}.bin", headers=KEY)
            same = r.status_code == 200 and r.content == data(bucket, i)
            good += same
            if not same:
                print(f"PROBLEM with {bucket}/file{i}.bin: HTTP {r.status_code}")
    print(f"{good} of {len(BUCKETS) * FILES} objects are identical to the originals")


def status():
    print("\nnode    alive  chunks  used MB")
    for n in sorted(requests.get(f"{META}/nodes").json(), key=lambda n: n["node_id"]):
        print(f"{n['node_id']:7} {str(n['alive']):6} {n['chunk_count']:6}  {n['used_bytes'] / 1e6:7.1f}")
    print("\ncounters:", requests.get(f"{META}/repair").json())
    jobs = requests.get(f"{META}/jobs").json()
    print("jobs waiting in RabbitMQ:", jobs["waiting"])
    for d in jobs["done"]:
        print(f"  {d['worker']:8} {d['type']:10} {d['count']}")


{"lb": lb, "upload": upload, "check": check, "status": status}[sys.argv[1]]()
