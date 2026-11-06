import random
import sys

import requests

API = "http://localhost:8000"
SIZE = 9 * 1024 * 1024                      # 9 MB -> 3 chunks of 4+4+1 MB
DATA = {"rep": random.Random(1).randbytes(SIZE),   # different data per bucket, so dedup
        "ecb": random.Random(2).randbytes(SIZE)}   # does not hide the real storage cost


def show_nodes():
    print("\nnode    alive  chunks  used_bytes")
    for n in sorted(requests.get(f"{API}/nodes").json(), key=lambda n: n["node_id"]):
        print(f"{n['node_id']:7} {str(n['alive']):6} {n['chunk_count']:6}  {n['used_bytes']}")


def upload():
    print(requests.put(f"{API}/buckets/rep", params={"mode": "replication"}).json())
    print(requests.put(f"{API}/buckets/ecb", params={"mode": "ec"}).json())
    for b in DATA:
        r = requests.put(f"{API}/objects/{b}/a.bin", data=DATA[b])
        print(f"PUT {b}/a.bin", r.status_code, r.json())
    import time; time.sleep(6)               # let heartbeats refresh the counts
    show_nodes()
    print("\nlogical data  :", SIZE * 2, "bytes (2 objects of", SIZE, ")")


def read():
    for b in DATA:
        r = requests.get(f"{API}/objects/{b}/a.bin")
        print(f"GET {b}/a.bin", r.status_code, "matches:", r.content == DATA[b])
    show_nodes()
    print("\nrepair status:", requests.get(f"{API}/repair").json())


def scrub():
    print("scrub result:", requests.post(f"{API}/scrub").json())
    read()


{"upload": upload, "read": read, "scrub": scrub}[sys.argv[1]]()