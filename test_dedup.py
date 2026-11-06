import os
import time

import requests

API = "http://localhost:8000"
data = os.urandom(9 * 1024 * 1024)   # 9MB -> 3 fixed chunks (4+4+1 MB)


def show(title, r):
    print(f"{title:28} {r.status_code} {r.json()}")


time.sleep(6)  # let heartbeats register the nodes
show("PUT lab/a.bin", requests.put(f"{API}/objects/lab/a.bin", data=data))
show("PUT lab/b.bin (same data)", requests.put(f"{API}/objects/lab/b.bin", data=data))
show("stats", requests.get(f"{API}/stats"))
print("GET a.bin matches:", requests.get(f"{API}/objects/lab/a.bin").content == data)

show("DELETE a.bin", requests.delete(f"{API}/objects/lab/a.bin"))
show("stats (b still uses chunks)", requests.get(f"{API}/stats"))
print("GET b.bin still matches:", requests.get(f"{API}/objects/lab/b.bin").content == data)

show("DELETE b.bin", requests.delete(f"{API}/objects/lab/b.bin"))
show("stats (nothing references)", requests.get(f"{API}/stats"))

time.sleep(6)
print("\nchunks stored per node:")
for n in requests.get(f"{API}/nodes").json():
    print(" ", n["node_id"], n["chunk_count"])