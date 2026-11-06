import hashlib
import random

from metadata_service.chunking import cdc_chunks, fixed_chunks
from metadata_service.ring import Ring


def hashes(chunks):
    return [hashlib.sha256(c).hexdigest() for c in chunks]


# ---- 1. Fixed-size vs content-defined chunking ----
data = random.Random(1).randbytes(2 * 1024 * 1024)
edited = b"INSERTED-BYTES" + data   # tiny edit at the start of the file

print("== Chunking: 14 bytes inserted at the start of a 2MB file ==")
for name, fn in [("fixed 64KB", lambda d: fixed_chunks(d, 64 * 1024)),
                 ("content-defined", cdc_chunks)]:
    a, b = hashes(fn(data)), hashes(fn(edited))
    shared = len(set(a) & set(b))
    print(f"{name:16} {len(a):3} chunks (avg {len(data)//len(a)//1024}KB), "
          f"unchanged after edit: {shared} ({shared / len(a) * 100:.0f}%)")

# ---- 2. Consistent hashing: add / remove a node ----
keys = [hashlib.sha256(str(i).encode()).hexdigest() for i in range(10000)]


def primary(ring):
    return {k: ring.get_nodes(k, 1)[0] for k in keys}


def moved(a, b):
    return sum(a[k] != b[k] for k in keys) / len(keys) * 100


ring = Ring(["node1", "node2", "node3", "node4"])
p4 = primary(ring)
counts = {n: list(p4.values()).count(n) for n in sorted(ring.nodes())}
print("\n== Ring with 4 nodes, 10000 chunks ==")
print("chunks per node:", counts)

ring.add_node("node5")
p5 = primary(ring)
print(f"\nAdd node5   : {moved(p4, p5):.1f}% of chunks move (ideal 20%)")

ring.remove_node("node5")
ring.remove_node("node4")
p3 = primary(ring)
print(f"Remove node4: {moved(p4, p3):.1f}% of chunks move (ideal 25%)")

naive = sum(int(k, 16) % 4 != int(k, 16) % 5 for k in keys) / len(keys) * 100
print(f"\nFor comparison, plain hash(key) mod N going 4 -> 5 nodes moves {naive:.1f}%")