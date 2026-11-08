import os
import uuid

import requests

API = os.getenv("API", "http://localhost:8080")
KEY = {"X-API-Key": os.getenv("API_KEY", "secret-midterm-key")}
BUCKET = "api-" + uuid.uuid4().hex[:6]
DATA = os.urandom(5 * 1024 * 1024)                 # 5 MB -> 2 chunks (4 MB + 1 MB)
OBJ = f"{API}/buckets/{BUCKET}/objects"


def test_missing_or_wrong_key_is_rejected():
    assert requests.get(f"{API}/buckets").status_code == 401
    assert requests.get(f"{API}/buckets", headers={"X-API-Key": "wrong"}).status_code == 401


def test_create_and_list_buckets():
    r = requests.post(f"{API}/buckets/{BUCKET}", headers=KEY)
    assert r.status_code == 200 and r.json()["mode"] == "replication"
    assert BUCKET in [b["bucket"] for b in requests.get(f"{API}/buckets", headers=KEY).json()]


def test_upload_and_download():
    r = requests.put(f"{OBJ}/big.bin", params={"tag": ["type:video", "owner:maroun"]}, data=DATA, headers=KEY)
    assert r.status_code == 200 and r.json()["chunks"] == 2
    d = requests.get(f"{OBJ}/big.bin", headers=KEY)
    assert d.status_code == 200 and d.content == DATA


def test_byte_range_requests():
    r = requests.get(f"{OBJ}/big.bin", headers={**KEY, "Range": "bytes=100-199"})
    assert r.status_code == 206 and r.content == DATA[100:200]
    assert r.headers["Content-Range"] == f"bytes 100-199/{len(DATA)}"
    edge = 4 * 1024 * 1024                          # a range that crosses the chunk boundary
    r = requests.get(f"{OBJ}/big.bin", headers={**KEY, "Range": f"bytes={edge - 10}-{edge + 9}"})
    assert r.status_code == 206 and r.content == DATA[edge - 10:edge + 10]
    r = requests.get(f"{OBJ}/big.bin", headers={**KEY, "Range": "bytes=-50"})
    assert r.status_code == 206 and r.content == DATA[-50:]
    r = requests.get(f"{OBJ}/big.bin", headers={**KEY, "Range": "bytes=999999999-"})
    assert r.status_code == 416


def test_object_metadata():
    m = requests.get(f"{API}/buckets/{BUCKET}/metadata/big.bin", headers=KEY).json()
    assert m["size"] == len(DATA) and m["version"] == 1 and m["chunk_count"] == 2
    assert m["tags"] == {"type": "video", "owner": "maroun"} and "created_at" in m


def test_list_and_search():
    for name, size, tag in [("logs/a.json", 1000, "type:log"), ("logs/b.json", 5000, "type:log"),
                            ("docs/c.txt", 3000, "type:doc")]:
        r = requests.put(f"{OBJ}/{name}", params={"tag": [tag]}, data=os.urandom(size), headers=KEY)
        assert r.status_code == 200

    def search(**params):
        r = requests.get(OBJ, params=params, headers=KEY).json()
        return sorted(o["key"] for o in r["objects"])

    assert search() == ["big.bin", "docs/c.txt", "logs/a.json", "logs/b.json"]
    assert search(prefix="logs/") == ["logs/a.json", "logs/b.json"]
    assert search(min_size=2000, max_size=4000) == ["docs/c.txt"]
    assert search(tag="type:log") == ["logs/a.json", "logs/b.json"]
    assert search(prefix="logs/", tag="type:log", min_size=2000) == ["logs/b.json"]
    assert search(after="2000-01-01") == ["big.bin", "docs/c.txt", "logs/a.json", "logs/b.json"]
    assert search(after="2999-01-01") == []
    assert search(before="2000-01-01") == []


def test_delete_object():
    assert requests.delete(f"{OBJ}/logs/a.json", headers=KEY).status_code == 200
    assert requests.get(f"{OBJ}/logs/a.json", headers=KEY).status_code == 404
    assert requests.get(f"{API}/buckets/{BUCKET}/metadata/logs/a.json", headers=KEY).status_code == 404


def test_erasure_coded_bucket():
    ec = "ec-" + uuid.uuid4().hex[:6]
    assert requests.post(f"{API}/buckets/{ec}", params={"mode": "ec"}, headers=KEY).status_code == 200
    data = os.urandom(300_000)
    r = requests.put(f"{API}/buckets/{ec}/objects/x.bin", data=data, headers=KEY)
    assert r.json()["redundancy"] == "ec"
    assert requests.get(f"{API}/buckets/{ec}/objects/x.bin", headers=KEY).content == data


def test_cluster_status():
    s = requests.get(f"{API}/cluster/status", headers=KEY).json()
    assert len(s["nodes"]) >= 4 and s["alive_nodes"] >= 4
    assert {"node_id", "alive", "free_space", "chunk_count"} <= set(s["nodes"][0])
    assert s["degraded_objects"] == 0


def test_rate_limit():
    key = {"X-API-Key": "rate-test-key"}
    codes = [requests.get(f"{API}/buckets", headers=key).status_code for _ in range(150)]
    assert 429 in codes and codes[0] == 200