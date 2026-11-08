import os
import time

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import PlainTextResponse

from gateway.auth import verify_api_key

app = FastAPI(title="Distributed Storage Gateway", dependencies=[Depends(verify_api_key)])

METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8000")
GATEWAY_ID = os.getenv("GATEWAY_ID", "gateway")
client = httpx.AsyncClient(base_url=METADATA_URL, timeout=None)


# --- Task 7: measure every request for Prometheus, and say which gateway answered ---
stats = {"requests": 0, "errors": 0, "seconds": 0.0, "bytes_received": 0, "bytes_sent": 0}


@app.middleware("http")
async def measure(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)
    start = time.perf_counter()
    response = await call_next(request)
    stats["requests"] += 1
    stats["seconds"] += time.perf_counter() - start
    stats["errors"] += response.status_code >= 500
    stats["bytes_received"] += int(request.headers.get("content-length") or 0)
    stats["bytes_sent"] += int(response.headers.get("content-length") or 0)
    response.headers["X-Gateway"] = GATEWAY_ID
    return response


async def metrics(request: Request):
    label = f'{{gateway="{GATEWAY_ID}"}}'
    lines = [f"gateway_requests_total{label} {stats['requests']}",
             f"gateway_errors_total{label} {stats['errors']}",
             f"gateway_request_seconds_total{label} {stats['seconds']:.6f}",
             f"gateway_bytes_received_total{label} {stats['bytes_received']}",
             f"gateway_bytes_sent_total{label} {stats['bytes_sent']}"]
    return PlainTextResponse("\n".join(lines) + "\n")


app.add_route("/metrics", metrics)   # added outside FastAPI's routes, so Prometheus needs no API key


async def forward(method, path, **kwargs):
    """Sends the request to the metadata service and returns its answer (status, body, range headers)."""
    try:
        res = await client.request(method, path, **kwargs)
    except httpx.RequestError:
        raise HTTPException(502, "Metadata service unreachable")
    headers = {k: v for k, v in res.headers.items() if k in ("content-range", "accept-ranges")}
    return Response(res.content, res.status_code, headers=headers, media_type=res.headers.get("content-type"))


# --- 1. Buckets ---
@app.post("/buckets/{bucket_name}")
async def create_bucket(bucket_name: str, mode: str = "replication"):
    return await forward("PUT", f"/buckets/{bucket_name}", params={"mode": mode})


@app.get("/buckets")
async def list_buckets():
    return await forward("GET", "/buckets")


# --- 2. Objects (upload / download / delete / metadata) ---
@app.put("/buckets/{bucket_name}/objects/{object_key:path}")
async def upload_object(bucket_name: str, object_key: str, request: Request,
                        mode: str = "fixed", tag: list[str] = Query([])):
    # the body is streamed to the metadata service, so a large file is never loaded in memory here
    params = [("mode", mode)] + [("tag", t) for t in tag]
    return await forward("PUT", f"/objects/{bucket_name}/{object_key}", params=params, content=request.stream())


@app.get("/buckets/{bucket_name}/objects/{object_key:path}")
async def download_object(bucket_name: str, object_key: str, request: Request, version: int | None = None):
    headers = {"range": request.headers["range"]} if "range" in request.headers else {}   # byte ranges
    params = {"version": version} if version else {}
    return await forward("GET", f"/objects/{bucket_name}/{object_key}", headers=headers, params=params)


@app.delete("/buckets/{bucket_name}/objects/{object_key:path}")
async def delete_object(bucket_name: str, object_key: str):
    return await forward("DELETE", f"/objects/{bucket_name}/{object_key}")


@app.get("/buckets/{bucket_name}/metadata/{object_key:path}")
async def object_metadata(bucket_name: str, object_key: str, version: int | None = None):
    params = {"version": version} if version else {}
    return await forward("GET", f"/metadata/{bucket_name}/{object_key}", params=params)


# --- 3. List and search: prefix, min_size, max_size, after, before, tag=key:value ---
@app.get("/buckets/{bucket_name}/objects")
async def search_objects(bucket_name: str, request: Request):
    return await forward("GET", f"/buckets/{bucket_name}/objects", params=list(request.query_params.multi_items()))


# --- 4. Cluster status: nodes, free space, chunks per node, degraded objects ---
@app.get("/cluster/status")
async def get_cluster_status():
    return await forward("GET", "/cluster")


# --- 5. Search with Elasticsearch ---
@app.get("/search")
async def search(q: str):
    return await forward("GET", "/search", params={"q": q})


@app.post("/search/reindex")
async def reindex():
    return await forward("POST", "/search/reindex")