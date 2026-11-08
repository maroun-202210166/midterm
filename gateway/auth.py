import os
import time
from collections import defaultdict, deque

from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader

API_KEYS = set(os.getenv("API_KEYS", "secret-midterm-key").split(","))
RATE_LIMIT = int(os.getenv("RATE_LIMIT", "100"))      # requests per minute for each API key

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
recent_requests = defaultdict(deque)                  # API key -> times of its requests in the last minute


def verify_api_key(api_key: str = Security(api_key_header)):
    """Checks the API key, then applies the rate limit (at most RATE_LIMIT requests per minute)."""
    if api_key not in API_KEYS:
        raise HTTPException(401, "Invalid or missing API key")
    now = time.time()
    times = recent_requests[api_key]
    while times and now - times[0] > 60:
        times.popleft()
    if len(times) >= RATE_LIMIT:
        retry = int(60 - (now - times[0])) + 1
        raise HTTPException(429, "Rate limit exceeded", headers={"Retry-After": str(retry)})
    times.append(now)
    return api_key