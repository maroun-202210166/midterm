import hashlib
import requests

NODE = "http://localhost:8001"
data = b"hello distributed storage"
h = hashlib.sha256(data).hexdigest()
url = f"{NODE}/chunks/{h}"

print("hash:", h)
print("PUT           ->", requests.put(url, data=data).status_code)          # 201
print("PUT again     ->", requests.put(url, data=data).json())               # exists
print("HEAD          ->", requests.head(url).status_code, requests.head(url).headers.get("X-Chunk-Size"))
r = requests.get(url)
print("GET           ->", r.status_code, "content matches:", r.content == data)
print("PUT bad hash  ->", requests.put(f"{NODE}/chunks/{'0'*64}", data=data).status_code)  # 400
print("GET missing   ->", requests.get(f"{NODE}/chunks/{'1'*64}").status_code)             # 404