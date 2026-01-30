from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any
import os
import hashlib
import httpx
import time

app = FastAPI(title="Gateway Router")

REGISTRY_URL = os.getenv("REGISTRY_URL", "http://127.0.0.1:9000")

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "20"))
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}

# path normalization
def norm_path(p: str) -> str:
    p = (p or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    while "//" in p:
        p = p.replace("//", "/")
    if p != "/" and p.endswith("/"):
        p = p.rstrip("/")
    return p

# cache functions
def cache_get(file_id: str) -> dict[str, Any] | None:
    item = _CACHE.get(file_id)
    if not item:
        return None
    expires_at, payload = item
    if time.time() > expires_at:
        _CACHE.pop(file_id, None)
        return None
    return payload

def cache_put(file_id: str, payload: dict[str, Any]) -> None:
    _CACHE[file_id] = (time.time() + CACHE_TTL_SECONDS, payload)

def cache_invalidate(file_id: str) -> None:
    _CACHE.pop(file_id, None)



class CreateFileRequest(BaseModel):
    file_id: str
    owner: str
    size: int

# fetch nodes from registry
async def fetch_nodes() -> dict[str, str]:
    async with httpx.AsyncClient(timeout=3) as client:
        r = await client.get(f"{REGISTRY_URL}/nodes")
        r.raise_for_status()
        nodes = r.json().get("nodes", {})
        return {str(k): str(v).rstrip("/") for k, v in nodes.items()}

# fallback for file_id
def ordered_nodes(file_id: str, nodes: dict[str, str]) -> list[str]:
    if not nodes:
        return []
    node_ids = sorted(nodes.keys())
    h = int(hashlib.sha256(file_id.encode("utf-8")).hexdigest(), 16)
    start = h % len(node_ids)
    return node_ids[start:] + node_ids[:start]

async def forward_with_fallback(method: str, file_id: str, nodes: dict[str, str], payload: dict[str, Any] | None = None):
    order = ordered_nodes(file_id, nodes)
    if not order:
        raise HTTPException(status_code=503, detail="No metadata nodes available")

    last_err = None
    for nid in order:
        base = nodes[nid]
        url = f"{base}/metadata/{file_id}"
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                if method == "PUT":
                    r = await client.put(url, json=payload)
                elif method == "GET":
                    r = await client.get(url)
                elif method == "DELETE":
                    r = await client.delete(url)
                else:
                    raise ValueError("Unsupported method")

            # file doesn't exist
            if r.status_code == 404:
                return {"_status_code": 404, "detail": "Not found", "routed_to": nid}

            #  succes
            if r.status_code < 400:
                data = r.json()
                if isinstance(data, dict):
                    data["routed_to"] = nid
                return data

            last_err = f"{r.status_code}: {r.text}"
        except Exception as e:
            last_err = str(e)
            continue

    raise HTTPException(status_code=503, detail=f"All metadata nodes unreachable. Last error: {last_err}")


# endpoints

@app.get("/nodes")
async def nodes():
    return {"nodes": await fetch_nodes()}

@app.get("/stats")
async def cluster_stats():
    nodes = await fetch_nodes()
    out: dict[str, Any] = {}
    for nid, base in nodes.items():
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                r = await client.get(f"{base}/stats")
                if r.status_code == 200:
                    out[nid] = r.json()
                else:
                    out[nid] = {"status": "error", "code": r.status_code}
        except Exception:
            out[nid] = {"status": "down"}
    return out

@app.post("/cache/invalidate/{file_id:path}")
async def invalidate(file_id: str):
    file_id = norm_path(file_id)
    cache_invalidate(file_id)
    return {"ok": True, "file_id": file_id}

# like ls
@app.get("/files")
async def list_files(prefix: str = "/"):
    prefix = norm_path(prefix)
    nodes = await fetch_nodes()

    results: list[dict[str, Any]] = []
    for nid, base in nodes.items():
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(f"{base}/metadata", params={"prefix": prefix})
                if r.status_code == 200:
                    data = r.json()
                    for it in data.get("items", []):
                        if isinstance(it, dict):
                            it["node_id"] = nid
                            results.append(it)
        except Exception:
            continue

    return {"count": len(results), "items": results, "prefix": prefix}

@app.post("/files")
async def create_or_update(req: CreateFileRequest):
    file_id = norm_path(req.file_id)
    nodes = await fetch_nodes()

    payload = {"file_id": file_id, "owner": req.owner, "size": req.size}
    data = await forward_with_fallback("PUT", file_id, nodes, payload=payload)

    cache_invalidate(file_id)
    return data

@app.get("/files/{file_id:path}")
async def get_file(file_id: str):
    file_id = norm_path(file_id)

    cached = cache_get(file_id)
    if cached is not None:
        out = dict(cached)
        out["cache"] = "HIT"
        return out

    nodes = await fetch_nodes()
    data = await forward_with_fallback("GET", file_id, nodes)

    if isinstance(data, dict) and data.get("_status_code") == 404:
        raise HTTPException(status_code=404, detail="Not found")

    if isinstance(data, dict):
        data["cache"] = "MISS"
        cache_put(file_id, data)

    return data

@app.delete("/files/{file_id:path}")
async def delete_file(file_id: str):
    file_id = norm_path(file_id)
    nodes = await fetch_nodes()

    data = await forward_with_fallback("DELETE", file_id, nodes)
    if isinstance(data, dict) and data.get("_status_code") == 404:
        raise HTTPException(status_code=404, detail="Not found")

    cache_invalidate(file_id)
    return data
