from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
import time
import os
import httpx
from collections import defaultdict

app = FastAPI(title="Metadata Node")

NODE_ID = os.getenv("NODE_ID", "node-1")
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://127.0.0.1:9000")
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:9101")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "10"))

def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def norm_path(p: str) -> str:
    p = (p or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    while "//" in p:
        p = p.replace("//", "/")
    if p != "/" and p.endswith("/"):
        p = p.rstrip("/")
    return p

class FileMetadata(BaseModel):
    file_id: str
    owner: str
    size: int
    version: int = 1
    created_at: str | None = None
    updated_at: str | None = None

STORE: dict[str, FileMetadata] = {}
STORE_LOCK = asyncio.Lock() # global
PATH_LOCKS = defaultdict(asyncio.Lock)  # per path


async def register_once() -> None:
    async with httpx.AsyncClient(timeout=3) as client:
        await client.post(
            f"{REGISTRY_URL}/register",
            json={"node_id": NODE_ID, "base_url": BASE_URL},
        )

# self-healing method
async def heartbeat() -> None:
    while True:
        try:
            await register_once()
        except Exception:
            pass
        await asyncio.sleep(HEARTBEAT_INTERVAL)

@app.on_event("startup")
async def startup():
    asyncio.create_task(heartbeat())

@app.get("/health")
async def health():
    return {"status": "ok", "node_id": NODE_ID}

@app.get("/stats")
async def stats():
    async with STORE_LOCK:
        keys = list(STORE.keys())
        count = len(keys)
    return {"node_id": NODE_ID, "files": count, "paths": keys}

@app.get("/metadata")
async def list_metadata(prefix: str = "/"):
    prefix = norm_path(prefix)
    async with STORE_LOCK:
        items: list[FileMetadata] = [
            m for k, m in STORE.items()
            if k.startswith(prefix)
        ]
    return {"count": len(items), "items": [m.model_dump() for m in items], "node_id": NODE_ID}

# create or update metadata for file_id
@app.put("/metadata/{file_id:path}")
async def put_metadata(file_id: str, meta: FileMetadata):
    file_id = norm_path(file_id)
    meta.file_id = file_id

    async with PATH_LOCKS[file_id]:
        async with STORE_LOCK:
            existing = STORE.get(file_id)
            now = iso_now()

            if existing is None:
                meta.created_at = now
                meta.version = 1
            else:
                meta.created_at = existing.created_at
                meta.version = int(existing.version) + 1

            meta.updated_at = now
            STORE[file_id] = meta

    return {"status": "stored", "node_id": NODE_ID, "version": meta.version}

# return metadata for file_id
@app.get("/metadata/{file_id:path}")
async def get_metadata(file_id: str):
    file_id = norm_path(file_id)
    async with STORE_LOCK:
        meta = STORE.get(file_id)
    if meta is None:
        raise HTTPException(status_code=404, detail="Not found")
    return meta.model_dump()

# delete metadata for file_id
@app.delete("/metadata/{file_id:path}")
async def delete_metadata(file_id: str):
    file_id = norm_path(file_id)
    async with PATH_LOCKS[file_id]:
        async with STORE_LOCK:
            existed = file_id in STORE
            if existed:
                del STORE[file_id]
    if not existed:
        raise HTTPException(status_code=404, detail="Not found")
    return {"status": "deleted", "node_id": NODE_ID}