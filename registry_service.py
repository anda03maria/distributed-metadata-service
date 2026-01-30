from fastapi import FastAPI
from pydantic import BaseModel, HttpUrl
import time #no of seconds since 1st Jan 1970
import asyncio

app = FastAPI(title="Service Registry")

class RegisterRequest(BaseModel):
    node_id: str
    base_url: HttpUrl

NODES: dict[str, dict] = {} # registry in-memory
LOCK = asyncio.Lock()
TTL_SECONDS = 30

# register or update a node
@app.post("/register")
async def register(req: RegisterRequest):
    async with LOCK:
        NODES[req.node_id] = {
            "base_url": str(req.base_url),
            "last_seen": time.time()
        }
    return {"status": "ok"}

# get the list of active nodes
@app.get("/nodes")
async def get_nodes():
    now = time.time()
    async with LOCK:
        # cleanup dead nodes
        dead = [nid for nid, data in NODES.items() if now - data["last_seen"] > TTL_SECONDS]
        for nid in dead:
            del NODES[nid]

        active = {nid: data["base_url"] for nid, data in NODES.items()}
    return {"nodes": active, "ttl_seconds": TTL_SECONDS}
