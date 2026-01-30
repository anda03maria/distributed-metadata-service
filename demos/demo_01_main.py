import time
import requests
import json

GW = "http://127.0.0.1:8000"

def jprint(x): print(json.dumps(x, indent=2))

def post_file(file_id, owner, size):
    r = requests.post(f"{GW}/files", json={"file_id": file_id, "owner": owner, "size": size}, timeout=5)
    r.raise_for_status()
    return r.json()

def get_file(file_id):
    r = requests.get(f"{GW}/files/{file_id}", timeout=5)
    if r.status_code == 404:
        return {"error": "not_found", "file_id": file_id}
    r.raise_for_status()
    return r.json()

def delete_file(file_id):
    r = requests.delete(f"{GW}/files/{file_id}", timeout=5)
    if r.status_code == 404:
        return {"error": "not_found", "file_id": file_id}
    r.raise_for_status()
    return r.json()

def list_prefix(prefix="/"):
    r = requests.get(f"{GW}/files", params={"prefix": prefix}, timeout=10)
    r.raise_for_status()
    return r.json()

def nodes():
    r = requests.get(f"{GW}/nodes", timeout=5)
    r.raise_for_status()
    return r.json()

def stats():
    r = requests.get(f"{GW}/stats", timeout=10)
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    print("=== Nodes ==="); jprint(nodes())

    print("\n=== Create ===")
    jprint(post_file("/docs/a.txt", "anda", 123))
    jprint(post_file("/docs/b.txt", "anda", 456))
    jprint(post_file("/pics/c.jpg", "anda", 999))

    print("\n=== Get (MISS then HIT) ===")
    jprint(get_file("/docs/a.txt"))
    jprint(get_file("/docs/a.txt"))

    print("\n=== Update ===")
    time.sleep(1)
    jprint(post_file("/docs/a.txt", "anda", 777))
    jprint(get_file("/docs/a.txt"))

    print("\n=== List prefixes ===")
    jprint(list_prefix("/docs"))
    jprint(list_prefix("/pics"))

    print("\n=== Cluster stats ===")
    jprint(stats())

    print("\n=== Delete + verify ===")
    jprint(delete_file("/docs/b.txt"))
    jprint(get_file("/docs/b.txt"))
