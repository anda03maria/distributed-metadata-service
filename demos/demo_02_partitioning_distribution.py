import random
import string
import requests
import json

GW = "http://127.0.0.1:8000"
def jprint(x): print(json.dumps(x, indent=2))

def post_file(file_id, owner="anda", size=1):
    r = requests.post(f"{GW}/files", json={"file_id": file_id, "owner": owner, "size": size}, timeout=5)
    r.raise_for_status()
    return r.json()

def stats():
    r = requests.get(f"{GW}/stats", timeout=10)
    r.raise_for_status()
    return r.json()

def rand_name(n=6):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))

if __name__ == "__main__":
    for i in range(40):
        fid = f"/bulk/{rand_name()}_{i}.dat"
        post_file(fid, size=random.randint(10, 1000))

    print("=== Cluster stats (distribution) ===")
    jprint(stats())
