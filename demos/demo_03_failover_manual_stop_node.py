import requests
import json

GW = "http://127.0.0.1:8000"
def jprint(x): print(json.dumps(x, indent=2))

def post_file(file_id, owner="anda", size=123):
    r = requests.post(f"{GW}/files", json={"file_id": file_id, "owner": owner, "size": size}, timeout=5)
    r.raise_for_status()
    return r.json()

def get_file(file_id):
    r = requests.get(f"{GW}/files/{file_id}", timeout=5)
    r.raise_for_status()
    return r.json()

def nodes():
    r = requests.get(f"{GW}/nodes", timeout=5)
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    print("=== Create a file ==="); jprint(post_file("/failover/x.txt"))
    print("\n=== Get file ==="); jprint(get_file("/failover/x.txt"))

    print("\n=== Nodes ==="); jprint(nodes())

    print("\nMANUAL STEP: Stop one metadata node.")
    print("Wait for registry TTL to remove it.")
    input("Press ENTER after you stopped a node and waited")

    print("\n=== Nodes after stop ==="); jprint(nodes())
    print("\n=== Get again (gateway should still work if at least one node is up) ===")
    jprint(get_file("/failover/x.txt"))
