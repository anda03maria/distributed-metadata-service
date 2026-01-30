import argparse
import json
import requests

DEFAULT_GATEWAY = "http://127.0.0.1:8000"

def main():
    p = argparse.ArgumentParser(prog="meta-dfs")
    p.add_argument("--gateway", default=DEFAULT_GATEWAY)

    sub = p.add_subparsers(dest="cmd", required=True)

    put = sub.add_parser("put")
    put.add_argument("--file-id", required=True)
    put.add_argument("--owner", required=True)
    put.add_argument("--size", type=int, required=True)

    get = sub.add_parser("get")
    get.add_argument("--file-id", required=True)

    delete = sub.add_parser("rm")
    delete.add_argument("--file-id", required=True)

    ls = sub.add_parser("ls")
    ls.add_argument("--prefix", default="/")

    nodes = sub.add_parser("nodes")
    stats = sub.add_parser("stats")

    inv = sub.add_parser("invalidate")
    inv.add_argument("--file-id", required=True)

    args = p.parse_args()
    gw = args.gateway.rstrip("/")

    def pr(x):
        print(json.dumps(x, indent=2))

    if args.cmd == "put":
        r = requests.post(f"{gw}/files", json={
            "file_id": args.file_id,
            "owner": args.owner,
            "size": args.size
        }, timeout=5)
        r.raise_for_status()
        pr(r.json())

    elif args.cmd == "get":
        r = requests.get(f"{gw}/files/{args.file_id}", timeout=5)
        if r.status_code == 404:
            pr({"error": "not_found", "file_id": args.file_id})
            return
        r.raise_for_status()
        pr(r.json())

    elif args.cmd == "rm":
        r = requests.delete(f"{gw}/files/{args.file_id}", timeout=5)
        if r.status_code == 404:
            pr({"error": "not_found", "file_id": args.file_id})
            return
        r.raise_for_status()
        pr(r.json())

    elif args.cmd == "ls":
        r = requests.get(f"{gw}/files", params={"prefix": args.prefix}, timeout=10)
        r.raise_for_status()
        pr(r.json())

    elif args.cmd == "nodes":
        r = requests.get(f"{gw}/nodes", timeout=5)
        r.raise_for_status()
        pr(r.json())

    elif args.cmd == "stats":
        r = requests.get(f"{gw}/stats", timeout=10)
        r.raise_for_status()
        pr(r.json())

    elif args.cmd == "invalidate":
        r = requests.post(f"{gw}/cache/invalidate/{args.file_id}", timeout=5)
        r.raise_for_status()
        pr(r.json())

if __name__ == "__main__":
    main()
