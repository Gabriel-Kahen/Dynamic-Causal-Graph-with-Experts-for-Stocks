import argparse, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path

def parse_ts(ts):
    # Accept "YYYY-MM-DDTHH:MM:SS[.us]+00:00" or "YYYY-MM-DD HH:MM:SS"
    try:
        return datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def main():
    ap = argparse.ArgumentParser(description="Rebuild latest_graph.json by replaying events.sqlite")
    ap.add_argument("--db", default="data/events.sqlite", help="Path to events.sqlite")
    ap.add_argument("--out", default="data/latest_graph.json", help="Output snapshot JSON")
    ap.add_argument("--since", default=None, help="Only replay events at/after this UTC time (ISO)")
    ap.add_argument("--until", default=None, help="Only replay events before this UTC time (ISO)")
    args = ap.parse_args()

    since = parse_ts(args.since) if args.since else None
    until = parse_ts(args.until) if args.until else None

    if not Path(args.db).exists():
        raise SystemExit(f"[replay] DB not found: {args.db}")

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Minimal schema assumptions: columns (id INTEGER PK, ts TEXT, action TEXT, payload TEXT)
    cur.execute("SELECT ts, action, payload FROM events ORDER BY id ASC")

    nodes = {}  # id -> node dict
    edges = {}  # (src,dst) or edge id -> edge dict

    applied = 0
    for row in cur:
        ts = row["ts"]
        ts_dt = parse_ts(ts) or datetime.now(timezone.utc)
        if since and ts_dt < since: 
            continue
        if until and ts_dt >= until:
            break

        action = row["action"]
        try:
            payload = json.loads(row["payload"])
        except Exception:
            continue

        if action == "add_node":
            nid = payload.get("id") or payload.get("node", {}).get("id")
            if not nid: 
                continue
            # normalize
            node = payload if "type" in payload else payload.get("node", {})
            node.setdefault("ts", ts)
            nodes[nid] = node
            applied += 1

        elif action == "prune_node":
            nid = payload.get("id")
            if nid and nid in nodes:
                nodes.pop(nid, None)
                applied += 1

        elif action == "add_edge":
            # payload usually has: src, dst, weight, polarity, ts, maybe id
            src = payload.get("src") or payload.get("source")
            dst = payload.get("dst") or payload.get("target")
            if not (src and dst):
                continue
            key = payload.get("id") or f"{src}->{dst}"
            e = {
                "id": key,
                "src": src,
                "dst": dst,
                "weight": payload.get("weight", payload.get("w", 0.0)),
                "polarity": payload.get("polarity", 0),
                "ts": payload.get("ts", ts),
            }
            # carry any extra attrs for your viewer
            for k in ("type", "rationale", "experts", "judge"):
                if k in payload:
                    e[k] = payload[k]
            edges[key] = e
            applied += 1

        elif action == "prune_edge":
            # payload may specify id or (src,dst)
            key = payload.get("id")
            if key and key in edges:
                edges.pop(key, None); applied += 1
            else:
                src = payload.get("src") or payload.get("source")
                dst = payload.get("dst") or payload.get("target")
                if src and dst:
                    edges.pop(f"{src}->{dst}", None); applied += 1

        else:
            # ignore alerts, etc.
            continue

    con.close()

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nodes": list(nodes.values()),
        "edges": list(edges.values()),
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(f"[replay] Applied {applied} events -> {args.out} "
          f"({len(nodes)} nodes, {len(edges)} edges)")

if __name__ == "__main__":
    main()
