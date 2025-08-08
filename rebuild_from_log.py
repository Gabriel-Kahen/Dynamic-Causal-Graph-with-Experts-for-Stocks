#!/usr/bin/env python3
import argparse, json, sqlite3, ast
from datetime import datetime, timezone
from pathlib import Path

EDGE_ACTIONS = {"add_edge","edge_add","update_edge","add_or_update_edge","upsert_edge"}
NODE_ADD_ACTIONS = {"add_node","node_add","update_node"}
NODE_PRUNE_ACTIONS = {"prune_node","node_prune","remove_node"}
EDGE_PRUNE_ACTIONS = {"prune_edge","edge_prune","remove_edge"}

def parse_ts(ts):
    if not ts: return None
    try:
        return datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        try:
            # "YYYY-MM-DD HH:MM:SS"
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def parse_payload(raw):
    if isinstance(raw, dict):  # already decoded
        return raw
    if not raw:
        return {}
    # try JSON
    try:
        return json.loads(raw)
    except Exception:
        pass
    # try Python literal (single quotes)
    try:
        obj = ast.literal_eval(raw)
        if isinstance(obj, dict):
            return obj
        return {}
    except Exception:
        return {}

def norm_edge(payload):
    # accept nested {"edge": {...}}
    e = payload.get("edge", payload)
    src = e.get("src") or e.get("source")
    dst = e.get("dst") or e.get("target")
    if not (src and dst):
        return None
    out = {
        "id": e.get("id") or f"{src}->{dst}",
        "src": src,
        "dst": dst,
        "weight": e.get("weight", e.get("w", 0.0)),
        "polarity": e.get("polarity", 0),
        "ts": e.get("ts") or payload.get("ts"),
    }
    # keep useful extras
    for k in ("type","rationale","experts","judge"):
        if k in e:
            out[k] = e[k]
        elif k in payload:
            out[k] = payload[k]
    return out

def norm_node(payload):
    # accept nested {"node": {...}}
    n = payload.get("node", payload)
    if "id" not in n and "id" in payload:
        n["id"] = payload["id"]
    return n

def main():
    ap = argparse.ArgumentParser(description="Rebuild latest_graph.json by replaying events.sqlite")
    ap.add_argument("--db", default="data/events.sqlite")
    ap.add_argument("--out", default="data/latest_graph.json")
    ap.add_argument("--since", default=None, help="ISO UTC start bound")
    ap.add_argument("--until", default=None, help="ISO UTC end bound (exclusive)")
    ap.add_argument("--ignore-prunes", action="store_true", help="Do not apply prune_node/prune_edge")
    ap.add_argument("--only-actions", default=None,
                    help="Comma-separated allowlist (e.g., add_node,add_edge)")
    args = ap.parse_args()

    since = parse_ts(args.since) if args.since else None
    until = parse_ts(args.until) if args.until else None
    only = set(a.strip() for a in args.only_actions.split(",")) if args.only_actions else None

    if not Path(args.db).exists():
        raise SystemExit(f"[replay] DB not found: {args.db}")

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT id, ts, action, payload FROM events ORDER BY id ASC")

    nodes, edges = {}, {}
    stats = {"rows":0,"applied":0,"skipped_parse":0,"skipped_filter":0,"node_add":0,"node_prune":0,"edge_add":0,"edge_prune":0}

    for row in cur:
        stats["rows"] += 1
        ts = row["ts"]; dt = parse_ts(ts) or datetime.now(timezone.utc)
        if since and dt < since: 
            continue
        if until and dt >= until:
            break

        action = (row["action"] or "").strip()
        if only and action not in only:
            stats["skipped_filter"] += 1
            continue

        payload = parse_payload(row["payload"])
        if payload == {} and row["payload"]:
            stats["skipped_parse"] += 1
            continue

        # Node add/update
        if action in NODE_ADD_ACTIONS:
            n = norm_node(payload)
            nid = n.get("id")
            if nid:
                n.setdefault("ts", ts)
                nodes[nid] = n
                stats["applied"] += 1; stats["node_add"] += 1
            continue

        # Edge add/update
        if action in EDGE_ACTIONS:
            e = norm_edge(payload)
            if e:
                edges[e["id"]] = e
                stats["applied"] += 1; stats["edge_add"] += 1
            continue

        if args.ignore_prunes:
            continue

        # Node prune
        if action in NODE_PRUNE_ACTIONS:
            nid = payload.get("id") or payload.get("node_id")
            if nid:
                nodes.pop(nid, None)
                stats["applied"] += 1; stats["node_prune"] += 1
            continue

        # Edge prune
        if action in EDGE_PRUNE_ACTIONS:
            key = payload.get("id")
            if key and key in edges:
                edges.pop(key, None)
                stats["applied"] += 1; stats["edge_prune"] += 1
            else:
                src = payload.get("src") or payload.get("source")
                dst = payload.get("dst") or payload.get("target")
                if src and dst:
                    edges.pop(f"{src}->{dst}", None)
                    stats["applied"] += 1; stats["edge_prune"] += 1
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

    print(f"[replay] rows={stats['rows']} applied={stats['applied']} "
          f"nodes={len(nodes)} edges={len(edges)} "
          f"(skipped_parse={stats['skipped_parse']} skipped_filter={stats['skipped_filter']})")
    print(f"[replay] node_add={stats['node_add']} node_prune={stats['node_prune']} "
          f"edge_add={stats['edge_add']} edge_prune={stats['edge_prune']} -> {args.out}")

if __name__ == "__main__":
    main()
