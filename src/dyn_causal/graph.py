from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List
from datetime import datetime, timezone
import networkx as nx
from .events import Event, utcnow

UTC = timezone.utc

@dataclass
class EdgeData:
    weight: float
    polarity: int
    last_updated: str
    evidence: List[Dict[str, Any]]

class DynamicCausalGraph:
    def __init__(self, half_lives_days: Dict[str, float]):
        self.G = nx.DiGraph()
        self.half_lives = half_lives_days

    def add_event_node(self, ev: Event):
        if ev.id in self.G:
            self.G.nodes[ev.id].update(ev.to_node())
        else:
            self.G.add_node(ev.id, **ev.to_node())

    def remove_node(self, node_id: str):
        if node_id in self.G:
            self.G.remove_node(node_id)

    def add_or_update_edge(self, src: str, dst: str, weight: float, polarity: int, evidence: Dict[str, Any]):
        now = utcnow().isoformat()
        if self.G.has_edge(src, dst):
            data = self.G[src][dst]
            data["weight"] = 0.5*data["weight"] + 0.5*weight
            data["polarity"] = polarity
            data["last_updated"] = now
            data["evidence"].append(evidence)
        else:
            self.G.add_edge(src, dst, weight=weight, polarity=polarity, last_updated=now, evidence=[evidence])

    def decay(self):
        # simple decay + prune light edges
        now = datetime.now(UTC)
        to_remove = []
        for u, v, data in list(self.G.edges(data=True)):
            last = datetime.fromisoformat(data["last_updated"])
            days = max((now - last).total_seconds()/86400.0, 0.0)
            src_t = self.G.nodes[u]["type"]; dst_t = self.G.nodes[v]["type"]
            hl = min(self.half_lives.get(src_t, 3.0), self.half_lives.get(dst_t, 3.0))
            factor = 0.0 if hl <= 0 else 0.5 ** (days/hl)
            data["weight"] *= factor
            data["last_updated"] = now.isoformat()
            if data["weight"] < 0.05:
                to_remove.append((u,v))
        for e in to_remove:
            self.G.remove_edge(*e)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "nodes": [{**d, "id": n} for n,d in self.G.nodes(data=True)],
            "edges": [{"src": u, "dst": v, **d} for u,v,d in self.G.edges(data=True)]
        }
