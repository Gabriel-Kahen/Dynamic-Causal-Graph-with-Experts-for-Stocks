from __future__ import annotations
from typing import Dict, Any, Tuple
import math

def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def aggregate_edge_signals(graph_snapshot: Dict[str, Any], ticker: str) -> Tuple[float, int]:
    nodes = {n["id"]: n for n in graph_snapshot["nodes"]}
    up = down = 0.0
    for e in graph_snapshot["edges"]:
        dst = nodes.get(e["dst"])
        if not dst: continue
        if dst.get("ticker") == ticker:
            w = e.get("weight", 0.0)
            pol = e.get("polarity", 0)
            if pol > 0: up += w
            elif pol < 0: down += w
    net = up - down
    return abs(net), (1 if net >= 0 else -1)

def probability_of_move(score: float) -> float:
    return logistic(2.5 * score)

def expected_alpha_and_prob(snapshot: Dict[str, Any], ticker: str) -> Dict[str, float]:
    score, polarity = aggregate_edge_signals(snapshot, ticker)
    p = probability_of_move(score)
    return {"p": p, "expected_sigma": score, "polarity": polarity}
