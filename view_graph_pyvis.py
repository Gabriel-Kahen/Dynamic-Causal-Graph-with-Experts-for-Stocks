import json, argparse
import networkx as nx
from pyvis.network import Network

def load_graph(path):
    with open(path) as f:
        snap = json.load(f)
    G = nx.DiGraph()
    for n in snap["nodes"]:
        G.add_node(n["id"], **n)
    for e in snap["edges"]:
        G.add_edge(e["src"], e["dst"], **{k:v for k,v in e.items() if k not in ("src","dst")})
    return G

def filter_subgraph(G, ticker=None):
    if not ticker: return G
    keep = {n for n,d in G.nodes(data=True) if d.get("ticker")==ticker}
    for n in list(keep):
        keep |= set(G.predecessors(n)) | set(G.successors(n))
    return G.subgraph(keep).copy()

def to_html(G, out_html):
    net = Network(height="800px", width="100%", directed=True, bgcolor="#ffffff", font_color="#222")
    net.barnes_hut(gravity=-2000, central_gravity=0.2, spring_length=150)
    type_color = {"price_event":"#1f77b4","news":"#ff7f0e","filing":"#9467bd","macro":"#7f7f7f","social":"#2ca02c"}
    for n,d in G.nodes(data=True):
        title = f"<b>{d.get('type')}</b>"
        if d.get("ticker"): title += f"<br>Ticker: {d['ticker']}"
        if d.get("summary"): title += f"<br>{d['summary'][:200]}"
        net.add_node(n, label=d.get("ticker") or d.get("type")[:2].upper(),
                     color=type_color.get(d.get("type"), "#999"), title=title, shape="dot", size=12)
    for u,v,d in G.edges(data=True):
        pol = d.get("polarity",0)
        color = "#2ca02c" if pol>0 else "#d62728"
        title = f"weight={d.get('weight',0):.2f}, pol={pol}"
        net.add_edge(u,v, value=max(1, d.get('weight',0)*5), color=color, title=title, arrows="to")
    net.write_html(out_html, notebook=False, open_browser=False)
    print(f"Wrote {out_html}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="data/latest_graph.json")
    ap.add_argument("--ticker")
    ap.add_argument("--out", default="graph.html")
    args = ap.parse_args()
    G = load_graph(args.path)
    G = filter_subgraph(G, args.ticker)
    to_html(G, args.out)
