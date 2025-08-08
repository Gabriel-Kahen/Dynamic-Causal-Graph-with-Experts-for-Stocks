from __future__ import annotations
from typing import Dict, List
from .rss import RSSIngestor

def build_news_ingestors_from_cfg(cfg) -> list:
    ticker_map: Dict[str, list] = {}
    for t in cfg.universe.tickers:
        meta = (cfg.universe_meta or {}).get(t, {})
        kws = meta.get("rss_keywords") or [meta.get("name") or t]
        if isinstance(kws, str):
            kws = [kws]
        if t not in kws:
            kws.append(t)
        # dedupe keeping order
        seen = set(); final = []
        for k in kws:
            if k and k not in seen:
                seen.add(k); final.append(k)
        ticker_map[t] = final
    feeds = (cfg.news or {}).get("feeds", [])
    return [RSSIngestor(url=f, ticker_map=ticker_map) for f in feeds]
