from __future__ import annotations
from typing import Dict, List
from .rss import RSSIngestor

def build_news_ingestors_from_cfg(cfg):
    feeds = list(cfg.news.get("feeds", []) or [])
    if cfg.news.get("generate_symbol_feeds", True):
        # Yahoo Finance per-symbol RSS (surprisingly decent coverage)
        for t in cfg.universe.tickers:
            feeds.append(f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={t}&region=US&lang=en-US")
    ingestors = []
    for url in feeds:
        ingestors.append(RSSIngestor(
            feed_url=url,
            tickers=cfg.universe.tickers,
            meta=cfg.universe_meta,
            lookback_minutes=int(cfg.news.get("lookback_minutes", 30))
        ))
    return ingestors

