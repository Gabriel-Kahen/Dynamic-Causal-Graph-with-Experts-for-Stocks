from __future__ import annotations
from typing import List
from datetime import datetime, timezone
import hashlib
try:
    import feedparser
except Exception:
    feedparser = None

from ..events import NewsEvent

UTC = timezone.utc

class RSSIngestor:
    def __init__(self, url: str, ticker_map: dict):
        self.url = url
        self.ticker_map = ticker_map

    def fetch(self) -> List[NewsEvent]:
        if feedparser is None:
            return []
        d = feedparser.parse(self.url)
        out: List[NewsEvent] = []
        for e in d.entries:
            title = e.get("title", "")
            link = e.get("link", "")
            published = e.get("published_parsed")
            ts = datetime(*published[:6], tzinfo=UTC) if published else datetime.now(UTC)
            # keyword-based matching, supports list of keywords per ticker
            hit_tickers = []
            for t, keys in self.ticker_map.items():
                if isinstance(keys, str):
                    keys = [keys]
                for k in keys:
                    if k and k.lower() in title.lower():
                        hit_tickers.append(t); break
            for t in hit_tickers:
                nid = hashlib.sha256((title+link+t).encode()).hexdigest()[:16]
                out.append(NewsEvent(
                    id=nid, type="news", ticker=t, ts=ts,
                    attrs={"headline": title, "source": self.url, "url": link},
                    summary=title
                ))
        return out
