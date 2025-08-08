from __future__ import annotations
import re
from datetime import datetime, timezone, timedelta
import feedparser

from ..events import NewsEvent

UTC = timezone.utc

CASHTAG = re.compile(r"\$([A-Z]{1,5})\b")
# ticker token (AAPL) with word boundaries in all caps
def _ticker_re(t): return re.compile(rf"\b{re.escape(t)}\b")

class RSSIngestor:
    def __init__(self, feed_url: str, tickers, meta: dict, lookback_minutes: int = 30):
        self.feed_url = feed_url
        self.tickers = list(tickers)
        self.meta = meta or {}
        self.lookback = lookback_minutes
        # build alias map per ticker (lowercased)
        self.aliases = {}
        for t in self.tickers:
            m = self.meta.get(t, {}) or {}
            names = []
            if m.get("name"): names.append(m["name"])
            names += (m.get("rss_keywords") or [])
            # add cashtag form and company name variations
            names += [f"${t}", t]  # cashtag & raw ticker (we'll handle case separately)
            self.aliases[t] = {s.lower() for s in names if s}

        # precompile ticker regexes
        self._ticker_res = {t: _ticker_re(t) for t in self.tickers}

    def _now(self):
        return datetime.now(UTC)

    def _extract_text(self, entry):
        # title + summary + content fields
        parts = []
        for k in ("title", "summary"):
            v = getattr(entry, k, None) or entry.get(k) if isinstance(entry, dict) else None
            if v: parts.append(v)
        try:
            for c in getattr(entry, "content", []) or entry.get("content", []):
                v = getattr(c, "value", None) or c.get("value")
                if v: parts.append(v)
        except Exception:
            pass
        return " \n ".join(parts)

    def _match_tickers(self, text: str):
        hits = set()
        low = text.lower()
        # cashtags
        for m in CASHTAG.findall(text):
            if m in self.tickers:
                hits.add(m)
        # aliases/keywords (lowercased)
        for t, al in self.aliases.items():
            if any(a in low for a in al):
                hits.add(t)
        # explicit ALL-CAPS ticker tokens (avoid 'meta' ambiguity)
        for t, rx in self._ticker_res.items():
            if rx.search(text):
                hits.add(t)
        return list(hits)

    def fetch(self):
        d = feedparser.parse(self.feed_url)
        if not d or not getattr(d, "entries", None):
            return []
        now = self._now()
        cutoff = now - timedelta(minutes=self.lookback)
        out = []
        for e in d.entries:
            # parse timestamp
            ts = None
            for k in ("published_parsed","updated_parsed"):
                v = getattr(e, k, None) or e.get(k)
                if v:
                    ts = datetime(*v[:6], tzinfo=UTC)
                    break
            if ts is None:  # keep if unsure but recent
                ts = now
            if ts < cutoff:
                continue
            text = self._extract_text(e) or ""
            if not text.strip():
                continue
            tickers = self._match_tickers(text)
            if not tickers:
                continue
            url = getattr(e, "link", None) or e.get("link", "")
            title = getattr(e, "title", None) or e.get("title", "") or "(no title)"
            for t in tickers:
                ev = NewsEvent(
                    id=f"news-{t}-{int(ts.timestamp())}-{hash(title)%100000}",
                    type="news",
                    ticker=t,
                    ts=ts,
                    attrs={"source": self.feed_url, "url": url},
                    summary=title[:160]
                )
                out.append(ev)
        return out
