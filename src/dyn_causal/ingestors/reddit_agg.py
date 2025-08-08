# src/dyn_causal/ingestors/reddit_agg.py
from __future__ import annotations

import os
import re
import math
import collections
from typing import List, Dict, Deque, Tuple

from datetime import datetime, timezone, timedelta
import praw
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from ..events import SocialAggregateEvent

UTC = timezone.utc
TICKER_RE = re.compile(r"\b([A-Z]{1,5})\b")


class RedditAggregator:
    """
    Aggregates Reddit posts/comments into ticker social-activity events with:
      - rolling 120m buffer
      - maturity + min score filter (configurable)
      - optional score refresh for aged posts (rate-limited)
      - curated link list per node (configurable)
      - 'viral surge' override: if raw 5m mentions >= threshold, emit immediately
    """

    def __init__(
        self,
        subreddits: List[str],
        tickers: List[str],
        *,
        # polling / aggregation windows
        window_min: int = 5,
        agg_windows: Tuple[int, ...] = (5, 30, 60),
        buffer_minutes: int = 120,
        # signal thresholds
        min_mentions_5m: int = 3,
        min_mentions_ambiguous: Dict[str, int] = None,  # e.g., {"V": 5}
        zscore_trigger_30m: float = 2.0,
        min_abs_sentiment_30m: float = 0.25,
        # link curation
        include_links: bool = True,
        max_links_per_node: int = 10,
        link_selection: str = "recent",  # "recent" | "top" | "sentiment"
        # maturity / score gating
        min_score: int = 3,
        score_maturity_minutes: int = 10,
        refresh_scores: bool = True,
        max_score_refresh_per_cycle: int = 30,
        # viral surge override
        viral_5m_threshold: int = 10,
    ):
        self.reddit = praw.Reddit(
            client_id=os.environ["REDDIT_CLIENT_ID"],
            client_secret=os.environ["REDDIT_CLIENT_SECRET"],
            user_agent=os.environ["REDDIT_USER_AGENT"],
        )
        self.reddit.read_only = True

        self.subs = subreddits
        self.tickers = set(tickers)
        self.sid = SentimentIntensityAnalyzer()

        self.window_min = window_min
        self.agg_windows = agg_windows
        self.buffer_minutes = buffer_minutes

        self.min_mentions_5m = min_mentions_5m
        self.min_mentions_ambiguous = min_mentions_ambiguous or {"V": 5}
        self.zscore_trigger_30m = zscore_trigger_30m
        self.min_abs_sentiment_30m = min_abs_sentiment_30m

        self.include_links = include_links
        self.max_links_per_node = max_links_per_node
        self.link_selection = link_selection

        self.min_score = min_score
        self.score_maturity_minutes = score_maturity_minutes
        self.refresh_scores = refresh_scores
        self.max_score_refresh_per_cycle = max_score_refresh_per_cycle

        self.viral_5m_threshold = viral_5m_threshold

        # rolling buffer per (ticker, subreddit)
        # entry schema: {"id","ts","sentiment","url","title","score","last_refresh"}
        self.buf_posts: Dict[Tuple[str, str], Deque[dict]] = collections.defaultdict(collections.deque)

        # rolling baseline (EMA) for 30m mentions per (ticker, subreddit)
        self.baseline_ct: Dict[Tuple[str, str], float] = collections.defaultdict(lambda: 1.0)
        self.baseline_var: Dict[Tuple[str, str], float] = collections.defaultdict(lambda: 1.0)

    # ---------- utils ----------

    def _score_text(self, text: str) -> float:
        return float(self.sid.polarity_scores(text or "")["compound"])

    def _extract_tickers(self, text: str) -> List[str]:
        hits = set(m for m in TICKER_RE.findall(text or "") if m in self.tickers)
        return list(hits)

    def _prune_old(self, dq: Deque[dict], now: datetime):
        cut = now - timedelta(minutes=self.buffer_minutes)
        while dq and dq[0]["ts"] < cut:
            dq.popleft()

    def _matured(self, post: dict, now: datetime) -> bool:
        age_min = (now - post["ts"]).total_seconds() / 60.0
        return age_min >= self.score_maturity_minutes

    def _maybe_refresh_scores(self, dq: Deque[dict], now: datetime):
        """Optionally refresh scores for matured posts (rate-limited)."""
        if not self.refresh_scores or self.max_score_refresh_per_cycle <= 0:
            return
        budget = self.max_score_refresh_per_cycle
        for p in list(dq):
            if budget <= 0:
                break
            if not self._matured(p, now):
                continue
            # avoid hammering the same post repeatedly
            if p["last_refresh"] and (now - p["last_refresh"]).total_seconds() < 300:
                continue
            pid = p.get("id")
            if not pid:
                continue
            try:
                subm = self.reddit.submission(id=pid)
                p["score"] = int(getattr(subm, "score", p["score"]))
                p["last_refresh"] = now
                budget -= 1
            except Exception:
                p["last_refresh"] = now  # mark so we don't retry immediately

    def _subset(self, dq: Deque[dict], now: datetime, minutes: int) -> List[dict]:
        start = now - timedelta(minutes=minutes)
        return [p for p in dq if p["ts"] >= start]

    def _count_sent_filtered(self, dq: Deque[dict], now: datetime, minutes: int) -> Tuple[int, float, List[dict]]:
        """Only include posts that are matured AND meet min_score."""
        subset = self._subset(dq, now, minutes)
        subset = [p for p in subset if (self._matured(p, now) and p["score"] >= self.min_score)]
        if not subset:
            return 0, 0.0, []
        return len(subset), sum(p["sentiment"] for p in subset) / len(subset), subset

    def _count_sent_raw(self, dq: Deque[dict], now: datetime, minutes: int) -> Tuple[int, float, List[dict]]:
        """No maturity/score filter. Used for viral override detection."""
        subset = self._subset(dq, now, minutes)
        if not subset:
            return 0, 0.0, []
        return len(subset), sum(p["sentiment"] for p in subset) / len(subset), subset

    def _update_baseline(self, key: Tuple[str, str], count_30m: int):
        # EMA baseline with alpha=0.1; variance via simple EWMA-of-squared-deviation
        a = 0.1
        mu = self.baseline_ct[key]
        var = self.baseline_var[key]
        new_mu = (1 - a) * mu + a * count_30m
        new_var = (1 - a) * (var + a * (count_30m - mu) ** 2)
        self.baseline_ct[key] = max(1.0, new_mu)
        self.baseline_var[key] = max(1.0, new_var)

    # ---------- main API ----------

    def fetch(self) -> List[SocialAggregateEvent]:
        now = datetime.now(UTC)
        start = now - timedelta(minutes=self.window_min)

        # ingest new posts
        for sub in self.subs:
            s = self.reddit.subreddit(sub)
            # limit tuned to easily stay under 100 QPM across subs
            for post in s.new(limit=150):
                ts = datetime.fromtimestamp(getattr(post, "created_utc", 0), UTC)
                if ts < start:
                    break
                title = (getattr(post, "title", "") or "")
                body = (getattr(post, "selftext", "") or "")
                text = f"{title}\n{body}"
                targets = self._extract_tickers(text)
                if not targets:
                    continue

                sc = self._score_text(text)
                pid = getattr(post, "id", None)
                permalink = getattr(post, "permalink", None)
                url = f"https://www.reddit.com{permalink}" if permalink else (getattr(post, "url", "") or "")
                score = int(getattr(post, "score", 0))

                for t in targets:
                    key = (t, sub)
                    dq = self.buf_posts[key]
                    dq.append(
                        {
                            "id": pid,
                            "ts": ts,
                            "sentiment": sc,
                            "url": url,
                            "title": title,
                            "score": score,
                            "last_refresh": None,
                        }
                    )
                    self._prune_old(dq, now)

        # aggregate & emit if thresholds hit
        out: List[SocialAggregateEvent] = []
        for (t, sub), dq in list(self.buf_posts.items()):
            self._prune_old(dq, now)
            if not dq:
                continue

            # refresh matured scores (optional)
            self._maybe_refresh_scores(dq, now)

            # counts with *filters* (maturity + min_score)
            c5, s5, posts5 = self._count_sent_filtered(dq, now, 5)
            c30, s30, posts30 = self._count_sent_filtered(dq, now, 30)
            c60, s60, posts60 = self._count_sent_filtered(dq, now, 60)

            # raw (no filters) for viral detection
            c5_raw, s5_raw, posts5_raw = self._count_sent_raw(dq, now, 5)

            # update EMA baseline & z-score on filtered 30m count
            self._update_baseline((t, sub), c30)
            mu = self.baseline_ct[(t, sub)]
            sd = math.sqrt(self.baseline_var[(t, sub)])
            z30 = 0.0 if sd == 0 else (c30 - mu) / sd

            # tougher threshold for ambiguous tickers (e.g., "V")
            min5 = self.min_mentions_ambiguous.get(t, self.min_mentions_5m)

            # viral override: if raw 5m mentions explode, emit immediately
            is_viral = c5_raw >= self.viral_5m_threshold

            should_emit = (
                is_viral
                or (c5 >= min5)
                or (z30 >= self.zscore_trigger_30m)
                or (abs(s30) >= self.min_abs_sentiment_30m and c30 >= max(5, min5))
            )
            if not should_emit:
                continue

            # curate links
            links = []
            if self.include_links:
                # If viral, show the last 5m raw posts; else show vetted 60m posts
                pool = posts5_raw if is_viral else posts60

                if self.link_selection == "recent":
                    pool = sorted(pool, key=lambda p: p["ts"], reverse=True)
                elif self.link_selection == "top":
                    pool = sorted(pool, key=lambda p: p["score"], reverse=True)
                elif self.link_selection == "sentiment":
                    pool = sorted(pool, key=lambda p: abs(p["sentiment"]), reverse=True)

                # de-dup by URL and cap
                uniq, curated = set(), []
                for p in pool:
                    u = p.get("url") or ""
                    if not u or u in uniq:
                        continue
                    uniq.add(u)
                    curated.append(p)
                    if len(curated) >= self.max_links_per_node:
                        break

                links = [
                    {
                        "url": p["url"],
                        "title": (p.get("title") or "")[:140],
                        "ts": p["ts"].isoformat(),
                        "score": int(p.get("score", 0)),
                        "sentiment": round(float(p.get("sentiment", 0.0)), 3),
                    }
                    for p in curated
                ]

            ev_id = f"reddit-{sub}-{t}-{int(now.timestamp())}"
            out.append(
                SocialAggregateEvent(
                    id=ev_id,
                    type="social",
                    ticker=t,
                    ts=now,
                    attrs={
                        "subreddit": sub,
                        # filtered (matured + min_score) metrics
                        "mentions_5m": c5,
                        "mentions_30m": c30,
                        "mentions_60m": c60,
                        "sentiment_5m": s5,
                        "sentiment_30m": s30,
                        "sentiment_60m": s60,
                        "z30": z30,
                        # raw 5m for visibility/viral detection
                        "mentions_5m_raw": c5_raw,
                        "sentiment_5m_raw": s5_raw,
                        "viral": is_viral,
                        **({"links": links} if self.include_links else {}),
                    },
                    summary=(
                        f"Reddit/{sub} {t}: "
                        f"5m={c5} (raw {c5_raw}), 30m={c30} (z≈{z30:.1f}), "
                        f"60m={c60}, sent30m={s30:.2f}{' [VIRAL]' if is_viral else ''}"
                    ),
                )
            )

        return out
