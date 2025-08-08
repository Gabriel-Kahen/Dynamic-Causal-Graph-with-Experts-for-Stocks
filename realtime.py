# realtime.py
import os
import time
import json
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()  # pulls GEMINI_API_KEY, ALPACA_*, REDDIT_* from .env

from dyn_causal.config import load_config
from dyn_causal.orchestrator import Orchestrator
from dyn_causal.ingestors.news_runner import build_news_ingestors_from_cfg

# Optional ingestors (constructed conditionally based on env/config)
from dyn_causal.ingestors.alpaca_bars import AlpacaBarsIngestor, DerivedBarThresholds
from dyn_causal.ingestors.reddit_agg import RedditAggregator

UTC = timezone.utc


def build_alpaca_bars(cfg):
    """Build Alpaca bars ingestor if keys are present; else return None."""
    if not (os.getenv("ALPACA_API_KEY_ID") and os.getenv("ALPACA_API_SECRET_KEY")):
        print("[realtime] Alpaca creds missing — skipping price bars.")
        return None
    return AlpacaBarsIngestor(
        tickers=cfg.universe.tickers,
        thresholds=DerivedBarThresholds(
            ret_sigma_up=cfg.derived_bars.return_sigma_up,
            ret_sigma_down=cfg.derived_bars.return_sigma_down,
            vol_pct=cfg.derived_bars.volume_percentile,
        ),
        lookback_bars=100,
        enforce_rth=cfg.rth.enforce,
    )


def build_reddit(cfg):
    """Build Reddit aggregator if creds are present; else return None."""
    have = all(
        os.getenv(k)
        for k in ("REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET", "REDDIT_USER_AGENT")
    )
    if not have:
        print("[realtime] Reddit creds missing — skipping Reddit aggregator.")
        return None

    rcfg = cfg.reddit
    return RedditAggregator(
        subreddits=rcfg.subreddits,
        tickers=cfg.universe.tickers,
        # link curation
        include_links=getattr(rcfg, "include_links", True),
        max_links_per_node=getattr(rcfg, "max_links_per_node", 10),
        link_selection=getattr(rcfg, "link_selection", "recent"),
        # maturity / score gating
        min_score=getattr(rcfg, "min_score", 3),
        score_maturity_minutes=getattr(rcfg, "score_maturity_minutes", 10),
        refresh_scores=getattr(rcfg, "refresh_scores", True),
        max_score_refresh_per_cycle=getattr(rcfg, "max_score_refresh_per_cycle", 30),
        # activity thresholds (filtered and viral override)
        min_mentions_5m=getattr(rcfg, "min_mentions_5m", 3),
        zscore_trigger_30m=getattr(rcfg, "zscore_trigger_30m", 2.0),
        min_abs_sentiment_30m=getattr(rcfg, "min_abs_sentiment_30m", 0.25),
        viral_5m_threshold=getattr(rcfg, "viral_5m_threshold", 10),
        # rolling buffer
        buffer_minutes=120,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=int(os.getenv("POLL_SECONDS", "300")),
                    help="Polling interval in seconds (default 300).")
    ap.add_argument("--once", action="store_true", help="Run a single cycle then exit.")
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    orch = Orchestrator(cfg)

    ingestors = []

    # News (RSS) — always on if feeds configured
    news_ingestors = build_news_ingestors_from_cfg(cfg)
    if news_ingestors:
        ingestors.extend(news_ingestors)
        print(f"[realtime] News RSS enabled ({len(news_ingestors)} feeds).")
    else:
        print("[realtime] No RSS feeds configured — skipping news.")

    # Reddit (social)
    reddit = build_reddit(cfg)
    if reddit:
        ingestors.append(reddit)
        print(f"[realtime] Reddit aggregator enabled: subs={cfg.reddit.subreddits}")

    # Price bars (Alpaca)
    bars = build_alpaca_bars(cfg)
    if bars:
        ingestors.append(bars)
        print("[realtime] Alpaca bars enabled (IEX feed).")

    if not ingestors:
        raise SystemExit("[realtime] No ingestors enabled. Add creds / feeds and try again.")

    print(f"[realtime] Loop started. Interval={args.interval}s. Press Ctrl+C to stop.")
    while True:
        cycle_start = datetime.now(UTC)
        added = 0
        for ing in ingestors:
            try:
                events = ing.fetch()
            except Exception as e:
                print(f"[realtime] Ingestor {ing.__class__.__name__} failed: {e}")
                continue

            for ev in events:
                try:
                    orch.insert_event(ev)
                    added += 1
                except Exception as e:
                    print(f"[realtime] insert_event failed for {ev.id}: {e}")

        # snapshot for viewer
        snap_path = "data/latest_graph.json"
        try:
            with open(snap_path, "w") as f:
                json.dump(orch.graph.snapshot(), f, indent=2)
            print(f"[{cycle_start.isoformat()}] {added} events | snapshot -> {snap_path}")
        except Exception as e:
            print(f"[realtime] snapshot error: {e}")

        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
