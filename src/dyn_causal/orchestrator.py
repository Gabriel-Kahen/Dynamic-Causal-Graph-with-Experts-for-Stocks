from __future__ import annotations
from typing import Dict, Any, List
from datetime import datetime, timezone, date
import json

from .config import load_config, Config
from .graph import DynamicCausalGraph
from .logging_store import EventLog
from .events import Event
from .gating import CandidateGenerator
from .llm import LLMClient, DebateOrchestrator
from .alerts import AlertEngine, AlertThresholds
from .inference import expected_alpha_and_prob
from .utils.market_hours import is_rth

UTC = timezone.utc

class Orchestrator:
    """Ingest -> summarize -> gate -> debate -> graph -> inference/alerts."""
    def __init__(self, cfg: Config):
        self.cfg = cfg

        # Build sectors & peers from config.universe_meta
        meta = cfg.universe_meta or {}
        self.sectors = {t: (meta.get(t, {}).get('sector')) for t in cfg.universe.tickers}
        self.peers = {t: (meta.get(t, {}).get('peers', [])) for t in cfg.universe.tickers}

        half_lives = {
            "price_event": cfg.decay.price_event_days,
            "news": cfg.decay.news_days,
            "filing": cfg.decay.filing_days,
            "macro": cfg.decay.macro_days,
            "social": cfg.decay.social_days,
        }
        self.graph = DynamicCausalGraph(half_lives_days=half_lives)
        self.event_log = EventLog(cfg.logging.sqlite_path)

        self.candidates = CandidateGenerator(cfg.gating, peers_map=self.peers, sector_map=self.sectors)
        self.llm = LLMClient(model=cfg.debate.model)
        self.debate = DebateOrchestrator(self.llm, max_rounds=cfg.debate.max_rounds)
        
        # --- Budget: cap edges/day to stay under $ limit ---
        self._budget_day = date.today()
        edges_per_day_cap = int(self.cfg.budget.daily_usd_cap / max(self.cfg.budget.est_usd_per_edge, 1e-9))
        self._edges_used_today = 0
        self._edges_day_cap = max(1, edges_per_day_cap)

        self.alerts = AlertEngine(cfg.alerts.jsonl_path, enable_console=cfg.alerts.enable_console)
        self.thresholds = AlertThresholds(k_sigma=cfg.horizon.spread_sigma_k, min_p=cfg.horizon.min_probability)

    def insert_event(self, ev: Event):
        self.graph.add_event_node(ev)
        self.event_log.append("add_node", ev.to_node())

        snapshot = self.graph.snapshot()
        existing = [Event(**{k:v for k,v in n.items() if k in ["id","type","ticker","summary"]},
                          ts=datetime.fromisoformat(n["ts"]), attrs=n["attrs"]) for n in snapshot["nodes"]]
        pairs = self.candidates.plausible_pairs(ev, existing)
        # RTH gating: evaluate edges only during RTH; optionally only when effect is a price_event
        if self.cfg.rth.enforce:
            if (not is_rth()) or (self.cfg.rth.require_price_event and ev.type != "price_event"):
                pairs = []  # keep the node, but skip LLM/edges/alerts now

        for cause, effect in pairs:
            if not self._budget_edge_available():
                self.event_log.append("budget_skip_pair", {
                    "src": cause.id, "dst": effect.id,
                    "reason": "daily edge cap reached",
                    "edges_used_today": self._edges_used_today,
                    "edges_day_cap": self._edges_day_cap
                })
                continue
            
            meta = {"ticker_cause": cause.ticker, "ticker_effect": effect.ticker, "type_cause": cause.type, "type_effect": effect.type}
            result = self.debate.run_debate(cause.to_node(), effect.to_node(), meta, rounds=self.cfg.debate.max_rounds)
            self._edges_used_today += 1

            judge = result["judge"]
            if int(judge.get("edge", 0)) != 1:
                continue
            polarity = int(judge.get("polarity", 0))
            conf = float(judge.get("confidence", 0.0))
            if conf < self.cfg.weights.min_confidence_to_add:
                continue

            blended = self.cfg.weights.alpha_blend * conf + (1 - self.cfg.weights.alpha_blend) * self.cfg.weights.initial_edge_weight
            self.graph.add_or_update_edge(cause.id, effect.id, weight=blended, polarity=polarity,
                                          evidence={"event_id": effect.id, "judge": judge, "experts": result["experts"]})
            self.event_log.append("add_or_update_edge", {"src": cause.id, "dst": effect.id, "weight": blended, "polarity": polarity})

        self.graph.decay()
        snap = self.graph.snapshot()
        if ev.ticker:
            info = expected_alpha_and_prob(snap, ev.ticker)
            rationale = f"Graph net support {('bullish' if info['polarity']>0 else 'bearish')} with score={info['expected_sigma']:.2f}σ."
            alert = self.alerts.maybe_alert(ev.ticker, self.cfg.horizon.minutes, info["p"], info["expected_sigma"], int(info["polarity"]), rationale, self.thresholds)
            if alert:
                self.event_log.append("alert", alert)

        return True

    def _budget_edge_available(self) -> bool:
        # reset at UTC midnight; switch to US/Eastern if you prefer
        today = date.today()
        if today != self._budget_day:
            self._budget_day = today
            self._edges_used_today = 0
        return self._edges_used_today < self._edges_day_cap
