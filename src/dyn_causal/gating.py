from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional, Set
from datetime import timedelta
import re
from .events import Event
from .config import GatingConfig

TICKER_RE = re.compile(r"\b[A-Z]{1,5}\b")

def extract_entities(text: str) -> Set[str]:
    tickers = set(re.findall(r"\$([A-Z]{1,5})", text or ""))
    for m in TICKER_RE.findall(text or ""):
        if len(m) <= 5:
            tickers.add(m)
    return tickers

class CandidateGenerator:
    def __init__(self, cfg: GatingConfig, peers_map: Dict[str, Any], sector_map: Dict[str, str]):
        self.cfg = cfg
        self.peers_map = peers_map
        self.sector_map = sector_map

    def plausible_pairs(self, new_event: Event, existing_events: List[Event]) -> List[Tuple[Event, Event]]:
        pairs = []
        for ev in existing_events:
            if ev.id == new_event.id: 
                continue
            if ev.ts >= new_event.ts:
                continue
            max_lag = timedelta(minutes=self.cfg.max_time_lag_minutes)
            if new_event.type == "price_event":
                max_lag = timedelta(minutes=self.cfg.max_bar_lag_minutes)
            if (new_event.ts - ev.ts) > max_lag:
                continue
            if self._is_plausible(ev, new_event):
                pairs.append((ev, new_event))
            if len(pairs) >= self.cfg.max_candidate_edges_per_node:
                break
        return pairs

    def _is_plausible(self, cause: Event, effect: Event) -> bool:
        if cause.ticker and effect.ticker and cause.ticker == effect.ticker:
            return True
        if cause.type == "macro" and self.cfg.allow_macro_to_sector_or_ticker and effect.ticker:
            return True
        if self.cfg.allow_cross_ticker_within_sector and cause.ticker and effect.ticker:
            if self.sector_map.get(cause.ticker) == self.sector_map.get(effect.ticker):
                return True
        if self.cfg.allow_supply_chain_links and cause.ticker and effect.ticker:
            if effect.ticker in set(self.peers_map.get(cause.ticker, [])):
                return True
        ct = (cause.attrs.get("headline") or "") + " " + (cause.summary or "")
        et = (effect.attrs.get("headline") or "") + " " + (effect.summary or "")
        if cause.ticker and cause.ticker in extract_entities(et):
            return True
        if effect.ticker and effect.ticker in extract_entities(ct):
            return True
        return False
