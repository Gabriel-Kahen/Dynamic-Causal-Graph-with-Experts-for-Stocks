from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import yaml, os
from dataclasses import dataclass, field, is_dataclass

@dataclass
class RTHConfig:
    enforce: bool = True
    require_price_event: bool = True

@dataclass
class DebateConfig:
    max_rounds: int = 1
    escalate_gray_band: Tuple[float,float] = (0.45, 0.60)
    summary_token_budget: int = 150
    model: str = "gemini-2.5-flash-lite"

@dataclass
class HorizonConfig:
    minutes: int = 90
    spread_sigma_k: float = 1.0
    min_probability: float = 0.65

@dataclass
class DecayConfig:
    price_event_days: float = 1.0
    news_days: float = 5.0
    filing_days: float = 10.0
    macro_days: float = 45.0
    social_days: float = 2.0

@dataclass
class WeightConfig:
    alpha_blend: float = 0.7
    initial_edge_weight: float = 0.55
    min_confidence_to_add: float = 0.50

@dataclass
class GatingConfig:
    max_candidate_edges_per_node: int = 10
    max_time_lag_minutes: int = 24*60
    max_bar_lag_minutes: int = 90
    allow_cross_ticker_within_sector: bool = True
    allow_supply_chain_links: bool = True
    allow_macro_to_sector_or_ticker: bool = True

@dataclass
class DerivedBarConfig:
    return_sigma_up: float = 1.0
    return_sigma_down: float = -1.25
    volume_percentile: float = 95.0
    gap_percent: float = 1.0

@dataclass
class AlertConfig:
    enable_console: bool = True
    enable_jsonl_sink: bool = True
    jsonl_path: str = "data/alerts.jsonl"

@dataclass
class LogConfig:
    sqlite_path: str = "data/events.sqlite"
    snapshot_json_path: str = "data/snapshots"

@dataclass
class UniverseConfig:
    tickers: List[str] = field(default_factory=lambda: [
        "AAPL","NVDA","MSFT","GOOG","AMZN","META","BRK-B","LLY","AVGO","TSLA","JPM","WMT","UNH","XOM","V"
    ])
    reference_index: str = "SPY"

@dataclass
class RedditConfig:
    subreddits: List[str] = field(default_factory=lambda: ["wallstreetbets","stocks","investing"])
    max_qpm: int = 100

@dataclass
class BudgetConfig:
    daily_usd_cap: float = 1.0
    est_usd_per_edge: float = 0.00050

@dataclass
class Config:
    debate: DebateConfig = field(default_factory=DebateConfig)
    horizon: HorizonConfig = field(default_factory=HorizonConfig)
    decay: DecayConfig = field(default_factory=DecayConfig)
    weights: WeightConfig = field(default_factory=WeightConfig)
    gating: GatingConfig = field(default_factory=GatingConfig)
    derived_bars: DerivedBarConfig = field(default_factory=DerivedBarConfig)
    alerts: AlertConfig = field(default_factory=AlertConfig)
    logging: LogConfig = field(default_factory=LogConfig)
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    reddit: RedditConfig = field(default_factory=RedditConfig)
    budget: BudgetConfig = field(default_factory=BudgetConfig)
    rth: RTHConfig = field(default_factory=RTHConfig)
    
    # unified metadata/feeds
    universe_meta: dict = field(default_factory=dict)  # ticker -> {name, sector, peers, rss_keywords: [...]}
    news: dict = field(default_factory=lambda: {"feeds": []})
    macro: dict = field(default_factory=lambda: {"fred_series": []})

def load_config(path: Optional[str] = None) -> Config:
    cfg = Config()
    if not path or not os.path.exists(path):
        return cfg

    with open(path, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f) or {}

    def deep_merge(dc_obj, data):
        for k, v in (data or {}).items():
            if not hasattr(dc_obj, k):
                continue
            cur = getattr(dc_obj, k)

            # Recurse into nested dataclasses
            if is_dataclass(cur) and isinstance(v, dict):
                deep_merge(cur, v)

            # Deep-merge dict fields (like news/macro/universe_meta)
            elif isinstance(cur, dict) and isinstance(v, dict):
                def upd(d, u):
                    for kk, vv in u.items():
                        if isinstance(vv, dict) and isinstance(d.get(kk), dict):
                            upd(d[kk], vv)
                        else:
                            d[kk] = vv
                upd(cur, v)

            else:
                setattr(dc_obj, k, v)

    deep_merge(cfg, y)
    return cfg