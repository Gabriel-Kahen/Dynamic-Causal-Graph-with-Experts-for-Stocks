from __future__ import annotations
import os
from dataclasses import dataclass
from typing import List, Dict
from datetime import datetime, timezone, timedelta
import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import DataFeed
from ..events import DerivedPriceEvent
from ..utils.market_hours import is_rth

PROVIDER_SYMBOL_OVERRIDES = {
    "BRK-B": "BRK.B",
}

def _alpaca_symbol(ticker: str) -> str:
    return PROVIDER_SYMBOL_OVERRIDES.get(ticker, ticker)

UTC = timezone.utc

@dataclass
class DerivedBarThresholds:
    ret_sigma_up: float
    ret_sigma_down: float
    vol_pct: float

class AlpacaBarsIngestor:
    
    def __init__(self, tickers, thresholds, lookback_bars: int = 80, enforce_rth: bool = True):
        self.enforce_rth = enforce_rth
        self.tickers = tickers
        self.thr = thresholds
        self.lookback_bars = lookback_bars
        self.client = StockHistoricalDataClient(os.environ["ALPACA_API_KEY_ID"], os.environ["ALPACA_API_SECRET_KEY"])
        self._df_hist: Dict[str, pd.DataFrame] = {}

    def _fetch_recent(self, ticker: str, since: datetime, until: datetime) -> pd.DataFrame:
        req = StockBarsRequest(
            symbol_or_symbols=_alpaca_symbol(ticker),
            timeframe=TimeFrame(5, TimeFrameUnit.Minute),
            start=since,
            end=until,
            adjustment='raw',
            feed=DataFeed.IEX,
        )
        bars = self.client.get_stock_bars(req).df
        if bars is None or bars.empty:
            return pd.DataFrame()
        bars = bars.reset_index()
        bars = bars[bars['symbol'] == ticker].copy()
        bars['ts'] = pd.to_datetime(bars['timestamp'], utc=True)
        bars.sort_values('ts', inplace=True)
        bars['ret'] = bars['close'].pct_change()
        return bars

    def _update_hist(self, ticker: str, df_new: pd.DataFrame):
        df_old = self._df_hist.get(ticker)
        if df_old is None or df_old.empty:
            self._df_hist[ticker] = df_new.tail(self.lookback_bars)
        else:
            merged = pd.concat([df_old, df_new], ignore_index=True)
            merged = merged.drop_duplicates(subset=['ts']).sort_values('ts').tail(self.lookback_bars)
            merged['ret'] = merged['close'].pct_change()
            self._df_hist[ticker] = merged

    def fetch(self):
        now = datetime.now(UTC)
        since = now - timedelta(minutes=25)
        out = []
        
        for t in self.tickers:
            df = self._fetch_recent(t, since, now)
            if df.empty: continue
            self._update_hist(t, df)
            hist = self._df_hist[t]
            if len(hist) < 20: continue
            ret_std = hist['ret'].rolling(40, min_periods=20).std()
            latest = hist.iloc[-1]
            sigma = latest['ret'] / max(ret_std.iloc[-1], 1e-9)
            volp = hist['volume'].rank(pct=True).iloc[-1] * 100.0
            latest = hist.iloc[-1]
            ts_dt = latest['ts'].to_pydatetime()  # tz-aware UTC
            if self.enforce_rth and not is_rth(ts_dt):
                continue  # skip non-RTH bars entirely
            if sigma >= self.thr.ret_sigma_up:
                out.append(DerivedPriceEvent(
                    id=f"{t}-bar-{latest['ts'].isoformat()}",
                    type="price_event", ticker=t, ts=latest['ts'].to_pydatetime(),
                    attrs={"kind":"ret_sigma_up","value":float(sigma),"close":float(latest['close']),"volume":int(latest['volume'])},
                    summary=f"{t} 5-min return ≈ +{sigma:.2f}σ; volume pct≈{volp:.1f}."
                ))
            if sigma <= self.thr.ret_sigma_down:
                out.append(DerivedPriceEvent(
                    id=f"{t}-bar-{latest['ts'].isoformat()}-dn",
                    type="price_event", ticker=t, ts=latest['ts'].to_pydatetime(),
                    attrs={"kind":"ret_sigma_down","value":float(sigma),"close":float(latest['close']),"volume":int(latest['volume'])},
                    summary=f"{t} 5-min return ≈ {sigma:.2f}σ (down); volume pct≈{volp:.1f}."
                ))
            if volp >= self.thr.vol_pct:
                out.append(DerivedPriceEvent(
                    id=f"{t}-vol-{latest['ts'].isoformat()}",
                    type="price_event", ticker=t, ts=latest['ts'].to_pydatetime(),
                    attrs={"kind":"vol_spike","value":float(volp)},
                    summary=f"{t} 5-min volume spike at {volp:.1f}th percentile."
                ))
        return out
