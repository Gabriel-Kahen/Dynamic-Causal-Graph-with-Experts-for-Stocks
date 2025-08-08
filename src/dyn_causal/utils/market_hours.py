# src/dyn_causal/utils/market_hours.py
from __future__ import annotations
from datetime import datetime, date, timezone
from functools import lru_cache

import pandas_market_calendars as mcal

_XNYS = mcal.get_calendar("XNYS")  # NYSE (holiday + early close aware)

@lru_cache(maxsize=512)
def _session_bounds_utc(d: date):
    """
    Return (open_utc, close_utc) for a given calendar date.
    If market closed/holiday, returns (None, None).
    """
    sched = _XNYS.schedule(start_date=d, end_date=d)
    if sched.empty:
        return (None, None)
    # mcal returns tz-aware pandas Timestamps in UTC
    o = sched.iloc[0]["market_open"].to_pydatetime()
    c = sched.iloc[0]["market_close"].to_pydatetime()
    # Ensure tz-aware UTC
    if o.tzinfo is None:
        o = o.replace(tzinfo=timezone.utc)
    if c.tzinfo is None:
        c = c.replace(tzinfo=timezone.utc)
    return (o.astimezone(timezone.utc), c.astimezone(timezone.utc))

def is_rth(dt: datetime | None = None) -> bool:
    """
    True if dt is within today's regular trading session (XNYS),
    including early closes, excluding holidays/weekends.
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    o, c = _session_bounds_utc(dt_utc.date())
    if o is None or c is None:
        return False
    return o <= dt_utc < c
