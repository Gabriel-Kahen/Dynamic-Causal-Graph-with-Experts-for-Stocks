from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime, timezone

UTC = timezone.utc

@dataclass
class Event:
    id: str
    type: str                     # "price_event" | "news" | "filing" | "macro" | "social"
    ticker: Optional[str]
    ts: datetime
    attrs: Dict[str, Any] = field(default_factory=dict)
    summary: Optional[str] = None

    def to_node(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "ticker": self.ticker,
            "ts": self.ts.isoformat(),
            "attrs": self.attrs,
            "summary": self.summary or "",
        }

class DerivedPriceEvent(Event): pass
class NewsEvent(Event): pass
class FilingEvent(Event): pass
class MacroEvent(Event): pass
class SocialAggregateEvent(Event): pass

def utcnow() -> datetime:
    return datetime.now(UTC)
