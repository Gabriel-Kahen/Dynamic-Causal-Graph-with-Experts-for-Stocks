from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import os, json

UTC = timezone.utc

@dataclass
class AlertThresholds:
    k_sigma: float
    min_p: float

class AlertEngine:
    def __init__(self, jsonl_path: str, enable_console: bool = True):
        self.jsonl_path = jsonl_path
        os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
        self.enable_console = enable_console

    def maybe_alert(self, ticker: str, horizon_min: int, p: float, expected_sigma: float, polarity: int, rationale: str, thresholds: AlertThresholds):
        if p >= thresholds.min_p and expected_sigma >= thresholds.k_sigma:
            payload = {
                "ts": datetime.now(UTC).isoformat(),
                "ticker": ticker,
                "horizon_min": horizon_min,
                "direction": "UP" if polarity > 0 else "DOWN",
                "probability": round(p, 4),
                "expected_sigma": round(expected_sigma, 3),
                "rationale": rationale,
            }
            with open(self.jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
            if self.enable_console:
                print("[ALERT]", json.dumps(payload))
            return payload
        return None
