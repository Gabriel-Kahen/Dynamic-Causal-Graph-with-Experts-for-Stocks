from __future__ import annotations
from typing import List
from ..events import Event

class BaseIngestor:
    def fetch(self) -> List[Event]:
        raise NotImplementedError
