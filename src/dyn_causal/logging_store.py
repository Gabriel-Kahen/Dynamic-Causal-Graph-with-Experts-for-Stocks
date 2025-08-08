from __future__ import annotations
import sqlite3, json, os
from datetime import datetime, timezone

UTC = timezone.utc

class EventLog:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(path)
        self._init()

    def _init(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            action TEXT NOT NULL,
            payload TEXT NOT NULL
        )
        """)
        self.conn.commit()

    def append(self, action: str, payload: dict):
        ts = datetime.now(UTC).isoformat()
        cur = self.conn.cursor()
        cur.execute("INSERT INTO events(ts, action, payload) VALUES (?,?,?)",
                    (ts, action, json.dumps(payload)))
        self.conn.commit()
