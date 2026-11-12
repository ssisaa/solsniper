"""Candle-momentum persistence: SQLite (state, dedupe, fills, round trips) + JSONL audit.

The audit file is append-only: every signal, intent, fill and skip, each with signalId and the
config digest in force; every config change is written in full once.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

DEDUPE_S = 26 * 3600     # >= 24 h


class Store:
    def __init__(self, db_path: str, audit_path: str) -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.audit_path = audit_path
        self.lock = threading.Lock()
        self.db = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS signals(id TEXT PRIMARY KEY, mint TEXT, ts REAL, side TEXT, open REAL,
            prev_close REAL, delta_pct REAL, candle_ts INTEGER, rule TEXT, provider TEXT, feed_lag_ms REAL,
            action TEXT, reason TEXT);
        CREATE TABLE IF NOT EXISTS executed(signal_id TEXT PRIMARY KEY, ts REAL);
        CREATE TABLE IF NOT EXISTS fills(signal_id TEXT PRIMARY KEY, mint TEXT, side TEXT, ts REAL, mode TEXT,
            status TEXT, sol REAL, tokens REAL, expected_tokens REAL, expected_sol REAL, slippage_bps REAL,
            pump_fee_sol REAL, network_fee_sol REAL, latency_ms REAL, fill_ms REAL, error TEXT, signature TEXT);
        CREATE TABLE IF NOT EXISTS positions(mint TEXT PRIMARY KEY, symbol TEXT, mode TEXT, tokens REAL,
            entry_sol REAL, entry_ts REAL, entry_signal TEXT, costs_sol REAL);
        CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY AUTOINCREMENT, mint TEXT, symbol TEXT, mode TEXT,
            entry_ts REAL, exit_ts REAL, entry_sol REAL, exit_sol REAL, pnl_sol REAL, pct REAL, costs_sol REAL,
            gross_pnl_sol REAL, entry_signal TEXT, exit_signal TEXT, why TEXT, paused_exit INTEGER);
        CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
        CREATE INDEX IF NOT EXISTS signals_ts ON signals(ts);
        CREATE INDEX IF NOT EXISTS trades_exit ON trades(exit_ts);
        """)
        for table, col, kind in (("positions", "lots", "TEXT"), ("trades", "lots", "INTEGER"),
                                 ("positions", "support", "TEXT")):
            try:
                self.db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {kind}")   # ladder lots (added later)
            except sqlite3.OperationalError:
                pass
        self.prune()

    # ---- audit
    def audit(self, kind: str, **fields: Any) -> None:
        row = {"ts": round(time.time(), 3), "kind": kind}
        row.update(fields)
        line = json.dumps(row, default=str, separators=(",", ":"))
        with self.lock:
            with open(self.audit_path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    # ---- kv
    def get(self, k: str, default: Optional[str] = None) -> Optional[str]:
        with self.lock:
            row = self.db.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
        return row[0] if row else default

    def put(self, k: str, v: str) -> None:
        with self.lock:
            self.db.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

    # ---- dedupe
    def claimed(self, signal_id: str) -> bool:
        with self.lock:
            return self.db.execute("SELECT 1 FROM executed WHERE signal_id=?", (signal_id,)).fetchone() is not None

    def claim(self, signal_id: str) -> bool:
        """Exactly once: True only for the first caller of this id."""
        with self.lock:
            cur = self.db.execute("INSERT OR IGNORE INTO executed(signal_id, ts) VALUES(?,?)", (signal_id, time.time()))
            return cur.rowcount == 1

    def prune(self) -> None:
        with self.lock:
            self.db.execute("DELETE FROM executed WHERE ts < ?", (time.time() - DEDUPE_S,))

    def reset(self) -> str:
        """Clear all Candles history (signals, fills, trades, paper positions, dedupe). The audit log is
        moved aside to candle_audit.<time>.jsonl, not deleted. Returns that file name ('' if none)."""
        with self.lock:
            self.db.execute("BEGIN")
            for t in ("signals", "executed", "fills", "positions", "trades"):
                self.db.execute(f"DELETE FROM {t}")
            self.db.execute("COMMIT")
            self.db.execute("VACUUM")
            moved = ""
            if os.path.exists(self.audit_path) and os.path.getsize(self.audit_path) > 0:
                moved = self.audit_path.replace(".jsonl", time.strftime(".%Y%m%d-%H%M%S.jsonl"))
                os.replace(self.audit_path, moved)
        return moved

    # ---- rows
    def signal(self, s: Dict[str, Any], action: str, reason: str = "") -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO signals VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "action=excluded.action, reason=excluded.reason",
                (s["id"], s["mint"], s["ts"], s["side"], s["open"], s["prevClose"], s["deltaPct"], s["candleTs"],
                 s.get("rule"), s.get("provider"), s.get("feedLagMs"), action, reason))

    def fill(self, f: Dict[str, Any]) -> None:
        cols = ("signal_id", "mint", "side", "ts", "mode", "status", "sol", "tokens", "expected_tokens", "expected_sol",
                "slippage_bps", "pump_fee_sol", "network_fee_sol", "latency_ms", "fill_ms", "error", "signature")
        with self.lock:
            self.db.execute(f"INSERT OR REPLACE INTO fills({','.join(cols)}) VALUES({','.join('?' * len(cols))})",
                            tuple(f.get(c) for c in cols))

    def open_position(self, p: Dict[str, Any]) -> None:
        with self.lock:
            self.db.execute("INSERT OR REPLACE INTO positions(mint, symbol, mode, tokens, entry_sol, entry_ts, "
                            "entry_signal, costs_sol, lots, support) VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (p["mint"], p.get("symbol"), p["mode"], p["tokens"], p["entry_sol"], p["entry_ts"],
                             p["entry_signal"], p.get("costs_sol", 0.0), json.dumps(p.get("lots") or []),
                             p.get("support") or None))

    def drop_position(self, mint: str) -> None:
        """Forget a position without booking a trade (support mode on/off: Candles stops tracking it)."""
        with self.lock:
            self.db.execute("DELETE FROM positions WHERE mint=?", (mint,))

    def positions(self) -> List[Dict[str, Any]]:
        with self.lock:
            cur = self.db.execute("SELECT * FROM positions")
            names = [d[0] for d in cur.description]
            rows = [dict(zip(names, r)) for r in cur.fetchall()]
        for r in rows:
            try:
                r["lots"] = json.loads(r.get("lots") or "[]")
            except (TypeError, ValueError):
                r["lots"] = []
            if not r.get("support"):
                r.pop("support", None)
        return rows

    def close_position(self, mint: str, trade: Dict[str, Any], keep: Optional[Dict[str, Any]] = None) -> None:
        """Record a round trip and remove the position (or, after a partial sell, store what is left)."""
        cols = ("mint", "symbol", "mode", "entry_ts", "exit_ts", "entry_sol", "exit_sol", "pnl_sol", "pct", "costs_sol",
                "gross_pnl_sol", "entry_signal", "exit_signal", "why", "paused_exit", "lots")
        with self.lock:
            self.db.execute("BEGIN")
            try:
                self.db.execute(f"INSERT INTO trades({','.join(cols)}) VALUES({','.join('?' * len(cols))})",
                                tuple(trade.get(c) for c in cols))
                if keep is None:
                    self.db.execute("DELETE FROM positions WHERE mint=?", (mint,))
                else:
                    self.db.execute("UPDATE positions SET tokens=?, entry_sol=?, costs_sol=?, lots=? WHERE mint=?",
                                    (keep["tokens"], keep["entry_sol"], keep.get("costs_sol", 0.0),
                                     json.dumps(keep.get("lots") or []), mint))
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise

    def realized_since(self, ts: float, mode: Optional[str] = None) -> float:
        """Realized PnL since ts; with mode, only that mode's trades (paper losses never block live)."""
        with self.lock:
            if mode:
                row = self.db.execute("SELECT COALESCE(SUM(pnl_sol),0) FROM trades WHERE exit_ts >= ? AND mode = ?",
                                      (ts, mode)).fetchone()
            else:
                row = self.db.execute("SELECT COALESCE(SUM(pnl_sol),0) FROM trades WHERE exit_ts >= ?", (ts,)).fetchone()
        return float(row[0] or 0.0)

    def trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self.lock:
            cur = self.db.execute("SELECT * FROM trades ORDER BY exit_ts DESC LIMIT ?", (limit,))
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]

    def totals(self) -> Dict[str, Any]:
        with self.lock:
            t = self.db.execute(
                "SELECT COUNT(*), COALESCE(SUM(pnl_sol>0),0), COALESCE(SUM(pnl_sol),0), COALESCE(SUM(gross_pnl_sol),0), "
                "COALESCE(SUM(costs_sol),0), COALESCE(SUM(CASE WHEN paused_exit=1 THEN -entry_sol ELSE pnl_sol END),0), "
                "MIN(entry_ts), MAX(exit_ts) FROM trades").fetchone()
            sig = dict(self.db.execute("SELECT side, COUNT(*) FROM signals GROUP BY side").fetchall())
            act = dict(self.db.execute("SELECT action, COUNT(*) FROM signals GROUP BY action").fetchall())
            skips = dict(self.db.execute(
                "SELECT reason, COUNT(*) FROM signals WHERE action='skipped' GROUP BY reason").fetchall())
            first = self.db.execute("SELECT MIN(ts) FROM signals").fetchone()[0]
        n = int(t[0] or 0)
        return {"trades": n, "wins": int(t[1] or 0), "winRate": round(100.0 * int(t[1] or 0) / n, 1) if n else None,
                "netPnlSol": round(float(t[2] or 0), 6), "grossPnlSol": round(float(t[3] or 0), 6),
                "costsSol": round(float(t[4] or 0), 6), "worstCasePnlSol": round(float(t[5] or 0), 6),
                "firstTradeTs": t[6], "lastTradeTs": t[7], "signalsBySide": sig, "signalsByAction": act,
                "skips": skips, "firstSignalTs": (first / 1000.0) if first else None}   # signals.ts is ms
