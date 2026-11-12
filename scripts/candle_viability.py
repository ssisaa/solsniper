"""Candle-momentum viability report (spec acceptance #10).

    venv/bin/python scripts/candle_viability.py [--db logs/candle_momo.sqlite] [--out logs/candle_viability.md]

Reads the paper-trading SQLite and writes a Markdown report: signal count, executed / skipped
(with reasons), win rate, gross vs net PnL after fees and slippage, worst case (coins that paused
while held counted as total losses), speed, and a go / no-go. GO needs all of: >= 24 h of data,
>= 50 round trips, net and worst-case PnL above zero, and the 95% lower bound of the mean
per-trade return above zero.
"""
from __future__ import annotations

import argparse
import math
import os
import sqlite3
import statistics as st
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def pct(vals, q):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    k = (len(vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def fmt(v, d=1, suffix=""):
    return "—" if v is None else f"{v:,.{d}f}{suffix}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(ROOT, "logs", "candle_momo.sqlite"))
    ap.add_argument("--out", default=os.path.join(ROOT, "logs", "candle_viability.md"))
    a = ap.parse_args()
    if not os.path.exists(a.db):
        print(f"No data yet: {a.db}")
        return 1
    db = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    q = lambda sql, *p: db.execute(sql, p).fetchall()
    first, last = q("SELECT MIN(ts), MAX(ts) FROM signals")[0]
    hours = ((last - first) / 3_600_000.0) if first and last else 0.0     # signals.ts is ms
    sides = dict(q("SELECT side, COUNT(*) FROM signals GROUP BY side"))
    actions = dict(q("SELECT action, COUNT(*) FROM signals GROUP BY action"))
    skips = q("SELECT reason, COUNT(*) FROM signals WHERE action='skipped' GROUP BY reason ORDER BY 2 DESC")
    fails = q("SELECT error, COUNT(*) FROM fills WHERE status='failed' GROUP BY error ORDER BY 2 DESC LIMIT 8")
    tr = q("SELECT pnl_sol, gross_pnl_sol, costs_sol, pct, entry_sol, paused_exit, exit_ts-entry_ts FROM trades")
    fills = q("SELECT latency_ms, fill_ms, slippage_bps FROM fills WHERE status='confirmed'")
    n = len(tr)
    net = sum(t[0] for t in tr)
    gross = sum(t[1] for t in tr)
    costs = sum(t[2] for t in tr)
    worst = sum((-t[4] if t[5] else t[0]) for t in tr)
    wins = sum(1 for t in tr if t[0] > 0)
    rets = [t[3] for t in tr]
    mean = st.mean(rets) if rets else None
    lo95 = (mean - 1.96 * st.stdev(rets) / math.sqrt(len(rets))) if len(rets) > 1 else None
    paused = sum(t[5] for t in tr)
    held = [t[6] for t in tr]
    lat = [f[0] for f in fills]
    fms = [f[1] for f in fills]
    slip = [f[2] for f in fills]
    checks = [
        ("≥ 24 h of paper data", hours >= 24, f"{hours:.1f} h"),
        ("≥ 50 round trips", n >= 50, str(n)),
        ("Net PnL > 0 after fees and slippage", net > 0, f"{net:+.5f} SOL"),
        ("Worst case (paused = total loss) > 0", worst > 0, f"{worst:+.5f} SOL"),
        ("95% lower bound of mean return > 0", lo95 is not None and lo95 > 0, fmt(lo95, 2, "%")),
    ]
    go = all(c[1] for c in checks)
    lines = [
        "# Candle-momentum viability report", "",
        f"Generated {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} from `{os.path.relpath(a.db, ROOT)}`. "
        "Paper trading only: fills priced on pump's bonding curve 0.45 s after the decision, pump fee and network "
        "fees included. Rule and thresholds per signal are in `logs/candle_audit.jsonl` (config digest on every row).", "",
        f"## Verdict: **{'GO' if go else 'NO-GO'}**", "",
        "| Check | Pass | Value |", "|---|---|---|",
        *[f"| {name} | {'✅' if ok else '❌'} | {val} |" for name, ok, val in checks], "",
        "## Signals", "",
        f"- Window: {hours:.1f} h · BUY {sides.get('BUY', 0)} · SELL {sides.get('SELL', 0)}",
        f"- Filled {actions.get('filled', 0)} · skipped {actions.get('skipped', 0)} · failed {actions.get('failed', 0)}",
        "- Skip reasons: " + (", ".join(f"{r} {c}" for r, c in skips) or "none"),
        "- Failures: " + (", ".join(f"{(e or '?')[:60]} ×{c}" for e, c in fails) or "none"), "",
        "## Results", "",
        "| Metric | Value |", "|---|---|",
        f"| Round trips | {n} |",
        f"| Win rate | {fmt(100.0 * wins / n if n else None, 1, '%')} |",
        f"| Gross PnL (before fees) | {gross:+.5f} SOL |",
        f"| Costs (pump + network fees) | {costs:.5f} SOL |",
        f"| Net PnL | {net:+.5f} SOL |",
        f"| Worst case (paused exits = −100%) | {worst:+.5f} SOL ({paused} paused exits) |",
        f"| Mean / median return per trade | {fmt(mean, 2, '%')} / {fmt(st.median(rets) if rets else None, 2, '%')} |",
        f"| Mean 95% lower bound | {fmt(lo95, 2, '%')} |",
        f"| Median hold | {fmt(st.median(held) if held else None, 1, ' s')} |", "",
        "## Speed", "",
        f"- Signal → signed tx (would be sent): p50 {fmt(pct(lat, .5), 0, ' ms')}, p95 {fmt(pct(lat, .95), 0, ' ms')} "
        "(target ≤ 150 / ≤ 400 ms)",
        f"- Signal → priced fill: p50 {fmt(pct(fms, .5), 0, ' ms')}",
        f"- Realized slippage: p50 {fmt(pct(slip, .5), 0, ' bps')}, p95 {fmt(pct(slip, .95), 0, ' bps')}",
        "- The candle itself is already 1–3 s old when the feed serves it (feed lag on the dashboard).", "",
    ]
    text = "\n".join(lines)
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(text)
    print(f"\nWritten to {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
