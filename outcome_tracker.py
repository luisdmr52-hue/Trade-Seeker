"""
outcome_tracker.py — Signal quality tracking for Trade Seeker v4.2
Writes confirmed alerts to outcomes.jsonl and fills price_5m / price_15m
using Binance klines 5-15 minutes after the alert fires.

Usage in main.py:
    import outcome_tracker
    outcome_tracker.start_filler()   # once at startup
    # after tg_send(alert_str):
    outcome_tracker.record(sym, "PUMP_FAST", price, extras)
"""

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

# ── Config ────────────────────────────────────────────────────────────────────
OUTCOMES_PATH  = Path("/root/trade-seeker/logs/outcomes.jsonl")
FILL_INTERVAL  = 60        # seconds between filler sweeps
FILL_WINDOWS   = [5, 15]   # minutes after alert to fill
KLINE_INTERVAL = "1m"      # Binance kline interval for price lookup

# ── Binance REST ──────────────────────────────────────────────────────────────
def _get_kline_close(symbol: str, open_time_ms: int) -> float | None:
    """
    Fetch the closing price of the 1m kline that opened at open_time_ms.
    Returns float or None on any error.
    """
    url = (
        f"https://api.binance.com/api/v3/klines"
        f"?symbol={symbol}&interval={KLINE_INTERVAL}"
        f"&startTime={open_time_ms}&limit=1"
    )
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
            if data and len(data[0]) >= 5:
                return float(data[0][4])  # index 4 = close price
    except Exception:
        pass
    return None


# ── JSONL helpers ─────────────────────────────────────────────────────────────
_lock = threading.Lock()


def _read_all() -> list:
    if not OUTCOMES_PATH.exists():
        return []
    records = []
    with open(OUTCOMES_PATH, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def _write_all(records: list) -> None:
    OUTCOMES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTCOMES_PATH, "w") as f:
        for rec in records:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")


def _append(record: dict) -> None:
    OUTCOMES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTCOMES_PATH, "a") as f:
        f.write(json.dumps(record, separators=(",", ":")) + "\n")


# ── Public API ────────────────────────────────────────────────────────────────
def record(symbol: str, rule: str, price: float, extras: dict) -> None:
    """
    Write one outcome record immediately after an alert fires.
    price_5m / price_15m / outcome_pct_* are null — filled later by the filler thread.
    """
    rec = {
        "ts":              datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbol":          symbol,
        "rule":            rule,
        "price_alert":     round(price, 8),
        "delta":           extras.get("delta"),
        "buy_ratio":       extras.get("buy_ratio"),
        "n_trades":        extras.get("n_trades"),
        "price_5m":        None,
        "price_15m":       None,
        "outcome_pct_5m":  None,
        "outcome_pct_15m": None,
    }
    with _lock:
        _append(rec)


# ── Filler thread ─────────────────────────────────────────────────────────────
def _ts_to_epoch_ms(ts_str: str) -> int:
    dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _fill_loop() -> None:
    """
    Every FILL_INTERVAL seconds: scan outcomes.jsonl for records where
    price_5m or price_15m is null and enough time has passed.
    Fills using the close price of the 1m kline at alert_ts + N minutes.
    """
    while True:
        try:
            time.sleep(FILL_INTERVAL)
            now_ms = int(time.time() * 1000)

            with _lock:
                records = _read_all()

            changed = False
            for rec in records:
                alert_ms = _ts_to_epoch_ms(rec["ts"])

                for window_m in FILL_WINDOWS:
                    field_price   = f"price_{window_m}m"
                    field_outcome = f"outcome_pct_{window_m}m"

                    if rec.get(field_price) is not None:
                        continue  # already filled

                    target_ms = alert_ms + window_m * 60 * 1000

                    if now_ms < target_ms + 60_000:
                        # kline hasn't closed yet — wait one more full minute
                        continue

                    close = _get_kline_close(rec["symbol"], target_ms)
                    if close is None:
                        continue

                    rec[field_price] = round(close, 8)
                    if rec["price_alert"] and rec["price_alert"] > 0:
                        pct = (close - rec["price_alert"]) / rec["price_alert"] * 100
                        rec[field_outcome] = round(pct, 4)
                    changed = True

            if changed:
                with _lock:
                    _write_all(records)

        except Exception:
            pass  # never crash the thread — silently retry next cycle


def start_filler() -> None:
    """Start the background filler thread. Call once at bot startup."""
    t = threading.Thread(target=_fill_loop, daemon=True, name="outcome-filler")
    t.start()
