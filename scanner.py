#!/usr/bin/env python3
"""
scanner.py — KTS TradeSeeker legacy REST scanner
v1.0 — 2026-03-28

Contiene la lógica de reglas REST extraída de main.py (v4.2).
Todas las reglas están APAGADAS en producción — el bot corre solo con
PUMP_FAST via fast_loop.py.

Este módulo existe para:
- Preservar el código sin tenerlo en main.py (que ahora es solo arranque)
- Permitir reactivar reglas individuales en el futuro sin rescatar backups
- Mantener la estructura modular completa del proyecto

Para activar una regla: habilitarla en config.yaml bajo rules.enable.*
poll_once() es llamado desde main.py solo si se descomenta en run().

Reglas disponibles (todas apagadas por defecto):
- PUMP / DUMP         → rule_pump / rule_dump
- BREAKOUT_UP/DN      → rule_breakout_up / rule_breakdown_dn
- EMA_CROSS_UP/DN     → rule_ema_cross
- PUMP_EARLY          → rule_pump_early
- BREAKOUT_UP_EARLY   → rule_breakout_up_early
"""

import time
import json
import statistics as stats
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils import log, load_config, cfg, tg_send, on_cooldown, mark_cooldown

# ---------------------------------------------------------------------------
# Binance REST
# ---------------------------------------------------------------------------

BINANCE_REST = "https://api.binance.com"


def get_klines(symbol: str, interval: str, limit: int = 200) -> List:
    """
    Fetch klines from Binance REST.
    Returns list of OHLCV arrays. Raises on HTTP error.
    """
    url = BINANCE_REST + "/api/v3/klines"
    r = requests.get(
        url,
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def ema(values: List[float], length: int) -> List[float]:
    """Exponential moving average. Returns same-length list."""
    if not values or length <= 1:
        return values or []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def median(x: List[float], default: float = 0.0) -> float:
    try:
        return stats.median(x)
    except Exception:
        return default


def notional(o: float, h: float, l: float, c: float, v: float) -> float:
    """Approximate notional value using typical price × volume."""
    return v * ((h + l + c) / 3.0)


# ---------------------------------------------------------------------------
# Rule functions
# ---------------------------------------------------------------------------

def rule_pump(
    bar: Tuple,
    tf_adj: Dict,
    v_med: float,
) -> Tuple[bool, Dict]:
    o, h, l, c, v = bar
    pct    = (c - o) / o * 100.0 if o else 0.0
    vol_ok = v >= (tf_adj["vol_mult"] * (v_med or 1))
    return (pct >= tf_adj["delta_pct"] and vol_ok), {"delta_pct": round(pct, 3)}


def rule_dump(
    bar: Tuple,
    tf_adj: Dict,
    v_med: float,
) -> Tuple[bool, Dict]:
    o, h, l, c, v = bar
    pct    = (c - o) / o * 100.0 if o else 0.0
    vol_ok = v >= (tf_adj["vol_mult"] * (v_med or 1))
    return (pct <= tf_adj["delta_pct"] and vol_ok), {"delta_pct": round(pct, 3)}


def rule_breakout_up(
    ohlcv: List[Tuple],
    tf_adj: Dict,
    ema_len: int,
) -> Tuple[bool, Dict]:
    closes = [b[3] for b in ohlcv]
    highs  = [b[1] for b in ohlcv]
    vols   = [b[4] for b in ohlcv]
    rng_hi = max(highs[-tf_adj["range_lookback"] - 1:-1])
    buf    = rng_hi * (1 + tf_adj["buffer_pct"] / 100.0)
    e      = ema(closes, ema_len)
    cond   = closes[-1] > buf
    if tf_adj.get("ema_confirm", True):
        cond = cond and closes[-1] > e[-1]
    vol_med = median(vols[-20:], 0.0)
    vol_ok  = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))
    return (cond and vol_ok), {"range_hi": round(rng_hi, 4), "ema": round(e[-1], 4)}


def rule_breakdown_dn(
    ohlcv: List[Tuple],
    tf_adj: Dict,
    ema_len: int,
) -> Tuple[bool, Dict]:
    closes = [b[3] for b in ohlcv]
    lows   = [b[2] for b in ohlcv]
    vols   = [b[4] for b in ohlcv]
    rng_lo = min(lows[-tf_adj["range_lookback"] - 1:-1])
    buf    = rng_lo * (1 - tf_adj["buffer_pct"] / 100.0)
    e      = ema(closes, ema_len)
    cond   = closes[-1] < buf
    if tf_adj.get("ema_confirm", True):
        cond = cond and closes[-1] < e[-1]
    vol_med = median(vols[-20:], 0.0)
    vol_ok  = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))
    return (cond and vol_ok), {"range_lo": round(rng_lo, 4), "ema": round(e[-1], 4)}


def rule_ema_cross(
    ohlcv: List[Tuple],
    fast_len: int,
    slow_len: int,
    direction: str,
    tf_adj: Dict,
) -> Tuple[bool, Dict]:
    closes = [b[3] for b in ohlcv]
    vols   = [b[4] for b in ohlcv]
    ef     = ema(closes, fast_len)
    es     = ema(closes, slow_len)
    if len(ef) < 2 or len(es) < 2:
        return False, {}
    if direction == "up":
        crossed = ef[-2] <= es[-2] and ef[-1] > es[-1]
    else:
        crossed = ef[-2] >= es[-2] and ef[-1] < es[-1]
    vol_med = median(vols[-20:], 0.0)
    vol_ok  = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))
    return (crossed and vol_ok), {
        "ema_fast": round(ef[-1], 4),
        "ema_slow": round(es[-1], 4),
    }


def rule_pump_early(
    bar: Tuple,
    early_cfg: Dict,
    v_med: float,
) -> Tuple[bool, Dict]:
    o, h, l, c, v = bar
    pct      = (c - o) / o * 100.0 if o else 0.0
    vol_ok   = v >= (early_cfg["vol_mult"] * (v_med or 1))
    triggered = pct >= early_cfg["delta_pct"] and vol_ok
    return triggered, {"delta_pct": round(pct, 3), "early": True}


def rule_breakout_up_early(
    ohlcv: List[Tuple],
    early_cfg: Dict,
    ema_len: int,
) -> Tuple[bool, Dict]:
    closes   = [b[3] for b in ohlcv]
    highs    = [b[1] for b in ohlcv]
    vols     = [b[4] for b in ohlcv]
    lookback = early_cfg.get("range_lookback", 32)
    rng_hi   = max(highs[-lookback - 1:-1])
    buf      = rng_hi * (1 + early_cfg.get("buffer_pct", 0.20) / 100.0)
    e        = ema(closes, ema_len)
    cond     = closes[-1] > buf
    if early_cfg.get("ema_confirm", True):
        cond = cond and closes[-1] > e[-1]
    vol_med = median(vols[-20:], 0.0)
    vol_ok  = vols[-1] >= (early_cfg["vol_mult"] * (vol_med or 1))
    return (cond and vol_ok), {
        "range_hi": round(rng_hi, 4),
        "ema":      round(e[-1], 4),
        "early":    True,
    }


# ---------------------------------------------------------------------------
# Alert formatter
# ---------------------------------------------------------------------------

def format_alert(
    sym: str,
    tf: str,
    rule_name: str,
    extras: Dict,
    price: float,
) -> str:
    pre = cfg("telegram.prefix", "TS")
    return (
        f"[{pre}] {rule_name} | {sym} {tf} @ {price:.6g} | "
        f"{json.dumps(extras, separators=(',', ':'))}"
    )


# ---------------------------------------------------------------------------
# Per-symbol scan
# ---------------------------------------------------------------------------

def scan_symbol(
    sym: str,
    tf: str,
    adj: Dict,
    rules_on: Dict,
    ema_len: int,
    ema_fast: int,
    ema_slow: int,
    lookback: int,
    cooldown: int,
    early_cfg: Dict,
) -> List[Tuple[str, str, str]]:
    """
    Scan a single symbol on a single timeframe.
    Returns list of (alert_str, cooldown_key, symbol) tuples.
    Isolated in its own try/except — one bad symbol never stops the scan.
    """
    alerts: List[Tuple[str, str, str]] = []
    try:
        k     = get_klines(sym, tf, limit=max(lookback + 50, 120))
        ohlcv = [
            (float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]))
            for x in k
        ]
        if len(ohlcv) < lookback + 5:
            return []

        last  = ohlcv[-1]
        o, h, l, c, v = last
        v_med    = median([b[4] for b in ohlcv[-20:]], 0.0)
        bar_not  = notional(o, h, l, c, v)

        # Late-entry filter
        late_pct = adj.get("late_filter_pct")
        if late_pct is not None:
            bar_move = abs((c - o) / o * 100.0) if o else 0.0
            if bar_move >= late_pct:
                return []

        # Build per-rule adj dicts
        pump_adj  = {**(cfg("rules.pump_spike",     {}) or {}), **(adj.get("pump_adjust",    {}) or {})}
        dump_adj  = {**(cfg("rules.dump_spike",     {}) or {}), **(adj.get("dump_adjust",    {}) or {})}
        bo_up_adj = {**(cfg("rules.breakout_up",    {}) or {}), **(adj.get("bo_up_adjust",   {}) or {})}
        bo_dn_adj = {**(cfg("rules.breakdown_down", {}) or {}), **(adj.get("bo_dn_adjust",   {}) or {})}
        cross_adj = {**(cfg("rules.ema_cross",      {}) or {}), **(adj.get("cross_adjust",   {}) or {})}

        # PUMP
        if rules_on.get("pump_spike", True) and not on_cooldown(sym, "pump", cooldown):
            ok, extras = rule_pump(last, pump_adj, v_med)
            if ok and bar_not >= pump_adj.get("min_notional", 0):
                alerts.append((format_alert(sym, tf, "PUMP", extras, c), "pump", sym))

        # DUMP
        if rules_on.get("dump_spike", True) and not on_cooldown(sym, "dump", cooldown):
            ok, extras = rule_dump(last, dump_adj, v_med)
            if ok and bar_not >= dump_adj.get("min_notional", 0):
                alerts.append((format_alert(sym, tf, "DUMP", extras, c), "dump", sym))

        # BREAKOUT_UP
        if rules_on.get("breakout_up", True) and not on_cooldown(sym, "bo_up", cooldown):
            ok, extras = rule_breakout_up(ohlcv, bo_up_adj, ema_len)
            if ok and bar_not >= bo_up_adj.get("min_notional", 0):
                alerts.append((format_alert(sym, tf, "BREAKOUT_UP", extras, c), "bo_up", sym))

        # BREAKDOWN_DN
        if rules_on.get("breakdown_down", True) and not on_cooldown(sym, "bo_dn", cooldown):
            ok, extras = rule_breakdown_dn(ohlcv, bo_dn_adj, ema_len)
            if ok and bar_not >= bo_dn_adj.get("min_notional", 0):
                alerts.append((format_alert(sym, tf, "BREAKDOWN_DN", extras, c), "bo_dn", sym))

        # EMA_CROSS_UP
        if rules_on.get("ema_cross_up", True) and not on_cooldown(sym, "cross_up", cooldown):
            ok, extras = rule_ema_cross(ohlcv, ema_fast, ema_slow, "up", cross_adj)
            if ok and bar_not >= cross_adj.get("min_notional", 0):
                alerts.append((format_alert(sym, tf, "EMA_CROSS_UP", extras, c), "cross_up", sym))

        # EMA_CROSS_DN
        if rules_on.get("ema_cross_down", True) and not on_cooldown(sym, "cross_dn", cooldown):
            ok, extras = rule_ema_cross(ohlcv, ema_fast, ema_slow, "down", cross_adj)
            if ok and bar_not >= cross_adj.get("min_notional", 0):
                alerts.append((format_alert(sym, tf, "EMA_CROSS_DN", extras, c), "cross_dn", sym))

        # PUMP_EARLY / BREAKOUT_UP_EARLY
        early_enabled = adj.get("early_enabled", True)
        early_cd      = early_cfg.get("cooldown_min", 10)

        if early_enabled and rules_on.get("pump_early", True) and not on_cooldown(sym, "pump_early", early_cd):
            ok, extras = rule_pump_early(last, early_cfg, v_med)
            if ok and bar_not >= early_cfg.get("min_notional", 50_000):
                alerts.append((format_alert(sym, tf, "PUMP_EARLY", extras, c), "pump_early", sym))

        if early_enabled and rules_on.get("breakout_up_early", True) and not on_cooldown(sym, "bo_up_early", early_cd):
            ok, extras = rule_breakout_up_early(ohlcv, early_cfg, ema_len)
            if ok and bar_not >= early_cfg.get("min_notional", 50_000):
                alerts.append((format_alert(sym, tf, "BREAKOUT_UP_EARLY", extras, c), "bo_up_early", sym))

    except Exception as e:
        log("ERR", f"scan_symbol [{sym} {tf}]: {type(e).__name__}: {e}")

    return alerts


# ---------------------------------------------------------------------------
# Poll cycle — scans all symbols × timeframes in parallel
# ---------------------------------------------------------------------------

def poll_once(symbols: List[str]) -> None:
    """
    One full scan cycle over all symbols × timeframes.
    Disabled in production — called only if explicitly enabled in main.py.

    Uses ThreadPoolExecutor for parallel REST calls.
    Alerts are sent after all futures complete to avoid partial-cycle Telegram spam.
    """
    load_config()

    rules_on  = cfg("rules.enable", {}) or {}
    ema_len   = int(cfg("rules.ema_len",       20))
    ema_fast  = int(cfg("rules.ema_fast_len",   9))
    ema_slow  = int(cfg("rules.ema_slow_len",  20))
    lookback  = int(cfg("rules.lookback_bars", 32))
    cooldown  = int(cfg("rules.cooldown_min",  20))
    throttle  = max(0.0, float(cfg("telegram.throttle_sec", 2.0)))
    workers   = int(cfg("scanner.parallel_workers", 25))

    early_defaults = {
        "delta_pct":      2.5,
        "vol_mult":       1.8,
        "min_notional":   50_000,
        "cooldown_min":   10,
        "range_lookback": 32,
        "buffer_pct":     0.20,
        "ema_confirm":    True,
    }
    early_cfg  = {**early_defaults, **(cfg("rules.early", {}) or {})}
    timeframes = cfg("timeframes", {}) or {}

    tasks = [
        (sym, tf, adj)
        for tf, adj in timeframes.items()
        for sym in symbols
    ]

    pending_alerts: List[Tuple[str, str, str]] = []
    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                scan_symbol,
                sym, tf, adj,
                rules_on, ema_len, ema_fast, ema_slow,
                lookback, cooldown, early_cfg,
            ): (sym, tf)
            for sym, tf, adj in tasks
        }
        for future in as_completed(futures):
            sym, tf = futures[future]
            try:
                result = future.result()
                if result:
                    pending_alerts.extend(result)
            except Exception as e:
                log("ERR", f"scanner future [{sym} {tf}]: {type(e).__name__}: {e}")

    scan_time = time.monotonic() - t0
    log("SCAN",
        f"completed {len(tasks)} tasks in {scan_time:.1f}s | "
        f"alerts={len(pending_alerts)}")

    for alert_str, cd_key, sym in pending_alerts:
        tg_send(alert_str)
        mark_cooldown(sym, cd_key)
        if throttle > 0:
            time.sleep(throttle)
