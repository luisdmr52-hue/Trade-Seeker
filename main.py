#!/usr/bin/env python3
"""
Trade Seeker — Multi-Rule Spot Scanner (Binance USDT)

Detectors (total = 5):
  1) Pump spike
  2) Dump spike
  3) Breakout Up
  4) Breakdown Down
  5) EMA Momentum Cross (UP/DOWN)

Design:
- Simple & robust: REST polling of klines (no websockets yet).
- Hot-reloads config.yaml on change.
- Optional Tier Advisor file for symbols (or static list).
- Telegram alerts (no trading; read-only public data).
"""

import os, sys, time, json
from datetime import datetime
from typing import Dict, List, Any
import requests
import statistics as stats
import yaml

import os, requests

def tg_ping(msg: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat  = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": msg},
            timeout=8
        )
    except Exception:
        pass

# ----------------------------- Telegram -------------------------------------

def tg_send(msg: str):
    tg = CONFIG.get("telegram", {})
    if not tg.get("enable", True):
        return
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat  = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": msg, "disable_web_page_preview": True, "parse_mode": "Markdown"},
            timeout=6
        )
    except Exception:
        pass

# --------------------------- Config / Hot reload ----------------------------

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
CONFIG_MTIME = 0
CONFIG: Dict[str, Any] = {}

def load_config(force=False):
    """Reload config.yaml when file mtime changes."""
    global CONFIG, CONFIG_MTIME
    try:
        mtime = os.path.getmtime(CONFIG_PATH)
        if force or mtime != CONFIG_MTIME:
            with open(CONFIG_PATH, "r") as f:
                CONFIG = yaml.safe_load(f) or {}
            CONFIG_MTIME = mtime
            print(f"[CFG] Reloaded at {datetime.now()}")
    except Exception as e:
        print(f"[CFG] load error: {e}")

def cfg(path, default=None):
    cur = CONFIG
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

# ------------------------------ Binance REST --------------------------------

BINANCE_REST = "https://api.binance.com"

def fetch_usdt_symbols() -> List[str]:
    """Get symbols from Tier Advisor file, or static list, or exchangeInfo fallback."""
    mode = cfg("symbols.mode", "tier_advisor")
    if mode == "static":
        return cfg("symbols.static_list", [])

    # Tier Advisor file
    path = cfg("symbols.tier_file")
    if path and os.path.exists(path):
        with open(path) as f:
            syms = [line.strip() for line in f if line.strip() and line.strip().endswith("USDT")]
        if syms:
            return syms

    # Fallback: public exchangeInfo
    try:
        r = requests.get(BINANCE_REST + "/api/v3/exchangeInfo", timeout=8)
        data = r.json()
        return [s["symbol"] for s in data["symbols"] if s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING"]
    except Exception as e:
        print(f"[AUTO] fetch_usdt_symbols error: {e}")
        return ["BTCUSDT","ETHUSDT","SOLUSDT"]

def get_klines(symbol: str, interval: str, limit: int = 200):
    url = BINANCE_REST + "/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

# ------------------------------ Indicators ----------------------------------

def ema(values: List[float], length: int) -> List[float]:
    if not values or length <= 1:
        return values or []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out

def median(x: List[float], default=0.0) -> float:
    try:
        return stats.median(x)
    except Exception:
        return default

def notional(o, h, l, c, v):
    # Approximate quote notional using average price * volume
    return v * ((h + l + c) / 3.0)

# ----------------------------- Rules (5) ------------------------------------

def rule_pump(bar, tf_adj, v_med):
    o,h,l,c,v = bar
    pct = (c - o) / o * 100.0 if o else 0.0
    vol_ok = v >= (tf_adj["vol_mult"] * (v_med or 1))
    return (pct >= tf_adj["delta_pct"] and vol_ok), {"delta_pct": round(pct,3)}

def rule_dump(bar, tf_adj, v_med):
    o,h,l,c,v = bar
    pct = (c - o) / o * 100.0 if o else 0.0
    vol_ok = v >= (tf_adj["vol_mult"] * (v_med or 1))
    return (pct <= tf_adj["delta_pct"] and vol_ok), {"delta_pct": round(pct,3)}

def rule_breakout_up(ohlcv, tf_adj, ema_len):
    closes = [b[3] for b in ohlcv]
    highs  = [b[1] for b in ohlcv]
    vols   = [b[4] for b in ohlcv]

    rng_hi = max(highs[-tf_adj["range_lookback"]-1:-1])
    buf    = rng_hi * (1 + tf_adj["buffer_pct"]/100.0)

    e = ema(closes, ema_len)
    cond = closes[-1] > buf
    if tf_adj.get("ema_confirm", True):
        cond = cond and closes[-1] > e[-1]

    vol_med = median(vols[-20:], 0.0)
    vol_ok  = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))

    return (cond and vol_ok), {"range_hi": round(rng_hi,4), "ema": round(e[-1],4)}

def rule_breakdown_dn(ohlcv, tf_adj, ema_len):
    closes = [b[3] for b in ohlcv]
    lows   = [b[2] for b in ohlcv]
    vols   = [b[4] for b in ohlcv]

    rng_lo = min(lows[-tf_adj["range_lookback"]-1:-1])
    buf    = rng_lo * (1 - tf_adj["buffer_pct"]/100.0)

    e = ema(closes, ema_len)
    cond = closes[-1] < buf
    if tf_adj.get("ema_confirm", True):
        cond = cond and closes[-1] < e[-1]

    vol_med = median(vols[-20:], 0.0)
    vol_ok  = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))

    return (cond and vol_ok), {"range_lo": round(rng_lo,4), "ema": round(e[-1],4)}

def rule_ema_cross(ohlcv, fast_len, slow_len, direction, tf_adj):
    closes = [b[3] for b in ohlcv]
    vols   = [b[4] for b in ohlcv]

    ef = ema(closes, fast_len)
    es = ema(closes, slow_len)
    if len(ef) < 2 or len(es) < 2:
        return (False, {})

    if direction == "up":
        crossed = ef[-2] <= es[-2] and ef[-1] > es[-1]
    else:
        crossed = ef[-2] >= es[-2] and ef[-1] < es[-1]

    vol_med = median(vols[-20:], 0.0)
    vol_ok  = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))

    return (crossed and vol_ok), {"ema_fast": round(ef[-1],4), "ema_slow": round(es[-1],4)}

# ------------------------------- Cooldowns ----------------------------------

COOLDOWNS: Dict[str, float] = {}

def on_cooldown(symbol: str, rule: str, cooldown_min: int) -> bool:
    now = time.time()
    key = f"{symbol}:{rule}"
    last = COOLDOWNS.get(key, 0)
    return (now - last) < (cooldown_min * 60)

def mark_cooldown(symbol: str, rule: str):
    COOLDOWNS[f"{symbol}:{rule}"] = time.time()

# ------------------------------ Main polling --------------------------------

def format_alert(sym, tf, rule, extras, price):
    pre = cfg("telegram.prefix","TS")
    return f"[{pre}] {rule} | {sym} {tf} @ {price:.6g} | {json.dumps(extras, separators=(',',':'))}"

def poll_once(symbols: List[str]):
    load_config()  # hot-reload if file changed

    rules_on = cfg("rules.enable", {})
    ema_len  = cfg("rules.ema_len", 20)
    ema_fast = cfg("rules.ema_fast_len", 9)
    ema_slow = cfg("rules.ema_slow_len", 20)
    lookback = cfg("rules.lookback_bars", 32)
    cooldown = cfg("rules.cooldown_min", 20)

    for tf, adj in (cfg("timeframes", {}) or {}).items():
        for sym in symbols:
            try:
                k = get_klines(sym, tf, limit=max(lookback+50, 120))
                # kline: [openTime, open, high, low, close, volume, closeTime, qVol, trades, ...]
                ohlcv = [
                    (float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]))
                    for x in k
                ]
                if len(ohlcv) < lookback + 5:
                    continue

                last = ohlcv[-1]
                o,h,l,c,v = last
                v_med = median([b[4] for b in ohlcv[-20:]], 0.0)
                bar_not = notional(o,h,l,c,v)

                # Per-TF merged settings
                pump_adj = {**(cfg("rules.pump_spike",{}) or {}), **(adj.get("pump_adjust",{}) or {})}
                dump_adj = {**(cfg("rules.dump_spike",{}) or {}), **(adj.get("dump_adjust",{}) or {})}
                bo_up_adj= {**(cfg("rules.breakout_up",{}) or {}), **(adj.get("bo_up_adjust",{}) or {})}
                bo_dn_adj= {**(cfg("rules.breakdown_down",{}) or {}), **(adj.get("bo_dn_adjust",{}) or {})}
                cross_adj= {**(cfg("rules.ema_cross",{}) or {}), **(adj.get("cross_adjust",{}) or {})}

                # 1) Pump
                if rules_on.get("pump_spike", True) and not on_cooldown(sym,"pump",cooldown):
                    ok, extras = rule_pump(last, pump_adj, v_med)
                    if ok and bar_not >= pump_adj["min_notional"]:
                        tg_send(format_alert(sym, tf, "PUMP", extras, c))
                        mark_cooldown(sym,"pump")

                # 2) Dump
                if rules_on.get("dump_spike", True) and not on_cooldown(sym,"dump",cooldown):
                    ok, extras = rule_dump(last, dump_adj, v_med)
                    if ok and bar_not >= dump_adj["min_notional"]:
                        tg_send(format_alert(sym, tf, "DUMP", extras, c))
                        mark_cooldown(sym,"dump")

                # 3) Breakout Up
                if rules_on.get("breakout_up", True) and not on_cooldown(sym,"bo_up",cooldown):
                    ok, extras = rule_breakout_up(ohlcv, bo_up_adj, ema_len)
                    if ok and bar_not >= bo_up_adj["min_notional"]:
                        tg_send(format_alert(sym, tf, "BREAKOUT_UP", extras, c))
                        mark_cooldown(sym,"bo_up")

                # 4) Breakdown Down
                if rules_on.get("breakdown_down", True) and not on_cooldown(sym,"bo_dn",cooldown):
                    ok, extras = rule_breakdown_dn(ohlcv, bo_dn_adj, ema_len)
                    if ok and bar_not >= bo_dn_adj["min_notional"]:
                        tg_send(format_alert(sym, tf, "BREAKDOWN_DN", extras, c))
                        mark_cooldown(sym,"bo_dn")

                # 5) EMA Cross UP
                if rules_on.get("ema_cross_up", True) and not on_cooldown(sym,"cross_up",cooldown):
                    ok, extras = rule_ema_cross(ohlcv, ema_fast, ema_slow, "up", cross_adj)
                    if ok and bar_not >= cross_adj.get("min_notional", 0):
                        tg_send(format_alert(sym, tf, "EMA_CROSS_UP", extras, c))
                        mark_cooldown(sym,"cross_up")

                # 5b) EMA Cross DOWN
                if rules_on.get("ema_cross_down", True) and not on_cooldown(sym,"cross_dn",cooldown):
                    ok, extras = rule_ema_cross(ohlcv, ema_fast, ema_slow, "down", cross_adj)
                    if ok and bar_not >= cross_adj.get("min_notional", 0):
                        tg_send(format_alert(sym, tf, "EMA_CROSS_DN", extras, c))
                        mark_cooldown(sym,"cross_dn")

                # small spacing between alerts
                time.sleep(max(0.0, float(cfg("telegram.throttle_sec", 2))) / 1000.0)

            except Exception as e:
                print(f"[{sym} {tf}] error: {e}")

# --------------------------------- Runner -----------------------------------

def run():
    print("[BOOT] loading config…")
    load_config(force=True)
    syms = fetch_usdt_symbols()
    tfs = list((cfg("timeframes", {}) or {}).keys())
    print(f"[BOOT] Symbols: {len(syms)} | TFs: {tfs}")

    while True:
        try:
            poll_once(syms)
        except KeyboardInterrupt:
            print("bye")
            break
        except Exception as e:
            print(f"[LOOP] {e}")
        time.sleep(5)  # idle; hot-reload handled by mtime check

if __name__ == "__main__":
    run()
