#!/usr/bin/env python3
"""
Trade Seeker — Multi-Rule Spot Scanner (Binance USDT)
v4.2 — P3-B intrabar volume confirmation via AggTradeConfirmer

Changes from v4.1:
- Fast loop uses AggTradeConfirmer for volume confirmation
- Spike detected → aggTrade WS opens on-demand for that symbol
- Alert fires only if buy_vol_ratio >= threshold in 30s window
- bv_ratio filter removed (buy_vol/vol_24h units incompatible)
- poll_once (CONFIRMED/EMA rules) unchanged
"""

import os, sys, time, json
import uuid, random
import threading
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import statistics as stats
import yaml
from datetime import datetime, timezone
from logx import boot, cfg, rule, guarded

from ts_feed import TSFeed, PriceTracker
from aggtrade_confirmer import AggTradeConfirmer
from universe_filter import UniverseFilter
import outcome_tracker

BOT_VER: str = "v4.2"
symbols: List[str] = []

BOOT_SENT = "/run/tradeseeker.booted"
BOOT_PING_DONE = False

def boot_ping_once(syms, tfs):
    try:
        global BOOT_PING_DONE
        if BOOT_PING_DONE:
            log("BOOT", "ping skipped (already sent: flag)")
            return
        try:
            with open(BOOT_SENT, "x") as f:
                f.write(str(time.time()))
                f.flush(); os.fsync(f.fileno())
        except FileExistsError:
            log("BOOT", "ping skipped (already sent: sentinel)")
            BOOT_PING_DONE = True
            return
        log("BOOT", "sending startup ping…")
        tg_ping(f"Trade Seeker {BOT_VER} started | {len(syms)} syms | TFs: {tfs}")
        BOOT_PING_DONE = True
    except Exception as e:
        log("ERR", f"ping error: {e}")

def log(tag: str, msg: str):
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} [{tag}] {msg}", flush=True)

class Circuit:
    def __init__(self, fail_threshold: int = 5, cooldown_s: int = 30):
        self.fail_th = fail_threshold
        self.cool = cooldown_s
        self.open_until = 0.0
        self.fails = 0

    def allow(self) -> bool:
        return time.time() >= self.open_until

    def report(self, ok: bool):
        if ok:
            self.fails = 0
        else:
            self.fails += 1
            if self.fails >= self.fail_th:
                self.open_until = time.time() + self.cool
                self.fails = 0

_http_circuit = Circuit()

def _sleep_backoff(i: int, base: float = 0.35):
    time.sleep(base * (2 ** i) * (0.85 + random.random() * 0.30))

def http_call(method: str, url: str, *, json_body=None, timeout: int = 6, retries: int = 3):
    if not _http_circuit.allow():
        log("HTTP", f"SKIP circuit-open {url}")
        return None
    rid = uuid.uuid4().hex[:8]
    for i in range(retries + 1):
        t0 = time.time()
        try:
            r = requests.get(url, timeout=timeout) if method == "GET" else requests.post(url, json=json_body, timeout=timeout)
            dt = time.time() - t0
            if r.status_code < 400:
                log("HTTP", f"req={rid} {method} {r.status_code} {dt:.3f}s {url}")
                _http_circuit.report(True)
                return r.text
            else:
                log("HTTP", f"req={rid} {method} {r.status_code} {dt:.3f}s retry={i} {url}")
        except Exception as e:
            log("HTTP", f"req={rid} {method} EXC {type(e).__name__}: {e} retry={i} {url}")
        _http_circuit.report(False)
        if i < retries:
            _sleep_backoff(i)
    log("ERR", f"http_call GIVEUP {method} {url}")
    return None

def http_get(url: str, **kw): return http_call("GET", url, **kw)
def http_post(url: str, json=None, **kw): return http_call("POST", url, json_body=json, **kw)

def tg_ping(msg: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        log("ALERT", msg)
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = {"chat_id": chat, "text": msg}
    log("RULE", f'alert | {{"text":"{msg[:60]}..."}}')
    return http_post(url, json=body) is not None

def tg_send(msg: str):
    return tg_ping(msg)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
CONFIG_MTIME = 0
CONFIG: Dict[str, Any] = {}

def load_config(force=False):
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

BINANCE_REST = "https://api.binance.com"

def fetch_usdt_symbols() -> List[str]:
    mode = cfg("symbols.mode", "tier_advisor")
    if mode == "static":
        return cfg("symbols.static_list", [])
    path = cfg("symbols.tier_file")
    if path and os.path.exists(path):
        with open(path) as f:
            syms = [line.strip() for line in f if line.strip() and line.strip().endswith("USDT")]
        if syms:
            return syms
    try:
        r = requests.get(BINANCE_REST + "/api/v3/exchangeInfo", timeout=8)
        data = r.json()
        return [s["symbol"] for s in data["symbols"]
                if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"]
    except Exception as e:
        print(f"[AUTO] fetch_usdt_symbols error: {e}")
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

def get_klines(symbol: str, interval: str, limit: int = 200):
    url = BINANCE_REST + "/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

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
    return v * ((h + l + c) / 3.0)

def rule_pump(bar, tf_adj, v_med):
    o, h, l, c, v = bar
    pct = (c - o) / o * 100.0 if o else 0.0
    vol_ok = v >= (tf_adj["vol_mult"] * (v_med or 1))
    return (pct >= tf_adj["delta_pct"] and vol_ok), {"delta_pct": round(pct, 3)}

def rule_dump(bar, tf_adj, v_med):
    o, h, l, c, v = bar
    pct = (c - o) / o * 100.0 if o else 0.0
    vol_ok = v >= (tf_adj["vol_mult"] * (v_med or 1))
    return (pct <= tf_adj["delta_pct"] and vol_ok), {"delta_pct": round(pct, 3)}

def rule_breakout_up(ohlcv, tf_adj, ema_len):
    closes = [b[3] for b in ohlcv]
    highs = [b[1] for b in ohlcv]
    vols = [b[4] for b in ohlcv]
    rng_hi = max(highs[-tf_adj["range_lookback"]-1:-1])
    buf = rng_hi * (1 + tf_adj["buffer_pct"] / 100.0)
    e = ema(closes, ema_len)
    cond = closes[-1] > buf
    if tf_adj.get("ema_confirm", True):
        cond = cond and closes[-1] > e[-1]
    vol_med = median(vols[-20:], 0.0)
    vol_ok = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))
    return (cond and vol_ok), {"range_hi": round(rng_hi, 4), "ema": round(e[-1], 4)}

def rule_breakdown_dn(ohlcv, tf_adj, ema_len):
    closes = [b[3] for b in ohlcv]
    lows = [b[2] for b in ohlcv]
    vols = [b[4] for b in ohlcv]
    rng_lo = min(lows[-tf_adj["range_lookback"]-1:-1])
    buf = rng_lo * (1 - tf_adj["buffer_pct"] / 100.0)
    e = ema(closes, ema_len)
    cond = closes[-1] < buf
    if tf_adj.get("ema_confirm", True):
        cond = cond and closes[-1] < e[-1]
    vol_med = median(vols[-20:], 0.0)
    vol_ok = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))
    return (cond and vol_ok), {"range_lo": round(rng_lo, 4), "ema": round(e[-1], 4)}

def rule_ema_cross(ohlcv, fast_len, slow_len, direction, tf_adj):
    closes = [b[3] for b in ohlcv]
    vols = [b[4] for b in ohlcv]
    ef = ema(closes, fast_len)
    es = ema(closes, slow_len)
    if len(ef) < 2 or len(es) < 2:
        return (False, {})
    if direction == "up":
        crossed = ef[-2] <= es[-2] and ef[-1] > es[-1]
    else:
        crossed = ef[-2] >= es[-2] and ef[-1] < es[-1]
    vol_med = median(vols[-20:], 0.0)
    vol_ok = vols[-1] >= (tf_adj["vol_mult"] * (vol_med or 1))
    return (crossed and vol_ok), {"ema_fast": round(ef[-1], 4), "ema_slow": round(es[-1], 4)}

def rule_pump_early(bar, early_cfg, v_med):
    o, h, l, c, v = bar
    pct = (c - o) / o * 100.0 if o else 0.0
    vol_ok = v >= (early_cfg["vol_mult"] * (v_med or 1))
    triggered = pct >= early_cfg["delta_pct"] and vol_ok
    return triggered, {"delta_pct": round(pct, 3), "early": True}

def rule_breakout_up_early(ohlcv, early_cfg, ema_len):
    closes = [b[3] for b in ohlcv]
    highs = [b[1] for b in ohlcv]
    vols = [b[4] for b in ohlcv]
    lookback = early_cfg.get("range_lookback", 32)
    rng_hi = max(highs[-lookback-1:-1])
    buf = rng_hi * (1 + early_cfg.get("buffer_pct", 0.20) / 100.0)
    e = ema(closes, ema_len)
    cond = closes[-1] > buf
    if early_cfg.get("ema_confirm", True):
        cond = cond and closes[-1] > e[-1]
    vol_med = median(vols[-20:], 0.0)
    vol_ok = vols[-1] >= (early_cfg["vol_mult"] * (vol_med or 1))
    return (cond and vol_ok), {"range_hi": round(rng_hi, 4), "ema": round(e[-1], 4), "early": True}

COOLDOWNS: Dict[str, float] = {}
_cooldown_lock = threading.Lock()

def on_cooldown(symbol: str, rule: str, cooldown_min: int) -> bool:
    now = time.time()
    key = f"{symbol}:{rule}"
    with _cooldown_lock:
        last = COOLDOWNS.get(key, 0)
    return (now - last) < (cooldown_min * 60)

def mark_cooldown(symbol: str, rule: str):
    with _cooldown_lock:
        COOLDOWNS[f"{symbol}:{rule}"] = time.time()

def format_alert(sym, tf, rule_name, extras, price):
    pre = cfg("telegram.prefix", "TS")
    return f"[{pre}] {rule_name} | {sym} {tf} @ {price:.6g} | {json.dumps(extras, separators=(',', ':'))}"

def scan_symbol(sym: str, tf: str, adj: dict, rules_on: dict,
                ema_len: int, ema_fast: int, ema_slow: int,
                lookback: int, cooldown: int, early_cfg: dict) -> List[str]:
    alerts = []
    try:
        k = get_klines(sym, tf, limit=max(lookback + 50, 120))
        ohlcv = [
            (float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]))
            for x in k
        ]
        if len(ohlcv) < lookback + 5:
            return []
        last = ohlcv[-1]
        o, h, l, c, v = last
        v_med = median([b[4] for b in ohlcv[-20:]], 0.0)
        bar_not = notional(o, h, l, c, v)
        late_pct = adj.get("late_filter_pct", None)
        if late_pct is not None:
            bar_move = abs((c - o) / o * 100.0) if o else 0.0
            if bar_move >= late_pct:
                return []
        pump_adj  = {**(cfg("rules.pump_spike", {}) or {}),     **(adj.get("pump_adjust", {}) or {})}
        dump_adj  = {**(cfg("rules.dump_spike", {}) or {}),     **(adj.get("dump_adjust", {}) or {})}
        bo_up_adj = {**(cfg("rules.breakout_up", {}) or {}),    **(adj.get("bo_up_adjust", {}) or {})}
        bo_dn_adj = {**(cfg("rules.breakdown_down", {}) or {}), **(adj.get("bo_dn_adjust", {}) or {})}
        cross_adj = {**(cfg("rules.ema_cross", {}) or {}),      **(adj.get("cross_adjust", {}) or {})}
        if rules_on.get("pump_spike", True) and not on_cooldown(sym, "pump", cooldown):
            ok, extras = rule_pump(last, pump_adj, v_med)
            if ok and bar_not >= pump_adj["min_notional"]:
                alerts.append((format_alert(sym, tf, "PUMP", extras, c), "pump"))
        if rules_on.get("dump_spike", True) and not on_cooldown(sym, "dump", cooldown):
            ok, extras = rule_dump(last, dump_adj, v_med)
            if ok and bar_not >= dump_adj["min_notional"]:
                alerts.append((format_alert(sym, tf, "DUMP", extras, c), "dump"))
        if rules_on.get("breakout_up", True) and not on_cooldown(sym, "bo_up", cooldown):
            ok, extras = rule_breakout_up(ohlcv, bo_up_adj, ema_len)
            if ok and bar_not >= bo_up_adj["min_notional"]:
                alerts.append((format_alert(sym, tf, "BREAKOUT_UP", extras, c), "bo_up"))
        if rules_on.get("breakdown_down", True) and not on_cooldown(sym, "bo_dn", cooldown):
            ok, extras = rule_breakdown_dn(ohlcv, bo_dn_adj, ema_len)
            if ok and bar_not >= bo_dn_adj["min_notional"]:
                alerts.append((format_alert(sym, tf, "BREAKDOWN_DN", extras, c), "bo_dn"))
        if rules_on.get("ema_cross_up", True) and not on_cooldown(sym, "cross_up", cooldown):
            ok, extras = rule_ema_cross(ohlcv, ema_fast, ema_slow, "up", cross_adj)
            if ok and bar_not >= cross_adj.get("min_notional", 0):
                alerts.append((format_alert(sym, tf, "EMA_CROSS_UP", extras, c), "cross_up"))
        if rules_on.get("ema_cross_down", True) and not on_cooldown(sym, "cross_dn", cooldown):
            ok, extras = rule_ema_cross(ohlcv, ema_fast, ema_slow, "down", cross_adj)
            if ok and bar_not >= cross_adj.get("min_notional", 0):
                alerts.append((format_alert(sym, tf, "EMA_CROSS_DN", extras, c), "cross_dn"))
        early_enabled = adj.get("early_enabled", True)
        if early_enabled and rules_on.get("pump_early", True) and not on_cooldown(sym, "pump_early", early_cfg.get("cooldown_min", 10)):
            ok, extras = rule_pump_early(last, early_cfg, v_med)
            if ok and bar_not >= early_cfg.get("min_notional", 50000):
                alerts.append((format_alert(sym, tf, "PUMP_EARLY", extras, c), "pump_early"))
        if early_enabled and rules_on.get("breakout_up_early", True) and not on_cooldown(sym, "bo_up_early", early_cfg.get("cooldown_min", 10)):
            ok, extras = rule_breakout_up_early(ohlcv, early_cfg, ema_len)
            if ok and bar_not >= early_cfg.get("min_notional", 50000):
                alerts.append((format_alert(sym, tf, "BREAKOUT_UP_EARLY", extras, c), "bo_up_early"))
    except Exception as e:
        log("ERR", f"[{sym} {tf}] {type(e).__name__}: {e}")
    return alerts


def poll_once(symbols: List[str]):
    load_config()
    rules_on  = cfg("rules.enable", {})
    ema_len   = cfg("rules.ema_len", 20)
    ema_fast  = cfg("rules.ema_fast_len", 9)
    ema_slow  = cfg("rules.ema_slow_len", 20)
    lookback  = cfg("rules.lookback_bars", 32)
    cooldown  = cfg("rules.cooldown_min", 20)
    throttle  = max(0.0, float(cfg("telegram.throttle_sec", 2)))
    workers   = cfg("scanner.parallel_workers", 25)
    early_defaults = {
        "delta_pct": 2.5, "vol_mult": 1.8, "min_notional": 50000,
        "cooldown_min": 10, "range_lookback": 32, "buffer_pct": 0.20, "ema_confirm": True,
    }
    early_cfg = {**early_defaults, **(cfg("rules.early", {}) or {})}
    timeframes = cfg("timeframes", {}) or {}
    tasks = [(sym, tf, adj) for tf, adj in timeframes.items() for sym in symbols]
    pending_alerts = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(scan_symbol, sym, tf, adj, rules_on, ema_len, ema_fast, ema_slow, lookback, cooldown, early_cfg): (sym, tf)
            for sym, tf, adj in tasks
        }
        for future in as_completed(futures):
            sym, tf = futures[future]
            try:
                result = future.result()
                if result:
                    pending_alerts.extend([(sym, alert_str, cd_key) for alert_str, cd_key in result])
            except Exception as e:
                log("ERR", f"future [{sym} {tf}]: {e}")
    scan_time = time.time() - t0
    log("SCAN", f"completed {len(tasks)} tasks in {scan_time:.1f}s | alerts={len(pending_alerts)}")
    for sym, alert_str, cd_key in pending_alerts:
        tg_send(alert_str)
        mark_cooldown(sym, cd_key)
        time.sleep(throttle)


def run_fast_loop(_symbols_unused: List[str], uf: UniverseFilter):
    """
    Fast loop v3 — Timescale detection + aggTrade confirmation (P3-B).

    Flow:
        1. Snapshot detects delta >= pump_pct → confirmer.add_candidate(sym)
        2. AggTradeConfirmer opens WS stream for that symbol
        3. Next cycle: if is_confirmed(sym) → fire PUMP_FAST alert
        4. Streams expire after TTL (default 10 min)

    Config keys (config.yaml under rules.fast_ts):
        poll_interval_s:  2
        pump_pct:         0.30
        min_vol_24h:      50000
        cooldown_min:     5
        confirm_ratio:    0.60
        confirm_window_s: 30
        confirm_ttl_s:    600
    """
    DEFAULT_POLL   = 2
    DEFAULT_PCT    = 0.30
    DEFAULT_VOL    = 50_000
    DEFAULT_CD     = 5
    DEFAULT_RATIO  = 0.60
    DEFAULT_WINDOW = 30
    DEFAULT_TTL    = 90

    feed      = TSFeed()
    tracker   = PriceTracker()
    confirmer = AggTradeConfirmer(
        window_s=DEFAULT_WINDOW,
        ttl_s=DEFAULT_TTL,
        vol_ratio=DEFAULT_RATIO,
        log_fn=log,
    )

    log("FAST", "Timescale fast loop v3 started (P3-B aggTrade confirmation)")

    consecutive_empty = 0
    cleanup_counter   = 0

    while True:
        try:
            load_config()

            poll_s   = cfg("rules.fast_ts.poll_interval_s", DEFAULT_POLL)
            pump_pct = cfg("rules.fast_ts.pump_pct",         DEFAULT_PCT)
            min_vol  = cfg("rules.fast_ts.min_vol_24h",      DEFAULT_VOL)
            cd_min   = cfg("rules.fast_ts.cooldown_min",     DEFAULT_CD)
            c_ratio  = cfg("rules.fast_ts.confirm_ratio",    DEFAULT_RATIO)
            c_window = cfg("rules.fast_ts.confirm_window_s", DEFAULT_WINDOW)
            c_ttl    = cfg("rules.fast_ts.confirm_ttl_s",    DEFAULT_TTL)
            throttle = max(0.0, float(cfg("telegram.throttle_sec", 2)))
            pre      = cfg("telegram.prefix", "TS")

            confirmer.vol_ratio = c_ratio
            confirmer.window_s  = c_window
            confirmer.ttl_s     = c_ttl

            t0 = time.time()
            snapshot = feed.get_snapshot()

            if not snapshot:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    log("FAST", "WARNING: 3 consecutive empty snapshots — check Timescale/Benthos")
                    consecutive_empty = 0
                time.sleep(poll_s)
                continue

            consecutive_empty = 0
            universe = set(uf.get_universe())
            confirmed_alerts  = []

            for sym, data in snapshot.items():
                if universe and sym not in universe:
                    continue
                price = data["price"]
                vol   = data["vol_24h"] or 0

                if vol < min_vol:
                    tracker.update(sym, price)
                    continue

                delta = tracker.update(sym, price)
                if delta is None:
                    continue

                if delta >= pump_pct:
                    confirmer.add_candidate(sym)

                if confirmer.is_confirmed(sym) and delta >= pump_pct:
                    if not on_cooldown(sym, "pump_fast", cd_min):
                        state = confirmer.debug_state(sym)
                        extras = {
                            "delta":     round(delta, 3),
                            "buy_ratio": round(state.get("buy_ratio") or 0, 2),
                            "n_trades":  state.get("n_events", 0),
                            "confirmed": True,
                        }
                        alert_str = f"[{pre}] PUMP_FAST | {sym} WS @ {price:.6g} | {json.dumps(extras, separators=(',', ':'))}"
                        confirmed_alerts.append((sym, alert_str, price, extras))
                    confirmer.remove(sym)

            query_ms = (time.time() - t0) * 1000

            if confirmed_alerts:
                log("FAST", f"query={query_ms:.1f}ms | symbols={len(snapshot)} | confirmed={len(confirmed_alerts)} | candidates={confirmer.active_count()}")
                for sym, alert_str, alert_price, alert_extras in confirmed_alerts:
                    tg_send(alert_str)
                    outcome_tracker.record(sym, "PUMP_FAST", alert_price, alert_extras)
                    mark_cooldown(sym, "pump_fast")
                    time.sleep(throttle)

            cleanup_counter += 1
            if cleanup_counter >= 30:
                confirmer.cleanup_expired()
                cleanup_counter = 0

            elapsed = time.time() - t0
            time.sleep(max(0.1, poll_s - elapsed))

        except Exception as e:
            log("ERR", f"fast_loop: {type(e).__name__}: {e}")
            time.sleep(5)


def run():
    log("BOOT", "loading config…")
    load_config(force=True)

    syms = fetch_usdt_symbols()
    if not syms:
        log("ERR", "no symbols resolved; check config.symbols.* or tier_file")
        sys.exit(2)

    global symbols
    symbols = list(syms)

    tfs = list((cfg("timeframes", {}) or {}).keys())
    log("CFG", f"Symbols: {len(syms)} | TFs: {tfs} | BOT_VER: {BOT_VER}")

    boot_ping_once(syms, tfs)
    outcome_tracker.start_filler()

    uf = UniverseFilter()
    uf.start()
    log("BOOT", f"UniverseFilter ready -- {len(uf.get_universe())} symbols")
    fast_thread = threading.Thread(target=run_fast_loop, args=(symbols, uf), daemon=True)
    fast_thread.start()
    log("BOOT", "Timescale fast loop thread started")

    while True:
        try:
            pass  # poll_once disabled — PUMP_FAST only
        except KeyboardInterrupt:
            log("BOOT", "shutdown requested (KeyboardInterrupt)")
            break
        except Exception as e:
            log("ERR", f"loop: {type(e).__name__}: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run()
