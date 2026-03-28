#!/usr/bin/env python3
"""
fast_loop.py — KTS TradeSeeker PUMP_FAST detection loop
v1.0 — 2026-03-28

Extracted from main.py (v4.2) — run_fast_loop() now lives here.
Depends on: utils.py, ts_feed.py, aggtrade_confirmer.py,
            universe_filter.py, volume_filter.py, outcome_tracker.py

Design:
- One function: run_fast_loop(uf, stop_event)
- stop_event (threading.Event) allows clean shutdown from main.py
- Stale feed detection: if snapshot data is older than STALE_THRESHOLD_S → skip cycle
- Per-symbol isolation: an error on one symbol never aborts the full cycle
- Config hot-reload: parameters refreshed every cycle via cfg()
- Cleanup periodic: confirmer.cleanup_expired() every CLEANUP_EVERY cycles
- Cooldowns and dedup: via utils.on_cooldown / mark_cooldown

Flow per cycle:
  1. load_config()
  2. get_snapshot() from Timescale
  3. Validate snapshot freshness
  4. Filter by universe (UniverseFilter)
  5. For each symbol: compute delta, add to confirmer if spike detected
  6. If confirmer.is_confirmed(sym): check rel_vol (Capa 3) → alert
  7. tg_send() + outcome_tracker.record() + mark_cooldown()
  8. sleep(poll_interval - elapsed)
"""

import time
import json
import threading
from typing import List, Optional

import outcome_tracker
from utils import log, load_config, cfg, tg_send, on_cooldown, mark_cooldown
from ts_feed import TSFeed, PriceTracker
from aggtrade_confirmer import AggTradeConfirmer
from universe_filter import UniverseFilter
from volume_filter import RelativeVolumeFilter

# ---------------------------------------------------------------------------
# Constants — defaults used when config key is missing
# ---------------------------------------------------------------------------

_DEFAULT_POLL_S    = 2
_DEFAULT_PUMP_PCT  = 0.30
_DEFAULT_MIN_VOL   = 50_000
_DEFAULT_CD_MIN    = 5
_DEFAULT_RATIO     = 0.60
_DEFAULT_WINDOW_S  = 30
_DEFAULT_TTL_S     = 90
_DEFAULT_REL_VOL   = 3.0

# Snapshot older than this is treated as stale → cycle skipped
_STALE_THRESHOLD_S = 15

# Run confirmer.cleanup_expired() every N cycles
_CLEANUP_EVERY     = 30

# Max consecutive empty snapshots before escalating the log warning
_EMPTY_WARN_EVERY  = 3


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _snapshot_age_s(snapshot: dict) -> Optional[float]:
    """
    Return the age in seconds of the most recent tick in the snapshot.
    Uses the 'ts' field (datetime or epoch float) from TSFeed rows.
    Returns None if snapshot is empty or timestamps are missing/unparseable.
    """
    newest = None
    for data in snapshot.values():
        ts = data.get("ts")
        if ts is None:
            continue
        # psycopg2 returns datetime objects for TIMESTAMPTZ columns
        try:
            if hasattr(ts, "timestamp"):
                epoch = ts.timestamp()
            else:
                epoch = float(ts)
            if newest is None or epoch > newest:
                newest = epoch
        except (TypeError, ValueError):
            continue

    if newest is None:
        return None
    return time.time() - newest


def _read_confirmer_params() -> dict:
    """Read all AggTradeConfirmer params from config in one place."""
    return {
        "vol_ratio":  cfg("rules.fast_ts.confirm_ratio",    _DEFAULT_RATIO),
        "window_s":   cfg("rules.fast_ts.confirm_window_s", _DEFAULT_WINDOW_S),
        "ttl_s":      cfg("rules.fast_ts.confirm_ttl_s",    _DEFAULT_TTL_S),
    }


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_fast_loop(uf: UniverseFilter, stop_event: threading.Event) -> None:
    """
    PUMP_FAST detection loop.

    Args:
        uf:         UniverseFilter instance (already started by caller).
        stop_event: Set this event from main.py to request clean shutdown.

    This function blocks until stop_event is set or an unrecoverable error
    occurs. It is designed to run in a daemon thread.

    Shutdown: stop_event.wait(timeout) is used instead of time.sleep()
    so the loop exits promptly when stop_event is set.
    """
    feed      = TSFeed()
    tracker   = PriceTracker()
    confirmer = AggTradeConfirmer(
        window_s=_DEFAULT_WINDOW_S,
        ttl_s=_DEFAULT_TTL_S,
        vol_ratio=_DEFAULT_RATIO,
        log_fn=log,
    )
    rvf = RelativeVolumeFilter(
        min_rel_volume=cfg("rules.fast_ts.rel_volume_min", _DEFAULT_REL_VOL)
    )

    log("FAST", "loop started (v1.0 — modular)")

    consecutive_empty = 0
    cleanup_counter   = 0

    while not stop_event.is_set():

        # ── 1. Config hot-reload ─────────────────────────────────────────
        load_config()

        poll_s    = float(cfg("rules.fast_ts.poll_interval_s", _DEFAULT_POLL_S))
        pump_pct  = float(cfg("rules.fast_ts.pump_pct",         _DEFAULT_PUMP_PCT))
        min_vol   = float(cfg("rules.fast_ts.min_vol_24h",      _DEFAULT_MIN_VOL))
        cd_min    = int(  cfg("rules.fast_ts.cooldown_min",     _DEFAULT_CD_MIN))
        throttle  = max(0.0, float(cfg("telegram.throttle_sec", 2.0)))
        prefix    = cfg("telegram.prefix", "TS")
        rel_vol_min = float(cfg("rules.fast_ts.rel_volume_min", _DEFAULT_REL_VOL))

        # Sync confirmer params with current config (cheap attribute set)
        params = _read_confirmer_params()
        confirmer.vol_ratio = params["vol_ratio"]
        confirmer.window_s  = params["window_s"]
        confirmer.ttl_s     = params["ttl_s"]

        # Sync rvf threshold
        rvf.min_rel_volume = rel_vol_min

        cycle_start = time.monotonic()

        # ── 2. Snapshot ──────────────────────────────────────────────────
        try:
            snapshot = feed.get_snapshot()
        except Exception as e:
            log("FAST", f"ERROR get_snapshot: {type(e).__name__}: {e}")
            stop_event.wait(timeout=poll_s)
            continue

        # ── 3. Empty snapshot handling ───────────────────────────────────
        if not snapshot:
            consecutive_empty += 1
            if consecutive_empty % _EMPTY_WARN_EVERY == 0:
                log("FAST", f"WARNING: {consecutive_empty} consecutive empty snapshots "
                            f"— check Timescale/Benthos pipeline")
            stop_event.wait(timeout=poll_s)
            continue

        consecutive_empty = 0

        # ── 4. Stale feed detection ──────────────────────────────────────
        age_s = _snapshot_age_s(snapshot)
        if age_s is not None and age_s > _STALE_THRESHOLD_S:
            log("FAST", f"STALE feed — newest tick is {age_s:.1f}s old "
                        f"(threshold={_STALE_THRESHOLD_S}s) — skipping cycle")
            stop_event.wait(timeout=poll_s)
            continue

        # ── 5. Universe filter ───────────────────────────────────────────
        universe = set(uf.get_universe())

        # ── 6. Per-symbol processing ─────────────────────────────────────
        confirmed_alerts: List[tuple] = []

        for sym, data in snapshot.items():

            # Universe gate
            if universe and sym not in universe:
                continue

            price = data.get("price")
            if price is None or price <= 0:
                log("FAST", f"bad price for {sym}: {price!r} — skipping")
                continue

            vol_24h = data.get("vol_24h") or 0.0

            # Liquidity gate
            if vol_24h < min_vol:
                tracker.update(sym, price)
                continue

            # Delta computation — per-symbol try/except: one bad symbol ≠ loop crash
            try:
                delta = tracker.update(sym, price)
            except Exception as e:
                log("FAST", f"ERROR tracker.update {sym}: {type(e).__name__}: {e}")
                continue

            if delta is None:
                continue

            # Spike detected → open aggTrade stream for confirmation
            if delta >= pump_pct:
                try:
                    confirmer.add_candidate(sym)
                except Exception as e:
                    log("FAST", f"ERROR add_candidate {sym}: {type(e).__name__}: {e}")
                    continue

            # Check if already confirmed
            try:
                confirmed = confirmer.is_confirmed(sym)
            except Exception as e:
                log("FAST", f"ERROR is_confirmed {sym}: {type(e).__name__}: {e}")
                continue

            if not confirmed or delta < pump_pct:
                continue

            # Cooldown gate
            if on_cooldown(sym, "pump_fast", cd_min):
                continue

            # ── Capa 3: volumen relativo ─────────────────────────────────
            try:
                state = confirmer.debug_state(sym)
                quote_vol_30s = state.get("quote_vol_30s", 0.0)

                # Use quote_vol_24h for correct USDT units (Capa 3 requirement)
                quote_vol_24h = data.get("quote_vol_24h") or data.get("vol_24h") or 0.0

                rel_vol, rv_reason = rvf.check(sym, quote_vol_30s, quote_vol_24h)
            except Exception as e:
                log("FAST", f"ERROR rel_vol check {sym}: {type(e).__name__}: {e}")
                confirmer.remove(sym)
                continue

            if rv_reason != "ok":
                log("FAST", f"rel_vol skip {sym}: {rv_reason} ({rel_vol:.2f}x)")
                confirmer.remove(sym)
                continue

            # ── Build alert ──────────────────────────────────────────────
            extras = {
                "delta":     round(delta, 3),
                "buy_ratio": round(state.get("buy_ratio") or 0.0, 2),
                "n_trades":  state.get("n_events", 0),
                "rel_vol":   round(rel_vol, 2),
                "confirmed": True,
            }
            alert_str = (
                f"[{prefix}] PUMP_FAST | {sym} WS @ {price:.6g} | "
                f"{json.dumps(extras, separators=(',', ':'))}"
            )
            confirmed_alerts.append((sym, alert_str, price, extras))
            confirmer.remove(sym)

        # ── 7. Emit alerts ───────────────────────────────────────────────
        query_ms = (time.monotonic() - cycle_start) * 1000

        if confirmed_alerts:
            log("FAST",
                f"query={query_ms:.1f}ms | symbols={len(snapshot)} | "
                f"confirmed={len(confirmed_alerts)} | candidates={confirmer.active_count()}")

            for sym, alert_str, alert_price, alert_extras in confirmed_alerts:
                tg_send(alert_str)
                try:
                    outcome_tracker.record(sym, "PUMP_FAST", alert_price, alert_extras)
                except Exception as e:
                    log("FAST", f"ERROR outcome_tracker.record {sym}: {type(e).__name__}: {e}")
                mark_cooldown(sym, "pump_fast")
                if throttle > 0 and not stop_event.is_set():
                    stop_event.wait(timeout=throttle)

        # ── 8. Periodic cleanup ──────────────────────────────────────────
        cleanup_counter += 1
        if cleanup_counter >= _CLEANUP_EVERY:
            try:
                confirmer.cleanup_expired()
            except Exception as e:
                log("FAST", f"ERROR cleanup_expired: {type(e).__name__}: {e}")
            cleanup_counter = 0

        # ── 9. Sleep remainder of poll interval (interruptible) ──────────
        elapsed = time.monotonic() - cycle_start
        sleep_s = max(0.05, poll_s - elapsed)
        stop_event.wait(timeout=sleep_s)

    # ── Shutdown ─────────────────────────────────────────────────────────
    log("FAST", "stop_event received — shutting down")
    try:
        feed.close()
    except Exception as e:
        log("FAST", f"ERROR feed.close: {type(e).__name__}: {e}")
    log("FAST", "loop stopped cleanly")
