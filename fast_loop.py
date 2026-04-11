#!/usr/bin/env python3
"""
fast_loop.py — KTS TradeSeeker PUMP_FAST detection loop
v1.3 — 2026-04-04

Changes v1.3:
- MoveState FSM: idle → building → triggered → confirming → idle
- _seed_noise_bulk(): bulk query 300s history, noise seed per symbol
- noise_floor relativo al precio (price * 0.0001) — cross-symbol coherente
- CUSUM shadow mode: S_t loggeado por tick en building, no bloquea en v1
- t_build_ms y t_confirm_ms añadidos a extras en outcome_tracker.record()
- _move_states lifecycle: creado on-demand, sincronizado en cleanup
- Parámetros: alpha=0.10, k=2.5, r=1.5, N=4, m=0.5 (DC-DT-06/08/09)
- DC-DT-16: triggered no resetea por precio — decisión consciente

CodeGuard fixes aplicados (2026-04-04):
- Fix 1: SQL INTERVAL parametrizado correctamente (f-string para constante)
- Fix 2: import math eliminado (sin uso)
- Fix 3: noise EWMA usa prev_price tick-a-tick, no delta acumulado desde local_min
- Fix 4: on_done con price=0 no resetea local_min (preserva cold-start rule)
- Fix 5: _move_states purgado en cleanup para símbolos fuera del snapshot
- Fix 6: CUSUM eliminado de _update_idle — no hay move_start en idle, acumulaba ceros
- Fix 7: now_ts calculado una vez por ciclo, no por símbolo
- Fix 8: active inicializado a set() antes de bloques de cleanup para evitar NameError

Depends on: utils.py, ts_feed.py, aggtrade_confirmer.py,
            universe_filter.py, volume_filter.py, outcome_tracker.py,
            entry_gate.py

Design:
- One function: run_fast_loop(uf, stop_event)
- stop_event (threading.Event) allows clean shutdown from main.py
- Stale feed detection: if snapshot data is older than STALE_THRESHOLD_S → skip cycle
- Per-symbol isolation: an error on one symbol never aborts the full cycle
- Config hot-reload: parameters refreshed every cycle via cfg()
- Cleanup periodic: confirmer.cleanup_expired() every CLEANUP_EVERY cycles
- Cooldowns and dedup: via utils.on_cooldown / mark_cooldown

Capa 5 (entry gate) — v1.1 — 2026-03-31:
- Rule 1 (timing): stage_pct > stage_pct_max → reject "late_entry"
- Rule 2 (buy_ratio): desactivada — cubierta upstream por is_confirmed()
- Rule 3 (persistencia): bloqueada — DC-C5-01
- _trigger_prices: dict[str, float] tracks price_at_trigger per candidate.
  Populated on spike detection, cleared on ALL candidate exit paths:
    - cooldown gate     → pop
    - rel_vol reject    → pop + confirmer.remove()
    - entry_gate error  → pop + confirmer.remove()
    - entry_gate reject → pop + confirmer.remove()
    - alert emitted     → pop + confirmer.remove()
    - cleanup_expired   → synced via confirmer.active_candidates()

MoveState FSM (v1.3):
- Lives in _move_states dict alongside _trigger_prices
- Created on-demand when symbol first appears in snapshot
- Seeded with noise from bulk Timescale query at loop start
- Lifecycle mirrors _trigger_prices: cleaned up in periodic sync
- CUSUM shadow: S_t accumulated per tick in building state, logged at
  trigger/reset — does NOT block signal emission in v1
"""

import time
import json
import threading
import statistics
import psycopg2
import psycopg2.extras
from collections import deque
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Set

import outcome_tracker
import entry_gate
from utils import log, load_config, cfg, tg_send, on_cooldown, mark_cooldown
from ts_feed import TSFeed, PriceTracker
from aggtrade_confirmer import AggTradeConfirmer
from universe_filter import UniverseFilter
from volume_filter import RelativeVolumeFilter
from downtrend_filter import DowntrendFilter
import time_gate
import dataset_capture
from dataset_capture import CandidateSnapshot
from datetime import datetime, timezone

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
_DEFAULT_STAGE_MAX = 4.0

# Snapshot older than this is treated as stale → cycle skipped
_STALE_THRESHOLD_S = 15

# Run confirmer.cleanup_expired() every N cycles
_CLEANUP_EVERY     = 30

# Max consecutive empty snapshots before escalating the log warning
_EMPTY_WARN_EVERY  = 3

# ---------------------------------------------------------------------------
# MoveState FSM — DC-DT-10 through DC-DT-16
# ---------------------------------------------------------------------------

# FSM states
_MS_IDLE       = "idle"
_MS_BUILDING   = "building"
_MS_TRIGGERED  = "triggered"
_MS_CONFIRMING = "confirming"

# MoveState parameters (DC-DT-06/07/08/09)
_MS_ALPHA          = 0.10    # EWMA decay for noise
_MS_K              = 2.5     # move declaration threshold (multiples of noise)
_MS_R              = 1.5     # reset threshold on retrace (multiples of noise)
_MS_N              = 4       # max consecutive snapshots without progress before reset
_MS_M              = 0.5     # progress definition (multiples of noise above peak)
_MS_NOISE_FLOOR_BP = 0.0001  # noise_floor = price * this (1 basis point)

# CUSUM shadow parameters
_CUSUM_C = 1.0   # reference value (slack)
_CUSUM_H = 2.0   # threshold (shadow only — does not block)

# Seed query window
_SEED_WINDOW_S   = 300
_SEED_MIN_DELTAS = 15   # use EWMA if >= this, else median


@dataclass
class MoveState:
    """
    Per-symbol FSM tracking the build-up of a price move.
    Implements DC-DT-01 through DC-DT-16.

    State machine: idle → building → triggered → confirming → idle

    Thread safety: accessed only from the main fast_loop thread — no lock needed.
    """
    symbol: str

    # FSM state
    state: str = _MS_IDLE

    # Noise EWMA (DC-DT-04) — seeded externally via seed_noise()
    noise: float = 0.0

    # Previous price for tick-to-tick noise delta (Fix 3)
    prev_price_for_noise: float = 0.0

    # Baseline tracking — updated only in idle (DC-DT-11 baseline rule)
    local_min_price: float = 0.0
    local_min_ts:    float = 0.0

    # Move identity — set on transition to building, immutable after (DC-DT-02)
    move_start_price: float = 0.0
    move_start_ts:    float = 0.0

    # Progress tracking in building
    peak_since_start:  float = 0.0
    no_progress_ticks: int   = 0

    # Trigger timestamps (DC-DT-12)
    trigger_ts:   float = 0.0
    t_build_ms:   float = 0.0
    confirm_ts:   float = 0.0
    t_confirm_ms: float = 0.0

    # Noise at declaration — for audit logging
    noise_at_start: float = 0.0

    # CUSUM shadow (DC-DT-05 / DC-DT-05A) — active only in building+
    cusum_s:       float = 0.0
    cusum_traj:    deque = field(default_factory=lambda: deque(maxlen=300))
    cusum_crossed: bool  = False

    def seed_noise(self, seed_value: float, price: float) -> None:
        """Initialize noise from historical data. Never zero (DC-DT-04A)."""
        noise_floor = price * _MS_NOISE_FLOOR_BP
        self.noise = max(seed_value, noise_floor) if seed_value > 0 else noise_floor
        self.local_min_price     = price
        self.local_min_ts        = time.time()
        self.prev_price_for_noise = price

    def update(self, price: float, ts: float) -> str:
        """
        Process one price tick. Returns FSM state after update.
        'ts' is wall-clock time of this tick — one value per cycle, not per symbol.
        """
        # Fix 3: noise EWMA uses tick-to-tick delta, not accumulated delta from local_min
        ref = self.prev_price_for_noise if self.prev_price_for_noise > 0 else price
        abs_delta = abs(price - ref)
        self.noise = _MS_ALPHA * abs_delta + (1 - _MS_ALPHA) * self.noise
        self.prev_price_for_noise = price

        if self.state == _MS_IDLE:
            return self._update_idle(price, ts)
        elif self.state == _MS_BUILDING:
            return self._update_building(price, ts)
        elif self.state == _MS_TRIGGERED:
            # DC-DT-16: triggered does not reset on price collapse — frozen
            return self.state
        elif self.state == _MS_CONFIRMING:
            return self.state
        return self.state

    def _update_idle(self, price: float, ts: float) -> str:
        # Update local_min — only in idle (DC-DT-11 baseline rule)
        if price <= self.local_min_price or self.local_min_price == 0:
            self.local_min_price = price
            self.local_min_ts    = ts

        # Fix 6: CUSUM removed from idle — move_start_price is 0 here,
        # so _cusum_update would accumulate only zeros. No semantic value.

        # Check if move is declaring (DC-DT-03)
        threshold = _MS_K * self.noise
        if threshold > 0 and (price - self.local_min_price) > threshold:
            # Transition to building
            self.state             = _MS_BUILDING
            self.move_start_price  = self.local_min_price
            self.move_start_ts     = self.local_min_ts
            self.noise_at_start    = self.noise
            self.peak_since_start  = price
            self.no_progress_ticks = 0
            self.cusum_traj.clear()
            self.cusum_s       = 0.0
            self.cusum_crossed = False

        return self.state

    def _update_building(self, price: float, ts: float) -> str:
        # Update peak
        if price > self.peak_since_start:
            self.peak_since_start = price

        # CUSUM shadow — only meaningful once move_start is declared
        self._cusum_update(price)

        # Reset by retrace (DC-DT-08): peak - price > r * noise
        retrace_threshold = _MS_R * self.noise
        if retrace_threshold > 0 and (self.peak_since_start - price) > retrace_threshold:
            self._reset_to_idle(price, ts, reason="retrace")
            return self.state

        # Reset by lateralization (DC-DT-09)
        progress_threshold = _MS_M * self.noise
        if progress_threshold > 0 and price > (self.peak_since_start + progress_threshold):
            self.no_progress_ticks = 0  # reset counter on real progress
        else:
            self.no_progress_ticks += 1
            if self.no_progress_ticks >= _MS_N:
                self._reset_to_idle(price, ts, reason="lateralization")
                return self.state

        return self.state

    def on_trigger(self, trigger_ts: float) -> None:
        """
        Called when pump_pct threshold is crossed.
        Freezes build state and records t_build_ms (DC-DT-11 freeze rule).
        """
        if self.state != _MS_BUILDING:
            return
        self.state      = _MS_TRIGGERED
        self.trigger_ts = trigger_ts
        self.t_build_ms = (trigger_ts - self.move_start_ts) * 1000.0
        cusum_pass = self.cusum_s >= _CUSUM_H
        log("MOVE", (
            f"triggered {self.symbol} | "
            f"t_build_ms={self.t_build_ms:.0f} | "
            f"move_start={self.move_start_price:.6g} | "
            f"peak={self.peak_since_start:.6g} | "
            f"noise={self.noise:.6g} | "
            f"cusum_s={self.cusum_s:.3f} | "
            f"cusum_pass={cusum_pass} (shadow)"
        ))

    def on_confirming(self, confirm_ts: float) -> None:
        """Called when AggTradeConfirmer returns is_confirmed=True."""
        if self.state not in (_MS_TRIGGERED, _MS_BUILDING):
            return
        self.state        = _MS_CONFIRMING
        self.confirm_ts   = confirm_ts
        self.t_confirm_ms = (
            (confirm_ts - self.trigger_ts) * 1000.0
            if self.trigger_ts > 0 else 0.0
        )

    def on_done(self, price: float, ts: float) -> None:
        """Called when signal is emitted or candidate is removed. Returns to idle."""
        self._reset_to_idle(price, ts, reason="done")

    def timing_extras(self) -> dict:
        """Returns timing fields to merge into outcome_tracker extras."""
        return {
            "t_build_ms":        round(self.t_build_ms, 1)       if self.t_build_ms   > 0 else None,
            "t_confirm_ms":      round(self.t_confirm_ms, 1)     if self.t_confirm_ms > 0 else None,
            "move_start_price":  round(self.move_start_price, 8) if self.move_start_price > 0 else None,
            "noise_at_start":    round(self.noise_at_start, 8)   if self.noise_at_start   > 0 else None,
            "peak_since_start":  round(self.peak_since_start, 8) if self.peak_since_start > 0 else None,
            "cusum_at_trigger":  round(self.cusum_s, 3),
            "cusum_shadow_pass": self.cusum_s >= _CUSUM_H,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _cusum_update(self, price: float) -> None:
        """CUSUM shadow — accumulates but never blocks (DC-DT-05).
        Only called from _update_building where move_start_price > 0."""
        increment = (
            (price - self.move_start_price) / max(self.noise, 1e-12) - _CUSUM_C
        )
        self.cusum_s = max(0.0, self.cusum_s + increment)
        self.cusum_traj.append(round(self.cusum_s, 4))
        if not self.cusum_crossed and self.cusum_s >= _CUSUM_H:
            self.cusum_crossed = True

    def _reset_to_idle(self, price: float, ts: float, reason: str) -> None:
        if self.state == _MS_BUILDING:
            log("MOVE", (
                f"reset {self.symbol} reason={reason} | "
                f"peak={self.peak_since_start:.6g} | "
                f"noise={self.noise:.6g} | "
                f"cusum_traj_len={len(self.cusum_traj)}"
            ))
        self.state             = _MS_IDLE
        self.move_start_price  = 0.0
        self.move_start_ts     = 0.0
        self.noise_at_start    = 0.0
        self.peak_since_start  = 0.0
        self.no_progress_ticks = 0
        self.trigger_ts        = 0.0
        self.t_build_ms        = 0.0
        self.confirm_ts        = 0.0
        self.t_confirm_ms      = 0.0
        self.cusum_s           = 0.0
        self.cusum_traj.clear()
        self.cusum_crossed     = False
        # Fix 4: only update local_min if price is valid — never set to 0
        # (would produce noise_floor=0 on next seed, violating DC-DT-04A)
        if price > 0:
            self.local_min_price = price
            self.local_min_ts    = ts


# ---------------------------------------------------------------------------
# Noise seed — bulk query (DC-DT-04A + v2.3 annexe)
# ---------------------------------------------------------------------------

_DSN = "host=localhost port=5432 dbname=tsdb user=postgres password=postgres"


def _seed_noise_bulk(symbols: list, snapshot: dict) -> Dict[str, float]:
    """
    Query metrics_ext for the last _SEED_WINDOW_S seconds for all symbols.
    Returns dict[symbol] → seed_noise_value (never zero).

    Uses EWMA(abs_delta) if n_deltas >= _SEED_MIN_DELTAS, else median.
    Falls back to noise_floor = price * _MS_NOISE_FLOOR_BP if no data.
    """
    if not symbols:
        return {}

    seeds: Dict[str, float] = {}
    rows = []
    try:
        conn = psycopg2.connect(_DSN)
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Fix 1: _SEED_WINDOW_S is a module constant, safe to embed directly.
            # INTERVAL parameter substitution via %s does not work inside string literals
            # in psycopg2 — the interval value must be part of the SQL string itself.
            cur.execute(f"""
                SELECT symbol, price
                FROM metrics_ext
                WHERE symbol = ANY(%s)
                  AND ts >= NOW() - INTERVAL '{_SEED_WINDOW_S} seconds'
                ORDER BY symbol, ts ASC
            """, (symbols,))
            rows = cur.fetchall()
        conn.close()
    except Exception as e:
        log("MOVE", f"WARN seed_noise_bulk query failed: {e} — using noise_floor for all")

    # Group prices by symbol
    by_symbol: Dict[str, list] = {}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(float(row["price"]))

    for sym in symbols:
        current_price = float((snapshot.get(sym) or {}).get("price") or 1.0)
        noise_floor   = current_price * _MS_NOISE_FLOOR_BP

        prices = by_symbol.get(sym, [])
        if len(prices) < 2:
            seeds[sym] = noise_floor
            continue

        abs_deltas = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
        abs_deltas = [d for d in abs_deltas if d > 0]

        if not abs_deltas:
            seeds[sym] = noise_floor
            continue

        if len(abs_deltas) >= _SEED_MIN_DELTAS:
            # EWMA over historical sequence
            ewma = abs_deltas[0]
            for d in abs_deltas[1:]:
                ewma = _MS_ALPHA * d + (1 - _MS_ALPHA) * ewma
            seed = ewma
        else:
            # Median — more robust for small samples (v2.3 annex A2)
            seed = statistics.median(abs_deltas)

        seeds[sym] = max(seed, noise_floor)

    return seeds


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

def run_fast_loop(
    uf:          UniverseFilter,
    stop_event:  threading.Event,
    proxy_pool=None,
    bot_version: str = "unknown",
) -> None:
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
        proxy_pool=proxy_pool,
    )
    dtf = DowntrendFilter()
    rvf = RelativeVolumeFilter(
        min_rel_volume=cfg("rules.fast_ts.rel_volume_min", _DEFAULT_REL_VOL)
    )

    # Capa 5 — price at trigger per candidate
    # Cleared on ALL candidate exit paths — see module docstring.
    _trigger_prices: Dict[str, float] = {}

    # MoveState FSM — one per symbol, created on-demand (v1.3)
    _move_states: Dict[str, MoveState] = {}

    # Noise seed populated on first valid snapshot
    _noise_seeded = False

    log("FAST", (
        f"loop started (v1.3 — "
        f"proxy={'enabled' if proxy_pool else 'disabled'} — "
        f"bot={bot_version})"
    ))

    consecutive_empty = 0
    cleanup_counter   = 0
    _tod_last_sig     = None  # (blocked, market_regime, operator_window) — anti-spam

    while not stop_event.is_set():

        # ── 1. Config hot-reload ─────────────────────────────────────────
        load_config()

        poll_s      = float(cfg("rules.fast_ts.poll_interval_s", _DEFAULT_POLL_S))
        pump_pct    = float(cfg("rules.fast_ts.pump_pct",         _DEFAULT_PUMP_PCT))
        min_vol     = float(cfg("rules.fast_ts.min_vol_24h",      _DEFAULT_MIN_VOL))
        cd_min      = int(  cfg("rules.fast_ts.cooldown_min",     _DEFAULT_CD_MIN))
        throttle    = max(0.0, float(cfg("telegram.throttle_sec", 2.0)))
        prefix      = cfg("telegram.prefix", "TS")
        rel_vol_min = float(cfg("rules.fast_ts.rel_volume_min",   _DEFAULT_REL_VOL))
        stage_max   = float(cfg("rules.fast_ts.stage_pct_max",    _DEFAULT_STAGE_MAX))

        if stage_max <= 0:
            log("FAST", f"WARNING: stage_pct_max={stage_max} inválido — "
                        f"usando default {_DEFAULT_STAGE_MAX}")
            stage_max = _DEFAULT_STAGE_MAX

        params = _read_confirmer_params()
        confirmer.vol_ratio = params["vol_ratio"]
        confirmer.window_s  = params["window_s"]
        confirmer.ttl_s     = params["ttl_s"]

        rvf.min_rel_volume = rel_vol_min

        cycle_start = time.monotonic()

        # Fix 7: now_ts computed once per cycle — all symbols in this cycle
        # share the same reference timestamp for FSM and trigger recording.
        now_ts = time.time()

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

        # ── 4b. Bulk noise seed — first valid cycle only ─────────────────
        if not _noise_seeded:
            try:
                universe_syms = list(snapshot.keys())
                seeds = _seed_noise_bulk(universe_syms, snapshot)
                for sym, seed_val in seeds.items():
                    p = float((snapshot.get(sym) or {}).get("price") or 1.0)
                    ms = MoveState(symbol=sym)
                    ms.seed_noise(seed_val, p)
                    _move_states[sym] = ms
                log("MOVE", f"noise seeded for {len(seeds)} symbols")
            except Exception as e:
                log("FAST", f"WARN seed_noise_bulk failed: {e} — will seed on-demand")
            finally:
                _noise_seeded = True  # never retry — on-demand fallback handles gaps

        # ── 5. Universe filter ───────────────────────────────────────────
        universe = set(uf.get_universe())

        # ── 5b. Time of day gate (Capa 4) ───────────────────────────────
        tod = time_gate.evaluate_now()
        _tod_sig = (tod["blocked"], tod["market_regime"], tod["operator_window"])
        if _tod_sig != _tod_last_sig:
            if tod["reason"] == "fail_open":
                log("FAST", "tod fail_open — gate skipped (fail-open)")
            elif tod["blocked"]:
                log("FAST",
                    f"tod block (reason={tod['reason']}, "
                    f"local_hour={tod['local_hour']}, regime={tod['market_regime']})")
            else:
                log("FAST",
                    f"tod allow (local_hour={tod['local_hour']}, "
                    f"regime={tod['market_regime']})")
            _tod_last_sig = _tod_sig
        if tod["blocked"]:
            stop_event.wait(timeout=poll_s)
            continue

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

            price   = float(price)
            vol_24h = data.get("vol_24h") or 0.0

            # Liquidity gate
            if vol_24h < min_vol:
                tracker.update(sym, price)
                continue

            # ── MoveState on-demand creation ────────────────────────────
            if sym not in _move_states:
                ms = MoveState(symbol=sym)
                ms.seed_noise(price * _MS_NOISE_FLOOR_BP, price)  # cold fallback
                _move_states[sym] = ms
            ms = _move_states[sym]

            # ── MoveState FSM tick ───────────────────────────────────────
            try:
                ms.update(price, now_ts)
            except Exception as e:
                log("FAST", f"ERROR MoveState.update {sym}: {type(e).__name__}: {e}")

            # Delta computation
            try:
                delta = tracker.update(sym, price)
            except Exception as e:
                log("FAST", f"ERROR tracker.update {sym}: {type(e).__name__}: {e}")
                continue

            if delta is None:
                continue

            # ── Spike detected ───────────────────────────────────────────
            if delta >= pump_pct:
                if dtf.is_downtrend(sym):
                    log("FAST", f"downtrend skip {sym}")
                    continue

                # Capa 4 — near-miss nocturno
                if tod["blocked"]:
                    log("FAST",
                        f"tod near-miss {sym} delta={delta:.3f}% "
                        f"regime={tod['market_regime']} local_hour={tod['local_hour']}")
                    try:
                        outcome_tracker.record(
                            symbol           = sym,
                            rule             = "PUMP_FAST",
                            signal_kind      = "near_miss",
                            decision         = "blocked",
                            price_alert      = price,
                            extras           = {
                                "delta":           round(delta, 3),
                                "market_regime":   tod["market_regime"],
                                "operator_window": tod["operator_window"],
                                "local_hour":      tod["local_hour"],
                                "utc_hour":        tod["utc_hour"],
                                "operator_sleep":  True,
                            },
                            rejection_reason = "operator_sleep",
                            bot_version      = bot_version,
                        )
                    except Exception as e:
                        log("FAST", f"ERROR outcome_tracker near-miss {sym}: {type(e).__name__}: {e}")
                    continue

                # Notify MoveState of trigger (freezes build, records t_build_ms)
                if ms.state == _MS_BUILDING:
                    ms.on_trigger(now_ts)
                elif ms.state == _MS_IDLE:
                    # Pump detected before FSM declared building
                    # (move too fast or noise too high) — record trigger with t_build_ms=0
                    ms.trigger_ts = now_ts
                    ms.t_build_ms = 0.0
                    ms.state      = _MS_TRIGGERED

                try:
                    confirmer.add_candidate(sym)
                except Exception as e:
                    log("FAST", f"ERROR add_candidate {sym}: {type(e).__name__}: {e}")
                    continue

                if sym not in _trigger_prices:
                    _trigger_prices[sym] = price

            # ── Confirmation check ───────────────────────────────────────
            try:
                confirmed = confirmer.is_confirmed(sym)
            except Exception as e:
                log("FAST", f"ERROR is_confirmed {sym}: {type(e).__name__}: {e}")
                continue

            if not confirmed or delta < pump_pct:
                continue

            # Notify MoveState of confirmation
            if ms.state in (_MS_TRIGGERED, _MS_BUILDING):
                ms.on_confirming(now_ts)

            # Cooldown gate
            if on_cooldown(sym, "pump_fast", cd_min):
                _trigger_prices.pop(sym, None)
                ms.on_done(price, now_ts)
                continue

            # ── Capa 3: volumen relativo ─────────────────────────────────
            try:
                conf_state    = confirmer.debug_state(sym)
                quote_vol_30s = conf_state.get("quote_vol_30s", 0.0)
                quote_vol_24h = data.get("quote_vol_24h") or data.get("vol_24h") or 0.0
                rel_vol, rv_reason = rvf.check(sym, quote_vol_30s, quote_vol_24h)
            except Exception as e:
                log("FAST", f"ERROR rel_vol check {sym}: {type(e).__name__}: {e}")
                confirmer.remove(sym)
                _trigger_prices.pop(sym, None)
                ms.on_done(price, now_ts)
                continue

            if rv_reason != "ok":
                log("FAST", f"rel_vol skip {sym}: {rv_reason} ({rel_vol:.2f}x)")
                confirmer.remove(sym)
                _trigger_prices.pop(sym, None)
                ms.on_done(price, now_ts)
                continue

            # ── Capa 5: entry gate ───────────────────────────────────────
            price_at_trigger = _trigger_prices.get(sym)

            if price_at_trigger is None:
                log("FAST", f"WARN entry_gate {sym}: no trigger price — gate skipped")
                gate_accepted    = True
                gate_result_dict = {"stage_pct": None, "pass_reason": "no_trigger_price"}
            else:
                try:
                    gate = entry_gate.evaluate(
                        price_at_trigger=price_at_trigger,
                        price_at_confirm=price,
                        stage_pct_max=stage_max,
                    )
                    gate_accepted    = gate.accepted
                    gate_result_dict = {
                        "stage_pct":        gate.stage_pct,
                        "rejection_reason": gate.rejection_reason,
                        "pass_reason":      gate.pass_reason,
                    }
                except ValueError as e:
                    log("FAST", f"ERROR entry_gate {sym}: {e} — skipping")
                    confirmer.remove(sym)
                    _trigger_prices.pop(sym, None)
                    ms.on_done(price, now_ts)
                    continue
                except Exception as e:
                    log("FAST", f"ERROR entry_gate {sym}: {type(e).__name__}: {e} — skipping")
                    confirmer.remove(sym)
                    _trigger_prices.pop(sym, None)
                    ms.on_done(price, now_ts)
                    continue

            # ── Dataset capture (Fase 1 — DC-DATASET-02) ─────────────────
            # Capturar TODOS los candidatos que llegan a confirming,
            # incluyendo los rechazados por entry_gate (DC-DATASET-02).
            # Fail-open: cualquier error loggea y no interrumpe el loop.
            try:
                _cap_start   = time.monotonic()          # primero — mide latencia real
                _tod_now     = time_gate.evaluate_now()
                _timing      = ms.timing_extras()
                _conf_state  = confirmer.debug_state(sym) or {}
                _buy_ratio   = _conf_state.get("buy_ratio")  # None si no disponible (N6)

                _snap = CandidateSnapshot(
                    symbol              = sym,
                    detector_version    = bot_version,
                    config_snapshot     = cfg("rules.fast_ts", {}),
                    t_trigger           = (
                        datetime.fromtimestamp(ms.trigger_ts, tz=timezone.utc)
                        if ms.trigger_ts > 0 else None
                    ),
                    t_confirm           = datetime.fromtimestamp(now_ts, tz=timezone.utc),
                    stage_pct           = gate_result_dict.get("stage_pct"),
                    delta_pct           = round(delta, 3),
                    t_build_ms          = _timing.get("t_build_ms"),
                    t_confirm_ms        = _timing.get("t_confirm_ms"),
                    move_start_price    = _timing.get("move_start_price"),
                    noise_at_start      = _timing.get("noise_at_start"),
                    peak_since_start    = _timing.get("peak_since_start"),
                    cusum_at_trigger    = _timing.get("cusum_at_trigger"),
                    cusum_shadow_pass   = _timing.get("cusum_shadow_pass"),
                    rel_vol             = round(rel_vol, 4),
                    confirm_ratio       = (round(_buy_ratio, 4) if _buy_ratio is not None else None),
                    n_trades            = _conf_state.get("n_events"),
                    utc_hour            = _tod_now.get("utc_hour"),
                    market_regime       = _tod_now.get("market_regime"),
                    operator_window     = _tod_now.get("operator_window"),
                    entry_gate_decision = "passed" if gate_accepted else "rejected",
                    rejection_reason    = (gate_result_dict.get("rejection_reason")
                                          if not gate_accepted else None),
                    capture_started_at  = _cap_start,
                )
                dataset_capture.capture(_snap)
            except Exception as _dce:
                log("FAST", f"WARN dataset_capture {sym}: {type(_dce).__name__}: {_dce}")

            if not gate_accepted:
                log("FAST",
                    f"entry_gate REJECT {sym}: "
                    f"reason={gate_result_dict.get('rejection_reason')} "
                    f"stage_pct={gate_result_dict.get('stage_pct')}")
                confirmer.remove(sym)
                _trigger_prices.pop(sym, None)
                ms.on_done(price, now_ts)
                continue

            # ── Build alert ──────────────────────────────────────────────
            extras = {
                "delta":     round(delta, 3),
                "buy_ratio": round(conf_state.get("buy_ratio") or 0.0, 2),
                "n_trades":  conf_state.get("n_events", 0),
                "rel_vol":   round(rel_vol, 2),
                "stage_pct": gate_result_dict.get("stage_pct"),
                "confirmed": True,
            }
            try:
                extras.update(ms.timing_extras())
            except Exception as e:
                log("FAST", f"WARN timing_extras {sym}: {e}")

            alert_str = (
                f"[{prefix}] PUMP_FAST | {sym} WS @ {price:.6g} | "
                f"{json.dumps(extras, separators=(',', ':'))}"
            )
            confirmed_alerts.append((sym, alert_str, price, extras))
            confirmer.remove(sym)
            _trigger_prices.pop(sym, None)
            ms.on_done(price, now_ts)

        # ── 7. Emit alerts ───────────────────────────────────────────────
        query_ms = (time.monotonic() - cycle_start) * 1000

        if confirmed_alerts:
            log("FAST",
                f"query={query_ms:.1f}ms | symbols={len(snapshot)} | "
                f"confirmed={len(confirmed_alerts)} | candidates={confirmer.active_count()}")

            for sym, alert_str, alert_price, alert_extras in confirmed_alerts:
                tg_send(alert_str)
                try:
                    outcome_tracker.record(
                        symbol      = sym,
                        rule        = "PUMP_FAST",
                        signal_kind = "alert",
                        decision    = "sent",
                        price_alert = alert_price,
                        extras      = alert_extras,
                        bot_version = bot_version,
                    )
                except Exception as e:
                    log("FAST", f"ERROR outcome_tracker.record {sym}: {type(e).__name__}: {e}")
                mark_cooldown(sym, "pump_fast")
                if throttle > 0 and not stop_event.is_set():
                    stop_event.wait(timeout=throttle)

        # ── 8. Periodic cleanup ──────────────────────────────────────────
        cleanup_counter += 1
        if cleanup_counter >= _CLEANUP_EVERY:

            # Fix 8: active initialized before all cleanup blocks to prevent
            # NameError if active_candidates() raises an exception
            active: Set[str] = set()

            try:
                confirmer.cleanup_expired()
                dtf.gc(set(uf.get_universe()))
            except Exception as e:
                log("FAST", f"ERROR cleanup_expired: {type(e).__name__}: {e}")

            try:
                active = confirmer.active_candidates()
                _trigger_prices = {s: p for s, p in _trigger_prices.items() if s in active}
            except Exception as e:
                log("FAST", f"ERROR trigger_prices sync: {type(e).__name__}: {e}")

            try:
                snapshot_syms = set(snapshot.keys())
                for sym in list(_move_states.keys()):
                    ms = _move_states[sym]
                    if sym not in active and ms.state in (_MS_TRIGGERED, _MS_CONFIRMING):
                        # Candidate expired without completing — return to idle
                        p = float((snapshot.get(sym) or {}).get("price") or 0.0)
                        ms.on_done(p, now_ts)
                    # Fix 5: purge idle states for symbols no longer in snapshot
                    # (left the universe or feed gap) — prevents unbounded growth
                    elif sym not in snapshot_syms and ms.state == _MS_IDLE:
                        del _move_states[sym]
            except Exception as e:
                log("FAST", f"ERROR move_states sync: {type(e).__name__}: {e}")

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
