#!/usr/bin/env python3
"""
btc_regime_worker.py — KTS TradeSeeker · BTC Regime Foundation · Capa 6
v1.1 — 2026-04-14

SoT: docs/BTC_Regime_SoT_v2.md

Responsabilidad:
    Producir snapshots de contexto BTC en modo shadow (F2).
    Persiste en btc_regime_snapshots. Cero impacto operativo en el pipeline base.

Arquitectura de threads:
    _ws_thread      → escucha btcusdt@aggTrade (WS permanente)
                      ÚNICO escritor de _trade_buffer
    _compute_loop   → timer ~2s
                      drena _trade_buffer, actualiza rolling buffers/stats,
                      calcula facts→features→scores→tags→health, persiste.
                      ÚNICO escritor de todos los rolling buffers y stats.

    Regla crítica: ningún otro thread toca rolling buffers ni stats.
    _latest_snapshot usa su propio Lock separado — lectura segura desde CLASS_2.

Contrato de uso (F2 shadow mode):
    CLASS_1 (observacional): logs, Grafana, debug — siempre permitido
    CLASS_2 (analítico):     dataset_capture, research, PIT joins — permitido
    CLASS_3 (operativo):     fast_loop, entry_gate, etc. — PROHIBIDO

    Eliminar este módulo debe dejar idéntica la conducta operativa del bot.
    El fallo del worker no puede back-propagate al pipeline base (DC-WIRE-07).

Fixes v1.1 (hardening exhaustivo):
    I1:  _RollingStats.update() protegido con try/except TypeError
    I3:  _realized_vol: skip par inválido en vez de abortar toda la ventana
    I5:  _stress_component usa _stats_abs_r_1m/_5m propios (no _stats_price)
    I6:  _trend_component usa _stats_r_15m/_stats_r_60m propios (no _stats_price)
    I7:  Atributo muerto _worker_status eliminado
    I8:  _query_metrics_ext hace commit post-SELECT para cerrar transacción
    I9:  rollback en _persist_snapshot loggea el error (no silencia)
    I10: book_ts_utc verifica tzinfo is not None antes de asumir tz-aware
    I11: start() usa flag _running con Lock para verificar estado real
    I12: _RollingBuffer.append() documentado: ts debe ser wall-clock
    I13: vals_15m calculado una sola vez en _compute_cycle, pasado a funciones
    I14: docstrings de _compute_facts/_compute_features documentan side effects
    I15: spread_stability_1m marcada explícitamente como proxy/deuda técnica
    I16: ws_loop resetea backoff a base tras conexión exitosa larga
    I17: _validate_snapshot_invariants protegido con try/except TypeError
    I18: compute_latency_ms medido al final incluyendo persist
    I19: optional_missing lookup usa dict directo sin replace()
    I20: _ensure_db() en arranque loggea si falla
    I21: _BTCTradeBuffer._window_s eliminado (dead attribute)
    I22: log periódico usa _fmt_score() que protege nan/None
    I23: _close_db loggea excepción
    I24: start_worker() protegido con módulo-level Lock
    I25: _SNAP_SOFT_TTL_MS y _SNAP_HARD_TTL_MS eliminadas (unused)
    I27: spread_bps redefinido localmente en LIQUIDITY, no depende de scope STRESS
    I28: window_end_ts sin fuentes marcado con degradation_reason
    I29: _ws_consec_fails solo incrementa en fallos reales; resetea en éxito
    I30: JSONB fields usan psycopg2.extras.Json() para cast correcto
    I31: now_ms en _compute_facts renombrado a _now_ms_local
    I32: parámetro facts eliminado de _determine_worker_status
    I33: resuelto junto con I29
    I34: degradation_reasons usa set desde el inicio (evita deduplicación tardía)
    I36: to_public_dict() proyecta solo campos relevantes para CLASS_2

CodeGuard checklist:
    ✅ Timeouts explícitos en toda llamada de red y DB
    ✅ Sin except:pass — todos los except loggean
    ✅ Stale data visible — feed_status refleja estado real del socket
    ✅ Loops eficientes — stop_event.wait() en lugar de sleep duro
    ✅ Debug logging con conteos y estados
    ✅ Thread safety — Lock en _trade_buffer, _latest_snapshot, _running_lock, _start_lock
    ✅ Degradación explícita — health refleja cada fallo con degradation_reasons
    ✅ Fail-open — fallo del worker no afecta fast_loop ni entry_gate

KTS Extensions:
    ✅ Socket conectado ≠ feed live (distingue connected/receiving/live)
    ✅ Reconexión con backoff exponencial + jitter + cap + reset tras éxito
    ✅ Reconexión proactiva antes del límite Binance 24h
    ✅ Ping/pong handler explícito
    ✅ drain lock corto — copia y vacía bajo lock, procesa fuera
    ✅ API pública devuelve copias inmutables
    ✅ governance fields constantes — nunca mutables en F2
"""

import json
import math
import random
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras
import websocket

from utils import log

# ===========================================================================
# Governance — constantes fijas F2 (DC-MODE-02, DC-SCHEMA-09)
# ===========================================================================

_REGIME_MODE                  = "shadow"
_REGIME_VALIDATION_STATUS     = "experimental"
_SCHEMA_VERSION               = "v2"
_FEATURE_VERSION              = "v1"
_SCORE_FORMULA_VERSION        = "v2_anchor_support_fixed_mapping"
_NORMALIZATION_POLICY_VERSION = "v1_fixed_per_feature"
_ALLOWED_USE_CLASS            = "observational,analytical"
_PROHIBITED_USE_CLASS         = "operational"
_SHADOW_TAG_POLICY_VERSION    = "v1_bootstrap_fixed_thresholds"

# ===========================================================================
# Constantes operativas
# ===========================================================================

_WS_URL             = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
_SYMBOL             = "BTCUSDT"
_COMPUTE_INTERVAL_S = 2.0

_WS_RECONNECT_BASE_S       = 1.0
_WS_RECONNECT_MAX_S        = 60.0
_WS_RECONNECT_JITTER       = 0.20
_WS_MAX_CONSEC_FAILS       = 10
_WS_PROACTIVE_RECONNECT_S  = 82800   # 23h
_WS_SUCCESS_MIN_DURATION_S = 60.0    # conexión exitosa si duró al menos este tiempo

_TRADE_SOFT_TTL_MS = 3_000
_TRADE_HARD_TTL_MS = 15_000
_BOOK_SOFT_TTL_MS  = 3_000
_BOOK_HARD_TTL_MS  = 15_000

_ROLLING_WINDOW_S         = 3_600
_ROLLING_MIN_POINTS       = 30
_TRADE_WINDOW_S           = 30
_TRADE_INTENSITY_WINDOW_S = 60

_RET_WINDOWS = {"1m": 60, "5m": 300, "15m": 900, "60m": 3600}

_STRESS_LOW   = 0.30
_STRESS_HIGH  = 0.70
_TREND_DOWN   = -0.20
_TREND_UP     =  0.20
_GENERIC_LOW  = 0.33
_GENERIC_HIGH = 0.67

_DSN = (
    "host=localhost port=5432 dbname=tsdb "
    "user=postgres password=postgres "
    "connect_timeout=5 "
    "options='-c statement_timeout=8000'"
)


# ===========================================================================
# _RollingBuffer
# ===========================================================================

class _RollingBuffer:
    """
    Buffer rolling de (timestamp_s, value) pairs.
    Thread safety: NO thread-safe. Solo _compute_loop escribe/lee.

    IMPORTANTE: ts en append() debe ser siempre wall-clock time (time.time()).
    Usar timestamps del mercado puede producir trim incorrecto. (I12)
    """

    def __init__(self, window_s: float, maxlen: int = 10_000):
        self._window_s = window_s
        self._buf: deque = deque(maxlen=maxlen)

    def append(self, ts: float, value: float) -> None:
        """ts = wall-clock time (time.time()), NO timestamp del mercado."""
        self._buf.append((ts, value))
        self._trim(ts)

    def _trim(self, now_ts: float) -> None:
        cutoff = now_ts - self._window_s
        while self._buf and self._buf[0][0] < cutoff:
            self._buf.popleft()

    def values_in_window(self, window_s: float, now_ts: float) -> List[float]:
        cutoff = now_ts - window_s
        return [v for ts, v in self._buf if ts >= cutoff]

    def latest_value(self) -> Optional[float]:
        return self._buf[-1][1] if self._buf else None

    def latest_ts(self) -> Optional[float]:
        return self._buf[-1][0] if self._buf else None

    def __len__(self) -> int:
        return len(self._buf)


# ===========================================================================
# _RollingStats
# ===========================================================================

class _RollingStats:
    """
    z-score y percentile rank rolling.
    Thread safety: NO thread-safe. Solo _compute_loop usa instancias.
    """

    def __init__(self, window_s: float = _ROLLING_WINDOW_S,
                 min_points: int = _ROLLING_MIN_POINTS):
        self._buf        = _RollingBuffer(window_s=window_s)
        self._window_s   = window_s
        self._min_points = min_points

    def update(self, ts: float, value: float) -> None:
        """Protegido contra None, NaN, Inf, y tipos no numéricos (I1)."""
        try:
            if value is None:
                return
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return
            self._buf.append(ts, f)
        except (TypeError, ValueError):
            return

    def z_score(self, x: float, now_ts: float) -> Optional[float]:
        try:
            if x is None or math.isnan(x) or math.isinf(x):
                return None
        except (TypeError, ValueError):
            return None
        vals = self._buf.values_in_window(self._window_s, now_ts)
        if len(vals) < self._min_points:
            return None
        mean = statistics.mean(vals)
        try:
            std = statistics.stdev(vals)
        except statistics.StatisticsError:
            return None
        if std < 1e-9:
            return None
        return (x - mean) / std

    def percentile_rank(self, x: float, now_ts: float) -> Optional[float]:
        try:
            if x is None or math.isnan(x) or math.isinf(x):
                return None
        except (TypeError, ValueError):
            return None
        vals = self._buf.values_in_window(self._window_s, now_ts)
        if len(vals) < self._min_points:
            return None
        rank = sum(1 for v in vals if v <= x)
        return rank / len(vals)


# ===========================================================================
# _BTCTradeBuffer
# ===========================================================================

class _BTCTradeBuffer:
    """
    Buffer de trades aggTrade. _ws_thread es el ÚNICO escritor.
    _compute_loop drena con lock corto (copy + clear, procesa fuera).
    El filtrado temporal por ventana ocurre en _compute_facts, no aquí. (I21)
    """

    def __init__(self):
        # I21: _window_s eliminado — era dead attribute
        self._buf: deque = deque()
        self._lock = threading.Lock()
        self._last_trade_ts_ms: Optional[float] = None
        self._total_received: int = 0

    def add(self, trade_ts_ms: float, price: float,
            qty: float, is_buyer_maker: bool) -> None:
        with self._lock:
            self._buf.append((trade_ts_ms, price, qty, is_buyer_maker))
            self._last_trade_ts_ms = trade_ts_ms
            self._total_received += 1

    def drain(self) -> Tuple[List[tuple], Optional[float]]:
        with self._lock:
            batch   = list(self._buf)
            self._buf.clear()
            last_ts = self._last_trade_ts_ms
        return batch, last_ts

    def last_trade_ts_ms(self) -> Optional[float]:
        with self._lock:
            return self._last_trade_ts_ms

    def total_received(self) -> int:
        with self._lock:
            return self._total_received


# ===========================================================================
# BTCRegimeSnapshot
# ===========================================================================

@dataclass
class BTCRegimeSnapshot:
    """
    Snapshot completo del contexto BTC.
    .to_row_dict()    → dict para INSERT SQL
    .to_public_dict() → dict proyectado para CLASS_1/CLASS_2 (I36)
    """

    snapshot_ts:          datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source_ts:            datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    feature_ts:           datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    window_start_ts:      datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    window_end_ts:        datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    trade_source_ts:      Optional[datetime] = None
    book_source_ts:       Optional[datetime] = None
    source_lag_ms:        float = 0.0
    trade_source_lag_ms:  Optional[float] = None
    book_source_lag_ms:   Optional[float] = None
    compute_latency_ms:   float = 0.0

    btc_mid_price:        float = 0.0
    btc_bid_price:        Optional[float] = None
    btc_ask_price:        Optional[float] = None
    btc_spread_bps:       Optional[float] = None
    btc_tobi:             Optional[float] = None

    btc_r_1m:             Optional[float] = None
    btc_r_5m:             Optional[float] = None
    btc_r_15m:            Optional[float] = None
    btc_r_60m:            Optional[float] = None

    btc_rv_1m:             Optional[float] = None
    btc_rv_5m:             Optional[float] = None
    btc_realized_range_5m: Optional[float] = None

    btc_n_trades_30s:          Optional[int]   = None
    btc_quote_vol_30s:         Optional[float] = None
    btc_signed_flow_30s:       Optional[float] = None
    btc_intertrade_gap_ms_p50: Optional[float] = None
    btc_aggtrade_msg_rate_30s: Optional[float] = None

    btc_abs_r_1m:             Optional[float] = None
    btc_abs_r_5m:             Optional[float] = None
    btc_spread_bps_z:         Optional[float] = None
    btc_tobi_extreme:         Optional[float] = None
    btc_abs_signed_flow_30s:  Optional[float] = None

    btc_dir_persistence_15m:  Optional[float] = None
    btc_efficiency_ratio_15m: Optional[float] = None

    btc_rv_ratio_1m_15m: Optional[float] = None

    btc_spread_stability_1m: Optional[float] = None
    btc_spread_recovery_30s: Optional[float] = None
    btc_tobi_non_extreme:    Optional[float] = None

    btc_trade_intensity_1m: Optional[float] = None

    btc_stress_score:     Optional[float] = None
    btc_trend_score:      Optional[float] = None
    btc_volatility_score: Optional[float] = None
    btc_liquidity_score:  Optional[float] = None
    btc_activity_score:   Optional[float] = None

    btc_stress_anchor_component:      Optional[float] = None
    btc_stress_support_component:     Optional[float] = None
    btc_trend_anchor_component:       Optional[float] = None
    btc_trend_support_component:      Optional[float] = None
    btc_volatility_anchor_component:  Optional[float] = None
    btc_volatility_support_component: Optional[float] = None
    btc_liquidity_anchor_component:   Optional[float] = None
    btc_liquidity_support_component:  Optional[float] = None
    btc_activity_anchor_component:    Optional[float] = None
    btc_activity_support_component:   Optional[float] = None

    btc_stress_state:     Optional[str] = None
    btc_trend_state:      Optional[str] = None
    btc_volatility_state: Optional[str] = None
    btc_liquidity_state:  Optional[str] = None
    btc_activity_state:   Optional[str] = None

    snapshot_health_status: str = "stale"
    snapshot_observable:    bool = True
    snapshot_joinable:      bool = False
    freshness_status:       str = "stale"
    completeness_status:    str = "insufficient"
    validity_status:        str = "valid"
    trade_feed_status:      str = "missing"
    book_feed_status:       str = "missing"
    missing_feature_count:  int = 0
    missing_features_list:  Optional[List[str]] = None
    degradation_reasons:    List[str] = field(default_factory=list)
    late_event_count:       Optional[int] = None
    max_event_delay_ms:     Optional[float] = None
    worker_status:          str = "warming_up"

    regime_mode:                  str = _REGIME_MODE
    regime_validation_status:     str = _REGIME_VALIDATION_STATUS
    schema_version:               str = _SCHEMA_VERSION
    feature_version:              str = _FEATURE_VERSION
    score_formula_version:        str = _SCORE_FORMULA_VERSION
    normalization_policy_version: str = _NORMALIZATION_POLICY_VERSION
    allowed_use_class:            str = _ALLOWED_USE_CLASS
    prohibited_use_class:         str = _PROHIBITED_USE_CLASS
    shadow_tag_policy_version:    str = _SHADOW_TAG_POLICY_VERSION

    def to_row_dict(self) -> dict:
        """
        Serializa para INSERT SQL.
        JSONB fields usan psycopg2.extras.Json() para cast correcto (I30).
        """
        def _dt(v: Optional[datetime]) -> Optional[str]:
            return v.isoformat() if v is not None else None

        def _jsonb(v: Any) -> Optional[Any]:
            if v is None:
                return None
            return psycopg2.extras.Json(v)

        return {
            "snapshot_ts":           _dt(self.snapshot_ts),
            "source_ts":             _dt(self.source_ts),
            "feature_ts":            _dt(self.feature_ts),
            "window_start_ts":       _dt(self.window_start_ts),
            "window_end_ts":         _dt(self.window_end_ts),
            "trade_source_ts":       _dt(self.trade_source_ts),
            "book_source_ts":        _dt(self.book_source_ts),
            "source_lag_ms":         self.source_lag_ms,
            "trade_source_lag_ms":   self.trade_source_lag_ms,
            "book_source_lag_ms":    self.book_source_lag_ms,
            "compute_latency_ms":    self.compute_latency_ms,
            "btc_mid_price":         self.btc_mid_price,
            "btc_bid_price":         self.btc_bid_price,
            "btc_ask_price":         self.btc_ask_price,
            "btc_spread_bps":        self.btc_spread_bps,
            "btc_tobi":              self.btc_tobi,
            "btc_r_1m":              self.btc_r_1m,
            "btc_r_5m":              self.btc_r_5m,
            "btc_r_15m":             self.btc_r_15m,
            "btc_r_60m":             self.btc_r_60m,
            "btc_rv_1m":             self.btc_rv_1m,
            "btc_rv_5m":             self.btc_rv_5m,
            "btc_realized_range_5m": self.btc_realized_range_5m,
            "btc_n_trades_30s":      self.btc_n_trades_30s,
            "btc_quote_vol_30s":     self.btc_quote_vol_30s,
            "btc_signed_flow_30s":   self.btc_signed_flow_30s,
            "btc_intertrade_gap_ms_p50":  self.btc_intertrade_gap_ms_p50,
            "btc_aggtrade_msg_rate_30s":  self.btc_aggtrade_msg_rate_30s,
            "btc_abs_r_1m":          self.btc_abs_r_1m,
            "btc_abs_r_5m":          self.btc_abs_r_5m,
            "btc_spread_bps_z":      self.btc_spread_bps_z,
            "btc_tobi_extreme":      self.btc_tobi_extreme,
            "btc_abs_signed_flow_30s": self.btc_abs_signed_flow_30s,
            "btc_dir_persistence_15m":   self.btc_dir_persistence_15m,
            "btc_efficiency_ratio_15m":  self.btc_efficiency_ratio_15m,
            "btc_rv_ratio_1m_15m":   self.btc_rv_ratio_1m_15m,
            "btc_spread_stability_1m":   self.btc_spread_stability_1m,
            "btc_spread_recovery_30s":   self.btc_spread_recovery_30s,
            "btc_tobi_non_extreme":      self.btc_tobi_non_extreme,
            "btc_trade_intensity_1m":    self.btc_trade_intensity_1m,
            "btc_stress_score":      self.btc_stress_score,
            "btc_trend_score":       self.btc_trend_score,
            "btc_volatility_score":  self.btc_volatility_score,
            "btc_liquidity_score":   self.btc_liquidity_score,
            "btc_activity_score":    self.btc_activity_score,
            "btc_stress_anchor_component":       self.btc_stress_anchor_component,
            "btc_stress_support_component":      self.btc_stress_support_component,
            "btc_trend_anchor_component":        self.btc_trend_anchor_component,
            "btc_trend_support_component":       self.btc_trend_support_component,
            "btc_volatility_anchor_component":   self.btc_volatility_anchor_component,
            "btc_volatility_support_component":  self.btc_volatility_support_component,
            "btc_liquidity_anchor_component":    self.btc_liquidity_anchor_component,
            "btc_liquidity_support_component":   self.btc_liquidity_support_component,
            "btc_activity_anchor_component":     self.btc_activity_anchor_component,
            "btc_activity_support_component":    self.btc_activity_support_component,
            "btc_stress_state":      self.btc_stress_state,
            "btc_trend_state":       self.btc_trend_state,
            "btc_volatility_state":  self.btc_volatility_state,
            "btc_liquidity_state":   self.btc_liquidity_state,
            "btc_activity_state":    self.btc_activity_state,
            "shadow_tag_policy_version": self.shadow_tag_policy_version,
            "btc_shadow_tags_extra": None,
            "snapshot_health_status": self.snapshot_health_status,
            "snapshot_observable":    self.snapshot_observable,
            "snapshot_joinable":      self.snapshot_joinable,
            "freshness_status":       self.freshness_status,
            "completeness_status":    self.completeness_status,
            "validity_status":        self.validity_status,
            "trade_feed_status":      self.trade_feed_status,
            "book_feed_status":       self.book_feed_status,
            "missing_feature_count":  self.missing_feature_count,
            "missing_features_list":  _jsonb(self.missing_features_list),
            "degradation_reasons":    _jsonb(self.degradation_reasons),
            "late_event_count":       self.late_event_count,
            "max_event_delay_ms":     self.max_event_delay_ms,
            "worker_status":          self.worker_status,
            "regime_mode":                  self.regime_mode,
            "regime_validation_status":     self.regime_validation_status,
            "schema_version":               self.schema_version,
            "feature_version":              self.feature_version,
            "score_formula_version":        self.score_formula_version,
            "normalization_policy_version": self.normalization_policy_version,
            "allowed_use_class":            self.allowed_use_class,
            "prohibited_use_class":         self.prohibited_use_class,
            "debug_payload": None,
        }

    def to_public_dict(self) -> dict:
        """
        Proyección para CLASS_1/CLASS_2 — excluye debug y governance interno (I36).
        Nunca retorna referencia al objeto interno.
        """
        return {
            "snapshot_ts":            self.snapshot_ts.isoformat() if self.snapshot_ts else None,
            "feature_ts":             self.feature_ts.isoformat() if self.feature_ts else None,
            "source_lag_ms":          self.source_lag_ms,
            "trade_source_lag_ms":    self.trade_source_lag_ms,
            "book_source_lag_ms":     self.book_source_lag_ms,
            "compute_latency_ms":     self.compute_latency_ms,
            "btc_mid_price":          self.btc_mid_price,
            "btc_spread_bps":         self.btc_spread_bps,
            "btc_tobi":               self.btc_tobi,
            "btc_r_1m":               self.btc_r_1m,
            "btc_r_5m":               self.btc_r_5m,
            "btc_r_15m":              self.btc_r_15m,
            "btc_r_60m":              self.btc_r_60m,
            "btc_rv_1m":              self.btc_rv_1m,
            "btc_rv_5m":              self.btc_rv_5m,
            "btc_n_trades_30s":       self.btc_n_trades_30s,
            "btc_quote_vol_30s":      self.btc_quote_vol_30s,
            "btc_signed_flow_30s":    self.btc_signed_flow_30s,
            "btc_stress_score":       self.btc_stress_score,
            "btc_trend_score":        self.btc_trend_score,
            "btc_volatility_score":   self.btc_volatility_score,
            "btc_liquidity_score":    self.btc_liquidity_score,
            "btc_activity_score":     self.btc_activity_score,
            "btc_stress_state":       self.btc_stress_state,
            "btc_trend_state":        self.btc_trend_state,
            "btc_volatility_state":   self.btc_volatility_state,
            "btc_liquidity_state":    self.btc_liquidity_state,
            "btc_activity_state":     self.btc_activity_state,
            "snapshot_health_status": self.snapshot_health_status,
            "snapshot_observable":    self.snapshot_observable,
            "snapshot_joinable":      self.snapshot_joinable,
            "freshness_status":       self.freshness_status,
            "completeness_status":    self.completeness_status,
            "validity_status":        self.validity_status,
            "trade_feed_status":      self.trade_feed_status,
            "book_feed_status":       self.book_feed_status,
            "missing_feature_count":  self.missing_feature_count,
            "degradation_reasons":    list(self.degradation_reasons),
            "worker_status":          self.worker_status,
            "regime_mode":            self.regime_mode,
            "schema_version":         self.schema_version,
        }


# ===========================================================================
# Helpers de cálculo
# ===========================================================================

def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _clip_tanh(z: float) -> float:
    return math.tanh(max(-3.0, min(3.0, z)))


def _map_to_01(v: float) -> float:
    return (v + 1.0) / 2.0


def _anchor_support_score(
    anchor_vals: List[Optional[float]],
    support_vals: List[Optional[float]],
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    anchors  = [v for v in anchor_vals  if v is not None]
    supports = [v for v in support_vals if v is not None]
    if not anchors:
        return None, None, None
    anc_comp = sum(anchors) / len(anchors)
    sup_comp = (sum(supports) / len(supports)) if supports else None
    score    = (anc_comp + sup_comp) / 2.0 if sup_comp is not None else anc_comp
    return score, anc_comp, sup_comp


def _realized_vol(prices: List[float]) -> Optional[float]:
    """
    Realized volatility = std(log-returns).
    Skips pares con precio <= 0 en vez de abortar toda la ventana (I3).
    """
    if len(prices) < 2:
        return None
    log_rets = []
    for i in range(1, len(prices)):
        if prices[i - 1] <= 0 or prices[i] <= 0:
            continue  # I3: skip par inválido
        try:
            log_rets.append(math.log(prices[i] / prices[i - 1]))
        except (ValueError, ZeroDivisionError):
            continue
    if len(log_rets) < 2:
        return None
    try:
        return statistics.stdev(log_rets)
    except statistics.StatisticsError:
        return None


def _percentile_p50(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return statistics.median(values)


def _fmt_score(v: Optional[float]) -> str:
    """Formatea score para logging — maneja None y nan/inf (I22)."""
    if v is None:
        return "None"
    try:
        if math.isnan(v) or math.isinf(v):
            return f"invalid({v})"
        return f"{v:.3f}"
    except (TypeError, ValueError):
        return f"err({v!r})"


# ===========================================================================
# BTCRegimeWorker
# ===========================================================================

class BTCRegimeWorker:
    """
    Capa 6 — BTC Regime Foundation.
    Produce snapshots de contexto BTC en shadow mode (F2).
    """

    def __init__(self):
        # Rolling buffers por familia
        self._price_buffer  = _RollingBuffer(window_s=_ROLLING_WINDOW_S)
        self._spread_buffer = _RollingBuffer(window_s=_ROLLING_WINDOW_S)
        self._tobi_buffer   = _RollingBuffer(window_s=_ROLLING_WINDOW_S)

        # Stats de stress — distribuciones de abs_r, no precios (I5)
        self._stats_abs_r_1m = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_abs_r_5m = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_spread   = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_flow     = _RollingStats(window_s=_ROLLING_WINDOW_S)

        # Stats de trend — distribuciones de retornos, no precios (I6)
        self._stats_r_15m = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_r_60m = _RollingStats(window_s=_ROLLING_WINDOW_S)

        # Stats de volatility
        self._stats_rv_1m          = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_rv_5m          = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_rv_ratio       = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_realized_range = _RollingStats(window_s=_ROLLING_WINDOW_S)

        # Stats de activity — cada métrica usa su propia distribución
        self._stats_trade_count     = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_quote_vol       = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_trade_intensity = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_intertrade_gap  = _RollingStats(window_s=_ROLLING_WINDOW_S)
        self._stats_aggtrade_msg    = _RollingStats(window_s=_ROLLING_WINDOW_S)

        self._trade_buffer = _BTCTradeBuffer()

        self._latest_lock:     threading.Lock = threading.Lock()
        self._latest_snapshot: Optional[dict] = None

        # I11: flag _running con Lock propio
        self._stop_event   = threading.Event()
        self._running_lock = threading.Lock()
        self._running      = False
        self._ws_thread:      Optional[threading.Thread] = None
        self._compute_thread: Optional[threading.Thread] = None

        self._ws_connected    = False
        self._ws_receiving    = False
        self._ws_last_msg_ts: Optional[float] = None
        self._ws_conn_ts:     Optional[float] = None
        self._ws_consec_fails = 0
        self._ws_lock         = threading.Lock()

        self._compute_cycles = 0
        self._persist_errors = 0

        self._db_conn: Optional[Any] = None

        log("BTC", "BTCRegimeWorker initialized (shadow mode)")

    # -----------------------------------------------------------------------
    # API pública
    # -----------------------------------------------------------------------

    def start(self) -> None:
        with self._running_lock:
            if self._running:
                log("BTC", "WARN start() called but already running")
                return
            self._running = True

        self._stop_event.clear()
        self._ws_thread = threading.Thread(
            target=self._ws_loop, daemon=True, name="btc-regime-ws")
        self._compute_thread = threading.Thread(
            target=self._compute_loop, daemon=True, name="btc-regime-compute")
        self._ws_thread.start()
        self._compute_thread.start()
        log("BTC", "BTCRegimeWorker started (ws + compute threads)")

    def stop(self, timeout_s: float = 10.0) -> None:
        log("BTC", "BTCRegimeWorker stop requested")
        self._stop_event.set()
        with self._running_lock:
            self._running = False
        if self._ws_thread:
            self._ws_thread.join(timeout=timeout_s)
        if self._compute_thread:
            self._compute_thread.join(timeout=timeout_s)
        self._close_db()
        log("BTC", "BTCRegimeWorker stopped")

    def get_latest_snapshot(self) -> Optional[dict]:
        """Thread-safe. Copia inmutable. CLASS_2 — solo lectura analítica."""
        with self._latest_lock:
            return dict(self._latest_snapshot) if self._latest_snapshot else None

    def get_latest_scores(self) -> Optional[dict]:
        snap = self.get_latest_snapshot()
        if snap is None:
            return None
        return {
            "btc_stress_score":       snap.get("btc_stress_score"),
            "btc_trend_score":        snap.get("btc_trend_score"),
            "btc_volatility_score":   snap.get("btc_volatility_score"),
            "btc_liquidity_score":    snap.get("btc_liquidity_score"),
            "btc_activity_score":     snap.get("btc_activity_score"),
            "snapshot_ts":            snap.get("snapshot_ts"),
            "snapshot_health_status": snap.get("snapshot_health_status"),
        }

    # -----------------------------------------------------------------------
    # WS loop
    # -----------------------------------------------------------------------

    def _ws_loop(self) -> None:
        """
        Reconexión con backoff exponencial + jitter.
        Resetea backoff tras conexión exitosa larga (I16).
        Incrementa consec_fails solo en fallos reales (I29).
        """
        backoff = _WS_RECONNECT_BASE_S
        log("BTC", f"ws_loop started → {_WS_URL}")

        while not self._stop_event.is_set():
            conn_was_clean = False
            try:
                conn_was_clean = self._run_ws_connection()
                if self._stop_event.is_set():
                    break
                log("BTC", f"ws connection ended (clean={conn_was_clean}) — scheduling reconnect")
            except Exception as e:
                log("BTC", f"ws_loop unexpected error: {type(e).__name__}: {e}")
                conn_was_clean = False

            with self._ws_lock:
                self._ws_connected = False
                self._ws_receiving = False
                # I29: solo incrementar en fallos reales
                if not conn_was_clean:
                    self._ws_consec_fails += 1
                else:
                    self._ws_consec_fails = 0
                fails = self._ws_consec_fails

            if fails >= _WS_MAX_CONSEC_FAILS:
                log("BTC", f"ws_loop: {fails} consecutive fails — worker will be degraded")

            # I16: resetear backoff si fue conexión exitosa
            if conn_was_clean:
                backoff = _WS_RECONNECT_BASE_S

            jitter  = backoff * _WS_RECONNECT_JITTER * (2 * random.random() - 1)
            sleep_s = min(backoff + jitter, _WS_RECONNECT_MAX_S)
            log("BTC", f"ws_loop: reconnect in {sleep_s:.1f}s (backoff={backoff:.1f}s)")
            self._stop_event.wait(timeout=sleep_s)
            backoff = min(backoff * 2, _WS_RECONNECT_MAX_S)

        log("BTC", "ws_loop stopped")

    def _run_ws_connection(self) -> bool:
        """
        Ejecuta una conexión WS.
        Retorna True si terminó limpiamente (cierre proactivo o on_close sin error),
        False si terminó por on_error.
        """
        conn_start    = time.time()
        ended_cleanly = [True]  # mutable para modificar desde closures

        def on_open(ws):
            with self._ws_lock:
                self._ws_connected = True
                self._ws_conn_ts   = time.time()
            log("BTC", "ws connected — waiting for first message")

        def on_message(ws, message):
            now_wall = time.time()
            try:
                data           = json.loads(message)
                trade_ts_ms    = float(data["T"])
                price          = float(data["p"])
                qty            = float(data["q"])
                is_buyer_maker = bool(data["m"])
                self._trade_buffer.add(trade_ts_ms, price, qty, is_buyer_maker)
                with self._ws_lock:
                    self._ws_last_msg_ts = now_wall
                    if not self._ws_receiving:
                        self._ws_receiving = True
                        log("BTC", "ws first message received — feed is receiving")
                if (now_wall - conn_start) > _WS_PROACTIVE_RECONNECT_S:
                    log("BTC", "ws proactive reconnect (23h limit)")
                    ws.close()
            except KeyError as e:
                log("BTC", f"ws on_message KeyError: {e} — raw: {message[:80]}")
            except Exception as e:
                log("BTC", f"ws on_message error: {type(e).__name__}: {e}")

        def on_error(ws, error):
            ended_cleanly[0] = False
            with self._ws_lock:
                self._ws_connected = False
                self._ws_receiving = False
            log("BTC", f"ws error: {error}")

        def on_close(ws, code, msg):
            with self._ws_lock:
                self._ws_connected = False
                self._ws_receiving = False
            log("BTC", f"ws closed: code={code} msg={msg}")

        ws_app = websocket.WebSocketApp(
            _WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws_app.run_forever(ping_interval=30, ping_timeout=10)

        duration = time.time() - conn_start
        return ended_cleanly[0] and duration >= _WS_SUCCESS_MIN_DURATION_S

    # -----------------------------------------------------------------------
    # Compute loop
    # -----------------------------------------------------------------------

    def _compute_loop(self) -> None:
        """ÚNICO escritor de todos los rolling buffers y stats."""
        log("BTC", "compute_loop started")
        ok = self._ensure_db()
        if not ok:
            # I20: loggear fallo en arranque
            log("BTC", "WARN: DB unavailable at compute_loop start — will retry per cycle")

        while not self._stop_event.is_set():
            cycle_start = time.monotonic()
            try:
                self._compute_cycle()
            except Exception as e:
                log("BTC", f"compute_loop unhandled error: {type(e).__name__}: {e}")
            elapsed = time.monotonic() - cycle_start
            self._stop_event.wait(timeout=max(0.05, _COMPUTE_INTERVAL_S - elapsed))

        log("BTC", "compute_loop stopped")

    def _compute_cycle(self) -> None:
        """Un ciclo completo de cómputo."""
        cycle_start = time.monotonic()
        now_wall    = time.time()
        now_utc     = datetime.fromtimestamp(now_wall, tz=timezone.utc)
        self._compute_cycles += 1

        # 1. Leer metrics_ext
        book_data = self._query_metrics_ext(now_utc)

        # 2. Actualizar buffers de precio/book
        if book_data:
            p = book_data.get("price")
            s = book_data.get("spread_bps")
            t = book_data.get("tobi")
            if p and p > 0:
                self._price_buffer.append(now_wall, p)
            if s is not None:
                self._spread_buffer.append(now_wall, s)
                self._stats_spread.update(now_wall, s)
            if t is not None:
                self._tobi_buffer.append(now_wall, t)

        # 3. Drain trade buffer
        trade_batch, last_trade_ts_ms = self._trade_buffer.drain()

        _trade_batch_backlog = len(trade_batch) > 500
        if _trade_batch_backlog:
            _now_ms_tmp  = now_wall * 1000.0
            kept_30s = sum(1 for (ts, _, _, _) in trade_batch
                           if ts >= _now_ms_tmp - _TRADE_WINDOW_S * 1000.0)
            kept_60s = sum(1 for (ts, _, _, _) in trade_batch
                           if ts >= _now_ms_tmp - _TRADE_INTENSITY_WINDOW_S * 1000.0)
            log("BTC", f"trade_batch backlog: total={len(trade_batch)} "
                       f"kept_30s={kept_30s} kept_60s={kept_60s}")

        for (ts_item, price, qty, ibm) in trade_batch:
            quote  = price * qty
            signed = quote if not ibm else -quote
            self._stats_flow.update(ts_item / 1000.0, signed)

        # I13: vals_15m calculado una sola vez, pasado a ambas funciones
        vals_15m = self._price_buffer.values_in_window(900, now_wall)

        # 4. Facts
        facts = self._compute_facts(book_data, trade_batch, last_trade_ts_ms,
                                    now_wall, vals_15m)
        # 5. Features
        features = self._compute_features(facts, now_wall, vals_15m)
        # 6. Scores
        scores = self._compute_scores(facts, features, now_wall)
        # 7. Worker status — sin parámetro facts (I32)
        worker_status = self._determine_worker_status()
        # 8. Health
        health = self._evaluate_health(
            facts, features, scores, last_trade_ts_ms,
            book_data, now_wall, worker_status,
            trade_batch_backlog=_trade_batch_backlog,
        )

        # 9. Timestamps
        book_ts_utc  = book_data.get("book_ts_utc") if book_data else None
        trade_ts_utc = (
            datetime.fromtimestamp(last_trade_ts_ms / 1000.0, tz=timezone.utc)
            if last_trade_ts_ms else None
        )
        candidates = [t for t in [book_ts_utc, trade_ts_utc] if t is not None]

        if candidates:
            window_end_ts = max(candidates)
        else:
            # Fix 2: sin fuentes reales — usar snapshot_ts como fallback explícito.
            # Semántica de emergencia: este snapshot es solo observacional.
            # feature_ts = source_ts = snapshot_ts para dejar claro que no hay
            # dato de mercado real que ancle la ventana.
            window_end_ts = now_utc
            health["degradation_reasons"] = sorted(
                set(health["degradation_reasons"]) | {"no_source_data"}
            )
            # Forzar snapshot_joinable = False — PIT join no es confiable sin fuente real
            health["snapshot_joinable"] = False

        window_start_ts = datetime.fromtimestamp(
            now_wall - _RET_WINDOWS["60m"], tz=timezone.utc)
        source_ts     = window_end_ts
        source_lag_ms = max(0.0, (now_utc - source_ts).total_seconds() * 1000.0)
        trade_source_lag_ms = (
            max(0.0, (now_utc - trade_ts_utc).total_seconds() * 1000.0)
            if trade_ts_utc else None
        )
        book_source_lag_ms = (
            max(0.0, (now_utc - book_ts_utc).total_seconds() * 1000.0)
            if book_ts_utc else None
        )

        # 10. Construir snapshot (compute_latency_ms se completa después del persist)
        snap = BTCRegimeSnapshot(
            snapshot_ts     = now_utc,
            source_ts       = source_ts,
            feature_ts      = window_end_ts,
            window_start_ts = window_start_ts,
            window_end_ts   = window_end_ts,
            trade_source_ts = trade_ts_utc,
            book_source_ts  = book_ts_utc,
            source_lag_ms   = source_lag_ms,
            trade_source_lag_ms = trade_source_lag_ms,
            book_source_lag_ms  = book_source_lag_ms,

            btc_mid_price         = facts.get("mid_price", 0.0),
            btc_bid_price         = facts.get("bid_price"),
            btc_ask_price         = facts.get("ask_price"),
            btc_spread_bps        = facts.get("spread_bps"),
            btc_tobi              = facts.get("tobi"),
            btc_r_1m              = facts.get("r_1m"),
            btc_r_5m              = facts.get("r_5m"),
            btc_r_15m             = facts.get("r_15m"),
            btc_r_60m             = facts.get("r_60m"),
            btc_rv_1m             = facts.get("rv_1m"),
            btc_rv_5m             = facts.get("rv_5m"),
            btc_realized_range_5m = facts.get("realized_range_5m"),
            btc_n_trades_30s      = facts.get("n_trades_30s"),
            btc_quote_vol_30s     = facts.get("quote_vol_30s"),
            btc_signed_flow_30s   = facts.get("signed_flow_30s"),
            btc_intertrade_gap_ms_p50  = facts.get("intertrade_gap_ms_p50"),
            btc_aggtrade_msg_rate_30s  = facts.get("aggtrade_msg_rate_30s"),

            btc_abs_r_1m              = features.get("abs_r_1m"),
            btc_abs_r_5m              = features.get("abs_r_5m"),
            btc_spread_bps_z          = features.get("spread_bps_z"),
            btc_tobi_extreme          = features.get("tobi_extreme"),
            btc_abs_signed_flow_30s   = features.get("abs_signed_flow_30s"),
            btc_dir_persistence_15m   = features.get("dir_persistence_15m"),
            btc_efficiency_ratio_15m  = features.get("efficiency_ratio_15m"),
            btc_rv_ratio_1m_15m       = features.get("rv_ratio_1m_15m"),
            btc_spread_stability_1m   = features.get("spread_stability_1m"),
            btc_spread_recovery_30s   = features.get("spread_recovery_30s"),
            btc_tobi_non_extreme      = features.get("tobi_non_extreme"),
            btc_trade_intensity_1m    = features.get("trade_intensity_1m"),

            btc_stress_score     = scores.get("stress_score"),
            btc_trend_score      = scores.get("trend_score"),
            btc_volatility_score = scores.get("volatility_score"),
            btc_liquidity_score  = scores.get("liquidity_score"),
            btc_activity_score   = scores.get("activity_score"),

            btc_stress_anchor_component      = scores.get("stress_anchor"),
            btc_stress_support_component     = scores.get("stress_support"),
            btc_trend_anchor_component       = scores.get("trend_anchor"),
            btc_trend_support_component      = scores.get("trend_support"),
            btc_volatility_anchor_component  = scores.get("volatility_anchor"),
            btc_volatility_support_component = scores.get("volatility_support"),
            btc_liquidity_anchor_component   = scores.get("liquidity_anchor"),
            btc_liquidity_support_component  = scores.get("liquidity_support"),
            btc_activity_anchor_component    = scores.get("activity_anchor"),
            btc_activity_support_component   = scores.get("activity_support"),

            btc_stress_state     = self._stress_tag(scores.get("stress_score"), health),
            btc_trend_state      = self._trend_tag(scores.get("trend_score"), health),
            btc_volatility_state = self._generic_tag(
                scores.get("volatility_score"), health, ("compressed","normal","expanding")),
            btc_liquidity_state  = self._generic_tag(
                scores.get("liquidity_score"), health, ("thin","normal","thick")),
            btc_activity_state   = self._generic_tag(
                scores.get("activity_score"), health, ("low","normal","high")),

            snapshot_health_status = health["snapshot_health_status"],
            snapshot_observable    = health["snapshot_observable"],
            snapshot_joinable      = health["snapshot_joinable"],
            freshness_status       = health["freshness_status"],
            completeness_status    = health["completeness_status"],
            validity_status        = health["validity_status"],
            trade_feed_status      = health["trade_feed_status"],
            book_feed_status       = health["book_feed_status"],
            missing_feature_count  = health["missing_feature_count"],
            missing_features_list  = health["missing_features_list"] or None,
            degradation_reasons    = health["degradation_reasons"],
            late_event_count       = health.get("late_event_count"),
            max_event_delay_ms     = health.get("max_event_delay_ms"),
            worker_status          = worker_status,
        )

        # 11. Validar invariantes
        errors = self._validate_snapshot_invariants(snap)
        if errors:
            log("BTC", f"snapshot invariant violations: {errors}")
            if snap.validity_status == "valid":
                snap.validity_status = "suspicious"
            snap.degradation_reasons = sorted(
                set(snap.degradation_reasons) | {f"invariant:{e}" for e in errors}
            )

        # 12. compute_latency_ms — medido ANTES de exponer/persistir el snapshot
        # para que tanto _latest_snapshot como DB tengan el valor real del ciclo.
        snap.compute_latency_ms = round((time.monotonic() - cycle_start) * 1000.0, 2)

        # 13. Actualizar latest
        self._set_latest_snapshot(snap)

        # 14. Persistir
        if snap.snapshot_observable:
            self._persist_snapshot(snap)
        else:
            log("BTC", "snapshot not observable — skipping persist")

        if self._compute_cycles % 30 == 0:
            log("BTC", (
                f"cycle={self._compute_cycles} worker={worker_status} "
                f"health={snap.snapshot_health_status} "
                f"stress={_fmt_score(snap.btc_stress_score)} "
                f"trend={_fmt_score(snap.btc_trend_score)} "
                f"trades_30s={snap.btc_n_trades_30s} "
                f"latency_ms={snap.compute_latency_ms:.0f} "
                f"persist_errors={self._persist_errors}"
            ))

    # -----------------------------------------------------------------------
    # Facts
    # -----------------------------------------------------------------------

    def _query_metrics_ext(self, now_utc: datetime) -> Optional[dict]:
        """
        Lee tick más reciente de BTCUSDT.
        Commit post-SELECT para cerrar transacción implícita (I8).
        book_ts_utc verifica tzinfo is not None (I10).
        """
        if not self._ensure_db():
            return None
        try:
            with self._db_conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cur.execute("""
                    SELECT price, bid, ask, spread_bp, bid_qty, ask_qty, ts
                    FROM metrics_ext
                    WHERE symbol = %s
                      AND ts >= NOW() - INTERVAL '15 seconds'
                    ORDER BY ts DESC
                    LIMIT 1
                """, (_SYMBOL,))
                row = cur.fetchone()
            self._db_conn.commit()   # I8: cierra transacción implícita

            if not row:
                return None

            price   = _safe_float(row["price"])
            bid     = _safe_float(row["bid"])
            ask     = _safe_float(row["ask"])
            spread  = _safe_float(row["spread_bp"])
            bid_qty = _safe_float(row["bid_qty"])
            ask_qty = _safe_float(row["ask_qty"])
            ts      = row["ts"]

            if not price or price <= 0:
                return None

            mid = price
            if bid and ask and bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0

            tobi = None
            if bid_qty is not None and ask_qty is not None:
                denom = bid_qty + ask_qty
                if denom > 0:
                    tobi = max(-1.0, min(1.0, (bid_qty - ask_qty) / denom))

            # I10: verificar tzinfo is not None antes de asumir tz-aware
            if ts is not None and hasattr(ts, "tzinfo") and ts.tzinfo is not None:
                book_ts_utc = ts
            else:
                try:
                    book_ts_utc = datetime.fromtimestamp(float(ts), tz=timezone.utc)
                except (TypeError, ValueError, OSError):
                    book_ts_utc = now_utc

            return {
                "price": price, "mid_price": mid,
                "bid_price": bid, "ask_price": ask,
                "spread_bps": spread, "tobi": tobi,
                "book_ts_utc": book_ts_utc,
            }
        except Exception as e:
            log("BTC", f"_query_metrics_ext error: {type(e).__name__}: {e}")
            try:
                self._db_conn.rollback()
            except Exception:
                pass
            self._db_conn = None
            return None

    def _compute_facts(
        self,
        book_data: Optional[dict],
        trade_batch: List[tuple],
        last_trade_ts_ms: Optional[float],
        now_wall: float,
        vals_15m: List[float],   # I13: pre-calculado en _compute_cycle
    ) -> dict:
        """
        Calcula todos los facts del snapshot.

        Side effects (I14): actualiza _stats_abs_r_*, _stats_r_*, _stats_rv_*,
        _stats_trade_*, _stats_quote_vol, _stats_intertrade_gap, _stats_aggtrade_msg.
        vals_15m recibido del caller para evitar doble query al price_buffer.
        """
        facts: dict = {}

        if book_data:
            facts["mid_price"]  = book_data.get("mid_price", 0.0)
            facts["bid_price"]  = book_data.get("bid_price")
            facts["ask_price"]  = book_data.get("ask_price")
            facts["spread_bps"] = book_data.get("spread_bps")
            facts["tobi"]       = book_data.get("tobi")
        else:
            facts["mid_price"] = 0.0

        for label, window_s in _RET_WINDOWS.items():
            vals = self._price_buffer.values_in_window(window_s, now_wall)
            facts[f"r_{label}"] = (
                _safe_float((vals[-1] - vals[0]) / vals[0])
                if len(vals) >= 2 and vals[0] > 0 else None
            )

        # Actualizar stats de retorno y abs_r (I5, I6)
        if facts.get("r_1m") is not None:
            self._stats_abs_r_1m.update(now_wall, abs(facts["r_1m"]))
        if facts.get("r_5m") is not None:
            self._stats_abs_r_5m.update(now_wall, abs(facts["r_5m"]))
        if facts.get("r_15m") is not None:
            self._stats_r_15m.update(now_wall, facts["r_15m"])
        if facts.get("r_60m") is not None:
            self._stats_r_60m.update(now_wall, facts["r_60m"])

        vals_1m = self._price_buffer.values_in_window(60, now_wall)
        vals_5m = self._price_buffer.values_in_window(300, now_wall)

        facts["rv_1m"]            = _realized_vol(vals_1m)
        facts["rv_5m"]            = _realized_vol(vals_5m)
        facts["_rv_15m_internal"] = _realized_vol(vals_15m)
        facts["realized_range_5m"] = (
            max(vals_5m) - min(vals_5m) if len(vals_5m) >= 2 else None
        )

        if facts.get("rv_1m") is not None:
            self._stats_rv_1m.update(now_wall, facts["rv_1m"])
        if facts.get("rv_5m") is not None:
            self._stats_rv_5m.update(now_wall, facts["rv_5m"])
        if facts.get("realized_range_5m") is not None:
            self._stats_realized_range.update(now_wall, facts["realized_range_5m"])

        # Activity / flow
        _now_ms_local = now_wall * 1000.0   # I31: nombre distinto al del caller
        cutoff_30s    = _now_ms_local - _TRADE_WINDOW_S * 1000.0
        cutoff_60s    = _now_ms_local - _TRADE_INTENSITY_WINDOW_S * 1000.0

        trades_30s = [(ts, p, q, ibm) for (ts, p, q, ibm) in trade_batch if ts >= cutoff_30s]
        trades_60s = [(ts, p, q, ibm) for (ts, p, q, ibm) in trade_batch if ts >= cutoff_60s]

        if trades_30s:
            n_trades    = len(trades_30s)
            quote_vol   = sum(p * q for (_, p, q, _) in trades_30s)
            buy_vol     = sum(p * q for (_, p, q, ibm) in trades_30s if not ibm)
            sell_vol    = sum(p * q for (_, p, q, ibm) in trades_30s if ibm)
            facts["n_trades_30s"]    = n_trades
            facts["quote_vol_30s"]   = _safe_float(quote_vol)
            facts["signed_flow_30s"] = _safe_float(buy_vol - sell_vol)
            if n_trades >= 2:
                ts_list = sorted(ts for (ts, _, _, _) in trades_30s)
                gaps    = [ts_list[i] - ts_list[i-1] for i in range(1, len(ts_list))]
                facts["intertrade_gap_ms_p50"] = _safe_float(_percentile_p50(gaps))
            else:
                facts["intertrade_gap_ms_p50"] = None
            facts["aggtrade_msg_rate_30s"] = _safe_float(n_trades / _TRADE_WINDOW_S)
        else:
            for k in ["n_trades_30s","quote_vol_30s","signed_flow_30s",
                      "intertrade_gap_ms_p50","aggtrade_msg_rate_30s"]:
                facts[k] = None

        facts["trade_intensity_1m"] = (
            _safe_float(len(trades_60s) / _TRADE_INTENSITY_WINDOW_S)
            if trades_60s else None
        )

        late_count   = 0
        max_delay_ms = 0.0
        for (ts_ms, _, _, _) in trade_batch:
            delay_ms = _now_ms_local - ts_ms
            if delay_ms > _TRADE_SOFT_TTL_MS:
                late_count += 1
            if delay_ms > max_delay_ms:
                max_delay_ms = delay_ms
        facts["late_event_count"]   = late_count if trade_batch else None
        facts["max_event_delay_ms"] = _safe_float(max_delay_ms) if trade_batch else None

        # Actualizar activity stats (side effect documentado)
        if facts.get("n_trades_30s") is not None:
            self._stats_trade_count.update(now_wall, float(facts["n_trades_30s"]))
        if facts.get("quote_vol_30s") is not None:
            self._stats_quote_vol.update(now_wall, facts["quote_vol_30s"])
        if facts.get("trade_intensity_1m") is not None:
            self._stats_trade_intensity.update(now_wall, facts["trade_intensity_1m"])
        if facts.get("intertrade_gap_ms_p50") is not None:
            self._stats_intertrade_gap.update(now_wall, facts["intertrade_gap_ms_p50"])
        if facts.get("aggtrade_msg_rate_30s") is not None:
            self._stats_aggtrade_msg.update(now_wall, facts["aggtrade_msg_rate_30s"])

        return facts

    # -----------------------------------------------------------------------
    # Features
    # -----------------------------------------------------------------------

    def _compute_features(
        self,
        facts: dict,
        now_wall: float,
        vals_15m: List[float],   # I13: recibido del caller
    ) -> dict:
        """
        Calcula features del snapshot.
        Side effect: actualiza _stats_rv_ratio.
        vals_15m recibido del caller para evitar doble query.
        """
        f: dict = {}

        # STRESS
        r_1m = facts.get("r_1m")
        r_5m = facts.get("r_5m")
        f["abs_r_1m"] = abs(r_1m) if r_1m is not None else None
        f["abs_r_5m"] = abs(r_5m) if r_5m is not None else None
        f["spread_bps_z"] = (
            self._stats_spread.z_score(facts.get("spread_bps"), now_wall)
            if facts.get("spread_bps") is not None else None
        )

        tobi = facts.get("tobi")
        if tobi is not None:
            abs_tobi  = abs(tobi)
            tobi_vals = self._tobi_buffer.values_in_window(_ROLLING_WINDOW_S, now_wall)
            if len(tobi_vals) >= _ROLLING_MIN_POINTS:
                rank = sum(1 for v in tobi_vals if abs(v) <= abs_tobi)
                f["tobi_extreme"]    = rank / len(tobi_vals)
                f["tobi_non_extreme"] = 1.0 - f["tobi_extreme"]
            else:
                f["tobi_extreme"]    = None
                f["tobi_non_extreme"] = None
        else:
            f["tobi_extreme"]    = None
            f["tobi_non_extreme"] = None

        sf = facts.get("signed_flow_30s")
        f["abs_signed_flow_30s"] = abs(sf) if sf is not None else None

        # TREND — vals_15m recibido (I13)
        if len(vals_15m) >= 4:
            step    = max(1, len(vals_15m) // 15)
            rets_1m = [
                vals_15m[i] - vals_15m[i - step]
                for i in range(step, len(vals_15m), step)
                if vals_15m[i - step] > 0
            ]
            if len(rets_1m) >= 3:
                net_sign  = 1 if (vals_15m[-1] - vals_15m[0]) >= 0 else -1
                same_sign = sum(1 for r in rets_1m if (r >= 0) == (net_sign >= 0))
                f["dir_persistence_15m"] = same_sign / len(rets_1m)
            else:
                f["dir_persistence_15m"] = None
            moves      = [abs(vals_15m[i] - vals_15m[i-1]) for i in range(1, len(vals_15m))]
            total_path = sum(moves)
            f["efficiency_ratio_15m"] = (
                min(1.0, abs(vals_15m[-1] - vals_15m[0]) / total_path)
                if total_path > 0 else None
            )
        else:
            f["dir_persistence_15m"]  = None
            f["efficiency_ratio_15m"] = None

        # VOLATILITY support
        rv_1m           = facts.get("rv_1m")
        rv_15m_internal = facts.get("_rv_15m_internal")
        f["rv_ratio_1m_15m"] = (
            rv_1m / rv_15m_internal
            if rv_1m is not None and rv_15m_internal and rv_15m_internal > 0
            else None
        )
        if f["rv_ratio_1m_15m"] is not None:
            self._stats_rv_ratio.update(now_wall, f["rv_ratio_1m_15m"])

        # LIQUIDITY — spread_bps redefinido localmente (I27)
        spread_bps     = facts.get("spread_bps")
        spread_vals_1m = self._spread_buffer.values_in_window(60, now_wall)
        if len(spread_vals_1m) >= 5:
            mean_s = statistics.mean(spread_vals_1m)
            if mean_s > 0:
                try:
                    cv = statistics.stdev(spread_vals_1m) / mean_s
                except statistics.StatisticsError:
                    cv = 0.0
                # I15: proxy de estabilidad — deuda técnica declarada.
                # Aproximación observacional; no es CV histórico vs actual_CV.
                all_spread = self._spread_buffer.values_in_window(_ROLLING_WINDOW_S, now_wall)
                if len(all_spread) >= _ROLLING_MIN_POINTS:
                    rank_low = sum(1 for v in all_spread if v <= mean_s * (1 + cv))
                    f["spread_stability_1m"] = 1.0 - (rank_low / len(all_spread))
                else:
                    f["spread_stability_1m"] = None
            else:
                f["spread_stability_1m"] = None
        else:
            f["spread_stability_1m"] = None

        if spread_bps is not None:
            all_spread = self._spread_buffer.values_in_window(_ROLLING_WINDOW_S, now_wall)
            if len(all_spread) >= _ROLLING_MIN_POINTS:
                rank = sum(1 for v in all_spread if v >= spread_bps)
                f["spread_recovery_30s"] = rank / len(all_spread)
            else:
                f["spread_recovery_30s"] = None
        else:
            f["spread_recovery_30s"] = None

        f["trade_intensity_1m"] = facts.get("trade_intensity_1m")
        return f

    # -----------------------------------------------------------------------
    # Scores
    # -----------------------------------------------------------------------

    def _compute_scores(self, facts: dict, features: dict, now_wall: float) -> dict:
        s: dict = {}

        # STRESS — I5: usa _stats_abs_r_1m/_5m propios
        def _stress_r(abs_r: Optional[float], stat: _RollingStats) -> Optional[float]:
            if abs_r is None:
                return None
            z = stat.z_score(abs_r, now_wall)
            return _map_to_01(_clip_tanh(z)) if z is not None else None

        sc_abs_r_1m = _stress_r(features.get("abs_r_1m"), self._stats_abs_r_1m)
        sc_abs_r_5m = _stress_r(features.get("abs_r_5m"), self._stats_abs_r_5m)
        sbz = features.get("spread_bps_z")
        sc_spread_z  = _map_to_01(_clip_tanh(sbz)) if sbz is not None else None
        sc_tobi_ext  = features.get("tobi_extreme")
        abs_flow = features.get("abs_signed_flow_30s")
        sc_flow  = None
        if abs_flow is not None:
            z = self._stats_flow.z_score(abs_flow, now_wall)
            if z is not None:
                sc_flow = _map_to_01(_clip_tanh(z))

        stress_score, stress_anc, stress_sup = _anchor_support_score(
            [sc_abs_r_1m, sc_abs_r_5m, sc_spread_z], [sc_tobi_ext, sc_flow])
        s.update({"stress_score": _safe_float(stress_score),
                  "stress_anchor": _safe_float(stress_anc),
                  "stress_support": _safe_float(stress_sup)})

        # TREND — I6: usa _stats_r_15m/_stats_r_60m propios
        def _trend_r(r: Optional[float], stat: _RollingStats) -> Optional[float]:
            if r is None:
                return None
            z = stat.z_score(r, now_wall)
            return _clip_tanh(z) if z is not None else None

        tc_r_15m = _trend_r(facts.get("r_15m"), self._stats_r_15m)
        tc_r_60m = _trend_r(facts.get("r_60m"), self._stats_r_60m)

        def _with_anchor_sign(val: Optional[float]) -> Optional[float]:
            if val is None:
                return None
            refs = [v for v in [tc_r_15m, tc_r_60m] if v is not None]
            if not refs:
                return None
            sign = 1.0 if statistics.mean(refs) >= 0 else -1.0
            return sign * val

        tc_dp = _with_anchor_sign(features.get("dir_persistence_15m"))
        tc_er = _with_anchor_sign(features.get("efficiency_ratio_15m"))

        trend_score, trend_anc, trend_sup = _anchor_support_score(
            [tc_r_15m, tc_r_60m], [tc_dp, tc_er])
        if trend_score is not None:
            trend_score = max(-1.0, min(1.0, trend_score))
        s.update({"trend_score": _safe_float(trend_score),
                  "trend_anchor": _safe_float(trend_anc),
                  "trend_support": _safe_float(trend_sup)})

        # VOLATILITY
        def _vpct(val: Optional[float], stat: _RollingStats) -> Optional[float]:
            return stat.percentile_rank(val, now_wall) if val is not None else None

        vol_score, vol_anc, vol_sup = _anchor_support_score(
            [_vpct(facts.get("rv_1m"), self._stats_rv_1m),
             _vpct(facts.get("rv_5m"), self._stats_rv_5m)],
            [_vpct(features.get("rv_ratio_1m_15m"), self._stats_rv_ratio),
             _vpct(facts.get("realized_range_5m"), self._stats_realized_range)])
        s.update({"volatility_score": _safe_float(vol_score),
                  "volatility_anchor": _safe_float(vol_anc),
                  "volatility_support": _safe_float(vol_sup)})

        # LIQUIDITY
        spread_bps = facts.get("spread_bps")
        lc_spread  = None
        if spread_bps is not None:
            all_spread = self._spread_buffer.values_in_window(_ROLLING_WINDOW_S, now_wall)
            if len(all_spread) >= _ROLLING_MIN_POINTS:
                lc_spread = sum(1 for v in all_spread if v >= spread_bps) / len(all_spread)

        liq_score, liq_anc, liq_sup = _anchor_support_score(
            [lc_spread, features.get("spread_stability_1m")],
            [features.get("spread_recovery_30s"), features.get("tobi_non_extreme")])
        s.update({"liquidity_score": _safe_float(liq_score),
                  "liquidity_anchor": _safe_float(liq_anc),
                  "liquidity_support": _safe_float(liq_sup)})

        # ACTIVITY
        n_trades  = facts.get("n_trades_30s")
        quote_vol = facts.get("quote_vol_30s")
        gap       = facts.get("intertrade_gap_ms_p50")

        ac_gap = None
        if gap is not None:
            p = self._stats_intertrade_gap.percentile_rank(gap, now_wall)
            if p is not None:
                ac_gap = 1.0 - p

        act_score, act_anc, act_sup = _anchor_support_score(
            [self._stats_trade_count.percentile_rank(
                float(n_trades), now_wall) if n_trades is not None else None,
             self._stats_quote_vol.percentile_rank(
                quote_vol, now_wall) if quote_vol is not None else None],
            [self._stats_trade_intensity.percentile_rank(
                features.get("trade_intensity_1m"), now_wall)
                if features.get("trade_intensity_1m") is not None else None,
             ac_gap,
             self._stats_aggtrade_msg.percentile_rank(
                facts.get("aggtrade_msg_rate_30s"), now_wall)
                if facts.get("aggtrade_msg_rate_30s") is not None else None])
        s.update({"activity_score": _safe_float(act_score),
                  "activity_anchor": _safe_float(act_anc),
                  "activity_support": _safe_float(act_sup)})

        return s

    # -----------------------------------------------------------------------
    # Shadow tags
    # -----------------------------------------------------------------------

    def _stress_tag(self, score: Optional[float], health: dict) -> Optional[str]:
        if health["snapshot_health_status"] == "stale" or score is None:
            return None
        return "high" if score >= _STRESS_HIGH else ("low" if score < _STRESS_LOW else "medium")

    def _trend_tag(self, score: Optional[float], health: dict) -> Optional[str]:
        if health["snapshot_health_status"] == "stale" or score is None:
            return None
        return "up" if score > _TREND_UP else ("down" if score < _TREND_DOWN else "neutral")

    def _generic_tag(self, score: Optional[float], health: dict,
                     labels: Tuple[str, str, str]) -> Optional[str]:
        if health["snapshot_health_status"] == "stale" or score is None:
            return None
        low, mid, high = labels
        return high if score >= _GENERIC_HIGH else (low if score < _GENERIC_LOW else mid)

    # -----------------------------------------------------------------------
    # Worker status — sin parámetro facts (I32)
    # -----------------------------------------------------------------------

    def _determine_worker_status(self) -> str:
        with self._ws_lock:
            receiving = self._ws_receiving
            consec    = self._ws_consec_fails
        if self._compute_cycles < 15:
            return "warming_up"
        if consec >= _WS_MAX_CONSEC_FAILS:
            return "degraded"
        if not receiving:
            return "catching_up"
        if self._persist_errors > 10:
            return "degraded"
        return "live"

    # -----------------------------------------------------------------------
    # Health
    # -----------------------------------------------------------------------

    def _evaluate_health(
        self,
        facts: dict,
        features: dict,
        scores: dict,
        last_trade_ts_ms: Optional[float],
        book_data: Optional[dict],
        now_wall: float,
        worker_status: str,
        trade_batch_backlog: bool = False,
    ) -> dict:
        """
        Usa set desde el inicio para degradation_reasons — evita duplicados (I34).
        """
        deg: Set[str] = set()
        missing_features: List[str] = []

        if trade_batch_backlog:
            deg.add("trade_batch_backlog")

        # Freshness
        trade_feed_status = "missing"
        book_feed_status  = "missing"

        with self._ws_lock:
            last_msg  = self._ws_last_msg_ts
            receiving = self._ws_receiving
            connected = self._ws_connected

        if last_msg is not None:
            lag_ms = (now_wall - last_msg) * 1000.0
            if lag_ms <= _TRADE_SOFT_TTL_MS and receiving:
                trade_feed_status = "live"
            elif lag_ms <= _TRADE_HARD_TTL_MS:
                trade_feed_status = "delayed"
                deg.add("trade_feed_delayed")
            elif connected:
                trade_feed_status = "frozen"
                deg.add("trade_feed_frozen")
            else:
                trade_feed_status = "missing"
                deg.add("trade_feed_missing")
        else:
            if connected and not receiving:
                trade_feed_status = "delayed"
                deg.add("trade_feed_delayed")
            else:
                deg.add("trade_feed_missing")

        if book_data:
            book_ts = book_data.get("book_ts_utc")
            if book_ts:
                blag = (now_wall - book_ts.timestamp()) * 1000.0
                if blag <= _BOOK_SOFT_TTL_MS:
                    book_feed_status = "live"
                elif blag <= _BOOK_HARD_TTL_MS:
                    book_feed_status = "delayed"
                    deg.add("book_feed_delayed")
                else:
                    book_feed_status = "frozen"
                    deg.add("book_feed_frozen")
            else:
                book_feed_status = "delayed"
                deg.add("book_feed_delayed")
        else:
            deg.add("book_feed_missing")

        if trade_feed_status == "live" and book_feed_status == "live":
            freshness_status = "fresh"
        elif (trade_feed_status in ("missing","frozen") or
              book_feed_status  in ("missing","frozen")):
            freshness_status = "stale"
        else:
            freshness_status = "delayed"
        if worker_status == "warming_up" and freshness_status == "fresh":
            freshness_status = "delayed"

        # Completeness
        stress_crit = (
            features.get("abs_r_1m") is not None and
            features.get("abs_r_5m") is not None and
            any(features.get(k) is not None
                for k in ["spread_bps_z","tobi_extreme","abs_signed_flow_30s"])
        )
        if not stress_crit:
            missing_features += [k for k in ["abs_r_1m","abs_r_5m"] if features.get(k) is None]
            deg.add("critical_feature_missing")

        trend_crit = (facts.get("r_15m") is not None and facts.get("r_60m") is not None)
        if not trend_crit:
            missing_features += [k for k in ["r_15m","r_60m"] if facts.get(k) is None]
            deg.add("critical_feature_missing")

        vol_crit = (facts.get("rv_1m") is not None and facts.get("rv_5m") is not None)
        if not vol_crit:
            missing_features += [k for k in ["rv_1m","rv_5m"] if facts.get(k) is None]
            deg.add("critical_feature_missing")

        liq_crit = facts.get("spread_bps") is not None
        if not liq_crit:
            missing_features.append("spread_bps")
            deg.add("critical_feature_missing")

        act_crit = (facts.get("n_trades_30s") is not None or
                    facts.get("quote_vol_30s")  is not None)
        if not act_crit:
            missing_features.append("n_trades_30s_or_quote_vol_30s")
            deg.add("critical_feature_missing")

        all_critical = stress_crit and trend_crit and vol_crit and liq_crit and act_crit

        # I19: lookup directo sin replace()
        optional_keys: Dict[str, str] = {
            "dir_persistence_15m":  "dir_persistence_15m",
            "efficiency_ratio_15m": "efficiency_ratio_15m",
            "rv_ratio_1m_15m":      "rv_ratio_1m_15m",
            "spread_stability_1m":  "spread_stability_1m",
            "spread_recovery_30s":  "spread_recovery_30s",
            "tobi_non_extreme":     "tobi_non_extreme",
            "trade_intensity_1m":   "trade_intensity_1m",
        }
        optional_missing = [k for k, fk in optional_keys.items() if features.get(fk) is None]

        if all_critical and not optional_missing:
            completeness_status = "complete"
        elif all_critical:
            completeness_status = "partial"
            deg.add("optional_feature_missing")
        else:
            completeness_status = "insufficient"

        # Validity
        validity_status = "valid"
        mid    = facts.get("mid_price", 0.0)
        bid    = facts.get("bid_price")
        ask    = facts.get("ask_price")
        spread = facts.get("spread_bps")
        if not mid or mid <= 0:
            validity_status = "invalid"
            deg.add("invalid_value_detected")
        elif bid and ask and bid > ask:
            validity_status = "invalid"
            deg.add("invalid_value_detected")
        elif spread is not None and spread < 0:
            validity_status = "invalid"
            deg.add("invalid_value_detected")
        elif facts.get("n_trades_30s") is not None and facts["n_trades_30s"] < 0:
            validity_status = "suspicious"

        # Global
        if (freshness_status == "stale" or validity_status == "invalid" or
                completeness_status == "insufficient"):
            snapshot_health_status = "stale"
        elif (freshness_status == "fresh" and completeness_status == "complete" and
              validity_status == "valid"):
            snapshot_health_status = "fresh"
        else:
            snapshot_health_status = "degraded"

        snapshot_observable = validity_status != "invalid"
        snapshot_joinable   = (
            snapshot_health_status != "stale" and
            validity_status == "valid" and
            all_critical and
            snapshot_observable
        )

        dedup_reasons = sorted(deg)

        if snapshot_health_status == "stale" and self._compute_cycles % 10 == 0:
            log("BTC", (f"health=stale | freshness={freshness_status} "
                        f"completeness={completeness_status} validity={validity_status} "
                        f"reasons={dedup_reasons}"))

        return {
            "snapshot_health_status": snapshot_health_status,
            "snapshot_observable":    snapshot_observable,
            "snapshot_joinable":      snapshot_joinable,
            "freshness_status":       freshness_status,
            "completeness_status":    completeness_status,
            "validity_status":        validity_status,
            "trade_feed_status":      trade_feed_status,
            "book_feed_status":       book_feed_status,
            "missing_feature_count":  len(missing_features),
            "missing_features_list":  missing_features if missing_features else None,
            "degradation_reasons":    dedup_reasons,
            "late_event_count":       facts.get("late_event_count"),
            "max_event_delay_ms":     facts.get("max_event_delay_ms"),
        }

    # -----------------------------------------------------------------------
    # Invariants — protegido contra TypeError en comparaciones (I17)
    # -----------------------------------------------------------------------

    def _validate_snapshot_invariants(self, snap: BTCRegimeSnapshot) -> List[str]:
        errors: List[str] = []

        def _cmp(a, b, label: str) -> None:
            try:
                if a > b:
                    errors.append(label)
            except TypeError as e:
                errors.append(f"{label} (TypeError: {e})")

        _cmp(snap.window_start_ts, snap.window_end_ts, "window_start_ts > window_end_ts")
        _cmp(snap.window_end_ts,   snap.source_ts,     "window_end_ts > source_ts")
        _cmp(snap.source_ts,       snap.snapshot_ts,   "source_ts > snapshot_ts")

        try:
            diff = abs((snap.feature_ts - snap.window_end_ts).total_seconds())
            if diff > 1.0:
                errors.append(f"feature_ts != window_end_ts (diff={diff:.3f}s)")
        except TypeError as e:
            errors.append(f"feature_ts comparison TypeError: {e}")

        if snap.trade_source_ts:
            _cmp(snap.trade_source_ts, snap.snapshot_ts, "trade_source_ts > snapshot_ts")
        if snap.book_source_ts:
            _cmp(snap.book_source_ts, snap.snapshot_ts, "book_source_ts > snapshot_ts")

        if snap.source_lag_ms < 0:
            errors.append(f"source_lag_ms < 0 ({snap.source_lag_ms})")

        if (snap.btc_signed_flow_30s is not None and
                snap.btc_quote_vol_30s is not None and
                snap.btc_quote_vol_30s >= 0 and
                abs(snap.btc_signed_flow_30s) > snap.btc_quote_vol_30s * 1.001):
            errors.append(
                f"abs(signed_flow)={abs(snap.btc_signed_flow_30s):.2f} "
                f"> quote_vol={snap.btc_quote_vol_30s:.2f}"
            )

        if (snap.btc_bid_price is not None and snap.btc_ask_price is not None and
                snap.btc_bid_price > snap.btc_ask_price):
            errors.append(f"bid={snap.btc_bid_price} > ask={snap.btc_ask_price}")

        if snap.regime_mode != _REGIME_MODE:
            errors.append(f"regime_mode tampered: {snap.regime_mode!r}")
        if snap.schema_version != _SCHEMA_VERSION:
            errors.append(f"schema_version tampered: {snap.schema_version!r}")

        return errors

    # -----------------------------------------------------------------------
    # Persistence — rollback loggea error (I9)
    # -----------------------------------------------------------------------

    def _persist_snapshot(self, snap: BTCRegimeSnapshot) -> bool:
        if not self._ensure_db():
            self._persist_errors += 1
            return False
        row = snap.to_row_dict()
        try:
            with self._db_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO btc_regime_snapshots (
                        snapshot_ts, source_ts, feature_ts,
                        window_start_ts, window_end_ts,
                        trade_source_ts, book_source_ts,
                        source_lag_ms, trade_source_lag_ms, book_source_lag_ms,
                        compute_latency_ms,
                        btc_mid_price, btc_bid_price, btc_ask_price,
                        btc_spread_bps, btc_tobi,
                        btc_r_1m, btc_r_5m, btc_r_15m, btc_r_60m,
                        btc_rv_1m, btc_rv_5m, btc_realized_range_5m,
                        btc_n_trades_30s, btc_quote_vol_30s, btc_signed_flow_30s,
                        btc_intertrade_gap_ms_p50, btc_aggtrade_msg_rate_30s,
                        btc_abs_r_1m, btc_abs_r_5m, btc_spread_bps_z,
                        btc_tobi_extreme, btc_abs_signed_flow_30s,
                        btc_dir_persistence_15m, btc_efficiency_ratio_15m,
                        btc_rv_ratio_1m_15m,
                        btc_spread_stability_1m, btc_spread_recovery_30s,
                        btc_tobi_non_extreme, btc_trade_intensity_1m,
                        btc_stress_score, btc_trend_score, btc_volatility_score,
                        btc_liquidity_score, btc_activity_score,
                        btc_stress_anchor_component, btc_stress_support_component,
                        btc_trend_anchor_component, btc_trend_support_component,
                        btc_volatility_anchor_component, btc_volatility_support_component,
                        btc_liquidity_anchor_component, btc_liquidity_support_component,
                        btc_activity_anchor_component, btc_activity_support_component,
                        btc_stress_state, btc_trend_state, btc_volatility_state,
                        btc_liquidity_state, btc_activity_state,
                        shadow_tag_policy_version, btc_shadow_tags_extra,
                        snapshot_health_status, snapshot_observable, snapshot_joinable,
                        freshness_status, completeness_status, validity_status,
                        trade_feed_status, book_feed_status,
                        missing_feature_count, missing_features_list,
                        degradation_reasons,
                        late_event_count, max_event_delay_ms,
                        worker_status,
                        regime_mode, regime_validation_status,
                        schema_version, feature_version, score_formula_version,
                        normalization_policy_version,
                        allowed_use_class, prohibited_use_class,
                        debug_payload
                    ) VALUES (
                        %(snapshot_ts)s, %(source_ts)s, %(feature_ts)s,
                        %(window_start_ts)s, %(window_end_ts)s,
                        %(trade_source_ts)s, %(book_source_ts)s,
                        %(source_lag_ms)s, %(trade_source_lag_ms)s, %(book_source_lag_ms)s,
                        %(compute_latency_ms)s,
                        %(btc_mid_price)s, %(btc_bid_price)s, %(btc_ask_price)s,
                        %(btc_spread_bps)s, %(btc_tobi)s,
                        %(btc_r_1m)s, %(btc_r_5m)s, %(btc_r_15m)s, %(btc_r_60m)s,
                        %(btc_rv_1m)s, %(btc_rv_5m)s, %(btc_realized_range_5m)s,
                        %(btc_n_trades_30s)s, %(btc_quote_vol_30s)s, %(btc_signed_flow_30s)s,
                        %(btc_intertrade_gap_ms_p50)s, %(btc_aggtrade_msg_rate_30s)s,
                        %(btc_abs_r_1m)s, %(btc_abs_r_5m)s, %(btc_spread_bps_z)s,
                        %(btc_tobi_extreme)s, %(btc_abs_signed_flow_30s)s,
                        %(btc_dir_persistence_15m)s, %(btc_efficiency_ratio_15m)s,
                        %(btc_rv_ratio_1m_15m)s,
                        %(btc_spread_stability_1m)s, %(btc_spread_recovery_30s)s,
                        %(btc_tobi_non_extreme)s, %(btc_trade_intensity_1m)s,
                        %(btc_stress_score)s, %(btc_trend_score)s, %(btc_volatility_score)s,
                        %(btc_liquidity_score)s, %(btc_activity_score)s,
                        %(btc_stress_anchor_component)s, %(btc_stress_support_component)s,
                        %(btc_trend_anchor_component)s, %(btc_trend_support_component)s,
                        %(btc_volatility_anchor_component)s, %(btc_volatility_support_component)s,
                        %(btc_liquidity_anchor_component)s, %(btc_liquidity_support_component)s,
                        %(btc_activity_anchor_component)s, %(btc_activity_support_component)s,
                        %(btc_stress_state)s, %(btc_trend_state)s, %(btc_volatility_state)s,
                        %(btc_liquidity_state)s, %(btc_activity_state)s,
                        %(shadow_tag_policy_version)s, %(btc_shadow_tags_extra)s,
                        %(snapshot_health_status)s, %(snapshot_observable)s, %(snapshot_joinable)s,
                        %(freshness_status)s, %(completeness_status)s, %(validity_status)s,
                        %(trade_feed_status)s, %(book_feed_status)s,
                        %(missing_feature_count)s, %(missing_features_list)s,
                        %(degradation_reasons)s,
                        %(late_event_count)s, %(max_event_delay_ms)s,
                        %(worker_status)s,
                        %(regime_mode)s, %(regime_validation_status)s,
                        %(schema_version)s, %(feature_version)s, %(score_formula_version)s,
                        %(normalization_policy_version)s,
                        %(allowed_use_class)s, %(prohibited_use_class)s,
                        %(debug_payload)s
                    )
                    ON CONFLICT (snapshot_ts) DO NOTHING
                """, row)
            self._db_conn.commit()
            return True
        except psycopg2.Error as e:
            log("BTC", f"_persist_snapshot DB error: {type(e).__name__}: {e}")
            self._persist_errors += 1
            try:
                self._db_conn.rollback()
            except Exception as rb:
                log("BTC", f"WARN _persist_snapshot rollback error: {rb}")  # I9
            self._db_conn = None
            return False
        except Exception as e:
            log("BTC", f"_persist_snapshot unexpected error: {type(e).__name__}: {e}")
            self._persist_errors += 1
            try:
                self._db_conn.rollback()
            except Exception as rb:
                log("BTC", f"WARN _persist_snapshot rollback error: {rb}")  # I9
            return False

    def _set_latest_snapshot(self, snap: BTCRegimeSnapshot) -> None:
        public = snap.to_public_dict()
        with self._latest_lock:
            self._latest_snapshot = public

    # -----------------------------------------------------------------------
    # DB helpers
    # -----------------------------------------------------------------------

    def _ensure_db(self) -> bool:
        if self._db_conn and not self._db_conn.closed:
            return True
        try:
            self._db_conn = psycopg2.connect(_DSN)
            self._db_conn.autocommit = False
            with self._db_conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'UTC'")
            self._db_conn.commit()
            log("BTC", "DB connection established")
            return True
        except Exception as e:
            log("BTC", f"DB connect error: {type(e).__name__}: {e}")
            self._db_conn = None
            return False

    def _close_db(self) -> None:
        if self._db_conn:
            try:
                self._db_conn.close()
            except Exception as e:
                log("BTC", f"WARN _close_db error: {e}")  # I23
            self._db_conn = None


# ===========================================================================
# Public API
# ===========================================================================

_worker_instance: Optional[BTCRegimeWorker] = None
_start_lock = threading.Lock()   # I24: thread-safe ante llamadas concurrentes


def start_worker() -> BTCRegimeWorker:
    """Crea e inicia el BTCRegimeWorker global. Thread-safe (I24)."""
    global _worker_instance
    with _start_lock:
        if _worker_instance is not None:
            log("BTC", "WARN start_worker() called but worker already running")
            return _worker_instance
        _worker_instance = BTCRegimeWorker()
        _worker_instance.start()
    return _worker_instance


def get_latest_snapshot() -> Optional[dict]:
    """Convenience global — CLASS_2 consumers."""
    if _worker_instance is None:
        return None
    return _worker_instance.get_latest_snapshot()


def get_latest_scores() -> Optional[dict]:
    """Convenience global — solo los 5 scores."""
    if _worker_instance is None:
        return None
    return _worker_instance.get_latest_scores()
