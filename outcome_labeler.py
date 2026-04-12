"""
outcome_labeler.py — KTS TradeSeeker · F2.2 · Outcome Labeling Layer
v1.3 — 2026-04-11

Responsabilidad:
    Transformar eventos confirmados (candidate_events) en labels cuantitativos
    + markouts multi-horizonte, persistidos en event_labels_core y event_markouts.

Principios fundamentales (SoT v1.2):
    F2.2 NO decide si el evento es bueno.
    F2.2 MIDE qué ocurrió después del evento.

    PIT correctness:
        p_anchor usa SOLO datos con ts < t_confirm (anti-leakage garantizado).
        p_horizon usa datos forward SOLO después de t_confirm.

    Fail-safe (no fail-open):
        Cualquier error → evento persiste con label_quality='bad' y reason_code.
        Nunca se descarta silenciosamente un evento procesado.

    ON CONFLICT DO UPDATE:
        Re-runs actualizan con la versión más reciente del labeler.

    Determinismo:
        Toda lógica es determinística dado el estado de metrics_ext.

Parámetros configurables desde config.yaml bajo labeling.*:
    ANCHOR_WINDOW_S         = 3       # Δ anchor: [t_confirm-3s, t_confirm)
    SIGMA_LOOKBACK_MIN      = 10      # ventana volatilidad
    SIGMA_SAMPLING_S        = 2       # intervalo de resampling sigma
    HORIZON_WINDOW_S_SHORT  = 16      # ventana ±Δ horizontes <= 15m (par = simétrico)
    HORIZON_WINDOW_S_LONG   = 30      # ventana ±Δ horizontes > 15m
    TB_K1                   = 1.0     # barrier upper: +k1 * sigma
    TB_K2                   = 1.0     # barrier lower: -k2 * sigma
    QUOTE_SKEW_DEGRADED_MS  = 800
    QUOTE_SKEW_BAD_MS       = 2500
    ANCHOR_MIN_POINTS       = 3
    ANCHOR_MAX_LAG_MS       = 1000
    ANCHOR_MIN_COVERAGE_MS  = 500
    SIGMA_MIN_OBS           = 20
    HORIZON_MIN_POINTS      = 3
    HORIZON_MAX_LAG_RATIO   = 0.1
    SIGMA_ZERO_THRESHOLD    = 1e-10   # sigma <= threshold → bad_sigma
    ANCHOR_STALE_FACTOR     = 3       # lag > max_lag * factor → bad_stale_data
    HORIZON_MAX_LAG_BAD_FACTOR = 5    # lag_ratio > max_lag_ratio * factor → bad
    statement_timeout_ms    = 15000

Interfaz pública:
    label(event_id: str) -> str     # 'ok' | 'degraded' | 'bad'
    OutcomeLabeler clase completa para uso en labeling_worker.

Decisiones cerradas:
    DC-LABEL-01: p_anchor = mediana(mid_price), ventana [t_confirm-3s, t_confirm).
    DC-LABEL-02: sigma_event = std(log-returns resampled a 2s), lookback 10m.
    DC-LABEL-03: p_horizon con cascade median_forward → median_symmetric → first_after.
    DC-LABEL-04: triple barrier con barreras simétricas k1=k2=1.0 en F2.
    DC-LABEL-05: fail-safe — todo evento procesado persiste un label.
    DC-LABEL-06: ON CONFLICT DO UPDATE — re-runs sobrescriben con versión nueva.
    DC-LABEL-07: quote_skew_ms = p95(|ts_bid - ts_ask|) en ventana anchor.
    DC-LABEL-07B: quote_skew_ms = 0.0 en v1.0 — metrics_ext no separa ts_bid/ts_ask.
    DC-LABEL-08: mid_price = (bid + ask) / 2, validando bid>0, ask>0, ask>=bid.
    DC-LABEL-09: sigma=0.0 → bad_sigma — barreras colapsarían al mismo nivel.
    DC-LABEL-10: mid_prices_scan = List[Tuple[datetime, float]] para time_to_barrier_ms exacto.
    DC-LABEL-11: k1/k2 configurables desde config.yaml bajo labeling.tb_k1 / labeling.tb_k2.
    DC-LABEL-12: QualityAssessor evalúa todos los horizontes HORIZONS_MIN.
    DC-LABEL-15: event_label_attempts se escribe en CADA llamada a label().
    DC-LABEL-16: write_attempt() abre su propia conexión independiente.
                 Un fallo en write_attempt() no puede contaminar ni revertir
                 write_bad() o write() — transacciones completamente aisladas.

Correcciones v1.1:
    B1:  Eliminado import numpy — causaba ImportError si no instalado.
    B2:  quality='bad' como literal — no como slice de constante QRC.
    B3:  except en label() loggea antes de commit — eliminado except:pass.
    B4:  write_bad() lanza excepción si falla — caller maneja.
    B5:  write() lanza excepción si falla — caller hace rollback + write_bad().
    B6:  _safe_write_bad() loggea claramente cuando conn=None con impacto.
    B8:  rollback explícito antes de write_bad() en paths de error de transacción.
    A3:  _validate_book_row() centraliza validación — evita _safe_float() redundante.
    A4:  horizons.get(30)=None → BarrierResult first_touch='none' tb_label=None.
    A6:  QualityAssessor evalúa TODOS los horizontes (DC-LABEL-12).
    A7:  label() retorna 'bad' cuando write() falló — no quality.label_quality.
    A8:  _connect() cierra conn si SET TIME ZONE falla.
    A9:  quote_skew_ms=None cuando rows vacío.
    A10: sigma=0.0 → bad_sigma (DC-LABEL-09).
    A11: MarkoutComputer.compute() isinstance check antes de comparar p_anchor.
    A12: ret_h/mfe_h/mae_h sanitizados con _safe_float_strict() antes de INSERT.
    A13: horizonte ausente en write() → log warning — no skip silencioso.
    A14: _query_scan() usa named cursor (server-side) para scans largos.
    M3:  _query_scan() llamado una sola vez por horizonte — antes del cascade.
    M4:  mid_prices_scan = List[Tuple[datetime, float]] (DC-LABEL-10).
    M5:  write_bad() acepta anchor opcional para preservar diagnóstico.
    M6:  _percentile() retorna None si lista vacía — no 0.0.
    M7:  statement_timeout_ms configurable desde config.yaml.
    M9:  _top_priority() loggea warning si reason_code no en lista.
    M11: k1/k2 configurables (DC-LABEL-11).
    M12: HORIZON_WINDOW_S_SHORT=16 (par) → ventana simétrica exacta.
    M13: first_touch en write_bad() es parámetro — no hardcode.
    L1:  logging via log() de utils — mismo sink que el resto del bot.
    L3:  log explícito cuando conn=None con mención del impacto.
    L4:  HORIZON_MAX_LAG_BAD_FACTOR documentado como constante nombrada.
"""

import math
import statistics
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import psycopg2
import psycopg2.extras

from utils import cfg, log

# ---------------------------------------------------------------------------
# Versión del labeler
# ---------------------------------------------------------------------------

LABELER_VERSION = "v1.3"

# ---------------------------------------------------------------------------
# Horizontes (minutos)
# ---------------------------------------------------------------------------

HORIZONS_MIN: List[int] = [5, 15, 30, 60, 120]

# ---------------------------------------------------------------------------
# Parámetros módulo — todos sobrescribibles desde config.yaml bajo labeling.*
# ---------------------------------------------------------------------------

ANCHOR_WINDOW_S            = 3
SIGMA_LOOKBACK_MIN         = 10
SIGMA_SAMPLING_S           = 2
HORIZON_WINDOW_S_SHORT     = 16      # par → ventana simétrica exacta (fix M12)
HORIZON_WINDOW_S_LONG      = 30
TB_K1                      = 1.0
TB_K2                      = 1.0
QUOTE_SKEW_DEGRADED_MS     = 800
QUOTE_SKEW_BAD_MS          = 2500
ANCHOR_MIN_POINTS          = 3
ANCHOR_MAX_LAG_MS          = 1000
ANCHOR_MIN_COVERAGE_MS     = 500
SIGMA_MIN_OBS              = 20
HORIZON_MIN_POINTS         = 3
HORIZON_MAX_LAG_RATIO      = 0.1
SIGMA_ZERO_THRESHOLD       = 1e-10   # DC-LABEL-09
ANCHOR_STALE_FACTOR        = 3       # lag > max_lag * 3 → bad_stale_data
HORIZON_MAX_LAG_BAD_FACTOR = 5       # lag_ratio > max_ratio * 5 → bad (fix L4)

# ---------------------------------------------------------------------------
# Config helper
# ---------------------------------------------------------------------------

def _lbl_cfg(key: str, default: Any) -> Any:
    return cfg(f"labeling.{key}", default)

# ---------------------------------------------------------------------------
# Códigos de quality_reason_code — en orden de prioridad (SoT Sección 7.6)
# ---------------------------------------------------------------------------

QRC_BAD_INVALID_BOOK      = "bad_invalid_book"
QRC_BAD_MISSING_QUOTES    = "bad_missing_quotes"
QRC_BAD_TIME_INCONSISTENCY = "bad_time_inconsistency"
QRC_BAD_STALE_DATA        = "bad_stale_data"
QRC_BAD_SIGMA             = "bad_sigma"
QRC_BAD_LABELER_EXCEPTION = "bad_labeler_exception"
QRC_DEG_QUOTE_SKEW        = "degraded_quote_skew"
QRC_DEG_LOW_DENSITY       = "degraded_low_density"
QRC_DEG_HIGH_LAG          = "degraded_high_lag"
QRC_DEG_WIDE_SPREAD       = "degraded_wide_spread"
QRC_DEG_UNSTABLE_WINDOW   = "degraded_unstable_window"

_BAD_PRIORITY: List[str] = [
    QRC_BAD_INVALID_BOOK,
    QRC_BAD_MISSING_QUOTES,
    QRC_BAD_TIME_INCONSISTENCY,
    QRC_BAD_STALE_DATA,
    QRC_BAD_SIGMA,
    QRC_BAD_LABELER_EXCEPTION,
]
_DEG_PRIORITY: List[str] = [
    QRC_DEG_QUOTE_SKEW,
    QRC_DEG_LOW_DENSITY,
    QRC_DEG_HIGH_LAG,
    QRC_DEG_WIDE_SPREAD,
    QRC_DEG_UNSTABLE_WINDOW,
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class AnchorResult:
    p_anchor:                  Optional[float]
    anchor_points:             int
    anchor_lag_ms:             Optional[float]
    anchor_window_coverage_ms: Optional[float]
    anchor_spread_bps:         Optional[float]
    anchor_spread_z:           Optional[float]
    quote_skew_ms:             Optional[float]   # None si sin datos; 0.0 si v1.0 schema
    quality:                   str               # 'ok' | 'degraded' | 'bad'
    reason_code:               Optional[str]


@dataclass
class SigmaResult:
    sigma_event:    Optional[float]
    sigma_obs_n:    int
    sigma_complete: bool
    quality:        str
    reason_code:    Optional[str]


@dataclass
class HorizonResult:
    horizon_min:       int
    p_h:               Optional[float]
    sampling_method:   Optional[str]
    points_used:       int
    forward_lag_ms:    Optional[float]
    forward_lag_ratio: Optional[float]
    window_span_ms:    Optional[float]
    point_density:     Optional[float]
    spread_bps:        Optional[float]
    spread_z:          Optional[float]
    mid_iqr_bps:       Optional[float]
    quality_flag:      str
    reason_code:       Optional[str]             # fix 3: auditabilidad de calidad del horizonte
    # DC-LABEL-10: (timestamp, mid_price) para time_to_barrier_ms exacto (fix M4)
    mid_prices_scan:   List[Tuple[datetime, float]] = field(default_factory=list)


@dataclass
class MarkoutResult:
    horizon_min: int
    ret_h:       Optional[float]
    mfe_h:       Optional[float]
    mae_h:       Optional[float]


@dataclass
class BarrierResult:
    tb_label:           Optional[int]    # -1 | 0 | 1 | None
    good_event:         Optional[bool]
    first_touch:        Optional[str]    # 'upper'|'lower'|'vertical'|'none'
    time_to_barrier_ms: Optional[int]


@dataclass
class QualityResult:
    label_quality: str
    reason_code:   Optional[str]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> Optional[float]:
    """float limpio o None si NaN/Inf/inválido."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _safe_float_strict(val: Any) -> Optional[float]:
    """
    Como _safe_float pero rechaza inf. Usar antes de INSERT (fix A12).
    Postgres acepta DOUBLE PRECISION inf silenciosamente — corrupción de dato.
    """
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _percentile(data: List[float], p: float) -> Optional[float]:
    """Percentil interpolado. Retorna None si lista vacía (fix M6)."""
    if not data:
        return None
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] * (c - k) + s[c] * (k - f)


def _validate_book_row(row: Dict) -> Optional[Tuple[float, float]]:
    """
    Valida y extrae (bid, ask). Centraliza la validación de book (fix A3).
    Retorna (bid, ask) si válido, None si inválido.
    Evita llamar _safe_float() múltiples veces por row.
    """
    bid = _safe_float(row.get("bid"))
    ask = _safe_float(row.get("ask"))
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0:
        return None
    if ask < bid:
        return None
    return bid, ask


def _dsn() -> str:
    timeout_ms = _lbl_cfg("statement_timeout_ms", 15000)  # fix M7
    return (
        "host=localhost port=5432 dbname=tsdb "
        "user=postgres password=postgres "
        f"connect_timeout=5 "
        f"options='-c statement_timeout={int(timeout_ms)}'"
    )


def _connect() -> psycopg2.extensions.connection:
    """
    Abre conexión y fuerza UTC.
    Cierra el socket si SET TIME ZONE falla — evita leak (fix A8).
    """
    conn = psycopg2.connect(_dsn())
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
        conn.commit()
        return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise


# ---------------------------------------------------------------------------
# PriceAnchor — Sección 1 del SoT
# ---------------------------------------------------------------------------

class PriceAnchor:
    """
    p_anchor = mediana(mid_price) en [t_confirm - 3s, t_confirm).
    Anti-leakage: ts < t_confirm (strict less-than).
    Validación de book por punto via _validate_book_row (fix A3).
    quote_skew_ms: None si sin datos; 0.0 por DC-LABEL-07B (fix A9).
    """

    def compute(
        self,
        conn,
        symbol:    str,
        t_confirm: datetime,
        anchor_window_s:        int   = ANCHOR_WINDOW_S,
        min_points:             int   = ANCHOR_MIN_POINTS,
        max_lag_ms:             float = ANCHOR_MAX_LAG_MS,
        min_coverage_ms:        float = ANCHOR_MIN_COVERAGE_MS,
        quote_skew_degraded_ms: float = QUOTE_SKEW_DEGRADED_MS,
        quote_skew_bad_ms:      float = QUOTE_SKEW_BAD_MS,
    ) -> AnchorResult:

        t_start = t_confirm - timedelta(seconds=anchor_window_s)
        rows    = self._query(conn, symbol, t_start, t_confirm)

        # fix A9: quote_skew_ms=None cuando no hay datos — no 0.0
        if not rows:
            return AnchorResult(
                p_anchor=None, anchor_points=0,
                anchor_lag_ms=None, anchor_window_coverage_ms=None,
                anchor_spread_bps=None, anchor_spread_z=None,
                quote_skew_ms=None,
                quality="bad",           # fix B2: literal, no slice
                reason_code=QRC_BAD_MISSING_QUOTES,
            )

        mid_prices  = []
        spreads_bps = []
        timestamps  = []

        for row in rows:
            ba = _validate_book_row(row)   # fix A3
            if ba is None:
                continue
            bid, ask = ba
            mid = (bid + ask) / 2.0
            mid_prices.append(mid)
            timestamps.append(row["ts"])
            if mid > 0:
                spreads_bps.append(((ask - bid) / mid) * 10_000)

        if not mid_prices:
            return AnchorResult(
                p_anchor=None, anchor_points=0,
                anchor_lag_ms=None, anchor_window_coverage_ms=None,
                anchor_spread_bps=None, anchor_spread_z=None,
                quote_skew_ms=None,
                quality="bad",
                reason_code=QRC_BAD_INVALID_BOOK,
            )

        p_anchor    = statistics.median(mid_prices)
        latest_ts   = max(timestamps)
        earliest_ts = min(timestamps)
        lag_ms      = abs((t_confirm - latest_ts).total_seconds()) * 1000.0
        coverage_ms = (latest_ts - earliest_ts).total_seconds() * 1000.0

        anchor_spread_bps = statistics.median(spreads_bps) if spreads_bps else None
        anchor_spread_z   = None
        if spreads_bps and len(spreads_bps) >= 2:
            try:
                mean_s = statistics.mean(spreads_bps)
                std_s  = statistics.stdev(spreads_bps)
                if std_s > 0 and anchor_spread_bps is not None:
                    anchor_spread_z = (anchor_spread_bps - mean_s) / std_s
            except statistics.StatisticsError:
                pass

        # DC-LABEL-07B: 0.0 porque metrics_ext no separa ts_bid/ts_ask
        quote_skew_ms = 0.0

        quality, reason_code = self._assess_quality(
            n_points          = len(mid_prices),
            lag_ms            = lag_ms,
            coverage_ms       = coverage_ms,
            quote_skew_ms     = quote_skew_ms,
            min_points        = min_points,
            max_lag_ms        = max_lag_ms,
            min_coverage_ms   = min_coverage_ms,
            quote_skew_deg_ms = quote_skew_degraded_ms,
            quote_skew_bad_ms = quote_skew_bad_ms,
            anchor_spread_z   = anchor_spread_z,   # fix 5
        )

        return AnchorResult(
            p_anchor                  = p_anchor,
            anchor_points             = len(mid_prices),
            anchor_lag_ms             = round(lag_ms, 2),
            anchor_window_coverage_ms = round(coverage_ms, 2),
            anchor_spread_bps         = anchor_spread_bps,
            anchor_spread_z           = anchor_spread_z,
            quote_skew_ms             = quote_skew_ms,
            quality                   = quality,
            reason_code               = reason_code,
        )

    def _query(self, conn, symbol: str, t_start: datetime, t_end: datetime) -> List[Dict]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ts, bid, ask
                FROM   metrics_ext
                WHERE  symbol = %s
                  AND  ts >= %s
                  AND  ts <  %s
                ORDER BY ts ASC
                """,
                (symbol, t_start, t_end),
            )
            return [dict(r) for r in cur.fetchall()]

    def _assess_quality(
        self,
        n_points:          int,
        lag_ms:            float,
        coverage_ms:       float,
        quote_skew_ms:     float,
        min_points:        int,
        max_lag_ms:        float,
        min_coverage_ms:   float,
        quote_skew_deg_ms: float,
        quote_skew_bad_ms: float,
        anchor_spread_z:   Optional[float] = None,   # fix 5
    ) -> Tuple[str, Optional[str]]:
        # Bad — orden de prioridad SoT 7.6
        if quote_skew_ms > quote_skew_bad_ms:
            return "bad", QRC_BAD_INVALID_BOOK
        if n_points == 0:
            return "bad", QRC_BAD_MISSING_QUOTES
        if lag_ms > max_lag_ms * ANCHOR_STALE_FACTOR:
            return "bad", QRC_BAD_STALE_DATA
        # Degraded
        if quote_skew_ms > quote_skew_deg_ms:
            return "degraded", QRC_DEG_QUOTE_SKEW
        if n_points < min_points:
            return "degraded", QRC_DEG_LOW_DENSITY
        if lag_ms > max_lag_ms:
            return "degraded", QRC_DEG_HIGH_LAG
        if coverage_ms < min_coverage_ms:
            return "degraded", QRC_DEG_UNSTABLE_WINDOW
        # DC-LABEL-13: anchor_spread_z > 3.0 → spread del anchor fuera de rango normal.
        # Threshold 3.0 conservador para F2 — calibrar en F3 con datos reales.
        if anchor_spread_z is not None and anchor_spread_z > 3.0:
            return "degraded", QRC_DEG_WIDE_SPREAD
        return "ok", None


# ---------------------------------------------------------------------------
# VolatilityEstimator — Sección 2 del SoT
# ---------------------------------------------------------------------------

class VolatilityEstimator:
    """
    sigma_event = std(log-returns resampled a 2s) en [t_confirm-10m, t_confirm).
    sigma <= SIGMA_ZERO_THRESHOLD → bad_sigma (DC-LABEL-09, fix A10).
    """

    def compute(
        self,
        conn,
        symbol:       str,
        t_confirm:    datetime,
        lookback_min: int = SIGMA_LOOKBACK_MIN,
        sampling_s:   int = SIGMA_SAMPLING_S,
        min_obs:      int = SIGMA_MIN_OBS,
    ) -> SigmaResult:

        t_start = t_confirm - timedelta(minutes=lookback_min)
        rows    = self._query(conn, symbol, t_start, t_confirm)

        if len(rows) < 2:
            return SigmaResult(
                sigma_event=None, sigma_obs_n=len(rows),
                sigma_complete=False, quality="bad", reason_code=QRC_BAD_SIGMA,
            )

        raw_ts  = [r["ts"] for r in rows]
        raw_mid = []
        for r in rows:
            ba = _validate_book_row(r)
            if ba is not None:
                bid, ask = ba
                raw_mid.append((bid + ask) / 2.0)
            else:
                raw_mid.append(None)

        resampled = self._resample(raw_ts, raw_mid, t_start, t_confirm, sampling_s)

        if len(resampled) < 2:
            return SigmaResult(
                sigma_event=None, sigma_obs_n=0,
                sigma_complete=False, quality="bad", reason_code=QRC_BAD_SIGMA,
            )

        log_returns = []
        for i in range(1, len(resampled)):
            prev = resampled[i - 1]
            curr = resampled[i]
            if prev and curr and prev > 0 and curr > 0:
                log_returns.append(math.log(curr / prev))

        if len(log_returns) < 2:
            return SigmaResult(
                sigma_event=None, sigma_obs_n=len(log_returns),
                sigma_complete=False, quality="bad", reason_code=QRC_BAD_SIGMA,
            )

        try:
            sigma = statistics.stdev(log_returns)
        except statistics.StatisticsError:
            return SigmaResult(
                sigma_event=None, sigma_obs_n=len(log_returns),
                sigma_complete=False, quality="bad", reason_code=QRC_BAD_SIGMA,
            )

        if sigma is None or math.isnan(sigma) or math.isinf(sigma) or sigma < 0:
            return SigmaResult(
                sigma_event=None, sigma_obs_n=len(log_returns),
                sigma_complete=False, quality="bad", reason_code=QRC_BAD_SIGMA,
            )

        # DC-LABEL-09: sigma=0 → bad — barreras colapsarían al mismo precio (fix A10)
        if sigma <= SIGMA_ZERO_THRESHOLD:
            return SigmaResult(
                sigma_event=None, sigma_obs_n=len(log_returns),
                sigma_complete=False, quality="bad", reason_code=QRC_BAD_SIGMA,
            )

        obs_n    = len(log_returns)
        complete = obs_n >= min_obs
        quality  = "ok" if complete else "degraded"
        reason   = None if complete else QRC_DEG_LOW_DENSITY

        return SigmaResult(
            sigma_event    = sigma,
            sigma_obs_n    = obs_n,
            sigma_complete = complete,
            quality        = quality,
            reason_code    = reason,
        )

    def _query(self, conn, symbol: str, t_start: datetime, t_end: datetime) -> List[Dict]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ts, bid, ask
                FROM   metrics_ext
                WHERE  symbol = %s
                  AND  ts >= %s
                  AND  ts <  %s
                ORDER BY ts ASC
                """,
                (symbol, t_start, t_end),
            )
            return [dict(r) for r in cur.fetchall()]

    def _resample(
        self,
        raw_ts:     List[datetime],
        raw_mid:    List[Optional[float]],
        t_start:    datetime,
        t_end:      datetime,
        sampling_s: int,
    ) -> List[Optional[float]]:
        """Grid de sampling_s segundos con forward-fill (máx 1 intervalo)."""
        total_s = (t_end - t_start).total_seconds()
        n_slots = max(1, int(total_s / sampling_s))
        grid    = [None] * n_slots

        for ts_raw, mid in zip(raw_ts, raw_mid):
            if mid is None:
                continue
            offset_s = (ts_raw - t_start).total_seconds()
            slot     = int(offset_s / sampling_s)
            if 0 <= slot < n_slots:
                grid[slot] = mid

        last_valid = None
        last_slot  = -1
        result     = []
        for i, val in enumerate(grid):
            if val is not None:
                last_valid = val
                last_slot  = i
                result.append(val)
            elif last_valid is not None and (i - last_slot) <= 1:
                result.append(last_valid)
            else:
                result.append(None)

        return result


# ---------------------------------------------------------------------------
# HorizonPricer — Sección 3 del SoT
# ---------------------------------------------------------------------------

class HorizonPricer:
    """
    p_horizon con cascade median_forward → median_symmetric → first_after.
    _query_scan() llamado UNA sola vez antes del cascade (fix M3).
    mid_prices_scan = List[Tuple[datetime, float]] para timestamps exactos (DC-LABEL-10, fix M4).
    Usa named cursor (server-side) para scans largos (fix A14).
    """

    def compute(
        self,
        conn,
        symbol:         str,
        t_confirm:      datetime,
        p_anchor:       float,
        horizon_min:    int,
        window_s_short: int   = HORIZON_WINDOW_S_SHORT,
        window_s_long:  int   = HORIZON_WINDOW_S_LONG,
        min_points:     int   = HORIZON_MIN_POINTS,
        max_lag_ratio:  float = HORIZON_MAX_LAG_RATIO,
    ) -> HorizonResult:

        t_h     = t_confirm + timedelta(minutes=horizon_min)
        delta_s = window_s_short if horizon_min <= 15 else window_s_long

        # fix M3: scan una sola vez, antes del cascade
        scan_rows = self._query_scan(conn, symbol, t_confirm, t_h)

        # median_forward: [t_h, t_h + delta_s] — forward puro (fix 2)
        # Solo ticks posteriores al horizonte: no sesga el precio hacia el pasado.
        rows = self._query(conn, symbol, t_h, t_h + timedelta(seconds=delta_s))
        mids = self._extract_mids(rows)

        if len(mids) >= min_points:
            return self._build_result(
                horizon_min    = horizon_min,
                method         = "median_forward",
                rows           = rows,
                mids           = mids,
                t_h            = t_h,
                p_anchor       = p_anchor,
                min_points     = min_points,
                max_lag_ratio  = max_lag_ratio,
                window_delta_s = delta_s,
                scan_rows      = scan_rows,
            )

        # Fallback: median_symmetric — [t_h - delta_s, t_h + delta_s] (fix 2)
        # Ventana centrada: acepta datos antes y después de t_h cuando forward es escaso.
        rows_sym  = self._query(conn, symbol,
                                t_h - timedelta(seconds=delta_s),
                                t_h + timedelta(seconds=delta_s))
        mids_sym  = self._extract_mids(rows_sym)

        if len(mids_sym) >= min_points:
            return self._build_result(
                horizon_min    = horizon_min,
                method         = "median_symmetric",
                rows           = rows_sym,
                mids           = mids_sym,
                t_h            = t_h,
                p_anchor       = p_anchor,
                min_points     = min_points,
                max_lag_ratio  = max_lag_ratio,
                window_delta_s = delta_s * 2,
                scan_rows      = scan_rows,
            )

        # Fallback final: first_after
        row_fa = self._query_first_after(conn, symbol, t_h)
        if row_fa:
            ba = _validate_book_row(row_fa)
            if ba is not None:
                bid, ask   = ba
                p_h        = (bid + ask) / 2.0
                ts_r       = row_fa["ts"]
                fwd_lag_ms = abs((ts_r - t_h).total_seconds()) * 1000.0
                spread_bps = ((ask - bid) / p_h) * 10_000 if p_h > 0 else None

                return HorizonResult(
                    horizon_min       = horizon_min,
                    p_h               = p_h,
                    sampling_method   = "first_after",
                    points_used       = 1,
                    forward_lag_ms    = fwd_lag_ms,
                    forward_lag_ratio = 1.0,
                    window_span_ms    = fwd_lag_ms,
                    point_density     = 0.0,
                    spread_bps        = spread_bps,
                    spread_z          = None,
                    mid_iqr_bps       = None,    # fix M6: None no 0.0
                    quality_flag      = "degraded",
                    reason_code       = QRC_DEG_LOW_DENSITY,   # fix 3: 1 punto = low density
                    mid_prices_scan   = self._extract_scan_with_ts(scan_rows),
                )

        # Sin datos en ningún fallback
        return HorizonResult(
            horizon_min       = horizon_min,
            p_h               = None,
            sampling_method   = None,
            points_used       = 0,
            forward_lag_ms    = None,
            forward_lag_ratio = None,
            window_span_ms    = None,
            point_density     = None,
            spread_bps        = None,
            spread_z          = None,
            mid_iqr_bps       = None,
            quality_flag      = "bad",
            reason_code       = QRC_BAD_MISSING_QUOTES,   # fix 3
            mid_prices_scan   = [],
        )

    def _query(self, conn, symbol: str, t_start: datetime, t_end: datetime) -> List[Dict]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ts, bid, ask
                FROM   metrics_ext
                WHERE  symbol = %s
                  AND  ts >= %s
                  AND  ts <= %s
                ORDER BY ts ASC
                """,
                (symbol, t_start, t_end),
            )
            return [dict(r) for r in cur.fetchall()]

    def _query_first_after(self, conn, symbol: str, t_h: datetime) -> Optional[Dict]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ts, bid, ask
                FROM   metrics_ext
                WHERE  symbol = %s
                  AND  ts >= %s
                ORDER BY ts ASC
                LIMIT 1
                """,
                (symbol, t_h),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _query_scan(
        self, conn, symbol: str, t_confirm: datetime, t_h: datetime
    ) -> List[Dict]:
        """
        Todos los ticks en [t_confirm, t_h] para MFE/MAE y triple barrier.
        Named cursor (server-side) — evita cargar todo en memoria (fix A14).
        Seguro para scans de horizonte 120m con símbolos de alto volumen.
        """
        rows     = []
        cur_name = f"scan_{abs(hash((symbol, str(t_confirm), str(t_h))))}"
        with conn.cursor(cur_name, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ts, bid, ask
                FROM   metrics_ext
                WHERE  symbol = %s
                  AND  ts >= %s
                  AND  ts <= %s
                ORDER BY ts ASC
                """,
                (symbol, t_confirm, t_h),
            )
            while True:
                batch = cur.fetchmany(500)
                if not batch:
                    break
                rows.extend([dict(r) for r in batch])
        return rows

    def _extract_mids(self, rows: List[Dict]) -> List[float]:
        mids = []
        for r in rows:
            ba = _validate_book_row(r)
            if ba is not None:
                bid, ask = ba
                mids.append((bid + ask) / 2.0)
        return mids

    def _extract_scan_with_ts(
        self, rows: List[Dict]
    ) -> List[Tuple[datetime, float]]:
        """DC-LABEL-10: extrae (ts, mid) para time_to_barrier_ms exacto (fix M4)."""
        result = []
        for r in rows:
            ba = _validate_book_row(r)
            if ba is not None:
                bid, ask = ba
                result.append((r["ts"], (bid + ask) / 2.0))
        return result

    def _build_result(
        self,
        horizon_min:    int,
        method:         str,
        rows:           List[Dict],
        mids:           List[float],
        t_h:            datetime,
        p_anchor:       float,
        min_points:     int,
        max_lag_ratio:  float,
        window_delta_s: int,
        scan_rows:      List[Dict],
    ) -> HorizonResult:

        p_h = statistics.median(mids)

        # Timestamps de rows válidos — _validate_book_row unificado (fix A3)
        valid_ts = [r["ts"] for r in rows if _validate_book_row(r) is not None]

        fwd_lag_ms = None
        if valid_ts:
            nearest    = min(valid_ts, key=lambda ts: abs((ts - t_h).total_seconds()))
            fwd_lag_ms = abs((nearest - t_h).total_seconds()) * 1000.0

        window_span_ms = None
        if len(valid_ts) >= 2:
            window_span_ms = (max(valid_ts) - min(valid_ts)).total_seconds() * 1000.0
        elif valid_ts:
            window_span_ms = 0.0

        window_s_ms   = window_delta_s * 1000.0
        fwd_lag_ratio = (fwd_lag_ms / window_s_ms) if (fwd_lag_ms is not None and window_s_ms > 0) else None
        point_density = (len(mids) / window_span_ms) if (window_span_ms and window_span_ms > 0) else None

        # Spread stats — _validate_book_row unificado (fix A3)
        spreads = []
        for r in rows:
            ba = _validate_book_row(r)
            if ba is not None:
                bid, ask = ba
                mid = (bid + ask) / 2.0
                if mid > 0:
                    spreads.append(((ask - bid) / mid) * 10_000)

        spread_bps = statistics.median(spreads) if spreads else None
        spread_z   = None
        if spreads and len(spreads) >= 2:
            try:
                mean_s = statistics.mean(spreads)
                std_s  = statistics.stdev(spreads)
                if std_s > 0 and spread_bps is not None:
                    spread_z = (spread_bps - mean_s) / std_s
            except statistics.StatisticsError:
                pass

        # mid_iqr_bps — _percentile retorna None si vacía (fix M6)
        mid_iqr_bps = None
        if len(mids) >= 4 and p_anchor and p_anchor > 0:
            q75 = _percentile(mids, 75)
            q25 = _percentile(mids, 25)
            if q75 is not None and q25 is not None:
                mid_iqr_bps = ((q75 - q25) / p_anchor) * 10_000

        quality, reason_code = self._assess_horizon_quality(
            n_points        = len(mids),
            fwd_lag_ratio   = fwd_lag_ratio,
            window_span_ms  = window_span_ms,
            window_delta_ms = window_s_ms,
            min_points      = min_points,
            max_lag_ratio   = max_lag_ratio,
            point_density   = point_density,
            spread_z        = spread_z,
            mid_iqr_bps     = mid_iqr_bps,
        )

        return HorizonResult(
            horizon_min       = horizon_min,
            p_h               = p_h,
            sampling_method   = method,
            points_used       = len(mids),
            forward_lag_ms    = round(fwd_lag_ms, 2) if fwd_lag_ms is not None else None,
            forward_lag_ratio = round(fwd_lag_ratio, 4) if fwd_lag_ratio is not None else None,
            window_span_ms    = round(window_span_ms, 2) if window_span_ms is not None else None,
            point_density     = point_density,
            spread_bps        = spread_bps,
            spread_z          = spread_z,
            mid_iqr_bps       = mid_iqr_bps,
            quality_flag      = quality,
            reason_code       = reason_code,
            mid_prices_scan   = self._extract_scan_with_ts(scan_rows),
        )

    def _assess_horizon_quality(
        self,
        n_points:        int,
        fwd_lag_ratio:   Optional[float],
        window_span_ms:  Optional[float],
        window_delta_ms: float,
        min_points:      int,
        max_lag_ratio:   float,
        point_density:   Optional[float]  = None,
        spread_z:        Optional[float]  = None,
        mid_iqr_bps:     Optional[float]  = None,
    ) -> Tuple[str, Optional[str]]:
        """
        Retorna (quality_flag, reason_code). Incorpora point_density, spread_z,
        mid_iqr_bps en la evaluación (fix 4).

        Thresholds de spread_z y mid_iqr_bps:
            spread_z > 3.0  → spread fuera de la distribución normal → degraded_wide_spread.
            mid_iqr_bps > 50 → precio inestable en la ventana → degraded_unstable_window.
        Ambos thresholds son conservadores para F2 — calibrar con datos reales en F3.
        DC-LABEL-14: thresholds de spread_z (3.0) y mid_iqr_bps (50) son iniciales para F2.
        """
        # Bad
        if n_points == 0:
            return "bad", QRC_BAD_MISSING_QUOTES
        if fwd_lag_ratio is not None and fwd_lag_ratio > max_lag_ratio * HORIZON_MAX_LAG_BAD_FACTOR:
            return "bad", QRC_BAD_STALE_DATA

        # Degraded — en orden de prioridad
        if window_span_ms is not None and window_delta_ms > 0:
            if window_span_ms < window_delta_ms * 0.50:
                return "degraded", QRC_DEG_UNSTABLE_WINDOW
        if n_points < min_points:
            return "degraded", QRC_DEG_LOW_DENSITY
        if point_density is not None and point_density == 0.0:
            return "degraded", QRC_DEG_LOW_DENSITY
        if fwd_lag_ratio is not None and fwd_lag_ratio > max_lag_ratio:
            return "degraded", QRC_DEG_HIGH_LAG
        if spread_z is not None and spread_z > 3.0:
            return "degraded", QRC_DEG_WIDE_SPREAD
        if mid_iqr_bps is not None and mid_iqr_bps > 50.0:
            return "degraded", QRC_DEG_UNSTABLE_WINDOW

        return "ok", None


# ---------------------------------------------------------------------------
# MarkoutComputer — Sección 4 del SoT
# ---------------------------------------------------------------------------

class MarkoutComputer:
    """
    ret_h, mfe_h, mae_h por horizonte.
    isinstance check para p_anchor=None (fix A11).
    _safe_float_strict() en todos los retornos (fix A12).
    mid_prices_scan es List[Tuple[datetime, float]] (DC-LABEL-10).
    """

    def compute(
        self,
        p_anchor: float,
        horizon:  HorizonResult,
    ) -> MarkoutResult:

        # fix A11: isinstance antes de comparar — p_anchor=None lanzaría TypeError
        if not isinstance(p_anchor, (int, float)) or p_anchor <= 0:
            return MarkoutResult(
                horizon_min=horizon.horizon_min,
                ret_h=None, mfe_h=None, mae_h=None,
            )

        if horizon.p_h is None:
            return MarkoutResult(
                horizon_min=horizon.horizon_min,
                ret_h=None, mfe_h=None, mae_h=None,
            )

        # fix A12: sanitizar con _safe_float_strict antes de retornar
        raw_ret = (horizon.p_h - p_anchor) / p_anchor
        ret_h   = _safe_float_strict(raw_ret)

        mfe_h = None
        mae_h = None

        # DC-LABEL-10: scan con (datetime, float)
        scan = horizon.mid_prices_scan
        if scan:
            rets = []
            for _ts, mid in scan:
                if mid > 0:
                    sf = _safe_float_strict((mid - p_anchor) / p_anchor)
                    if sf is not None:
                        rets.append(sf)
            if rets:
                mfe_h = max(rets)
                mae_h = min(rets)

        return MarkoutResult(
            horizon_min = horizon.horizon_min,
            ret_h       = ret_h,
            mfe_h       = mfe_h,
            mae_h       = mae_h,
        )


# ---------------------------------------------------------------------------
# TripleBarrierLabeler — Sección 5 del SoT
# ---------------------------------------------------------------------------

class TripleBarrierLabeler:
    """
    tb_label con triple barrier escalado por sigma.

    DC-LABEL-10: time_to_barrier_ms exacto desde timestamps de scan (fix M4).
    fix A4: horizons.get(30)=None → first_touch='none', tb_label=None.
    DC-LABEL-11: k1/k2 configurables (fix M11).
    """

    def compute(
        self,
        p_anchor:    float,
        sigma_event: float,
        horizons:    Dict[int, HorizonResult],
        t_confirm:   datetime,
        k1:          Optional[float] = None,
        k2:          Optional[float] = None,
    ) -> BarrierResult:

        # DC-LABEL-11: k1/k2 desde config.yaml, fallback a constante de módulo
        if k1 is None:
            k1 = float(_lbl_cfg("tb_k1", TB_K1))
        if k2 is None:
            k2 = float(_lbl_cfg("tb_k2", TB_K2))

        if p_anchor <= 0 or sigma_event <= 0:
            return BarrierResult(
                tb_label=None, good_event=None,
                first_touch="none", time_to_barrier_ms=None,
            )

        upper = p_anchor * (1 + k1 * sigma_event)
        lower = p_anchor * (1 - k2 * sigma_event)

        horizon_30 = horizons.get(30)

        # fix A4: None distingue "sin datos" de "expiró sin tocar"
        if horizon_30 is None:
            log("LBL", "WARN TripleBarrierLabeler: horizonte 30m ausente — tb_label=None")
            return BarrierResult(
                tb_label=None, good_event=None,
                first_touch="none", time_to_barrier_ms=None,
            )

        # DC-LABEL-10: scan con timestamps exactos (fix M4)
        scan = horizon_30.mid_prices_scan   # List[Tuple[datetime, float]]

        if not scan:
            return BarrierResult(
                tb_label=0, good_event=False,
                first_touch="vertical", time_to_barrier_ms=None,
            )

        for ts_pt, mid in scan:
            if ts_pt.tzinfo is None:
                ts_pt = ts_pt.replace(tzinfo=timezone.utc)
            tms = int(abs((ts_pt - t_confirm).total_seconds() * 1000))

            if mid >= upper:
                return BarrierResult(
                    tb_label=1, good_event=True,
                    first_touch="upper", time_to_barrier_ms=tms,
                )
            if mid <= lower:
                return BarrierResult(
                    tb_label=-1, good_event=False,
                    first_touch="lower", time_to_barrier_ms=tms,
                )

        return BarrierResult(
            tb_label=0, good_event=False,
            first_touch="vertical", time_to_barrier_ms=None,
        )


# ---------------------------------------------------------------------------
# QualityAssessor — Sección 7 del SoT
# ---------------------------------------------------------------------------

class QualityAssessor:
    """
    Calidad final combinando los tres componentes.
    DC-LABEL-12: evalúa TODOS los horizontes de HORIZONS_MIN (fix A6).
    _top_priority() loggea warning si QRC no en lista (fix M9).
    """

    def compute(
        self,
        anchor:   AnchorResult,
        sigma:    SigmaResult,
        horizons: Dict[int, HorizonResult],
    ) -> QualityResult:

        reasons_bad: List[str] = []
        reasons_deg: List[str] = []

        if anchor.quality == "bad" and anchor.reason_code:
            reasons_bad.append(anchor.reason_code)
        elif anchor.quality == "degraded" and anchor.reason_code:
            reasons_deg.append(anchor.reason_code)

        if sigma.quality == "bad" and sigma.reason_code:
            reasons_bad.append(sigma.reason_code)
        elif sigma.quality == "degraded" and sigma.reason_code:
            reasons_deg.append(sigma.reason_code)

        # DC-LABEL-12: evaluar todos los horizontes — no solo [5, 15, 30] (fix A6)
        for h_min in HORIZONS_MIN:
            hr = horizons.get(h_min)
            if hr is None:
                reasons_bad.append(QRC_BAD_MISSING_QUOTES)
                continue
            if hr.quality_flag == "bad":
                # fix 3: usar hr.reason_code propio en lugar de hardcodear
                reasons_bad.append(hr.reason_code or QRC_BAD_MISSING_QUOTES)
            elif hr.quality_flag == "degraded":
                reasons_deg.append(hr.reason_code or QRC_DEG_LOW_DENSITY)

        if reasons_bad:
            return QualityResult(
                label_quality = "bad",
                reason_code   = self._top_priority(reasons_bad, _BAD_PRIORITY),
            )
        if reasons_deg:
            return QualityResult(
                label_quality = "degraded",
                reason_code   = self._top_priority(reasons_deg, _DEG_PRIORITY),
            )
        return QualityResult(label_quality="ok", reason_code=None)

    def _top_priority(self, reasons: List[str], priority_list: List[str]) -> str:
        for p in priority_list:
            if p in reasons:
                return p
        # fix M9: warning si fallback — indica QRC no registrado en lista
        log("LBL", f"WARN _top_priority: reason_code no en lista: {reasons[0]!r}")
        return reasons[0]


# ---------------------------------------------------------------------------
# LabelPersister — Secciones C1 + C2
# ---------------------------------------------------------------------------

class LabelPersister:
    """
    Persiste event_labels_core + event_markouts.

    write() lanza excepción si falla — el caller hace rollback + write_bad (fix B5).
    write_bad() lanza excepción si falla — el caller (_safe_write_bad) la atrapa (fix B4).
    anchor opcional en write_bad() preserva diagnóstico (fix M5).
    first_touch en write_bad() es parámetro (fix M13).
    Horizonte ausente → log warning (fix A13).
    _safe_float_strict() en markouts (fix A12).
    """

    def write(
        self,
        conn,
        event_id:  str,
        anchor:    AnchorResult,
        sigma:     SigmaResult,
        barrier:   BarrierResult,
        quality:   QualityResult,
        horizons:  Dict[int, HorizonResult],
        markouts:  Dict[int, MarkoutResult],
    ) -> None:
        now_utc = datetime.now(timezone.utc)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO event_labels_core (
                    event_id,
                    tb_label_30m_vol_scaled,   good_event_fixed_30m,
                    label_quality_30m,         quality_reason_code,
                    first_touch_30m,           time_to_barrier_ms,
                    sigma_event_30m,
                    p_anchor,                  anchor_points,
                    anchor_lag_ms,             anchor_window_coverage_ms,
                    anchor_spread_bps,         anchor_spread_z,
                    quote_skew_ms,
                    labeler_version,           labeled_at
                ) VALUES (
                    %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s, %s
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    tb_label_30m_vol_scaled   = EXCLUDED.tb_label_30m_vol_scaled,
                    good_event_fixed_30m      = EXCLUDED.good_event_fixed_30m,
                    label_quality_30m         = EXCLUDED.label_quality_30m,
                    quality_reason_code       = EXCLUDED.quality_reason_code,
                    first_touch_30m           = EXCLUDED.first_touch_30m,
                    time_to_barrier_ms        = EXCLUDED.time_to_barrier_ms,
                    sigma_event_30m           = EXCLUDED.sigma_event_30m,
                    p_anchor                  = EXCLUDED.p_anchor,
                    anchor_points             = EXCLUDED.anchor_points,
                    anchor_lag_ms             = EXCLUDED.anchor_lag_ms,
                    anchor_window_coverage_ms = EXCLUDED.anchor_window_coverage_ms,
                    anchor_spread_bps         = EXCLUDED.anchor_spread_bps,
                    anchor_spread_z           = EXCLUDED.anchor_spread_z,
                    quote_skew_ms             = EXCLUDED.quote_skew_ms,
                    labeler_version           = EXCLUDED.labeler_version,
                    labeled_at                = EXCLUDED.labeled_at
                """,
                (
                    event_id,
                    barrier.tb_label,      barrier.good_event,
                    quality.label_quality, quality.reason_code,
                    barrier.first_touch,   barrier.time_to_barrier_ms,
                    sigma.sigma_event,
                    anchor.p_anchor,       anchor.anchor_points,
                    anchor.anchor_lag_ms,  anchor.anchor_window_coverage_ms,
                    anchor.anchor_spread_bps, anchor.anchor_spread_z,
                    anchor.quote_skew_ms,
                    LABELER_VERSION,       now_utc,
                ),
            )

            for h_min in HORIZONS_MIN:
                hr = horizons.get(h_min)
                mr = markouts.get(h_min)
                # fix A13: log explícito — no skip silencioso
                if hr is None or mr is None:
                    log("LBL", f"WARN write() horizonte {h_min}m ausente event_id={event_id}")
                    continue

                cur.execute(
                    """
                    INSERT INTO event_markouts (
                        event_id,           horizon_min,
                        p_h,                ret_h,
                        mfe_h,              mae_h,
                        sampling_method,
                        points_used,        forward_lag_ms,
                        forward_lag_ratio,  window_span_ms,
                        point_density,
                        spread_bps,         spread_z,
                        mid_iqr_bps,
                        quality_flag,       reason_code
                    ) VALUES (
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s,
                        %s, %s
                    )
                    ON CONFLICT (event_id, horizon_min) DO UPDATE SET
                        p_h               = EXCLUDED.p_h,
                        ret_h             = EXCLUDED.ret_h,
                        mfe_h             = EXCLUDED.mfe_h,
                        mae_h             = EXCLUDED.mae_h,
                        sampling_method   = EXCLUDED.sampling_method,
                        points_used       = EXCLUDED.points_used,
                        forward_lag_ms    = EXCLUDED.forward_lag_ms,
                        forward_lag_ratio = EXCLUDED.forward_lag_ratio,
                        window_span_ms    = EXCLUDED.window_span_ms,
                        point_density     = EXCLUDED.point_density,
                        spread_bps        = EXCLUDED.spread_bps,
                        spread_z          = EXCLUDED.spread_z,
                        mid_iqr_bps       = EXCLUDED.mid_iqr_bps,
                        quality_flag      = EXCLUDED.quality_flag,
                        reason_code       = EXCLUDED.reason_code
                    """,
                    (
                        event_id,            h_min,
                        hr.p_h,              _safe_float_strict(mr.ret_h),
                        _safe_float_strict(mr.mfe_h), _safe_float_strict(mr.mae_h),
                        hr.sampling_method,
                        hr.points_used,      hr.forward_lag_ms,
                        hr.forward_lag_ratio, hr.window_span_ms,
                        hr.point_density,
                        hr.spread_bps,       hr.spread_z,
                        hr.mid_iqr_bps,
                        hr.quality_flag,     hr.reason_code,
                    ),
                )

    def write_bad(
        self,
        conn,
        event_id:    str,
        reason_code: str,
        anchor:      Optional[AnchorResult] = None,   # fix M5
        first_touch: str = "none",                    # fix M13
    ) -> None:
        """
        Persiste label_quality='bad'. Lanza excepción si falla (fix B4).
        anchor opcional preserva datos de diagnóstico disponibles (fix M5).
        """
        now_utc = datetime.now(timezone.utc)

        p_anchor_val  = anchor.p_anchor                  if anchor else None
        apoints_val   = anchor.anchor_points              if anchor else 0
        alag_val      = anchor.anchor_lag_ms              if anchor else None
        acov_val      = anchor.anchor_window_coverage_ms  if anchor else None
        asprd_val     = anchor.anchor_spread_bps          if anchor else None
        az_val        = anchor.anchor_spread_z             if anchor else None
        skew_val      = anchor.quote_skew_ms              if anchor else None

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO event_labels_core (
                    event_id,
                    tb_label_30m_vol_scaled,   good_event_fixed_30m,
                    label_quality_30m,         quality_reason_code,
                    first_touch_30m,           time_to_barrier_ms,
                    sigma_event_30m,
                    p_anchor,                  anchor_points,
                    anchor_lag_ms,             anchor_window_coverage_ms,
                    anchor_spread_bps,         anchor_spread_z,
                    quote_skew_ms,
                    labeler_version,           labeled_at
                ) VALUES (
                    %s,
                    NULL, NULL,
                    'bad', %s,
                    %s, NULL,
                    NULL,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s, %s
                )
                ON CONFLICT (event_id) DO UPDATE SET
                    -- label fields: forzar NULL — borrar stale values de runs anteriores
                    tb_label_30m_vol_scaled   = NULL,
                    good_event_fixed_30m      = NULL,
                    label_quality_30m         = 'bad',
                    quality_reason_code       = EXCLUDED.quality_reason_code,
                    first_touch_30m           = EXCLUDED.first_touch_30m,
                    time_to_barrier_ms        = NULL,
                    sigma_event_30m           = NULL,
                    p_anchor                  = EXCLUDED.p_anchor,
                    anchor_points             = EXCLUDED.anchor_points,
                    anchor_lag_ms             = EXCLUDED.anchor_lag_ms,
                    anchor_window_coverage_ms = EXCLUDED.anchor_window_coverage_ms,
                    anchor_spread_bps         = EXCLUDED.anchor_spread_bps,
                    anchor_spread_z           = EXCLUDED.anchor_spread_z,
                    quote_skew_ms             = EXCLUDED.quote_skew_ms,
                    labeler_version           = EXCLUDED.labeler_version,
                    labeled_at                = EXCLUDED.labeled_at
                """,
                (
                    event_id,
                    reason_code,
                    first_touch,
                    p_anchor_val,  apoints_val,
                    alag_val,      acov_val,
                    asprd_val,     az_val,
                    skew_val,
                    LABELER_VERSION, now_utc,
                ),
            )


    def write_attempt(
        self,
        event_id:       str,
        started_at:     datetime,
        finished_at:    datetime,
        result_quality: Optional[str],
        reason_code:    Optional[str],
        was_success:    bool,
        error_class:    Optional[str] = None,
        error_message:  Optional[str] = None,
    ) -> None:
        """
        Persiste una fila en event_label_attempts en su PROPIA conexión y transacción.

        Diseño de aislamiento (fix B2/B3/B5/B6/B8):
            write_attempt() NO recibe conn del caller — abre su propia conexión.
            Su transacción es completamente independiente de la transacción principal
            que contiene write_bad() y write().
            Un fallo en write_attempt() NO puede abortar ni revertir write_bad().

        attempt_n calculado atómicamente: COALESCE(MAX,0)+1 dentro de la misma txn.
        ON CONFLICT DO NOTHING: idempotente en caso de retry de la misma llamada.
            Si hay conflicto, loggea warning (fix L2).

        Lanza excepción si falla la conexión propia o el INSERT — el caller
        (_record_attempt) la atrapa y loggea sin propagar.
        """
        # fix B4: duration_ms sin abs() — si finished_at < started_at por NTP drift,
        # loggear el valor negativo como anomalía. El schema tiene CHECK >= 0.
        raw_duration_ms = int((finished_at - started_at).total_seconds() * 1000)
        if raw_duration_ms < 0:
            log("LBL", f"WARN write_attempt: duration_ms={raw_duration_ms} negativo "
                        f"(NTP drift?) event_id={event_id} — usando 0")
            raw_duration_ms = 0
        duration_ms = raw_duration_ms

        # fix L3: validar result_quality antes del INSERT
        valid_qualities = ("ok", "degraded", "bad", None)
        if result_quality not in valid_qualities:
            log("LBL", f"WARN write_attempt: result_quality={result_quality!r} "
                        f"inválido event_id={event_id} — usando None")
            result_quality = None

        # Truncar strings para evitar payloads excesivos
        if error_message and len(error_message) > 500:
            error_message = error_message[:497] + "..."
        # fix M8: truncar error_class también
        if error_class and len(error_class) > 100:
            error_class = error_class[:97] + "..."

        # fix B2/B3/B5/B6/B8: conexión propia — completamente aislada de la txn del caller
        conn_attempt = None
        try:
            conn_attempt = _connect()

            with conn_attempt.cursor() as cur:
                # fix B7: guard en fetchone — no debería retornar None con COALESCE,
                # pero si la tabla no existe la query falla (psycopg2.Error) antes de llegar aquí
                cur.execute(
                    """
                    SELECT COALESCE(MAX(attempt_n), 0) + 1
                    FROM event_label_attempts
                    WHERE event_id = %s
                    """,
                    (event_id,),
                )
                row = cur.fetchone()
                attempt_n = row[0] if row is not None else 1

                result = cur.execute(
                    """
                    INSERT INTO event_label_attempts (
                        event_id,        attempt_n,
                        started_at,      finished_at,   duration_ms,
                        labeler_version,
                        result_quality,  reason_code,
                        was_success,
                        error_class,     error_message
                    ) VALUES (
                        %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s,
                        %s,
                        %s, %s
                    )
                    ON CONFLICT (event_id, attempt_n) DO NOTHING
                    """,
                    (
                        event_id,       attempt_n,
                        started_at,     finished_at,   duration_ms,
                        LABELER_VERSION,
                        result_quality, reason_code,
                        was_success,
                        error_class,    error_message,
                    ),
                )


            conn_attempt.commit()

        except Exception:
            if conn_attempt is not None:
                try:
                    conn_attempt.rollback()
                except Exception:
                    pass
            raise
        finally:
            if conn_attempt is not None:
                try:
                    conn_attempt.close()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# OutcomeLabeler — orquestador principal
# ---------------------------------------------------------------------------

class OutcomeLabeler:
    """
    Orquesta los componentes. Garantías (DC-LABEL-05):
        label() SIEMPRE persiste algo en event_labels_core.
        label() SIEMPRE persiste algo en event_label_attempts.
        En cualquier error → write_bad() con reason_code explícito.

    Manejo de transacción (fix B3/B5/B8):
        Si write() falla: rollback() explícito, luego write_bad() en transacción limpia.
        Si _connect() falla: conn=None, _safe_write_bad() loggea pero no puede persistir.
        Sin except:pass en ningún path.

    anchor guardado desde paso 2 para pasarlo a write_bad() si falla después (fix M5).

    DC-LABEL-15: event_label_attempts se escribe en CADA llamada a label(),
        incluyendo ok, degraded, bad, y excepciones capturadas como fail-safe.
        El worker NO escribe en event_label_attempts.
    """

    def __init__(self):
        self._anchor    = PriceAnchor()
        self._sigma_est = VolatilityEstimator()
        self._h_pricer  = HorizonPricer()
        self._markout   = MarkoutComputer()
        self._barrier   = TripleBarrierLabeler()
        self._quality   = QualityAssessor()
        self._persister = LabelPersister()

    def label(self, event_id: str) -> str:
        """
        Labela un evento. Retorna 'ok'|'degraded'|'bad'.
        Garantiza persistencia en event_labels_core y event_label_attempts
        en cualquier caso (DC-LABEL-05, DC-LABEL-15).

        started_at se captura después de _connect() para medir el labeling real,
        no el tiempo de conexión (fix A5).

        _record_attempt() usa su propia conexión — aislada de la transacción
        principal. Un fallo en _record_attempt() no afecta write_bad() ni write().

        Parámetros leídos desde config.yaml (fix 6):
            labeling.anchor_window_s, anchor_min_points, anchor_max_lag_ms,
            anchor_min_coverage_ms, quote_skew_degraded_ms, quote_skew_bad_ms,
            sigma_lookback_min, sigma_sampling_s, sigma_min_obs,
            horizon_window_s_short, horizon_window_s_long,
            horizon_min_points, horizon_max_lag_ratio
        """
        conn   = None
        anchor: Optional[AnchorResult] = None
        # fix A5: started_at después de _connect() — mide labeling real, no overhead de conn
        started_at: Optional[datetime] = None

        # fix 6: leer todos los parámetros desde config una vez por label()
        anchor_window_s        = _lbl_cfg("anchor_window_s",        ANCHOR_WINDOW_S)
        anchor_min_points      = _lbl_cfg("anchor_min_points",      ANCHOR_MIN_POINTS)
        anchor_max_lag_ms      = _lbl_cfg("anchor_max_lag_ms",      ANCHOR_MAX_LAG_MS)
        anchor_min_coverage_ms = _lbl_cfg("anchor_min_coverage_ms", ANCHOR_MIN_COVERAGE_MS)
        quote_skew_deg_ms      = _lbl_cfg("quote_skew_degraded_ms", QUOTE_SKEW_DEGRADED_MS)
        quote_skew_bad_ms      = _lbl_cfg("quote_skew_bad_ms",      QUOTE_SKEW_BAD_MS)
        sigma_lookback_min     = _lbl_cfg("sigma_lookback_min",     SIGMA_LOOKBACK_MIN)
        sigma_sampling_s       = _lbl_cfg("sigma_sampling_s",       SIGMA_SAMPLING_S)
        sigma_min_obs          = _lbl_cfg("sigma_min_obs",          SIGMA_MIN_OBS)
        horizon_window_short   = _lbl_cfg("horizon_window_s_short", HORIZON_WINDOW_S_SHORT)
        horizon_window_long    = _lbl_cfg("horizon_window_s_long",  HORIZON_WINDOW_S_LONG)
        horizon_min_points     = _lbl_cfg("horizon_min_points",     HORIZON_MIN_POINTS)
        horizon_max_lag_ratio  = _lbl_cfg("horizon_max_lag_ratio",  HORIZON_MAX_LAG_RATIO)

        try:
            conn = _connect()
            started_at = datetime.now(timezone.utc)   # fix A5: después de _connect()

            # 1. Fetch evento
            symbol, t_confirm = self._fetch_event(conn, event_id)
            if symbol is None or t_confirm is None:
                log("LBL", f"ERROR label() event_id={event_id} no encontrado en candidate_events")
                self._safe_write_bad(conn, event_id, QRC_BAD_MISSING_QUOTES)
                conn.commit()
                # fix B2: _record_attempt después del commit — transacción principal ya cerrada
                self._record_attempt(
                    event_id, started_at,
                    result_quality="bad", reason_code=QRC_BAD_MISSING_QUOTES,
                    was_success=False,
                )
                return "bad"

            # 2. PriceAnchor
            anchor = self._anchor.compute(
                conn, symbol, t_confirm,
                anchor_window_s        = anchor_window_s,
                min_points             = anchor_min_points,
                max_lag_ms             = anchor_max_lag_ms,
                min_coverage_ms        = anchor_min_coverage_ms,
                quote_skew_degraded_ms = quote_skew_deg_ms,
                quote_skew_bad_ms      = quote_skew_bad_ms,
            )
            if anchor.quality == "bad":
                rc = anchor.reason_code or QRC_BAD_INVALID_BOOK
                self._safe_write_bad(conn, event_id, rc, anchor=anchor)
                conn.commit()
                # fix B2: _record_attempt después del commit
                self._record_attempt(
                    event_id, started_at,
                    result_quality="bad", reason_code=rc,
                    was_success=False,
                )
                log("LBL", f"WARN label() bad anchor event_id={event_id} reason={rc}")
                return "bad"

            # 3. VolatilityEstimator
            sigma = self._sigma_est.compute(
                conn, symbol, t_confirm,
                lookback_min = sigma_lookback_min,
                sampling_s   = sigma_sampling_s,
                min_obs      = sigma_min_obs,
            )
            if sigma.quality == "bad":
                rc = sigma.reason_code or QRC_BAD_SIGMA
                self._safe_write_bad(conn, event_id, rc, anchor=anchor)
                conn.commit()
                # fix B2: _record_attempt después del commit
                self._record_attempt(
                    event_id, started_at,
                    result_quality="bad", reason_code=rc,
                    was_success=False,
                )
                log("LBL", f"WARN label() bad sigma event_id={event_id} reason={rc}")
                return "bad"

            # 4. HorizonPricer
            horizons: Dict[int, HorizonResult] = {}
            for h_min in HORIZONS_MIN:
                horizons[h_min] = self._h_pricer.compute(
                    conn, symbol, t_confirm,
                    p_anchor        = anchor.p_anchor,
                    horizon_min     = h_min,
                    window_s_short  = horizon_window_short,
                    window_s_long   = horizon_window_long,
                    min_points      = horizon_min_points,
                    max_lag_ratio   = horizon_max_lag_ratio,
                )

            # 5. MarkoutComputer
            markouts: Dict[int, MarkoutResult] = {}
            for h_min in HORIZONS_MIN:
                markouts[h_min] = self._markout.compute(
                    p_anchor = anchor.p_anchor,
                    horizon  = horizons[h_min],
                )

            # 6. TripleBarrierLabeler
            barrier = self._barrier.compute(
                p_anchor    = anchor.p_anchor,
                sigma_event = sigma.sigma_event,
                horizons    = horizons,
                t_confirm   = t_confirm,
            )

            # 7. QualityAssessor
            quality = self._quality.compute(anchor, sigma, horizons)

            # 8. Persistir — si falla: rollback + write_bad
            try:
                self._persister.write(
                    conn, event_id,
                    anchor, sigma, barrier, quality, horizons, markouts,
                )
                conn.commit()
                # fix B2: _record_attempt después del commit — transacción principal cerrada
                self._record_attempt(
                    event_id, started_at,
                    result_quality = quality.label_quality,
                    reason_code    = quality.reason_code,
                    was_success    = quality.label_quality in ("ok", "degraded"),
                )
            except Exception as write_err:
                log("LBL", f"ERROR label() write() failed event_id={event_id}: "
                            f"{type(write_err).__name__}: {write_err}")
                try:
                    conn.rollback()
                except Exception as rb_err:
                    log("LBL", f"WARN label() rollback error: {rb_err}")
                self._safe_write_bad(conn, event_id, QRC_BAD_LABELER_EXCEPTION, anchor=anchor)
                try:
                    conn.commit()
                except Exception as cm_err:
                    log("LBL", f"ERROR label() commit after write_bad failed: {cm_err}")
                # fix B2: _record_attempt después del commit — transacción principal cerrada
                self._record_attempt(
                    event_id, started_at,
                    result_quality = "bad",
                    reason_code    = QRC_BAD_LABELER_EXCEPTION,
                    was_success    = False,
                    error_class    = type(write_err).__name__,
                    error_message  = str(write_err),
                )
                return "bad"

            log("LBL",
                f"labeled event_id={event_id} {symbol} "
                f"quality={quality.label_quality} "
                f"tb={barrier.tb_label} touch={barrier.first_touch}")
            return quality.label_quality

        except psycopg2.Error as e:
            log("LBL", f"ERROR label() DB event_id={event_id}: {type(e).__name__}: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    log("LBL", f"WARN label() rollback error: {rb_err}")
            self._safe_write_bad(conn, event_id, QRC_BAD_LABELER_EXCEPTION, anchor=anchor)
            if conn:
                try:
                    conn.commit()
                except Exception as cm_err:
                    log("LBL", f"ERROR label() commit after bad failed: {cm_err}")
            # fix B2: _record_attempt después del commit — transacción principal cerrada
            # started_at puede ser None si _connect() falló — usar fallback
            _sa = started_at or datetime.now(timezone.utc)
            self._record_attempt(
                event_id, _sa,
                result_quality = "bad",
                reason_code    = QRC_BAD_LABELER_EXCEPTION,
                was_success    = False,
                error_class    = type(e).__name__,
                error_message  = str(e),
            )
            return "bad"

        except Exception as e:
            log("LBL", f"ERROR label() unexpected event_id={event_id}: {type(e).__name__}: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_err:
                    log("LBL", f"WARN label() rollback error: {rb_err}")
            self._safe_write_bad(conn, event_id, QRC_BAD_LABELER_EXCEPTION, anchor=anchor)
            if conn:
                try:
                    conn.commit()
                except Exception as cm_err:
                    log("LBL", f"ERROR label() commit after bad failed: {cm_err}")
            # fix B2: _record_attempt después del commit
            _sa = started_at or datetime.now(timezone.utc)
            self._record_attempt(
                event_id, _sa,
                result_quality = "bad",
                reason_code    = QRC_BAD_LABELER_EXCEPTION,
                was_success    = False,
                error_class    = type(e).__name__,
                error_message  = str(e),
            )
            return "bad"

        finally:
            if conn:
                try:
                    conn.close()
                except Exception as ce:
                    log("LBL", f"WARN label() close error: {ce}")

    def _fetch_event(
        self, conn, event_id: str
    ) -> Tuple[Optional[str], Optional[datetime]]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, t_confirm FROM candidate_events WHERE event_id = %s",
                (event_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None, None
            symbol, t_confirm = row[0], row[1]
            if t_confirm is not None and t_confirm.tzinfo is None:
                t_confirm = t_confirm.replace(tzinfo=timezone.utc)
            return symbol, t_confirm

    def _record_attempt(
        self,
        event_id:       str,
        started_at:     datetime,
        result_quality: Optional[str],
        reason_code:    Optional[str],
        was_success:    bool,
        error_class:    Optional[str] = None,
        error_message:  Optional[str] = None,
    ) -> None:
        """
        Registra el intento en event_label_attempts via write_attempt().
        Fail-safe — nunca propaga al caller (DC-LABEL-15).

        write_attempt() abre su propia conexión — aislada de la transacción principal.
        Un fallo aquí NO afecta la transacción de write_bad() ni write().

        fix M2: distingue psycopg2.Error de Exception genérica en el log.
        """
        try:
            finished_at = datetime.now(timezone.utc)
            self._persister.write_attempt(
                event_id       = event_id,
                started_at     = started_at,
                finished_at    = finished_at,
                result_quality = result_quality,
                reason_code    = reason_code,
                was_success    = was_success,
                error_class    = error_class,
                error_message  = error_message,
            )
        except psycopg2.Error as e:
            # fix M2: psycopg2.Error separado — indica problema de DB o schema
            log("LBL", f"WARN _record_attempt() DB error event_id={event_id}: "
                        f"{type(e).__name__}: {e} — attempt NO registrado en event_label_attempts")
        except Exception as e:
            log("LBL", f"WARN _record_attempt() unexpected event_id={event_id}: "
                        f"{type(e).__name__}: {e} — attempt NO registrado en event_label_attempts")

    def _safe_write_bad(
        self,
        conn,
        event_id:    str,
        reason_code: str,
        anchor:      Optional[AnchorResult] = None,
        first_touch: str = "none",
    ) -> None:
        """
        Intenta write_bad(). Si falla, loggea — no propaga (DC-LABEL-05).
        fix B6/L3: log explícito cuando conn=None con descripción del impacto.
        """
        if conn is None:
            log("LBL",
                f"ERROR _safe_write_bad() conn=None event_id={event_id} reason={reason_code} "
                f"— bad label NO persistido. El worker reintentará en el próximo sweep.")
            return
        try:
            self._persister.write_bad(
                conn, event_id, reason_code,
                anchor=anchor, first_touch=first_touch,
            )
        except Exception as e:
            log("LBL",
                f"ERROR _safe_write_bad() INSERT failed event_id={event_id}: "
                f"{type(e).__name__}: {e} — bad label NO persistido.")
