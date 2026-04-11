"""
dataset_capture.py — KTS TradeSeeker · Dataset Foundation · Fase 1
v1.1 — 2026-04-10

Responsabilidad:
    Persistir todos los candidatos que alcanzan el estado FSM "confirming"
    en las tablas candidate_events + event_features_pti.

    Independiente del outcome tracker (signals / signal_outcomes / outcome_jobs).
    No reemplaza nada — agrega captura para el dataset futuro de scoring.

Principios (no negociables):
    PIT correctness: toda feature refleja el estado observable en t_confirm.
    No lookahead: ningún dato posterior a t_confirm entra en las tablas.
    Fail-open: un error nunca propaga excepción al caller (fast_loop).
    Degradación explícita: si una feature falta → NULL + is_degraded=True.

Interfaz pública:
    capture(snap: CandidateSnapshot) -> Optional[str]
        Persiste el evento. Retorna event_id si OK, None si error.

Decisiones cerradas:
    DC-DATASET-01: detector_version = bot_version. Sin versionado separado.
    DC-DATASET-02: capturar todos los candidatos en confirming, incluyendo
                   los rechazados por entry_gate. entry_gate_decision registrado.

Fixes aplicados (v1.1):
    P1:  feature_latency_ms clampada a max(0.0, ...) via _safe_ms()
    P4:  t_event documentado explícitamente como recorded_at en F1
    P5:  config_snapshot validado como dict antes de cualquier cómputo
    P6:  symbol y detector_version validados al inicio — log defensivo con sym
    P7:  _make_feature_set_hash convertida a constante de módulo _FEATURE_SET_HASH
    P9:  field eliminado del import (no se usaba)
    P10: statement_timeout=5000ms en _connect()
    N1:  validación de snap es lo primero — antes de cualquier cómputo
    N2:  _connect() cierra la conexión si falla después de crearla
    N5:  round() protegido con _safe_ms() que clampea a [0.0, inf)
    N6:  stage_pct movido a opcional — su ausencia es válida en no_trigger_price
    N7:  log de validación usa sym = snap.symbol or '<unknown>'
    N8:  feature_keys como constante de módulo _FEATURE_KEYS
    T1:  operator_window y market_regime normalizados antes del INSERT

CodeGuard:
    - sin IO de red — solo DB local
    - sin except:pass — todos los except loggean
    - fail-open garantizado en todos los paths
    - insert atómico candidate_events → event_features_pti (una transacción)
    - idempotente: ON CONFLICT DO NOTHING en ambas tablas
    - no modifica lógica de detección ni entry_gate
    - statement_timeout en DB para no bloquear fast_loop
"""

import hashlib
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import psycopg2

from utils import log

# ---------------------------------------------------------------------------
# Versión del schema de features
# Incrementar cuando cambie la semántica o el conjunto de features.
# ---------------------------------------------------------------------------

FEATURE_VERSION = "v1.0"

# Lineage — constantes de módulo
_SOURCE_COMPONENT = "fast_loop.confirming_snapshot"
_TRANSFORMATION   = "confirming.snapshot.v1"
_FSM_STATE_PATH   = "idle→building→triggered→confirming"
_TYPE_HINT        = "pump_fast_candidate"

# Dominios válidos para campos con CHECK en el schema (T1)
_VALID_ENTRY_GATE = frozenset({"passed", "rejected", "unknown"})
_VALID_OP_WINDOW  = frozenset({"awake", "sleep", "unknown"})

# feature_set_hash — constante de módulo (P7/N8)
# Representa el conjunto de feature keys activas en esta versión del schema.
# Recalcular si se agregan o eliminan features del INSERT.
_FEATURE_KEYS: Tuple[str, ...] = (
    "stage_pct", "delta_pct", "t_build_ms", "t_confirm_ms",
    "move_start_price", "noise_at_start", "peak_since_start",
    "cusum_at_trigger", "cusum_shadow_pass",
    "rel_vol", "confirm_ratio", "n_trades",
    "utc_hour", "market_regime", "operator_window",
)

def _sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()

_FEATURE_SET_HASH: str = _sha256_str(
    json.dumps(
        {"feature_version": FEATURE_VERSION, "keys": list(_FEATURE_KEYS)},
        sort_keys=True,
    )
)[:16]


# ---------------------------------------------------------------------------
# CandidateSnapshot — contrato de entrada
# ---------------------------------------------------------------------------

@dataclass
class CandidateSnapshot:
    """
    Snapshot del candidato en el instante de transición FSM a "confirming".
    Todos los campos reflejan el estado observable en t_confirm (PIT).

    El caller (fast_loop.py) es responsable de poblar correctamente.
    Campos opcionales → None si no disponibles → marcados como stale.

    DC-DATASET-01: detector_version = bot_version del caller.
    DC-DATASET-02: entry_gate_decision + rejection_reason registran la
                   decisión operacional. Capturar incluso si entry_gate rechaza.
    """
    # ── Obligatorios ─────────────────────────────────────────────────────
    symbol:             str
    detector_version:   str    # = bot_version (DC-DATASET-01)
    config_snapshot:    dict   # bloque rules.fast_ts activo en t_confirm

    # ── Timing ───────────────────────────────────────────────────────────
    t_trigger:          Optional[datetime] = None  # momento en que delta >= pump_pct
    t_confirm:          Optional[datetime] = None  # anchor PIT — usa now() si None

    # ── TimingBlock ──────────────────────────────────────────────────────
    # stage_pct es OPCIONAL (N6): puede ser None cuando entry_gate_decision='unknown'
    # (caso no_trigger_price). No es fallo de captura — es resultado válido.
    stage_pct:          Optional[float] = None
    delta_pct:          Optional[float] = None
    t_build_ms:         Optional[float] = None   # 0.0 si FSM estaba en idle al trigger
    t_confirm_ms:       Optional[float] = None
    move_start_price:   Optional[float] = None
    noise_at_start:     Optional[float] = None
    peak_since_start:   Optional[float] = None
    cusum_at_trigger:   Optional[float] = None
    cusum_shadow_pass:  Optional[bool]  = None

    # ── FlowBlock ────────────────────────────────────────────────────────
    rel_vol:            Optional[float] = None
    confirm_ratio:      Optional[float] = None
    n_trades:           Optional[int]   = None

    # ── ContextBlock ─────────────────────────────────────────────────────
    utc_hour:           Optional[int]   = None
    market_regime:      Optional[str]   = None
    operator_window:    Optional[str]   = None

    # ── Decisión operacional (DC-DATASET-02) ─────────────────────────────
    entry_gate_decision: Optional[str]  = None  # "passed"|"rejected"|"unknown"
    rejection_reason:    Optional[str]  = None  # "late_entry"|"no_trigger_price"|None

    # ── Metadata de captura ───────────────────────────────────────────────
    # time.monotonic() antes de ensamblar el snapshot en fast_loop.
    # Si None → capture() mide desde su propio inicio (subestima latencia real).
    capture_started_at: Optional[float] = None


# ---------------------------------------------------------------------------
# Helpers internos
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


def _safe_int(val: Any) -> Optional[int]:
    """int limpio o None si inválido."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_ms(val: float) -> float:
    """
    Clampea un valor de milisegundos a [0.0, inf).
    Evita negativos por drift de monotonic clock y OverflowError en round(). (P1/N5)
    """
    try:
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return max(0.0, val)
    except Exception:
        return 0.0


def _sha256_dict(d: dict) -> str:
    """Hash determinístico de un dict — keys ordenadas para estabilidad."""
    canonical = json.dumps(d, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _make_event_id(
    symbol:           str,
    t_trigger:        Optional[datetime],
    t_confirm:        datetime,
    detector_version: str,
    config_hash:      str,
) -> str:
    """
    event_id determinístico y reproducible.
    Formato: "ev_" + sha256[:32] — verificado por CHECK en el schema.
    t_trigger=None produce "none" como componente — colisión posible solo
    si mismo símbolo + mismo t_confirm + mismo ciclo (prácticamente imposible).
    """
    t_trig_iso = t_trigger.isoformat() if t_trigger else "none"
    t_conf_iso = t_confirm.isoformat()
    raw = "|".join([symbol, t_trig_iso, t_conf_iso, detector_version, config_hash])
    return "ev_" + _sha256_str(raw)[:32]


def _assess_quality(snap: CandidateSnapshot) -> Tuple[bool, int, Dict[str, str]]:
    """
    Evalúa calidad de features. Retorna (is_degraded, missing_count, stale_mask).

    Críticos: delta_pct, rel_vol, confirm_ratio.
    Su ausencia indica problema real de captura → is_degraded=True.

    stage_pct es OPCIONAL (N6): puede ser None cuando entry_gate_decision='unknown'
    (caso no_trigger_price en fast_loop). No es fallo de captura.

    Opcionales: ausencia tolerable en F1 — contados pero no degradan.
    """
    critical: Dict[str, Any] = {
        "delta_pct":     snap.delta_pct,
        "rel_vol":       snap.rel_vol,
        "confirm_ratio": snap.confirm_ratio,
    }
    optional: Dict[str, Any] = {
        "stage_pct":        snap.stage_pct,
        "t_build_ms":       snap.t_build_ms,
        "t_confirm_ms":     snap.t_confirm_ms,
        "move_start_price": snap.move_start_price,
        "n_trades":         snap.n_trades,
        "utc_hour":         snap.utc_hour,
        "market_regime":    snap.market_regime,
        "operator_window":  snap.operator_window,
    }

    stale_mask: Dict[str, str] = {}
    missing_count = 0
    is_degraded   = False

    for name, val in critical.items():
        if val is None:
            stale_mask[name] = "missing_critical"
            missing_count += 1
            is_degraded = True

    for name, val in optional.items():
        if val is None:
            stale_mask[name] = "missing_optional"
            missing_count += 1

    return is_degraded, missing_count, stale_mask


def _dsn() -> str:
    # statement_timeout=5000ms: evita bloquear fast_loop si Timescale está bajo carga (P10)
    return (
        "host=localhost port=5432 dbname=tsdb "
        "user=postgres password=postgres "
        "connect_timeout=5 "
        "options='-c statement_timeout=5000'"
    )


def _connect():
    """
    Abre conexión y fuerza UTC.
    Cierra el socket si falla después de crearlo — evita leak de conexión (N2).
    """
    conn = None
    try:
        conn = psycopg2.connect(_dsn())
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
        conn.commit()
        return conn
    except Exception:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        raise


# ---------------------------------------------------------------------------
# capture() — interfaz pública
# ---------------------------------------------------------------------------

def capture(snap: CandidateSnapshot) -> Optional[str]:
    """
    Persiste un candidato en confirming en candidate_events + event_features_pti.

    Orden garantizado:
        1. INSERT candidate_events
        2. INSERT event_features_pti
    Ambas en una sola transacción — o ambas committed, o ninguna.

    Idempotente: ON CONFLICT DO NOTHING — un retry no duplica el evento.
    Nota: no distingue insert real de descarte por conflicto — ambos retornan
    event_id. En F1 es aceptable dado el diseño determinístico de event_id.

    Retorna event_id (str) si OK.
    Retorna None en cualquier error — loggea pero nunca propaga excepción.
    El caller (fast_loop.py) no debe atrapar excepciones de esta función.

    t_event en candidate_events = recorded_at en F1. (P4)
    El inicio del ciclo de fast_loop (now_ts) no se incluye en el snapshot actual.
    Campo reservado para F2+ cuando se agregue t_cycle al CandidateSnapshot.
    """
    # ── 0. Tiempo de inicio propio ────────────────────────────────────────
    capture_start = snap.capture_started_at or time.monotonic()

    # ── 1. Validación de campos obligatorios — ANTES de cualquier cómputo (N1) ──
    sym = snap.symbol or "<unknown>"   # defensivo para logs (N7)

    if not snap.symbol:
        log("DC", "ERROR capture() symbol vacío o None — abortando")
        return None

    if not snap.detector_version:
        log("DC", f"ERROR capture() detector_version vacío o None [{sym}] — abortando")
        return None

    if not isinstance(snap.config_snapshot, dict):
        log("DC", (
            f"ERROR capture() config_snapshot no es dict "
            f"(got {type(snap.config_snapshot).__name__}) [{sym}] — abortando"
        ))
        return None

    # ── 2. Anchor temporal ────────────────────────────────────────────────
    now_utc   = datetime.now(timezone.utc)
    t_confirm = snap.t_confirm or now_utc

    # Validar timezone de t_trigger — naive datetime produce timestamps desalineados (N4)
    t_trigger_safe: Optional[datetime] = None
    if snap.t_trigger is not None:
        if snap.t_trigger.tzinfo is None:
            log("DC", f"WARN capture() t_trigger es naive datetime [{sym}] — descartando")
        else:
            t_trigger_safe = snap.t_trigger

    # ── 3. Hashes ─────────────────────────────────────────────────────────
    if snap.config_snapshot:
        config_hash = _sha256_dict(snap.config_snapshot)[:16]
    else:
        log("DC", f"WARN capture() config_snapshot vacío [{sym}]")
        config_hash = _sha256_str("empty_config")[:16]

    event_id         = _make_event_id(
        snap.symbol, t_trigger_safe, t_confirm,
        snap.detector_version, config_hash,
    )
    feature_set_hash = _FEATURE_SET_HASH   # constante de módulo (P7)

    # ── 4. Calidad de features ────────────────────────────────────────────
    is_degraded, missing_count, stale_mask = _assess_quality(snap)

    # ── 5. Métricas de captura ────────────────────────────────────────────
    feature_latency_ms   = _safe_ms((time.monotonic() - capture_start) * 1000.0)
    feature_freshness_ms = _safe_ms(abs((now_utc - t_confirm).total_seconds()) * 1000.0)

    # ── 6. Normalización de campos con CHECK en schema (T1) ───────────────
    op_window = snap.operator_window if snap.operator_window in _VALID_OP_WINDOW else None
    if snap.operator_window is not None and snap.operator_window not in _VALID_OP_WINDOW:
        log("DC", f"WARN capture() operator_window inesperado {snap.operator_window!r} [{sym}] → NULL")

    market_regime = snap.market_regime if isinstance(snap.market_regime, str) else None

    entry_gate_decision = (
        snap.entry_gate_decision
        if snap.entry_gate_decision in _VALID_ENTRY_GATE
        else "unknown"
    )

    # ── 7. Stale mask → JSON ─────────────────────────────────────────────
    stale_mask_json = json.dumps(stale_mask) if stale_mask else None

    # ── 8. Persistencia ──────────────────────────────────────────────────
    conn = None
    try:
        conn = _connect()

        with conn.cursor() as cur:

            # INSERT 1 — candidate_events
            cur.execute(
                """
                INSERT INTO candidate_events (
                    event_id,            symbol,
                    t_event,             t_trigger,           t_confirm,
                    phase_at_event,      fsm_state_path,
                    type_hint,
                    entry_gate_decision, rejection_reason,
                    detector_version,    config_hash,
                    source_event_time,   recorded_at
                ) VALUES (
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s, %s
                )
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event_id,              snap.symbol,
                    now_utc,               t_trigger_safe,      t_confirm,
                    "confirming",          _FSM_STATE_PATH,
                    _TYPE_HINT,
                    entry_gate_decision,   snap.rejection_reason,
                    snap.detector_version, config_hash,
                    t_confirm,             now_utc,
                ),
            )

            # INSERT 2 — event_features_pti
            # config_hash NO va aquí — vive solo en candidate_events (fix P2 del schema)
            cur.execute(
                """
                INSERT INTO event_features_pti (
                    event_id,                feature_timestamp,

                    stage_pct,               delta_pct,
                    t_build_ms,              t_confirm_ms,
                    move_start_price,        noise_at_start,
                    peak_since_start,        cusum_at_trigger,
                    cusum_shadow_pass,

                    rel_vol,                 confirm_ratio,
                    n_trades,

                    utc_hour,                market_regime_timegate,
                    operator_window_state,

                    feature_latency_ms,      feature_freshness_ms,
                    stale_feature_mask,      is_degraded,
                    missing_feature_count,

                    detector_version,        feature_version,
                    feature_set_hash,
                    source_runtime_component, transformation_path
                ) VALUES (
                    %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event_id,                          t_confirm,

                    _safe_float(snap.stage_pct),       _safe_float(snap.delta_pct),
                    _safe_float(snap.t_build_ms),      _safe_float(snap.t_confirm_ms),
                    _safe_float(snap.move_start_price), _safe_float(snap.noise_at_start),
                    _safe_float(snap.peak_since_start), _safe_float(snap.cusum_at_trigger),
                    snap.cusum_shadow_pass,

                    _safe_float(snap.rel_vol),         _safe_float(snap.confirm_ratio),
                    _safe_int(snap.n_trades),

                    _safe_int(snap.utc_hour),          market_regime,
                    op_window,

                    round(feature_latency_ms, 2),      round(feature_freshness_ms, 2),
                    stale_mask_json,                   is_degraded,
                    missing_count,

                    snap.detector_version,             FEATURE_VERSION,
                    feature_set_hash,
                    _SOURCE_COMPONENT,                 _TRANSFORMATION,
                ),
            )

        conn.commit()

        degraded_note = " [DEGRADED]" if is_degraded else ""
        log("DC", (
            f"captured event_id={event_id} {snap.symbol} "
            f"gate={entry_gate_decision}{degraded_note} "
            f"missing={missing_count} latency={feature_latency_ms:.1f}ms"
        ))
        return event_id

    except psycopg2.Error as e:
        log("DC", f"ERROR capture() DB {sym}: {type(e).__name__}: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception as _ce:
                log("DC", f"WARN capture() rollback error: {_ce}")
        return None
    except Exception as e:
        log("DC", f"ERROR capture() unexpected {sym}: {type(e).__name__}: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception as _ce:
                log("DC", f"WARN capture() rollback error: {_ce}")
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception as _ce:
                log("DC", f"WARN capture() close error: {_ce}")
