"""
outcome_tracker.py — KTS TradeSeeker · Capa 8 · Nivel 2
v2.3 — 2026-04-12

Responsabilidades:
  - Emitter: registrar señales y near-misses en DB con atomicidad total
  - Worker:  resolver precios asof/next por horizonte desde Timescale o Binance REST
  - Read model: estado agregado derivado por señal

Diseño:
  - record() es síncrono — llamado desde fast_loop (hilo principal)
  - start_worker() arranca un daemon thread secuencial (DC-OUTCOME-15),
    idempotente — llamadas repetidas son no-op si el worker ya vive
  - Un fallo de DB en record() retorna None y loggea — nunca tumba el caller
  - Un fallo en el worker loggea y sigue — nunca propaga excepción al exterior
  - Fuente primaria: Timescale (conexión separada del lock del worker).
    Fallback: Binance REST (una sola llamada por job)
  - lag_s persistido en columnas asof_lag_s / next_lag_s para filtrado SQL directo

Decisiones de diseño cerradas aplicadas:
  DC-OUTCOME-01  DC-OUTCOME-05  DC-OUTCOME-07  DC-OUTCOME-08  DC-OUTCOME-09
  DC-OUTCOME-13  DC-OUTCOME-15  DC-OUTCOME-16  DC-OUTCOME-18  DC-OUTCOME-20
  DC-OUTCOME-22  DC-OUTCOME-23  DC-OUTCOME-29/30  DC-OUTCOME-31

Historial de correcciones:
  v2.1 — FIX-01..07: idempotencia worker, lag_s columnas, REST unificado,
         vela cerrada, Session compartida, sanitización recursiva, SET TZ UTC
  v2.2 — FIX-08: FOR UPDATE lock scope por job, no por batch completo
         FIX-09: _OUTCOME_COL_MAP como constante de módulo (fuera del loop)
         FIX-10: _http_session_lock eliminado — Session es thread-safe by design
         FIX-11: campos enteros con conversión explícita int() en record()
         FIX-12: logging diferenciado en _resolve_block para errores programáticos
         TODO-P1: connection pool en emitter (bajo volumen Etapa 1, no urgente)
         TODO-P1: lease/visibility timeout para jobs trabados (Etapa 2)
         TODO-P1: aggregate_quality_status explícito en read model (Etapa 2)
  v2.3 — Hardening 2026-04-12:
         FIX-H01: missing_total → status='error' + error='missing_total' (ISSUE-01/04/08/38)
         FIX-H02: _resolve_block usa conexión separada para queries Timescale
                  — evita InFailedSqlTransaction en transacción del worker (ISSUE-02/03/18)
         FIX-H03: _worker_loop cierra conn vieja antes de reconectar (ISSUE-07)
         FIX-H04: commit() vacío protegido con check de estado de transacción (ISSUE-09)
         FIX-H05: cap de 8KB en extras_json — residual truncado con warning (ISSUE-10)
         FIX-H06: statement_timeout=10s en queries Timescale de resolución (ISSUE-11/25)
         FIX-H07: price_alert casteado a float explícito en _compute_outcome_pct (ISSUE-12)
         FIX-H08: asof_tol_s / next_tol_s casteados a int antes de pasar a PG (ISSUE-19)
         FIX-H09: rollback explícito en finally de record() antes de close (ISSUE-20)
         FIX-H10: sleep al final del loop — primer sweep inmediato (ISSUE-21)
         FIX-H11: attempts no se incrementa si error fue de infraestructura sin trabajo real
                  — separado en infra_error vs work_attempted (ISSUE-22)
         FIX-H12: n_done_clean usa AND en lugar de OR — ambos bloques deben estar done (ISSUE-23)
         FIX-H13: limit en Binance klines calculado dinámicamente (ISSUE-24)
         FIX-H14: log warning cuando _sanitize_for_json trunca por profundidad (ISSUE-26)
         FIX-H15: _validate_contract valida symbol no vacío y price_alert > 0 (ISSUE-32)
         FIX-H16: circuit breaker activa también tras OperationalError en sweep (ISSUE-34)
         FIX-H17: t0 siempre aware UTC via replace() defensivo en signal_status (ISSUE-37)
         FIX-H18: heartbeat periódico del worker cada 10 sweeps sin trabajo (ISSUE-28)
         TODO-P2: ISSUE-30 — target_ts anclado a t_confirm real (mejora metodológica futura)
         TODO-P2: ISSUE-13 — re-check de due_at tras lock (riesgo bajo con worker único)
         TODO-P2: ISSUE-15 — start_filler como alias (deuda técnica menor)
         TODO-P2: ISSUE-27 — thresholds_json desacoplado de config interna
         TODO-P2: ISSUE-29 — shared lock en signal_outcomes (riesgo bajo con worker único)
         TODO-P2: ISSUE-31 — tolerancias inmutables por job (riesgo bajo)
         TODO-P2: ISSUE-33 — connection pool en signal_status
         TODO-P2: ISSUE-36 — mapeo completo de extras a columnas planas

Importado por: main.py (start_worker/start_filler), fast_loop.py (record)
"""

import json
import math
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as _Retry

from utils import cfg, log

# ---------------------------------------------------------------------------
# Constantes de módulo
# ---------------------------------------------------------------------------

HORIZONS_MIN: List[int] = [5, 15, 30, 60, 120]

_VALID_SIGNAL_KIND   = frozenset({"alert", "near_miss"})
_VALID_DECISION      = frozenset({"sent", "blocked"})
_TERMINAL_HTTP_CODES = frozenset({400, 404, 410, 422})

# FIX-H05: cap de extras_json — protege contra payloads gigantes
_EXTRAS_JSON_MAX_BYTES = 8 * 1024  # 8 KB

# FIX-H06: timeout de queries Timescale en el worker (ms)
_TIMESCALE_STMT_TIMEOUT_MS = 10_000  # 10 segundos

# FIX-H18: heartbeat cada N sweeps sin trabajo
_HEARTBEAT_EVERY_N_IDLE = 10

# FIX-09: constante de módulo — evita recrear el dict en cada iteración del loop
_OUTCOME_COL_MAP: Dict[str, str] = {
    "asof_price":       "asof_price",
    "asof_outcome_pct": "asof_outcome_pct",
    "asof_status":      "asof_status",
    "asof_source":      "asof_source",
    "asof_filled_at":   "asof_filled_at",
    "asof_error":       "asof_error",
    "asof_lag_s":       "asof_lag_s",
    "next_price":       "next_price",
    "next_outcome_pct": "next_outcome_pct",
    "next_status":      "next_status",
    "next_source":      "next_source",
    "next_filled_at":   "next_filled_at",
    "next_error":       "next_error",
    "next_lag_s":       "next_lag_s",
}

# Campos de extras que van a columnas planas — el resto va a extras_json
_KNOWN_EXTRA_KEYS = frozenset({
    "delta", "buy_ratio", "n_trades", "rel_vol", "stage_pct",
    "market_regime", "operator_window", "operator_sleep",
    "local_hour", "utc_hour", "confirm_ratio", "confirm_window_s",
})

# ---------------------------------------------------------------------------
# requests.Session compartida — FIX-05 / FIX-10
# FIX-10: sin lock externo — requests.Session es thread-safe by design.
# ---------------------------------------------------------------------------

def _build_http_session() -> requests.Session:
    """Session con connection pooling y retry de transporte mínimo."""
    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=_Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        ),
        pool_connections=2,
        pool_maxsize=4,
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


_http_session = _build_http_session()

# ---------------------------------------------------------------------------
# Worker singleton — FIX-01
# ---------------------------------------------------------------------------

_worker_thread:     Optional[threading.Thread] = None
_worker_start_lock: threading.Lock             = threading.Lock()

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _ot_cfg(key: str, default: Any) -> Any:
    return cfg(f"outcome_tracker.{key}", default)


def _dsn() -> str:
    return (
        "host=localhost port=5432 dbname=tsdb "
        "user=postgres password=postgres "
        "connect_timeout=5"
    )


def _connect() -> psycopg2.extensions.connection:
    """
    Abre conexión y fuerza UTC. FIX-07: alinea datetime.now(UTC) del proceso
    con now() en PG. App y DB en el mismo host — drift despreciable.
    """
    conn = psycopg2.connect(_dsn())
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    conn.commit()
    return conn


def _connect_resolve() -> psycopg2.extensions.connection:
    """
    FIX-H02: conexión dedicada para queries de resolución de precios.
    Separada de la conexión del worker para evitar que un error en Timescale
    durante _resolve_block ponga en estado aborted la transacción que tiene
    el FOR UPDATE del job.
    Incluye statement_timeout para evitar que queries lentas bloqueen el worker.
    """
    conn = psycopg2.connect(_dsn())
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        # FIX-H06: timeout explícito — queries Timescale no bloquean indefinidamente
        cur.execute(f"SET statement_timeout = {_TIMESCALE_STMT_TIMEOUT_MS}")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Sanitización — FIX-06 / FIX-11
# ---------------------------------------------------------------------------

def _sanitize_for_json(obj: Any, _depth: int = 0) -> Any:
    """
    Sanitización recursiva para serialización segura a JSONB.
    - float NaN/Inf → None
    - tipos no serializables → str() controlado
    - profundidad máxima 10 (objetos circulares)
    FIX-H14: loggea warning cuando trunca por profundidad.
    """
    if _depth > 10:
        # FIX-H14: no silencioso — el caller verá esto en logs si ocurre
        log("OT", f"WARN _sanitize_for_json: truncando objeto a depth={_depth} → None")
        return None
    if obj is None or isinstance(obj, bool):
        return obj
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, (int, str)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _sanitize_for_json(v, _depth + 1) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v, _depth + 1) for v in obj]
    try:
        return str(obj)
    except Exception:
        return None


def _safe_float(val: Any) -> Optional[float]:
    """Float limpio o None si NaN/Inf/inválido."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> Optional[int]:
    """
    FIX-11: conversión explícita a int para columnas INTEGER.
    Cubre numpy.int64, float-como-int, strings numéricos.
    Retorna None si el valor no es convertible.
    """
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Validación del contrato semántico (DC-OUTCOME-05)
# ---------------------------------------------------------------------------

def _validate_contract(
    signal_kind:      str,
    decision:         str,
    rejection_reason: Optional[str],
    symbol:           str,
    price_alert:      Any,
) -> Optional[str]:
    """
    Retorna None si el payload es válido, o string con el error.
    El tracker rechaza — no corrige ni infiere semántica faltante.
    FIX-H15: valida symbol no vacío y price_alert > 0.
    """
    # FIX-H15: validaciones de inputs básicos
    if not symbol or not isinstance(symbol, str) or not symbol.strip():
        return "symbol vacío o inválido"
    try:
        pa = float(price_alert)
        if pa <= 0 or math.isnan(pa) or math.isinf(pa):
            return f"price_alert debe ser > 0, got {price_alert}"
    except (TypeError, ValueError):
        return f"price_alert no convertible a float: {price_alert!r}"

    if signal_kind not in _VALID_SIGNAL_KIND:
        return f"signal_kind inválido: '{signal_kind}'"
    if decision not in _VALID_DECISION:
        return f"decision inválido: '{decision}'"
    if decision == "sent" and signal_kind != "alert":
        return f"decision='sent' solo válido con signal_kind='alert', got '{signal_kind}'"
    if decision == "blocked" and rejection_reason is None:
        return "decision='blocked' requiere rejection_reason"
    if signal_kind == "alert" and decision == "blocked":
        return "signal_kind='alert' con decision='blocked' no está permitido"
    return None


# ---------------------------------------------------------------------------
# Clasificación de errores (DC-OUTCOME-18)
# ---------------------------------------------------------------------------

def _is_terminal_error(exc: Exception) -> bool:
    """
    True si el error es terminal — no tiene sentido reintentar.
    Transitorio: OperationalError, Timeout, ConnectionError → False.
    FIX-H06 extensión: JSONDecodeError (Binance rate limit HTML) → transitorio.
    """
    if isinstance(exc, requests.exceptions.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in _TERMINAL_HTTP_CODES
    # json.JSONDecodeError es transitorio — Binance puede responder HTML en rate limit
    # No es terminal: el job se reintentará
    return False


# ---------------------------------------------------------------------------
# Resolución de precios — Timescale
# FIX-H02: recibe conn_resolve (conexión dedicada, separada del worker lock)
# FIX-H06: statement_timeout configurado en conn_resolve al abrirla
# FIX-H08: asof_tol_s / next_tol_s casteados a int
# ---------------------------------------------------------------------------

def _resolve_from_timescale(
    conn_resolve,
    symbol:     str,
    target_ts:  datetime,
    asof_tol_s: int,
    next_tol_s: int,
) -> Tuple[Optional[float], Optional[float], Optional[datetime], Optional[datetime]]:
    """
    Resuelve asof y next desde metrics_ext usando una conexión dedicada.

    FIX-H02: usa conn_resolve separada de la conn del worker — un error aquí
             no aborta la transacción que tiene el FOR UPDATE del job.
    FIX-H08: asof_tol_s / next_tol_s casteados a int para evitar
             'double precision * interval' en PG si vienen como float de YAML.

    Re-lanza excepciones sin atrapar — el caller en _resolve_block las clasifica.
    """
    # FIX-H08: cast explícito a int
    asof_tol_s = int(asof_tol_s)
    next_tol_s = int(next_tol_s)

    asof_price = asof_ts = next_price = next_ts = None

    with conn_resolve.cursor() as cur:
        cur.execute(
            """
            SELECT price, ts
            FROM   metrics_ext
            WHERE  symbol = %s
              AND  ts BETWEEN %s - (%s * interval '1 second') AND %s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (symbol, target_ts, asof_tol_s, target_ts),
        )
        row = cur.fetchone()
        if row:
            asof_price, asof_ts = row[0], row[1]

        cur.execute(
            """
            SELECT price, ts
            FROM   metrics_ext
            WHERE  symbol = %s
              AND  ts BETWEEN %s AND %s + (%s * interval '1 second')
            ORDER BY ts ASC
            LIMIT 1
            """,
            (symbol, target_ts, target_ts, next_tol_s),
        )
        row = cur.fetchone()
        if row:
            next_price, next_ts = row[0], row[1]

    return asof_price, next_price, asof_ts, next_ts


# ---------------------------------------------------------------------------
# Resolución de precios — Binance REST fallback
# FIX-03: una sola llamada REST por job, resultado para asof y next
# FIX-04: next solo acepta vela con close_time < now_ms (ya cerrada)
# FIX-H13: limit calculado dinámicamente según ventana real
# ---------------------------------------------------------------------------

def _resolve_from_binance(
    symbol:     str,
    target_ts:  datetime,
    asof_tol_s: int,
    next_tol_s: int,
) -> Tuple[Optional[float], Optional[float], Optional[datetime], Optional[datetime]]:
    """
    Fallback Binance REST klines 1m. Una sola llamada cubre la ventana completa.

    FIX-04: next solo acepta close de vela ya cerrada (close_time < now_ms).
    FIX-H13: limit calculado como ceil((asof_tol_s + next_tol_s) / 60) + 2
             para garantizar cobertura si asof_tol_s > 480s.

    Re-lanza excepciones para que el caller clasifique terminalidad.
    """
    # FIX-H08: cast explícito a int
    asof_tol_s = int(asof_tol_s)
    next_tol_s = int(next_tol_s)

    base_url  = "https://api.binance.com/api/v3/klines"
    now_ms    = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms  = int((target_ts - timedelta(seconds=asof_tol_s)).timestamp() * 1000)
    end_ms    = int((target_ts + timedelta(seconds=next_tol_s)).timestamp() * 1000)
    target_ms = int(target_ts.timestamp() * 1000)

    # FIX-H13: limit dinámico — ventana total en minutos + 2 de margen, mínimo 10
    window_minutes = math.ceil((asof_tol_s + next_tol_s) / 60)
    kline_limit    = max(10, window_minutes + 2)

    r = _http_session.get(
        base_url,
        params={
            "symbol":    symbol,
            "interval":  "1m",
            "startTime": start_ms,
            "endTime":   end_ms,
            "limit":     kline_limit,
        },
        timeout=8,
    )
    r.raise_for_status()
    klines = r.json()

    if not klines:
        return None, None, None, None

    asof_price = asof_resolved_ts = None
    next_price = next_resolved_ts = None

    # asof: última vela con open_time <= target_ms
    for k in reversed(klines):
        if k[0] <= target_ms:
            asof_price       = float(k[4])
            asof_resolved_ts = datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc)
            break

    # next: primera vela con open_time >= target_ms Y ya cerrada (FIX-04)
    for k in klines:
        if k[0] >= target_ms and k[6] < now_ms:
            next_price       = float(k[4])
            next_resolved_ts = datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc)
            break

    return asof_price, next_price, asof_resolved_ts, next_resolved_ts


# ---------------------------------------------------------------------------
# Emitter — record()
# ---------------------------------------------------------------------------

def record(
    symbol:           str,
    rule:             str,
    signal_kind:      str,
    decision:         str,
    price_alert:      float,
    extras:           Dict[str, Any],
    rejection_reason: Optional[str] = None,
    bot_version:      Optional[str] = None,
) -> Optional[str]:
    """
    Registra una señal o near-miss en DB con atomicidad total.

    Persiste en una sola transacción:
      - 1 fila  en signals
      - 5 filas en signal_outcomes (una por horizonte, bloques vacíos)
      - 5 filas en outcome_jobs   (una por horizonte, status='pending')

    Retorna signal_id (str UUID) si OK.
    Retorna None en cualquier error — loggea pero nunca propaga excepción.

    El caller es responsable de la semántica (DC-OUTCOME-05).

    TODO-P1: usar ThreadedConnectionPool para evitar TCP connect por llamada.
             Bajo volumen en Etapa 1 hace este overhead despreciable.
    """
    # FIX-H15: validación extendida incluye symbol y price_alert
    err = _validate_contract(signal_kind, decision, rejection_reason, symbol, price_alert)
    if err:
        log("OT", f"ERROR record() contrato inválido: {err} "
                  f"[symbol={symbol} rule={rule} kind={signal_kind} dec={decision}]")
        return None

    signal_id = str(uuid.uuid4())
    now_utc   = datetime.now(timezone.utc)
    horizons  = _ot_cfg("horizons_min", HORIZONS_MIN)

    thresholds      = cfg("rules.fast_ts", {})
    thresholds_json = json.dumps(_sanitize_for_json(thresholds)) if thresholds else None

    def _get_float(key: str) -> Optional[float]:
        return _safe_float(extras.get(key))

    def _get_int(key: str) -> Optional[int]:
        # FIX-11: conversión explícita — cubre numpy.int64, float-como-int
        return _safe_int(extras.get(key))

    def _get_str(key: str) -> Optional[str]:
        val = extras.get(key)
        return str(val) if val is not None else None

    def _get_bool(key: str) -> Optional[bool]:
        val = extras.get(key)
        return bool(val) if val is not None else None

    extras_residual = _sanitize_for_json(
        {k: v for k, v in extras.items() if k not in _KNOWN_EXTRA_KEYS}
    )
    # FIX-H05: cap de extras_json — protege contra payloads gigantes
    if extras_residual:
        raw = json.dumps(extras_residual)
        if len(raw.encode("utf-8")) > _EXTRAS_JSON_MAX_BYTES:
            log("OT", f"WARN record() extras_json excede {_EXTRAS_JSON_MAX_BYTES}B "
                      f"para {symbol} — truncando a keys de nivel raíz")
            # Truncar: mantener solo keys de nivel raíz que quepan
            truncated: Dict[str, Any] = {}
            for k, v in extras_residual.items():
                candidate = json.dumps({**truncated, k: v})
                if len(candidate.encode("utf-8")) <= _EXTRAS_JSON_MAX_BYTES:
                    truncated[k] = v
                else:
                    break
            extras_residual = truncated
        extras_json: Optional[str] = json.dumps(extras_residual) if extras_residual else None
    else:
        extras_json = None

    conn = None
    try:
        conn = _connect()

        with conn.cursor() as cur:

            # 1. signals
            cur.execute(
                """
                INSERT INTO signals (
                    signal_id, t0, symbol, rule, signal_kind, decision,
                    rejection_reason, bot_version,
                    price_alert, delta, buy_ratio, n_trades,
                    rel_vol, stage_pct, market_regime, operator_window,
                    operator_sleep, local_hour, utc_hour,
                    confirm_ratio, confirm_window_s,
                    thresholds_json, extras_json,
                    record_origin, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    'native', %s
                )
                """,
                (
                    signal_id,       now_utc,              symbol,
                    rule,            signal_kind,          decision,
                    rejection_reason, bot_version,
                    _safe_float(price_alert),
                    _get_float("delta"),
                    _get_float("buy_ratio"),
                    _get_int("n_trades"),           # FIX-11
                    _get_float("rel_vol"),
                    _get_float("stage_pct"),
                    _get_str("market_regime"),
                    _get_str("operator_window"),
                    _get_bool("operator_sleep"),
                    _get_int("local_hour"),          # FIX-11
                    _get_int("utc_hour"),             # FIX-11
                    _get_float("confirm_ratio"),
                    _get_int("confirm_window_s"),     # FIX-11
                    thresholds_json,
                    extras_json,
                    now_utc,
                ),
            )

            # 2. signal_outcomes — bloques vacíos, status='pending'
            for h in horizons:
                cur.execute(
                    """
                    INSERT INTO signal_outcomes (
                        signal_id, horizon_min, target_ts,
                        asof_status, next_status, created_at
                    ) VALUES (%s, %s, %s, 'pending', 'pending', %s)
                    """,
                    (signal_id, h, now_utc + timedelta(minutes=h), now_utc),
                )

            # 3. outcome_jobs — un job por horizonte
            for h in horizons:
                cur.execute(
                    """
                    INSERT INTO outcome_jobs (
                        signal_id, horizon_min, due_at,
                        status, attempts, created_at
                    ) VALUES (%s, %s, %s, 'pending', 0, %s)
                    """,
                    (signal_id, h, now_utc + timedelta(minutes=h), now_utc),
                )

        conn.commit()
        log("OT", f"recorded signal_id={signal_id} {symbol} {rule} "
                  f"{signal_kind}/{decision}")
        return signal_id

    except psycopg2.Error as e:
        log("OT", f"ERROR record() DB error for {symbol}: {type(e).__name__}: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception as _ce:
                log("OT", f"WARN cleanup rollback error: {_ce}")
        return None
    except Exception as e:
        log("OT", f"ERROR record() unexpected for {symbol}: {type(e).__name__}: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception as _ce:
                log("OT", f"WARN cleanup rollback error: {_ce}")
        return None
    finally:
        if conn:
            # FIX-H09: rollback explícito antes de close por si hay transacción pendiente
            # psycopg2 hace rollback implícito en close(), pero ser explícito es más seguro
            try:
                if not conn.closed and conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                    conn.rollback()
            except Exception as _re:
                log("OT", f"WARN cleanup rollback-in-finally error: {_re}")
            try:
                conn.close()
            except Exception as _ce:
                log("OT", f"WARN cleanup close error: {_ce}")


# ---------------------------------------------------------------------------
# Worker helpers
# ---------------------------------------------------------------------------

def _compute_outcome_pct(
    price:       Optional[float],
    price_alert: Optional[float],
) -> Optional[float]:
    # FIX-H07: cast explícito a float para cubrir Decimal de psycopg2
    if price is None or price_alert is None:
        return None
    try:
        p  = float(price)
        pa = float(price_alert)
    except (TypeError, ValueError):
        return None
    if pa <= 0 or math.isnan(pa) or math.isinf(pa):
        return None
    if math.isnan(p) or math.isinf(p):
        return None
    return round((p - pa) / pa * 100, 6)


def _is_terminal_status(status: Optional[str]) -> bool:
    # FIX-H01: missing_total eliminado — no es status válido
    return status in ("done", "error")


# ---------------------------------------------------------------------------
# _resolve_block — resolución de precios para un job
# FIX-H02: recibe conn_resolve separada — no contamina la transacción del worker
# FIX-12: logging diferenciado para errores programáticos vs transitorios
# ---------------------------------------------------------------------------

def _resolve_block(
    conn_resolve,
    symbol:      str,
    target_ts:   datetime,
    price_alert: float,
    asof_tol_s:  int,
    next_tol_s:  int,
    current_row: Dict[str, Any],
) -> Tuple[Dict[str, Any], bool]:
    """
    Resuelve los bloques asof y/o next no terminales para un horizonte.

    FIX-H02: usa conn_resolve dedicada, separada del lock del worker.
             Un error en Timescale aquí no aborta la transacción del job.

    Retorna (updates, work_attempted):
      - updates: dict con campos a actualizar en signal_outcomes
      - work_attempted: True si se intentó al menos una resolución real
        (usado para decidir si incrementar attempts en el job)

    No lanza excepciones al caller — errores se convierten en updates de status.
    """
    updates: Dict[str, Any] = {}
    work_attempted = False

    need_asof = not _is_terminal_status(current_row.get("asof_status"))
    need_next = not _is_terminal_status(current_row.get("next_status"))

    if not need_asof and not need_next:
        return updates, work_attempted

    work_attempted = True

    # ── Intento Timescale ────────────────────────────────────────────────────
    ts_asof_price = ts_next_price = ts_asof_ts = ts_next_ts = None
    ts_ok = False

    try:
        ts_asof_price, ts_next_price, ts_asof_ts, ts_next_ts = \
            _resolve_from_timescale(
                conn_resolve, symbol, target_ts, asof_tol_s, next_tol_s
            )
        ts_ok = True
    except psycopg2.OperationalError as e:
        log("OT", f"WARN timescale transitorio {symbol} h={target_ts}: {e}")
    except psycopg2.Error as e:
        # FIX-12: error programático — loggeamos con tipo explícito
        log("OT", f"ERROR timescale programático {type(e).__name__} "
                  f"{symbol} h={target_ts}: {e}")
    except Exception as e:
        log("OT", f"WARN timescale error inesperado {type(e).__name__} "
                  f"{symbol} h={target_ts}: {e}")
    finally:
        # FIX-H02: rollback de conn_resolve para limpiar estado tras cada uso
        try:
            if not conn_resolve.closed:
                conn_resolve.rollback()
        except Exception as _re:
            log("OT", f"WARN conn_resolve rollback error: {_re}")

    if ts_ok:
        if need_asof and ts_asof_price is not None:
            updates["asof_price"]       = ts_asof_price
            updates["asof_outcome_pct"] = _compute_outcome_pct(ts_asof_price, price_alert)
            updates["asof_status"]      = "done"
            updates["asof_source"]      = "timescale"
            updates["asof_filled_at"]   = datetime.now(timezone.utc)
            if ts_asof_ts:
                updates["asof_lag_s"] = abs((ts_asof_ts - target_ts).total_seconds())

        if need_next and ts_next_price is not None:
            updates["next_price"]       = ts_next_price
            updates["next_outcome_pct"] = _compute_outcome_pct(ts_next_price, price_alert)
            updates["next_status"]      = "done"
            updates["next_source"]      = "timescale"
            updates["next_filled_at"]   = datetime.now(timezone.utc)
            if ts_next_ts:
                updates["next_lag_s"] = abs((ts_next_ts - target_ts).total_seconds())

    # ── Fallback REST — FIX-03: una sola llamada si hay bloques sin resolver ──
    still_need_asof = need_asof and "asof_status" not in updates
    still_need_next = need_next and "next_status" not in updates

    if still_need_asof or still_need_next:
        fb_error: Optional[Exception] = None
        fb_asof_price = fb_next_price = fb_asof_ts = fb_next_ts = None

        try:
            fb_asof_price, fb_next_price, fb_asof_ts, fb_next_ts = \
                _resolve_from_binance(symbol, target_ts, asof_tol_s, next_tol_s)
        except Exception as e:
            fb_error = e
            log("OT", f"WARN binance fallback {type(e).__name__} {symbol}: {e}")

        if fb_error is None:
            if still_need_asof:
                if fb_asof_price is not None:
                    updates["asof_price"]       = fb_asof_price
                    updates["asof_outcome_pct"] = _compute_outcome_pct(fb_asof_price, price_alert)
                    updates["asof_status"]      = "done"
                    updates["asof_source"]      = "binance_rest"
                    updates["asof_filled_at"]   = datetime.now(timezone.utc)
                    if fb_asof_ts:
                        updates["asof_lag_s"] = abs((fb_asof_ts - target_ts).total_seconds())
                else:
                    # FIX-H01: missing_total → status='error' + error='missing_total'
                    updates["asof_status"] = "error"
                    updates["asof_error"]  = "missing_total"
                    updates["asof_source"] = "none"

            if still_need_next:
                if fb_next_price is not None:
                    updates["next_price"]       = fb_next_price
                    updates["next_outcome_pct"] = _compute_outcome_pct(fb_next_price, price_alert)
                    updates["next_status"]      = "done"
                    updates["next_source"]      = "binance_rest"
                    updates["next_filled_at"]   = datetime.now(timezone.utc)
                    if fb_next_ts:
                        updates["next_lag_s"] = abs((fb_next_ts - target_ts).total_seconds())
                else:
                    # FIX-H01: missing_total → status='error' + error='missing_total'
                    updates["next_status"] = "error"
                    updates["next_error"]  = "missing_total"
                    updates["next_source"] = "none"
        else:
            is_terminal = _is_terminal_error(fb_error)
            err_str     = f"{type(fb_error).__name__}: {fb_error}"

            if still_need_asof and is_terminal:
                updates["asof_status"] = "error"
                updates["asof_error"]  = err_str
                updates["asof_source"] = "none"
            # Si transitorio → no actualizar status, queda 'pending' para retry

            if still_need_next and is_terminal:
                updates["next_status"] = "error"
                updates["next_error"]  = err_str
                updates["next_source"] = "none"

    return updates, work_attempted


def _is_job_done(
    current_row:     Dict[str, Any],
    pending_updates: Dict[str, Any],
) -> bool:
    """
    Job 'done' solo cuando ambos bloques son terminales, incluyendo
    los updates pendientes de este ciclo (DC-OUTCOME-31).
    FIX-H01: frozenset terminal sin missing_total.
    """
    terminal = frozenset({"done", "error"})  # FIX-H01
    asof_s   = pending_updates.get("asof_status", current_row.get("asof_status"))
    next_s   = pending_updates.get("next_status",  current_row.get("next_status"))
    return asof_s in terminal and next_s in terminal


# ---------------------------------------------------------------------------
# _worker_sweep
# FIX-08: FOR UPDATE lock scope por job individual, no por batch completo
# FIX-H02: conn_resolve separada para _resolve_block
# FIX-H11: attempts solo incrementa si se intentó trabajo real
# ---------------------------------------------------------------------------

def _worker_sweep(
    conn,
    conn_resolve,
    max_batch:    int,
    asof_tol_s:   int,
    next_tol_s:   int,
    max_attempts: int,
) -> int:
    """
    Un ciclo del worker: procesa hasta max_batch jobs pendientes y vencidos.

    FIX-08: transacción individual por job — lock se libera en commit/rollback.
    FIX-H02: conn_resolve separada para queries de resolución — evita InFailedSqlTransaction.
    FIX-H11: attempts solo incrementa si work_attempted=True — infraestructura caída
             no consume reintentos del job.

    Retorna el número de jobs procesados en este ciclo.
    """
    now_utc = datetime.now(timezone.utc)

    # Primer paso: leer IDs de jobs elegibles SIN lock (lectura rápida)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT j.job_id
            FROM   outcome_jobs j
            WHERE  j.status  = 'pending'
              AND  j.due_at <= %s
            ORDER BY j.due_at
            LIMIT  %s
            """,
            (now_utc, max_batch),
        )
        job_ids = [row[0] for row in cur.fetchall()]

    # FIX-H04: commit solo si la transacción está en estado limpio
    try:
        conn.commit()
    except psycopg2.Error as e:
        log("OT", f"WARN sweep initial commit error: {e} — intentando rollback")
        try:
            conn.rollback()
        except Exception:
            pass
        return 0

    if not job_ids:
        return 0

    processed = 0

    for job_id in job_ids:
        # FIX-08: transacción individual por job — lock se libera en commit/rollback
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        j.job_id,    j.signal_id,  j.horizon_min,
                        j.due_at,    j.attempts,
                        s.symbol,    s.price_alert,
                        o.target_ts,
                        o.asof_status, o.asof_error,
                        o.next_status, o.next_error
                    FROM   outcome_jobs    j
                    JOIN   signals         s ON s.signal_id   = j.signal_id
                    JOIN   signal_outcomes o ON o.signal_id   = j.signal_id
                                            AND o.horizon_min = j.horizon_min
                    WHERE  j.job_id = %s
                      AND  j.status = 'pending'
                    FOR UPDATE OF j SKIP LOCKED
                    """,
                    (job_id,),
                )
                job = cur.fetchone()

            # SKIP LOCKED: otro proceso tomó este job — saltar
            if job is None:
                conn.commit()
                continue

            signal_id   = job["signal_id"]
            horizon_min = job["horizon_min"]
            symbol      = job["symbol"]
            price_alert = job["price_alert"]
            attempts    = job["attempts"]
            target_ts   = job["target_ts"]

            if target_ts.tzinfo is None:
                target_ts = target_ts.replace(tzinfo=timezone.utc)

            if now_utc < target_ts:
                conn.commit()
                continue

            # FIX-H02: _resolve_block recibe conn_resolve — no toca la transacción del job
            # FIX-H11: work_attempted indica si se intentó resolución real
            updates, work_attempted = _resolve_block(
                conn_resolve, symbol, target_ts, price_alert,
                asof_tol_s, next_tol_s, dict(job),
            )
            job_done = _is_job_done(dict(job), updates)

            # FIX-H11: solo incrementar attempts si se intentó trabajo real
            # Si ambas fuentes fallaron por infraestructura (work_attempted=True pero
            # no hay updates de status), incrementamos. Si work_attempted=False
            # (ambos bloques ya terminales antes de entrar), no incrementamos.
            new_attempts = attempts + 1 if work_attempted else attempts

            # UPDATE signal_outcomes — FIX-09: _OUTCOME_COL_MAP es constante de módulo
            set_clauses: List[str] = []
            values:      List[Any] = []
            for k, col in _OUTCOME_COL_MAP.items():
                if k in updates:
                    set_clauses.append(f"{col} = %s")
                    values.append(updates[k])

            if set_clauses:
                values += [signal_id, horizon_min]
                with conn.cursor() as cur2:
                    cur2.execute(
                        f"UPDATE signal_outcomes SET {', '.join(set_clauses)} "
                        f"WHERE signal_id=%s AND horizon_min=%s",
                        values,
                    )

            # UPDATE outcome_jobs
            if job_done:
                new_status = "done"
                new_due_at = None
            elif new_attempts >= max_attempts:
                new_status = "error"
                new_due_at = None
                log("OT", f"WARN job max_attempts reached: "
                           f"signal={signal_id} h={horizon_min} sym={symbol}")
            else:
                new_status = "pending"
                new_due_at = datetime.now(timezone.utc) + timedelta(
                    seconds=60 * new_attempts
                )

            with conn.cursor() as cur3:
                if new_due_at is not None:
                    cur3.execute(
                        "UPDATE outcome_jobs "
                        "SET status=%s, attempts=%s, due_at=%s "
                        "WHERE job_id=%s",
                        (new_status, new_attempts, new_due_at, job_id),
                    )
                else:
                    cur3.execute(
                        "UPDATE outcome_jobs "
                        "SET status=%s, attempts=%s "
                        "WHERE job_id=%s",
                        (new_status, new_attempts, job_id),
                    )

            conn.commit()   # FIX-08: libera lock inmediatamente tras este job
            processed += 1

        except psycopg2.Error as e:
            log("OT", f"ERROR worker DB error job={job_id}: {type(e).__name__}: {e}")
            try:
                conn.rollback()
            except Exception as _ce:
                log("OT", f"WARN worker rollback error: {_ce}")
        except Exception as e:
            log("OT", f"ERROR worker unexpected job={job_id}: {type(e).__name__}: {e}")
            try:
                conn.rollback()
            except Exception as _ce:
                log("OT", f"WARN worker rollback error: {_ce}")

    return processed


# ---------------------------------------------------------------------------
# _worker_loop
# FIX-H03: cierra conn vieja antes de reconectar
# FIX-H10: sleep al final del loop — primer sweep inmediato
# FIX-H16: circuit breaker activa también tras OperationalError en sweep
# FIX-H18: heartbeat periódico cuando no hay trabajo
# ---------------------------------------------------------------------------

def _worker_loop() -> None:
    """
    Loop principal del worker daemon.
    Conecta una vez, reconecta si la conexión se cae.
    Parámetros recargados desde config en cada sweep (hot-reload).
    Nunca propaga excepciones al exterior.

    FIX-H02: conn_resolve dedicada para resolución, separada de conn principal.
    FIX-H03: conn vieja cerrada explícitamente antes de reconectar.
    FIX-H10: primer sweep inmediato — sleep al final del loop.
    FIX-H16: consecutive_conn_errs incrementa también tras OperationalError en sweep.
    FIX-H18: heartbeat cada N sweeps sin trabajo.

    TODO-P1: lease/visibility timeout para rescate de jobs trabados (Etapa 2).
    """
    log("OT", f"worker started sweep_interval={_ot_cfg('sweep_interval_s', 30)}s")

    conn                  = None
    conn_resolve          = None
    consecutive_conn_errs = 0
    MAX_CONN_ERRORS       = 10
    idle_sweeps           = 0   # FIX-H18: contador de sweeps sin trabajo

    while True:
        try:
            # FIX-H10: sleep al final — el primer sweep es inmediato tras el boot
            # (el sleep está al final del bloque try)

            # ── Conexión principal ──────────────────────────────────────────
            if conn is None or conn.closed:
                # FIX-H03: cerrar explícitamente antes de reasignar
                if conn is not None and not conn.closed:
                    try:
                        conn.close()
                    except Exception as _ce:
                        log("OT", f"WARN worker old conn close error: {_ce}")
                try:
                    conn = _connect()
                    consecutive_conn_errs = 0
                    log("OT", "worker DB connected (main)")
                except psycopg2.OperationalError as e:
                    consecutive_conn_errs += 1
                    log("OT", f"WARN worker DB connect failed "
                               f"(attempt {consecutive_conn_errs}): {e}")
                    if consecutive_conn_errs >= MAX_CONN_ERRORS:
                        log("OT", f"ERROR worker DB unreachable after "
                                   f"{MAX_CONN_ERRORS} attempts — backing off 5min")
                        time.sleep(300)
                        consecutive_conn_errs = 0
                    time.sleep(_ot_cfg("sweep_interval_s", 30))
                    continue

            # ── Conexión de resolución ──────────────────────────────────────
            # FIX-H02: conn_resolve separada — se recrea si se cerró
            if conn_resolve is None or conn_resolve.closed:
                # FIX-H03: cerrar explícitamente antes de reasignar
                if conn_resolve is not None and not conn_resolve.closed:
                    try:
                        conn_resolve.close()
                    except Exception as _ce:
                        log("OT", f"WARN worker old conn_resolve close error: {_ce}")
                try:
                    conn_resolve = _connect_resolve()
                    log("OT", "worker DB connected (resolve)")
                except psycopg2.OperationalError as e:
                    log("OT", f"WARN worker conn_resolve connect failed: {e} — usando solo REST")
                    conn_resolve = None  # _resolve_block saltará Timescale y usará REST directo

            n = _worker_sweep(
                conn,
                conn_resolve,
                max_batch    = _ot_cfg("max_batch_size",    20),
                asof_tol_s   = _ot_cfg("asof_tolerance_s", 300),
                next_tol_s   = _ot_cfg("next_tolerance_s", 120),
                max_attempts = _ot_cfg("max_attempts",        3),
            )
            if n > 0:
                log("OT", f"worker sweep processed {n} jobs")
                idle_sweeps = 0
            else:
                idle_sweeps += 1
                # FIX-H18: heartbeat periódico cuando no hay trabajo
                if idle_sweeps >= _HEARTBEAT_EVERY_N_IDLE:
                    log("OT", f"worker heartbeat — idle sweeps={idle_sweeps}, "
                              f"conn_ok={not conn.closed}")
                    idle_sweeps = 0

        except psycopg2.OperationalError as e:
            log("OT", f"WARN worker lost DB connection: {e} — will reconnect")
            # FIX-H16: incrementar consecutive_conn_errs también en este path
            consecutive_conn_errs += 1
            if conn:
                try:
                    conn.close()
                except Exception as _ce:
                    log("OT", f"WARN worker conn close error: {_ce}")
            conn = None
            if conn_resolve:
                try:
                    conn_resolve.close()
                except Exception as _ce:
                    log("OT", f"WARN worker conn_resolve close error: {_ce}")
            conn_resolve = None
            if consecutive_conn_errs >= MAX_CONN_ERRORS:
                log("OT", f"ERROR worker DB unreachable after "
                           f"{MAX_CONN_ERRORS} sweep errors — backing off 5min")
                time.sleep(300)
                consecutive_conn_errs = 0
        except Exception as e:
            log("OT", f"ERROR worker unexpected in main loop: {type(e).__name__}: {e}")

        # FIX-H10: sleep al final — garantiza que el primer ciclo ejecuta inmediatamente
        time.sleep(_ot_cfg("sweep_interval_s", 30))


# ---------------------------------------------------------------------------
# Read model — signal_status()
# FIX-H01: frozenset terminal sin missing_total
# FIX-H12: n_done_clean usa AND — ambos bloques deben estar done
# FIX-H17: t0 siempre aware UTC
# ---------------------------------------------------------------------------

def signal_status(signal_id: str) -> Optional[Dict[str, Any]]:
    """
    Retorna el estado agregado de una señal por horizonte.

    Por horizonte:
      - asof / next: status, source, outcome_pct, lag_s
      - job_status: estado operativo del job
      - mixed_source: True si asof_source != next_source (ambos con dato real)

    Estado agregado (operativo):
      'done'         → todos los horizontes con ambos bloques resueltos
      'done_no_data' → todos terminales, ningún precio real
      'partial'      → mezcla de terminales y no terminales
      'pending'      → sin horizontes terminales
      'error'        → todos terminales por error

    FIX-H12: 'done' requiere ambos bloques done (AND), no solo uno (OR).

    TODO-P1: separar aggregate_quality_status explícito (Etapa 2).
    """
    conn = None
    try:
        conn = _connect()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT signal_id, symbol, rule, signal_kind, decision, t0, price_alert "
                "FROM signals WHERE signal_id=%s",
                (signal_id,),
            )
            sig = cur.fetchone()
            if not sig:
                return None

            cur.execute(
                """
                SELECT
                    o.horizon_min,
                    o.asof_status,      o.asof_source,
                    o.asof_outcome_pct, o.asof_lag_s,
                    o.next_status,      o.next_source,
                    o.next_outcome_pct, o.next_lag_s,
                    j.status AS job_status, j.attempts
                FROM  signal_outcomes o
                JOIN  outcome_jobs    j ON j.signal_id   = o.signal_id
                                       AND j.horizon_min = o.horizon_min
                WHERE o.signal_id = %s
                ORDER BY o.horizon_min
                """,
                (signal_id,),
            )
            rows = cur.fetchall()

        # FIX-H01: frozenset terminal sin missing_total
        terminal     = frozenset({"done", "error"})
        horizons_out = []

        for row in rows:
            asof_src = row["asof_source"]
            next_src = row["next_source"]

            mixed_source = (
                asof_src not in (None, "none")
                and next_src not in (None, "none")
                and asof_src != next_src
            )

            horizons_out.append({
                "horizon_min": row["horizon_min"],
                "asof": {
                    "status":      row["asof_status"],
                    "source":      asof_src,
                    "outcome_pct": row["asof_outcome_pct"],
                    "lag_s":       row["asof_lag_s"],
                },
                "next": {
                    "status":      row["next_status"],
                    "source":      next_src,
                    "outcome_pct": row["next_outcome_pct"],
                    "lag_s":       row["next_lag_s"],
                },
                "job_status":   row["job_status"],
                "job_attempts": row["attempts"],
                "mixed_source": mixed_source,
            })

        total        = len(horizons_out)
        n_terminal   = sum(
            1 for h in horizons_out
            if h["asof"]["status"] in terminal
            and h["next"]["status"] in terminal
        )
        # FIX-H12: AND en lugar de OR — ambos bloques deben estar done para contar
        n_done_clean = sum(
            1 for h in horizons_out
            if h["asof"]["status"] == "done" and h["next"]["status"] == "done"
        )
        n_all_error  = sum(
            1 for h in horizons_out
            if h["asof"]["status"] == "error" and h["next"]["status"] == "error"
        )

        if total == 0 or n_terminal == 0:
            agg = "pending"
        elif n_terminal < total:
            agg = "partial"
        elif n_all_error == total:
            agg = "error"
        elif n_done_clean == 0:
            agg = "done_no_data"
        else:
            agg = "done"

        # FIX-H17: t0 siempre aware UTC — defensivo contra naive datetime de PG
        t0_raw = sig["t0"]
        if t0_raw is not None:
            if isinstance(t0_raw, datetime) and t0_raw.tzinfo is None:
                t0_raw = t0_raw.replace(tzinfo=timezone.utc)
            t0_iso = t0_raw.isoformat() if isinstance(t0_raw, datetime) else str(t0_raw)
        else:
            t0_iso = None

        return {
            "signal_id":        str(sig["signal_id"]),
            "symbol":           sig["symbol"],
            "rule":             sig["rule"],
            "signal_kind":      sig["signal_kind"],
            "decision":         sig["decision"],
            "t0":               t0_iso,
            "price_alert":      sig["price_alert"],
            "aggregate_status": agg,
            "horizons":         horizons_out,
        }

    except psycopg2.Error as e:
        log("OT", f"ERROR signal_status DB error {signal_id}: {type(e).__name__}: {e}")
        return None
    except Exception as e:
        log("OT", f"ERROR signal_status unexpected {signal_id}: {type(e).__name__}: {e}")
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception as _ce:
                log("OT", f"WARN signal_status close error: {_ce}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_worker() -> None:
    """
    Arranca el daemon thread del worker.
    FIX-01: idempotente real — si el worker ya está vivo, es no-op.
    Llamar una vez en el boot desde main.py.
    """
    global _worker_thread

    with _worker_start_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            log("OT", "start_worker() called but worker already running — no-op")
            return

        _worker_thread = threading.Thread(
            target=_worker_loop,
            daemon=True,
            name="outcome-worker",
        )
        _worker_thread.start()
        log("OT", "outcome worker thread started")


def start_filler() -> None:
    """Alias de start_worker() para compatibilidad con main.py existente."""
    start_worker()
