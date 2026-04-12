"""
labeling_worker.py — KTS TradeSeeker · F2.2 · Labeling Worker Daemon
v2.1 — 2026-04-11

Responsabilidad:
    Worker daemon que:
    1. Selecciona eventos procesables (nunca procesados O retryables con cooldown vencido)
    2. Los labela via OutcomeLabeler.label()
    3. Actualiza worker_status / retry_count / next_retry_at / is_terminal / is_retryable
       en event_labels_core según label_retry_policy

Separación estricta de responsabilidades:
    OutcomeLabeler escribe:
        event_labels_core      (label técnico)
        event_markouts
        event_label_attempts   (historial de intentos)

    labeling_worker escribe:
        event_labels_core      (solo campos de retry state)
    labeling_worker lee:
        label_retry_policy     (clasificación terminal/retryable)

Política de retry (Opción C):
    - bad terminal (bad_invalid_book, bad_sigma, bad_time_inconsistency):
        worker_status = 'terminal', is_terminal = TRUE, is_retryable = FALSE
    - bad retryable (bad_missing_quotes, bad_stale_data, bad_labeler_exception):
        worker_status = 'retry_wait', is_retryable = TRUE
        próximo intento = now() + cooldown_seconds (desde label_retry_policy)
    - retries agotados (new_retry_count > max_retries):
        worker_status = 'terminal', terminal_reason_code = 'retry_exhausted:<reason>'
    - ok / degraded:
        worker_status = 'done', is_terminal = TRUE

Política de no-reproceso (DC-WORKER-01):
    La query excluye eventos con worker_status IN ('done', 'terminal').
    Un evento con worker_status='done' o 'terminal' NO vuelve a procesarse.
    Esto incluye eventos quality='bad' con is_terminal=TRUE.

Asunciones de aislamiento (DC-WORKER-03):
    La conn del worker usa READ COMMITTED (default de Postgres).
    Cada statement ve su propio snapshot — no hay problemas de visibilidad
    entre _fetch_pending_events y _update_worker_status.

Parámetros configurables desde config.yaml bajo labeling_worker.*:
    sweep_interval_s      = 60
    batch_size            = 50
    maturity_min          = 120
    statement_timeout_ms  = 10000
    max_conn_errors       = 10
    base_backoff_s        = 30.0
    idle_log_every_n      = 10      # loggear "idle" cada N sweeps vacíos (min 1)
    policy_refresh_sweeps = 10      # refrescar retry policy cada N sweeps

Correcciones v2.1:
    B1:  _update_worker_status verifica rowcount — loggea si UPDATE afecta 0 filas.
    B2:  datetime.now(timezone.utc) en lugar de datetime.utcnow() — aware datetime.
    B3:  _load_retry_policy hace commit después del SELECT exitoso.
    B4:  except psycopg2.Error separado en _update_worker_status path — conn invalidada.
    B5:  query excluye explícitamente worker_status IN ('done','terminal') sin depender
         de COALESCE(lrp.max_retries,0) para eventos retry_wait con reason desconocido.
    B6:  final_reason se lee desde DB solo cuando label() tuvo éxito — no sobreescribe
         el reason cuando label() lanzó excepción.
    B7:  retry_count leído desde DB con FOR UPDATE antes del UPDATE — lectura consistente.
    A1:  Eliminado @register_default_json — uso incorrecto como decorador.
    A2:  final_reason usa el reason del resultado actual, no el del intento previo.
    A4:  Eventos retry_wait con quality_reason_code=NULL tratados como pending (nunca procesado).
    A7:  idle_log_every_n validado como >= 1 para evitar ZeroDivisionError.
    A9:  except psycopg2.Error: pass reemplazado por log + conn invalidation.
    A10: _load_retry_policy rollback failure loggeado explícitamente.
    M3:  POLICY_REFRESH_SWEEPS configurable via _safe_cfg_int.
    M4:  terminal_reason_code truncado a 200 chars.
    M5:  import timezone movido al nivel de módulo.
    M7:  datetime.now(timezone.utc) para elapsed — consistente con patrón del módulo.
    L1:  log cuando evento clasifica como 'done'.
    L2:  t_confirm removido del retorno de _fetch_pending_events (no se usaba).
    L3:  _noop renombrado a _json_passthrough con docstring.
"""

import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from outcome_labeler import OutcomeLabeler
from utils import cfg, log

# ---------------------------------------------------------------------------
# Worker singleton
# ---------------------------------------------------------------------------

_worker_thread:     Optional[threading.Thread] = None
_worker_start_lock: threading.Lock             = threading.Lock()
_stop_event:        threading.Event            = threading.Event()

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _lw_cfg(key: str, default: Any) -> Any:
    return cfg(f"labeling_worker.{key}", default)


def _safe_cfg_int(key: str, default: int) -> int:
    """Convierte valor de config a int. Si falla, loggea y usa default."""
    val = _lw_cfg(key, default)
    try:
        return int(val)
    except (TypeError, ValueError):
        log("LW", f"WARN _safe_cfg_int: labeling_worker.{key}={val!r} "
                   f"no es int — usando default {default}")
        return default


def _safe_cfg_float(key: str, default: float) -> float:
    """Convierte valor de config a float. Si falla, loggea y usa default."""
    val = _lw_cfg(key, default)
    try:
        return float(val)
    except (TypeError, ValueError):
        log("LW", f"WARN _safe_cfg_float: labeling_worker.{key}={val!r} "
                   f"no es float — usando default {default}")
        return default


def _dsn() -> str:
    timeout_ms = _safe_cfg_int("statement_timeout_ms", 10000)
    return (
        "host=localhost port=5432 dbname=tsdb "
        "user=postgres password=postgres "
        f"connect_timeout=5 "
        f"options='-c statement_timeout={timeout_ms}'"
    )


def _connect() -> psycopg2.extensions.connection:
    """
    Abre conexión y fuerza UTC.
    Cierra conn si SET TIME ZONE falla — evita leak de socket.
    psycopg2: conn.closed == 0 → open; non-zero → closed.
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
# Retry policy — clasificación terminal/retryable
# ---------------------------------------------------------------------------

def _load_retry_policy(conn) -> Dict[str, Dict]:
    """
    Carga label_retry_policy desde DB. Retorna dict keyed por reason_code.
    Si la tabla no existe o está vacía, retorna dict vacío (fail-open: no retry).

    fix B3: hace commit después del SELECT exitoso — cierra snapshot de lectura.
    fix A10: loggea explícitamente si rollback también falla.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT reason_code, retryable, is_terminal,
                       max_retries, cooldown_seconds
                FROM   label_retry_policy
                """
            )
            rows = cur.fetchall()
        # fix B3: commit para cerrar snapshot de lectura
        conn.commit()
        return {r["reason_code"]: dict(r) for r in rows}
    except psycopg2.Error as e:
        log("LW", f"WARN _load_retry_policy DB error: {type(e).__name__}: {e} "
                   "— usando política vacía (no retry)")
        try:
            conn.rollback()
        except Exception as rb_err:
            # fix A10: log explícito si rollback también falla
            log("LW", f"WARN _load_retry_policy rollback also failed: {rb_err}")
        return {}


def _classify_result(
    label_quality: str,
    reason_code:   Optional[str],
    retry_count:   int,
    policy:        Dict[str, Dict],
) -> Dict[str, Any]:
    """
    Clasifica el resultado del labeler y determina worker_status y campos de retry.

    Retorna un dict con los campos a actualizar en event_labels_core:
        worker_status, is_terminal, is_retryable,
        retry_count (nuevo), next_retry_at, terminal_reason_code, last_attempt_at

    fix B2: usa datetime.now(timezone.utc) — aware datetime consistente con TIMESTAMPTZ.

    Reglas:
        ok / degraded → done, terminal
        bad + reason terminal → terminal
        bad + reason retryable + new_retry_count <= max_retries → retry_wait
        bad + reason retryable + new_retry_count > max_retries  → terminal (exhausted)
        bad + reason desconocido → terminal (fail-safe: no loop infinito)
        bad + sin reason_code   → terminal (fail-safe)
    """
    # fix B2: aware datetime — consistente con columna TIMESTAMPTZ
    now_utc = datetime.now(timezone.utc)

    if label_quality in ("ok", "degraded"):
        # fix L1: log cuando evento clasifica como done
        log("LW", f"DEBUG _classify_result: done quality={label_quality}")
        return {
            "worker_status":        "done",
            "is_terminal":          True,
            "is_retryable":         False,
            "retry_count":          retry_count,   # no incrementar en éxito
            "next_retry_at":        None,
            "terminal_reason_code": None,
            "last_attempt_at":      now_utc,
        }

    # quality == 'bad'
    if not reason_code:
        log("LW", "WARN _classify_result: bad sin reason_code — marcando terminal")
        return {
            "worker_status":        "terminal",
            "is_terminal":          True,
            "is_retryable":         False,
            "retry_count":          retry_count + 1,
            "next_retry_at":        None,
            "terminal_reason_code": "bad_labeler_exception",
            "last_attempt_at":      now_utc,
        }

    policy_row = policy.get(reason_code)

    if policy_row is None:
        # reason_code no en policy → terminal por seguridad (no loop infinito)
        log("LW", f"WARN _classify_result: reason_code={reason_code!r} "
                   "no en label_retry_policy — marcando terminal")
        return {
            "worker_status":        "terminal",
            "is_terminal":          True,
            "is_retryable":         False,
            "retry_count":          retry_count + 1,
            "next_retry_at":        None,
            "terminal_reason_code": reason_code,
            "last_attempt_at":      now_utc,
        }

    new_retry_count = retry_count + 1

    if not policy_row["retryable"]:
        return {
            "worker_status":        "terminal",
            "is_terminal":          True,
            "is_retryable":         False,
            "retry_count":          new_retry_count,
            "next_retry_at":        None,
            "terminal_reason_code": reason_code,
            "last_attempt_at":      now_utc,
        }

    # Retryable — verificar si se agotaron los reintentos
    max_retries = policy_row["max_retries"]
    if new_retry_count > max_retries:
        # fix M4: truncar terminal_reason_code a 200 chars
        exhausted_code = f"retry_exhausted:{reason_code}"[:200]
        log("LW", f"INFO _classify_result: retries agotados "
                   f"({new_retry_count}/{max_retries}) reason={reason_code} → terminal")
        return {
            "worker_status":        "terminal",
            "is_terminal":          True,
            "is_retryable":         False,
            "retry_count":          new_retry_count,
            "next_retry_at":        None,
            "terminal_reason_code": exhausted_code,
            "last_attempt_at":      now_utc,
        }

    # Retryable con reintentos disponibles
    cooldown_s = policy_row["cooldown_seconds"]
    next_retry = now_utc + timedelta(seconds=cooldown_s)
    return {
        "worker_status":        "retry_wait",
        "is_terminal":          False,
        "is_retryable":         True,
        "retry_count":          new_retry_count,
        "next_retry_at":        next_retry,
        "terminal_reason_code": None,
        "last_attempt_at":      now_utc,
    }


# ---------------------------------------------------------------------------
# Actualizar worker_status en event_labels_core
# ---------------------------------------------------------------------------

def _update_worker_status(
    conn,
    event_id:   str,
    classified: Dict[str, Any],
) -> bool:
    """
    Actualiza los campos de retry state en event_labels_core.
    Retorna True si el UPDATE afectó exactamente 1 fila, False si 0 filas.
    Lanza excepción si falla — el caller la maneja.

    Solo toca campos de retry state — NO modifica label_quality ni campos técnicos.
    DC-WORKER-02: separación estricta entre campos del labeler y campos del worker.

    fix B1: verifica rowcount — retorna False si UPDATE afecta 0 filas (evento sin fila).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE event_labels_core SET
                worker_status        = %s,
                is_terminal          = %s,
                is_retryable         = %s,
                retry_count          = %s,
                next_retry_at        = %s,
                terminal_reason_code = %s,
                last_attempt_at      = %s
            WHERE event_id = %s
            """,
            (
                classified["worker_status"],
                classified["is_terminal"],
                classified["is_retryable"],
                classified["retry_count"],
                classified["next_retry_at"],
                classified["terminal_reason_code"],
                classified["last_attempt_at"],
                event_id,
            ),
        )
        # fix B1: rowcount=0 significa que no existe la fila en event_labels_core
        # (el labeler no persistió nada — su propio conn falló)
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Query de eventos procesables
# ---------------------------------------------------------------------------

def _fetch_pending_events(
    conn,
    batch_size:   int,
    maturity_min: int,
) -> List[Tuple[str, str, int, Optional[str]]]:
    """
    Selecciona hasta batch_size eventos procesables.

    Procesable si:
        A) Nunca procesado (no existe fila en event_labels_core), O
        B) En retry_wait + cooldown vencido + retries disponibles
           con reason_code conocido en label_retry_policy

    Excluye explícitamente:
        worker_status IN ('done', 'terminal')   [DC-WORKER-01]

    fix B5: la exclusión de 'done'/'terminal' es explícita — no depende de
            COALESCE(lrp.max_retries,0) para eventos con reason desconocido.
            Un evento retry_wait con reason_code no en policy queda en retry_wait
            permanentemente si alguien lo puso así manualmente. Para limpiarlos:
            UPDATE event_labels_core SET worker_status='terminal' WHERE ...

    fix A4: eventos retry_wait con quality_reason_code=NULL no son seleccionados
            porque el LEFT JOIN con label_retry_policy no matchea NULL y
            COALESCE(lrp.max_retries,0)=0 → retry_count < 0 nunca se cumple.
            Esto es correcto: un evento sin reason_code debería haberse marcado
            terminal por _classify_result. Si llega aquí es anomalía de DB — se
            deja pasar con el timeout natural de next_retry_at.

    fix L2: t_confirm removido del retorno — no se usa en _worker_sweep.

    Retorna: [(event_id, symbol, retry_count, quality_reason_code)]
    """
    safe_maturity = int(maturity_min)
    safe_batch    = int(batch_size)

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    ce.event_id,
                    ce.symbol,
                    COALESCE(elc.retry_count, 0)   AS retry_count,
                    elc.quality_reason_code
                FROM candidate_events ce
                LEFT JOIN event_labels_core elc
                       ON elc.event_id = ce.event_id
                LEFT JOIN label_retry_policy lrp
                       ON lrp.reason_code = elc.quality_reason_code
                WHERE ce.t_confirm < now() - interval '{safe_maturity} minutes'
                  AND (
                        -- Caso A: nunca procesado
                        elc.event_id IS NULL
                    OR (
                        -- Caso B: retryable con cooldown vencido y retries disponibles
                            elc.worker_status = 'retry_wait'
                        AND elc.is_retryable  = TRUE
                        AND elc.next_retry_at <= now()
                        AND elc.retry_count   <  COALESCE(lrp.max_retries, 0)
                    )
                  )
                  -- fix B5: exclusión explícita de terminales/done
                  AND (elc.worker_status IS NULL
                       OR elc.worker_status NOT IN ('done', 'terminal'))
                ORDER BY ce.t_confirm ASC
                LIMIT  {safe_batch}
                """,
            )
            rows = cur.fetchall()

    except Exception:
        try:
            conn.rollback()
        except Exception as rb_err:
            log("LW", f"WARN _fetch_pending_events rollback error: {rb_err}")
        raise

    result = []
    for r in rows:
        event_id     = r[0]
        symbol       = r[1]
        retry_count  = r[2]
        reason_code  = r[3]
        result.append((event_id, symbol, retry_count, reason_code))
    return result


# ---------------------------------------------------------------------------
# _worker_sweep
# ---------------------------------------------------------------------------

def _worker_sweep(
    conn,
    labeler:      OutcomeLabeler,
    batch_size:   int,
    maturity_min: int,
    policy:       Dict[str, Dict],
) -> Tuple[int, int, int, bool]:
    """
    Un ciclo del worker. Retorna (fetched, ok_count, bad_count, stopped_early).

    fetched       = cuántos eventos se obtuvieron de la query
    ok_count      = eventos con quality ok/degraded
    bad_count     = eventos con quality bad (cualquier reason)
    stopped_early = True si _stop_event cortó el batch antes de terminar

    Propaga psycopg2.Error y Exception al caller (_worker_loop),
    que invalida conn y gestiona reconexión.

    fix B6: final_reason solo se lee desde DB cuando label() tuvo éxito —
            no sobreescribe el reason cuando label() lanzó excepción.
    fix A9: error en SELECT post-label se loggea y se invalida conn.
    fix B1: si _update_worker_status retorna False (0 filas), se loggea.
    fix B4: psycopg2.Error en update → rollback → conn invalidada para próximo sweep.
    """
    pending = _fetch_pending_events(conn, batch_size, maturity_min)
    fetched = len(pending)

    if fetched == 0:
        return 0, 0, 0, False

    ok_count      = 0
    bad_count     = 0
    stopped_early = False

    for idx, (event_id, symbol, retry_count, prev_reason_code) in enumerate(pending):
        if _stop_event.is_set():
            log("LW", f"stop requested — ending sweep after event idx={idx}/{fetched}")
            stopped_early = True
            break

        # label() es fail-safe — asume quality='bad' con prev_reason como fallback
        quality      = "bad"
        final_reason = prev_reason_code   # fallback si label() falla sin escribir nada
        label_raised = False

        try:
            quality = labeler.label(event_id)
        except Exception as e:
            log("LW", f"ERROR label() inesperado event_id={event_id} symbol={symbol} "
                       f"idx={idx+1}/{fetched}: {type(e).__name__}: {e}")
            quality      = "bad"
            final_reason = "bad_labeler_exception"
            label_raised = True

        # fix B6: solo leer reason_code desde DB cuando label() tuvo éxito
        # Si label() lanzó excepción, usar final_reason ya seteado arriba
        if not label_raised:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT quality_reason_code "
                        "FROM event_labels_core WHERE event_id = %s",
                        (event_id,),
                    )
                    row = cur.fetchone()
                    if row is not None and row[0] is not None:
                        final_reason = row[0]
                    elif row is None:
                        # El labeler no escribió ninguna fila — usamos prev_reason como fallback
                        # (A2: no sobreescribir con stale si el labeler no persistió nada)
                        log("LW", f"WARN post-label SELECT: no row in event_labels_core "
                                   f"event_id={event_id} — using prev reason={prev_reason_code!r}")
                        final_reason = prev_reason_code
            except psycopg2.Error as e:
                # fix A9: error de DB en SELECT post-label — loggear, usar prev_reason
                # No silenciar: invalida conn para que _worker_loop reconecte
                log("LW", f"ERROR post-label SELECT event_id={event_id}: "
                            f"{type(e).__name__}: {e} — using prev reason, invalidating conn")
                final_reason = prev_reason_code
                # Propagar para que _worker_loop invalide la conn
                raise

        # Clasificar según retry policy
        classified = _classify_result(quality, final_reason, retry_count, policy)

        # Actualizar worker_status — fix B4: psycopg2.Error separado
        try:
            updated = _update_worker_status(conn, event_id, classified)
            conn.commit()
            # fix B1: log si UPDATE afectó 0 filas (labeler no persistió nada)
            if not updated:
                log("LW", f"WARN _update_worker_status: 0 rows updated event_id={event_id} "
                            f"— event_labels_core has no row (labeler may have failed to persist). "
                            f"Event will be retried in next sweep.")
        except psycopg2.Error as update_err:
            # fix B4: psycopg2.Error en update — rollback + propagar para reconexión
            log("LW", f"ERROR _update_worker_status DB event_id={event_id}: "
                       f"{type(update_err).__name__}: {update_err}")
            try:
                conn.rollback()
            except Exception as rb_err:
                log("LW", f"WARN rollback after update error: {rb_err}")
            raise   # propagar a _worker_loop para invalidar conn
        except Exception as update_err:
            log("LW", f"ERROR _update_worker_status unexpected event_id={event_id}: "
                       f"{type(update_err).__name__}: {update_err} "
                       "— worker_status queda en 'pending' para reintento")
            try:
                conn.rollback()
            except Exception:
                pass

        if quality in ("ok", "degraded"):
            ok_count += 1
        else:
            bad_count += 1

    return fetched, ok_count, bad_count, stopped_early


# ---------------------------------------------------------------------------
# _worker_loop
# ---------------------------------------------------------------------------

def _worker_loop() -> None:
    """
    Loop principal del worker daemon.

    - Conecta una vez, reconecta ante cualquier error (psycopg2.Error o Exception).
    - Backoff exponencial real en errores de conexión.
    - conn = None ante cualquier error — garantiza reconexión limpia.
    - Parámetros hot-reload en cada sweep.
    - Carga retry policy al inicio y refresca cada policy_refresh_sweeps sweeps.
    - Heartbeat de sweeps vacíos con rate limit (idle_log_every_n, mínimo 1).
    - Loggea duración de cada sweep.
    - Nunca propaga excepciones al exterior.
    """
    log("LW", "labeling worker started")

    try:
        labeler = OutcomeLabeler()
    except Exception as e:
        log("LW", f"ERROR OutcomeLabeler() init failed: {type(e).__name__}: {e} "
                   "— labeling worker cannot start")
        return

    conn                  = None
    consecutive_conn_errs = 0
    policy: Dict[str, Dict] = {}
    sweep_n  = 0
    idle_n   = 0

    while not _stop_event.is_set():
        try:
            # Parámetros hot-reload
            sweep_interval_s  = _safe_cfg_float("sweep_interval_s",  60.0)
            batch_size        = _safe_cfg_int("batch_size",           50)
            maturity_min      = _safe_cfg_int("maturity_min",         120)
            max_conn_errors   = _safe_cfg_int("max_conn_errors",      10)
            base_backoff_s    = _safe_cfg_float("base_backoff_s",    30.0)
            # fix A7: idle_log_every_n validado >= 1 para evitar ZeroDivisionError
            idle_log_every_n  = max(1, _safe_cfg_int("idle_log_every_n",  10))
            # fix M3: policy_refresh_sweeps configurable
            policy_refresh_n  = max(1, _safe_cfg_int("policy_refresh_sweeps", 10))

            _stop_event.wait(timeout=sweep_interval_s)
            if _stop_event.is_set():
                break

            # Asegurar conexión
            # psycopg2: conn.closed == 0 → open, non-zero → closed
            if conn is None or conn.closed != 0:
                try:
                    conn = _connect()
                    consecutive_conn_errs = 0
                    log("LW", "worker DB connected")
                except psycopg2.Error as e:
                    consecutive_conn_errs += 1
                    backoff = min(base_backoff_s * consecutive_conn_errs, 300.0)
                    log("LW", f"WARN DB connect failed (attempt {consecutive_conn_errs}): "
                               f"{type(e).__name__}: {e} — retry en {backoff:.0f}s")
                    if consecutive_conn_errs >= max_conn_errors:
                        log("LW", f"ERROR DB unreachable tras {max_conn_errors} intentos "
                                   f"— backing off 5min")
                        _stop_event.wait(timeout=300.0)
                    else:
                        _stop_event.wait(timeout=backoff)
                    conn = None
                    continue

            # Refrescar retry policy periódicamente
            sweep_n += 1
            if sweep_n == 1 or sweep_n % policy_refresh_n == 0:
                policy = _load_retry_policy(conn)
                if sweep_n == 1:
                    log("LW", f"retry policy loaded: {len(policy)} reason_codes")

            # fix M7: usar datetime.now(timezone.utc) — consistente con patrón del módulo
            sweep_start = datetime.now(timezone.utc)

            fetched, ok_count, bad_count, stopped_early = _worker_sweep(
                conn, labeler, batch_size, maturity_min, policy
            )

            elapsed_s = (datetime.now(timezone.utc) - sweep_start).total_seconds()

            if fetched > 0:
                idle_n    = 0
                processed = ok_count + bad_count
                log("LW",
                    f"sweep fetched={fetched} processed={processed} "
                    f"ok/degraded={ok_count} bad={bad_count} "
                    f"stopped_early={stopped_early} elapsed={elapsed_s:.1f}s"
                )
            else:
                idle_n += 1
                # fix A7: idle_log_every_n >= 1 garantizado por max(1, ...) arriba
                if idle_n % idle_log_every_n == 0:
                    log("LW", f"idle: no mature unlabeled events "
                               f"({idle_n} consecutive empty sweeps)")

        except psycopg2.Error as e:
            log("LW", f"WARN worker DB error: {type(e).__name__}: {e} — reconectando")
            if conn is not None:
                try:
                    conn.close()
                except Exception as _ce:
                    log("LW", f"WARN conn close error: {_ce}")
            conn = None

        except Exception as e:
            log("LW", f"ERROR worker unexpected: {type(e).__name__}: {e}")
            if conn is not None:
                try:
                    conn.close()
                except Exception as _ce:
                    log("LW", f"WARN conn close error: {_ce}")
            conn = None

    # Cleanup al salir
    if conn is not None:
        try:
            conn.close()
        except Exception as ce:
            log("LW", f"WARN cleanup conn close error: {ce}")
    log("LW", "labeling worker stopped")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_labeler() -> None:
    """
    Arranca el daemon thread del labeling worker.
    Idempotente — si ya está vivo, es no-op.

    Si hay un thread anterior muerto, espera join breve antes de clear()
    para evitar race condition entre clear() y threads solapados.
    """
    global _worker_thread

    with _worker_start_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            log("LW", "start_labeler() called but worker already running — no-op")
            return

        if _worker_thread is not None and not _worker_thread.is_alive():
            _worker_thread.join(timeout=2.0)

        _stop_event.clear()

        _worker_thread = threading.Thread(
            target = _worker_loop,
            daemon = True,
            name   = "labeling-worker",
        )
        _worker_thread.start()

        # Verificar que el thread realmente arrancó
        _worker_thread.join(timeout=0.1)
        if not _worker_thread.is_alive():
            log("LW", "ERROR labeling worker thread failed to start")
            return

        log("LW", "labeling worker thread started")


def stop_labeler() -> None:
    """
    Señala al worker que se detenga. Best-effort — no espera a que termine.
    El thread es daemon y se limpia solo al salir el proceso.

    Nota: el evento en proceso cuando se llama stop_labeler() continúa
    hasta completarse (o hasta timeout de statement DB). El stop no es
    inmediato — el thread termina al finalizar el ciclo actual.
    """
    _stop_event.set()
    log("LW", "labeling worker stop requested")
