#!/usr/bin/env python3
"""
main.py — KTS TradeSeeker entry point
v5.0 — 2026-03-28

Responsabilidades:
- Cargar config y resolver símbolos
- Registrar signal handlers (SIGTERM / SIGINT) en el main thread
- Arrancar UniverseFilter y fast_loop como threads supervisados
- Supervisar que fast_loop siga vivo — reiniciarlo si muere
- Shutdown limpio: stop_event → join con timeout → exit

Lo que ya NO vive aquí (extraído a módulos propios):
- run_fast_loop        → fast_loop.py
- scan_symbol / rules  → scanner.py  (legacy, apagado)
- log / cfg / http     → utils.py

Señales:
- SIGTERM (systemd stop / kill)  → stop_event.set()
- SIGINT  (Ctrl+C)               → stop_event.set()
- Los signal handlers solo setean el Event — nunca hacen I/O ni raises
  (Python docs: keep handlers minimal, delegate to main loop)

Thread supervision:
- Main thread duerme en stop_event.wait(SUPERVISOR_INTERVAL_S)
- Si fast_loop thread muere sin stop_event activo → restart automático
- Si fast_loop muere MAX_RESTARTS veces seguidas → shutdown total
"""

import os
import sys
import signal
import threading
import time
from typing import List, Optional

import outcome_tracker
from utils import log, load_config, cfg, tg_ping
from universe_filter import UniverseFilter
from fast_loop import run_fast_loop

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

BOT_VER             = "v5.1"
BOOT_SENT           = "/run/tradeseeker.booted"   # sentinel para boot ping único

SUPERVISOR_INTERVAL_S = 5      # frecuencia de check del supervisor loop
THREAD_JOIN_TIMEOUT_S = 10     # tiempo máximo de espera en join al shutdown
MAX_RESTARTS          = 5      # reintentos antes de rendirse y salir
RESTART_BACKOFF_S     = 3.0    # espera entre reinicios

# ---------------------------------------------------------------------------
# Estado global mínimo
# ---------------------------------------------------------------------------

_stop_event   = threading.Event()
_restart_count = 0

# ---------------------------------------------------------------------------
# Signal handlers — solo setean el Event, sin I/O ni raises
# ---------------------------------------------------------------------------

def _handle_signal(signum: int, frame) -> None:
    """
    Handler para SIGTERM y SIGINT.
    Regla: handlers de señales deben ser mínimos y no hacer I/O.
    El stop_event.set() es thread-safe y es la única acción aquí.
    El log y el tg_ping ocurren en el main loop después de detectar el evento.
    """
    _stop_event.set()


def _register_signals() -> None:
    """
    Registrar handlers en el main thread.
    signal.signal() solo puede llamarse desde el main thread en Python.
    """
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)


# ---------------------------------------------------------------------------
# Resolución de símbolos
# ---------------------------------------------------------------------------

def fetch_usdt_symbols() -> List[str]:
    """
    Resuelve la lista de símbolos USDT desde config o Binance REST.
    Fallback a lista mínima si todo falla — nunca devuelve lista vacía.
    """
    import requests

    mode = cfg("symbols.mode", "auto")

    if mode == "static":
        syms = cfg("symbols.static_list", [])
        if syms:
            log("BOOT", f"symbols: static list ({len(syms)} symbols)")
            return syms

    tier_file = cfg("symbols.tier_file")
    if tier_file and os.path.exists(tier_file):
        try:
            with open(tier_file) as f:
                syms = [line.strip() for line in f
                        if line.strip() and line.strip().endswith("USDT")]
            if syms:
                log("BOOT", f"symbols: tier_file ({len(syms)} symbols)")
                return syms
        except OSError as e:
            log("BOOT", f"tier_file read error: {e}")

    # Auto: fetch desde Binance
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/exchangeInfo",
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        syms = [
            s["symbol"] for s in data.get("symbols", [])
            if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
        ]
        if syms:
            log("BOOT", f"symbols: Binance REST ({len(syms)} symbols)")
            return syms
    except Exception as e:
        log("BOOT", f"Binance exchangeInfo error: {e}")

    # Fallback mínimo — el UniverseFilter los filtrará igual
    fallback = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    log("BOOT", f"symbols: FALLBACK list {fallback}")
    return fallback


# ---------------------------------------------------------------------------
# Boot ping — una sola vez por proceso (sentinel file)
# ---------------------------------------------------------------------------

_boot_ping_done = False

def _boot_ping(syms: List[str]) -> None:
    """
    Envía ping de arranque a Telegram exactamente una vez.
    Usa sentinel file para sobrevivir a reinicios del supervisor interno.
    """
    global _boot_ping_done

    if _boot_ping_done:
        return

    try:
        with open(BOOT_SENT, "x") as f:
            f.write(str(time.time()))
            f.flush()
            os.fsync(f.fileno())
    except FileExistsError:
        _boot_ping_done = True
        log("BOOT", "ping skipped (sentinel exists)")
        return
    except OSError as e:
        log("BOOT", f"ping sentinel write error: {e} — sending anyway")

    tg_ping(f"KTS TradeSeeker {BOT_VER} started | {len(syms)} symbols")
    _boot_ping_done = True



# ---------------------------------------------------------------------------
# ProxyPool setup
# ---------------------------------------------------------------------------

def _init_proxy_pool():
    nord_user = os.getenv("NORDVPN_SERVICE_USER", "").strip()
    nord_pass = os.getenv("NORDVPN_SERVICE_PASS", "").strip()
    if not nord_user or not nord_pass:
        log("BOOT", "NORDVPN_SERVICE_USER/PASS not set — running without proxy pool")
        return None
    try:
        from proxy_pool import ProxyPool
        pool = ProxyPool(
            nord_user=nord_user,
            nord_pass=nord_pass,
            env_file=os.getenv("KTS_COMPOSE_ENV", "/root/kts-lab/compose/.env"),
            benthos_metrics_url=os.getenv("KTS_BENTHOS_METRICS", "http://localhost:4199/metrics"),
            compose_dir=os.getenv("KTS_COMPOSE_DIR", "/root/kts-lab/compose"),
            log_fn=log,
        )
        pool.start()
        pool.enable_direct_ip()
        log("BOOT", "ProxyPool started — direct IP slot enabled")
        return pool
    except Exception as e:
        log("BOOT", f"ProxyPool init error (non-fatal): {type(e).__name__}: {e}")
        return None


# ---------------------------------------------------------------------------
# Factory de fast_loop thread
# ---------------------------------------------------------------------------

def _make_fast_thread(uf: UniverseFilter, proxy_pool=None) -> threading.Thread:
    """Crea (pero no arranca) el fast_loop thread."""
    return threading.Thread(
        target=run_fast_loop,
        args=(uf, _stop_event, proxy_pool),
        daemon=True,
        name="fast-loop",
    )


# ---------------------------------------------------------------------------
# run() — entry point principal
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Arranque, supervisión y shutdown del bot.

    Flujo:
    1. Registrar signal handlers
    2. Cargar config
    3. Resolver símbolos
    4. Arrancar UniverseFilter
    5. Arrancar outcome_tracker
    6. Boot ping
    7. Arrancar fast_loop thread
    8. Supervisor loop: detecta muerte del thread y reinicia hasta MAX_RESTARTS
    9. Shutdown limpio al recibir stop_event
    """
    global _restart_count

    _register_signals()

    log("BOOT", f"KTS TradeSeeker {BOT_VER} starting")
    load_config(force=True)

    syms = fetch_usdt_symbols()
    if not syms:
        log("ERR", "no symbols resolved — aborting")
        sys.exit(2)

    proxy_pool = _init_proxy_pool()

    # Arrancar UniverseFilter (daemon thread interno)
    uf = UniverseFilter()
    uf.start()
    log("BOOT", f"UniverseFilter ready — {len(uf.get_universe())} symbols in universe")

    # Arrancar outcome tracker (daemon thread interno)
    try:
        outcome_tracker.start_filler()
        log("BOOT", "outcome_tracker started")
    except Exception as e:
        log("BOOT", f"outcome_tracker start error (non-fatal): {type(e).__name__}: {e}")

    _boot_ping(syms)

    # Arrancar fast_loop
    fast_thread = _make_fast_thread(uf, proxy_pool)
    fast_thread.start()
    log("BOOT", f"fast_loop thread started (name={fast_thread.name})")

    # ── Supervisor loop ──────────────────────────────────────────────────────
    #
    # El main thread duerme en stop_event.wait(SUPERVISOR_INTERVAL_S).
    # Esto permite:
    #   a) Despertar inmediatamente cuando llega SIGTERM/SIGINT
    #   b) Chequear periódicamente si el fast_loop sigue vivo
    #
    # Si fast_loop muere sin stop_event activo → restart con backoff.
    # Si supera MAX_RESTARTS → log + tg_ping + sys.exit(1).
    #
    while not _stop_event.is_set():

        _stop_event.wait(timeout=SUPERVISOR_INTERVAL_S)

        if _stop_event.is_set():
            break

        # Check thread health
        if not fast_thread.is_alive():
            _restart_count += 1
            log("BOOT",
                f"fast_loop thread died unexpectedly "
                f"(restart {_restart_count}/{MAX_RESTARTS})")

            if _restart_count > MAX_RESTARTS:
                log("ERR", f"fast_loop exceeded MAX_RESTARTS={MAX_RESTARTS} — stopping bot")
                tg_ping(
                    f"[KTS] CRITICAL: fast_loop died {MAX_RESTARTS}x — bot stopping"
                )
                _stop_event.set()
                break

            tg_ping(
                f"[KTS] WARNING: fast_loop restart {_restart_count}/{MAX_RESTARTS}"
            )
            time.sleep(RESTART_BACKOFF_S * _restart_count)   # backoff incremental

            fast_thread = _make_fast_thread(uf, proxy_pool)
            fast_thread.start()
            log("BOOT", f"fast_loop restarted (attempt {_restart_count})")

    # ── Shutdown ─────────────────────────────────────────────────────────────
    sig_name = "SIGTERM/SIGINT" if _stop_event.is_set() else "unknown"
    log("BOOT", f"shutdown requested ({sig_name}) — waiting for fast_loop to exit")

    fast_thread.join(timeout=THREAD_JOIN_TIMEOUT_S)

    if fast_thread.is_alive():
        log("BOOT",
            f"fast_loop did not exit within {THREAD_JOIN_TIMEOUT_S}s — forcing exit")
    else:
        log("BOOT", "fast_loop exited cleanly")

    # Limpiar sentinel para que el próximo arranque envíe boot ping
    try:
        os.remove(BOOT_SENT)
    except FileNotFoundError:
        pass
    except OSError as e:
        log("BOOT", f"sentinel cleanup error: {e}")

    log("BOOT", f"KTS TradeSeeker {BOT_VER} stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()
