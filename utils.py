#!/usr/bin/env python3
"""
utils.py — KTS TradeSeeker shared utilities
v1.0 — 2026-03-28

Provides: logging, config, HTTP, Telegram, Circuit breaker, cooldowns.
All stateful objects are thread-safe.
Imported by: main.py, fast_loop.py, scanner.py

Fixes vs original main.py:
- Circuit: thread-safe Lock, 3-state (closed/open/half-open)
- Circuit: report(False) only on final giveup, not each retry attempt
- HTTP: requests.Session + HTTPAdapter, explicit timeouts, retryable-only retry
- HTTP: reset_http_session() for recovery after session corruption
- Telegram: dedup key only registered after confirmed send
- Config: RLock (reentrant), handles FileNotFoundError + YAMLError explicitly
- Cooldowns: time.monotonic() throughout (no clock-jump sensitivity)
"""

import os
import time
import random
import hashlib
import threading
import yaml
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as _UrllibRetry
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log(tag: str, msg: str) -> None:
    """Timestamped stdout log. Flush=True ensures systemd/journald sees it."""
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} [{tag}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Config — thread-safe hot-reload via mtime check
# ---------------------------------------------------------------------------

_CONFIG_PATH  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
_config_lock  = threading.RLock()  # RLock: cfg() can be called inside load_config() safely
_CONFIG: Dict[str, Any] = {}
_CONFIG_MTIME: float     = 0.0


def load_config(force: bool = False) -> None:
    """
    Reload config.yaml if the file's mtime changed, or if force=True.
    Thread-safe. No-op if nothing changed.
    Logs errors but never raises — caller loop must continue on config failures.
    """
    global _CONFIG, _CONFIG_MTIME
    try:
        mtime = os.path.getmtime(_CONFIG_PATH)
    except FileNotFoundError:
        log("CFG", f"ERROR config file not found: {_CONFIG_PATH}")
        return
    except OSError as e:
        log("CFG", f"ERROR reading config mtime: {e}")
        return

    with _config_lock:
        if not force and mtime == _CONFIG_MTIME:
            return
        try:
            with open(_CONFIG_PATH, "r") as f:
                data = yaml.safe_load(f) or {}
            _CONFIG       = data
            _CONFIG_MTIME = mtime
            log("CFG", f"Reloaded config (mtime={mtime:.0f})")
        except yaml.YAMLError as e:
            log("CFG", f"ERROR yaml parse — keeping previous config: {e}")
        except OSError as e:
            log("CFG", f"ERROR reading config file: {e}")
        except Exception as e:
            log("CFG", f"ERROR unexpected in load_config: {type(e).__name__}: {e}")


def cfg(path: str, default: Any = None) -> Any:
    """
    Read a dot-separated key path from the loaded config.
    Returns default if any segment is missing or config is empty.
    Thread-safe (uses same RLock as load_config).

    Example:
        cfg("rules.fast_ts.pump_pct", 0.30)
    """
    with _config_lock:
        cur = _CONFIG
    for key in path.split("."):
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    return cur


# ---------------------------------------------------------------------------
# Circuit breaker — thread-safe, 3-state: closed → open → half-open
# ---------------------------------------------------------------------------

class Circuit:
    """
    3-state circuit breaker per integration point.

    States:
        CLOSED    — normal operation, all calls allowed
        OPEN      — blocking calls after fail_threshold consecutive failures
        HALF_OPEN — one probe allowed after cooldown_s; re-opens on probe failure

    Usage:
        if not circuit.allow():
            return None          # fast-fail, no call attempted
        result = do_call()
        circuit.report(result is not None)

    One instance per integration point (http, telegram, etc.).
    Instances are safe to share across threads.
    """

    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        fail_threshold: int = 5,
        cooldown_s:     int = 30,
        name:           str = "circuit",
    ) -> None:
        self.fail_threshold = fail_threshold
        self.cooldown_s     = cooldown_s
        self.name           = name

        self._lock       = threading.Lock()
        self._state      = self.CLOSED
        self._fails      = 0
        self._open_until = 0.0

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow(self) -> bool:
        """Return True if a call should be attempted now."""
        with self._lock:
            now = time.monotonic()
            if self._state == self.CLOSED:
                return True
            if self._state == self.OPEN:
                if now >= self._open_until:
                    self._state = self.HALF_OPEN
                    log("CIRCUIT", f"{self.name} → half-open (probe allowed)")
                    return True   # allow exactly one probe
                return False
            # HALF_OPEN: allow the probe
            return True

    def report(self, ok: bool) -> None:
        """
        Report the outcome of a call.
        Call this once per logical request, not per retry attempt.
        """
        with self._lock:
            if ok:
                if self._state != self.CLOSED:
                    log("CIRCUIT", f"{self.name} → closed (recovered after probe)")
                self._state = self.CLOSED
                self._fails = 0
            else:
                self._fails += 1
                if self._state == self.HALF_OPEN:
                    self._state      = self.OPEN
                    self._open_until = time.monotonic() + self.cooldown_s
                    log("CIRCUIT", f"{self.name} probe failed → open for {self.cooldown_s}s")
                elif self._fails >= self.fail_threshold:
                    self._state      = self.OPEN
                    self._open_until = time.monotonic() + self.cooldown_s
                    self._fails      = 0
                    log("CIRCUIT", f"{self.name} → open for {self.cooldown_s}s "
                                   f"({self.fail_threshold} consecutive failures)")


# ---------------------------------------------------------------------------
# HTTP — session with connection pooling, transport retry, circuit breaker
# ---------------------------------------------------------------------------

# HTTP codes that are transient and safe to retry
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

_http_session: requests.Session
_http_circuit  = Circuit(fail_threshold=5, cooldown_s=30, name="http")
_http_sess_lock = threading.Lock()   # protects session rebuild


def _build_session() -> requests.Session:
    """
    Build a requests.Session with:
    - connection pool (reuse TCP connections)
    - one transport-level retry for transient network errors
    - application-level retry with backoff is handled in http_call()
    """
    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=_UrllibRetry(
            total=1,
            backoff_factor=0.3,
            status_forcelist=list(_RETRYABLE_STATUS),
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        ),
        pool_connections=4,
        pool_maxsize=16,
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


_http_session = _build_session()


def reset_http_session() -> None:
    """
    Rebuild the HTTP session. Call when the session is suspected corrupted
    (e.g. after circuit recovery, or repeated pool exhaustion errors).
    Thread-safe.
    """
    global _http_session
    with _http_sess_lock:
        try:
            _http_session.close()
        except Exception:
            pass
        _http_session = _build_session()
        log("HTTP", "session rebuilt")


def _backoff_s(attempt: int, base: float = 0.5, cap: float = 30.0) -> float:
    """
    Full-jitter exponential backoff.
    Returns a random value in [0, min(base * 2^attempt, cap)].
    Full jitter (vs equal jitter) gives best load distribution
    when multiple callers retry simultaneously.
    """
    ceiling = min(base * (2 ** attempt), cap)
    return random.uniform(0.0, ceiling)


def http_call(
    method:    str,
    url:       str,
    *,
    json_body: Any      = None,
    params:    Any      = None,
    timeout:   int      = 8,
    retries:   int      = 3,
) -> Optional[str]:
    """
    HTTP call with circuit breaker, application-level retry, and backoff.

    Retry policy:
    - Retries only on 5xx, 429, or network exceptions (Timeout, ConnectionError)
    - Does NOT retry on 4xx (client errors — bad request won't self-heal)
    - circuit.report() called once at the end (not per retry) to avoid
      inflating the failure counter on transient errors

    Returns response text on success, None on failure.
    """
    if not _http_circuit.allow():
        log("HTTP", f"SKIP circuit-open [{method} {url[:60]}]")
        return None

    rid = f"{method[0]}{abs(hash(url)) % 0xFFFF:04x}"
    last_ok = False

    for attempt in range(retries + 1):
        t0 = time.monotonic()
        try:
            with _http_sess_lock:
                sess = _http_session

            if method == "GET":
                r = sess.get(url, params=params, timeout=timeout)
            elif method == "POST":
                r = sess.post(url, json=json_body, timeout=timeout)
            else:
                log("HTTP", f"unsupported method: {method}")
                _http_circuit.report(False)
                return None

            dt = time.monotonic() - t0

            if r.status_code < 400:
                log("HTTP", f"ok rid={rid} {method} {r.status_code} {dt:.3f}s")
                last_ok = True
                break

            # 4xx (except 429) — client error, don't retry
            if r.status_code < 500 and r.status_code != 429:
                log("HTTP", f"client-err rid={rid} {r.status_code} — no retry [{url[:60]}]")
                last_ok = False
                break

            # 5xx or 429 — retryable
            log("HTTP", f"server-err rid={rid} status={r.status_code} attempt={attempt}/{retries}")

        except requests.exceptions.Timeout:
            log("HTTP", f"timeout rid={rid} attempt={attempt}/{retries} [{url[:60]}]")
        except requests.exceptions.ConnectionError as e:
            log("HTTP", f"conn-err rid={rid} attempt={attempt}/{retries}: {e}")
        except Exception as e:
            log("HTTP", f"exc rid={rid} {type(e).__name__}: {e} attempt={attempt}/{retries}")

        if attempt < retries:
            wait = _backoff_s(attempt)
            log("HTTP", f"retry in {wait:.2f}s rid={rid}")
            time.sleep(wait)

    # Report to circuit once — after all attempts are exhausted or success
    _http_circuit.report(last_ok)

    if not last_ok:
        log("HTTP", f"GIVEUP rid={rid} [{method} {url[:60]}] after {retries + 1} attempts")
        return None

    # r is defined here because last_ok=True only reached via `break` after successful r
    return r.text  # type: ignore[return-value]


def http_get(url: str, params: Any = None, **kw) -> Optional[str]:
    """Convenience wrapper for GET requests."""
    return http_call("GET", url, params=params, **kw)


def http_post(url: str, json: Any = None, **kw) -> Optional[str]:
    """Convenience wrapper for POST requests."""
    return http_call("POST", url, json_body=json, **kw)


# ---------------------------------------------------------------------------
# Telegram — circuit breaker + dedup window to prevent double-alerts
# ---------------------------------------------------------------------------

_tg_circuit   = Circuit(fail_threshold=3, cooldown_s=60, name="telegram")
_tg_lock      = threading.Lock()
_tg_sent: Dict[str, float] = {}  # dedup_key → monotonic timestamp of confirmed send
_TG_DEDUP_S   = 5.0              # suppress exact-duplicate messages within this window


def _tg_dedup_key(msg: str) -> str:
    """Short hash of message content — used as dedup key."""
    return hashlib.md5(msg.encode(), usedforsecurity=False).hexdigest()[:12]


def _tg_prune_sent() -> None:
    """
    Remove expired dedup entries.
    Must be called under _tg_lock.
    """
    cutoff  = time.monotonic() - _TG_DEDUP_S
    expired = [k for k, t in _tg_sent.items() if t < cutoff]
    for k in expired:
        del _tg_sent[k]


def tg_send(msg: str) -> bool:
    """
    Send a Telegram message with:
    - dedup: same message within _TG_DEDUP_S seconds is dropped (after a confirmed send)
    - circuit breaker: stops attempts when Telegram API is consistently down
    - dedup key is only registered AFTER a confirmed successful send,
      so failed sends don't block the next legitimate retry

    Returns True on successful send, False on skip/failure.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat  = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat:
        log("ALERT", msg)
        return False

    key = _tg_dedup_key(msg)

    # Check dedup before doing any network call
    with _tg_lock:
        _tg_prune_sent()
        if key in _tg_sent:
            ago = time.monotonic() - _tg_sent[key]
            log("TG", f"dedup skip (sent {ago:.1f}s ago): {msg[:40]}")
            return False

    if not _tg_circuit.allow():
        log("TG", "SKIP circuit-open")
        return False

    url    = f"https://api.telegram.org/bot{token}/sendMessage"
    body   = {"chat_id": chat, "text": msg}

    log("RULE", f"alert | {msg[:80]}")
    result = http_post(url, json=body, timeout=6, retries=2)
    ok     = result is not None

    _tg_circuit.report(ok)

    if ok:
        # Register dedup ONLY on successful send
        with _tg_lock:
            _tg_sent[key] = time.monotonic()
    else:
        log("TG", f"FAILED to send: {msg[:60]}")

    return ok


def tg_ping(msg: str) -> bool:
    """Alias for tg_send. Used for boot/health pings."""
    return tg_send(msg)


# ---------------------------------------------------------------------------
# Cooldowns — per (symbol, rule) throttle using monotonic clock
# ---------------------------------------------------------------------------

_cooldowns:     Dict[str, float] = {}
_cooldown_lock: threading.Lock   = threading.Lock()


def on_cooldown(symbol: str, rule: str, cooldown_min: int) -> bool:
    """
    Return True if symbol:rule is still within its cooldown window.
    Uses monotonic clock — immune to system clock adjustments.
    """
    key = f"{symbol}:{rule}"
    with _cooldown_lock:
        last = _cooldowns.get(key, 0.0)
    return (time.monotonic() - last) < (cooldown_min * 60.0)


def mark_cooldown(symbol: str, rule: str) -> None:
    """Record that symbol:rule fired right now."""
    key = f"{symbol}:{rule}"
    with _cooldown_lock:
        _cooldowns[key] = time.monotonic()


def clear_cooldowns() -> None:
    """Clear all cooldowns. Intended for testing only."""
    with _cooldown_lock:
        _cooldowns.clear()
