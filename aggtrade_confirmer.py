"""
aggtrade_confirmer.py — P3-B intrabar volume confirmation via aggTrade WS
Trade Seeker v4.2 — patched 2026-03-31

Flow:
  1. Fast loop detects price spike → confirmer.add_candidate(symbol)
  2. Confirmer opens aggTrade WS stream for that symbol (on-demand)
  3. Accumulates buy/sell volume in a 30s rolling window (in-memory deque)
  4. is_confirmed(symbol) → True if buy_vol_30s >= vol_ratio_min × total_vol_30s
  5. Streams auto-close after TTL (default 10 min) or on explicit remove

Design principles:
- Logic-free ingestion: aggTrade stream only accumulates raw events
- Confirmation logic lives in is_confirmed(), not in the WS handler
- One stream per active candidate — no pool, no pre-warming
- Thread-safe: uses threading.Lock for candidate dict access
- Defensive: stream errors → logged, candidate marked degraded (not crashed)

Patch 2026-03-28:
- debug_state() now exposes quote_vol_30s (USDT volume in current window)
  Required by fast_loop.py → RelativeVolumeFilter (Capa 3).

Patch 2026-03-31:
- active_candidates() added — returns set of active symbol names (thread-safe).
  Required by fast_loop.py to sync _trigger_prices cleanup after cleanup_expired().
"""

import time
import json
import threading
import urllib.parse
import websocket
from collections import deque
from typing import Dict, Optional, Set

WS_BASE           = "wss://stream.binance.com:9443/ws"
DEFAULT_WINDOW_S  = 30
DEFAULT_TTL_S     = 600
DEFAULT_VOL_RATIO = 0.60


class _Candidate:
    def __init__(self, symbol: str, window_s: int, ttl_s: int):
        self.symbol       = symbol
        self.activated_at = time.time()
        self.expires_at   = self.activated_at + ttl_s
        self.window_s     = window_s
        self.events: deque = deque()
        self.ws           = None
        self.ws_thread    = None
        self.connected    = False
        self.error        = None

    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    def add_event(self, ts: float, price: float, qty: float, is_buyer_maker: bool):
        # is_buyer_maker=True → seller is taker (sell pressure) → negative
        # is_buyer_maker=False → buyer is taker (buy pressure) → positive
        quote      = price * qty
        signed_qty = qty if not is_buyer_maker else -qty
        self.events.append((ts, signed_qty, quote))
        self._trim()

    def _trim(self):
        cutoff = time.time() - self.window_s
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def buy_ratio(self) -> Optional[float]:
        self._trim()
        if not self.events:
            return None
        buy   = sum(qty for _, qty, _ in self.events if qty > 0)
        sell  = sum(abs(qty) for _, qty, _ in self.events if qty < 0)
        total = buy + sell
        if total == 0:
            return None
        return buy / total

    def quote_vol_30s(self) -> float:
        """Total USDT volume (buy + sell) in the current window."""
        self._trim()
        return sum(q for _, _, q in self.events)

    def n_events(self) -> int:
        self._trim()
        return len(self.events)


class AggTradeConfirmer:
    """
    Manages on-demand aggTrade WS streams for active candidates.

    Usage in fast loop:
        confirmer = AggTradeConfirmer(log_fn=log)

        # When spike detected:
        confirmer.add_candidate(sym)

        # Before alerting (needs ~1-2s of data after add):
        if confirmer.is_confirmed(sym):
            send_alert(...)
            confirmer.remove(sym)

        # Call periodically to clean up expired streams:
        confirmer.cleanup_expired()

        # Sync _trigger_prices after cleanup:
        active = confirmer.active_candidates()
    """

    def __init__(
        self,
        window_s:   int   = DEFAULT_WINDOW_S,
        ttl_s:      int   = DEFAULT_TTL_S,
        vol_ratio:  float = DEFAULT_VOL_RATIO,
        min_events: int   = 3,
        log_fn=None,
        proxy_pool=None,
    ):
        self.window_s   = window_s
        self.ttl_s      = ttl_s
        self.vol_ratio  = vol_ratio
        self.min_events = min_events
        self._log        = log_fn or (lambda tag, msg: print(f"[{tag}] {msg}"))
        self._proxy_pool = proxy_pool
        self._candidates: Dict[str, _Candidate] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_candidate(self, symbol: str):
        """Register symbol and open aggTrade stream."""
        with self._lock:
            if symbol in self._candidates:
                self._candidates[symbol].expires_at = time.time() + self.ttl_s
                return
            c = _Candidate(symbol, self.window_s, self.ttl_s)
            self._candidates[symbol] = c
        self._log("CONF", f"candidate added: {symbol}")
        self._open_stream(c)

    def is_confirmed(self, symbol: str) -> bool:
        """Returns True if buy pressure exceeds vol_ratio threshold."""
        with self._lock:
            c = self._candidates.get(symbol)
        if c is None:
            return False
        if c.n_events() < self.min_events:
            return False
        ratio = c.buy_ratio()
        if ratio is None:
            return False
        return ratio >= self.vol_ratio

    def remove(self, symbol: str):
        """Close stream and remove candidate."""
        with self._lock:
            c = self._candidates.pop(symbol, None)
        if c:
            self._close_stream(c)
            self._log("CONF", f"candidate removed: {symbol}")

    def cleanup_expired(self):
        """Close and remove expired candidates. Call periodically."""
        with self._lock:
            expired = [s for s, c in self._candidates.items() if c.is_expired()]
        for symbol in expired:
            self.remove(symbol)
            self._log("CONF", f"candidate expired: {symbol}")

    def active_count(self) -> int:
        with self._lock:
            return len(self._candidates)

    def active_candidates(self) -> Set[str]:
        """
        Returns a snapshot of currently active symbol names.
        Thread-safe — acquires lock before reading keys.
        Used by fast_loop.py to sync _trigger_prices after cleanup_expired().
        """
        with self._lock:
            return set(self._candidates.keys())

    def debug_state(self, symbol: str) -> dict:
        """
        Returns a snapshot dict of the candidate's current state.
        Includes quote_vol_30s (USDT volume in current window) —
        required by RelativeVolumeFilter (Capa 3) in fast_loop.py.
        """
        with self._lock:
            c = self._candidates.get(symbol)
        if c is None:
            return {
                "active":        False,
                "quote_vol_30s": 0.0,
            }
        return {
            "active":        True,
            "connected":     c.connected,
            "n_events":      c.n_events(),
            "buy_ratio":     c.buy_ratio(),
            "quote_vol_30s": c.quote_vol_30s(),
            "expires_in":    round(c.expires_at - time.time(), 1),
            "error":         c.error,
        }

    # ------------------------------------------------------------------
    # Internal — WS management
    # ------------------------------------------------------------------

    def _open_stream(self, c: _Candidate):
        url = f"{WS_BASE}/{c.symbol.lower()}@aggTrade"

        def on_message(ws, message):
            try:
                data           = json.loads(message)
                price          = float(data["p"])
                qty            = float(data["q"])
                is_buyer_maker = bool(data["m"])
                ts             = data["T"] / 1000.0
                c.add_event(ts, price, qty, is_buyer_maker)
            except Exception as e:
                c.error = str(e)

        proxy_url = None
        if self._proxy_pool:
            proxy_url = self._proxy_pool.acquire("confirmer")
        proxy_kwargs: dict = {}
        if proxy_url:
            try:
                import urllib.parse as _up
                parsed = _up.urlparse(proxy_url)
                proxy_kwargs = {
                    "proxy_type":      parsed.scheme,
                    "http_proxy_host": parsed.hostname,
                    "http_proxy_port": parsed.port,
                    "http_proxy_auth": (parsed.username, parsed.password),
                }
                self._log("CONF", f"stream {c.symbol} via proxy host={parsed.hostname}")
            except Exception as e:
                self._log("CONF", f"proxy parse error {c.symbol}: {e} — direct")
                proxy_kwargs = {}

        def on_open(ws):
            c.connected = True
            self._log("CONF", f"stream open: {c.symbol}")
            if self._proxy_pool:
                self._proxy_pool.release("confirmer", success=True)

        def on_error(ws, error):
            c.connected = False
            c.error = str(error)
            self._log("CONF", f"stream error: {c.symbol} — {error}")
            if self._proxy_pool:
                self._proxy_pool.release("confirmer", success=False)

        def on_close(ws, code, msg):
            c.connected = False
            self._log("CONF", f"stream closed: {c.symbol} code={code}")

        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_open=on_open,
            on_error=on_error,
            on_close=on_close,
        )
        c.ws = ws
        t = threading.Thread(
            target=ws.run_forever,
            kwargs={"ping_interval": 30, "ping_timeout": 10, **proxy_kwargs},
            daemon=True,
            name=f"aggtrade-{c.symbol}",
        )
        c.ws_thread = t
        t.start()

    def _close_stream(self, c: _Candidate):
        if c.ws:
            try:
                c.ws.close()
            except Exception:
                pass
