"""
coverage_universe_builder.py — Capa 0 (F1) — KTS Trade Seeker

Builds and serves the ~210-220 symbol Coverage Universe used as input
for BenthosRuntime. This is NOT the bot trading universe (UniverseFilter)
— it is the wide observability layer fed into Bento.

Inclusion criteria (DC-COV-01/02/03, validated probe_01 2026-03-31):
    quoteAsset  = USDT
    status      = TRADING
    quoteVolume >= 200_000 USDT/24h
    tradeCount  >= 500/24h
    NOT a leveraged ETF  (UP/DOWN/BULL/BEAR suffix)
    NOT a structural stable pair (pegged assets)

NO mcap filter, NO vol ceiling, NO perp filter — those are bot-universe
concerns, not coverage concerns.

Design:
- refresh() is synchronous — called at boot and by background thread.
- Thread-safe: get_coverage_universe() / get_coverage_signature() safe
  to call from any thread at any time.
- Background refresh via daemon thread with Event.wait (no busy-loop).
- Signature = sha256(sorted symbols) — used by BenthosRuntime for
  anti-churn (avoids needless Bento restarts on minor vol fluctuations).
"""

import hashlib
import re
import threading
import time
from typing import List, Optional

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_REST = "https://api.binance.com"

MIN_QUOTE_VOL_24H  = 200_000   # USDT  (DC-COV-01)
MIN_TRADE_COUNT    = 500       # trades/24h

REBUILD_INTERVAL_S = 6 * 3600  # 6h
REQUEST_TIMEOUT    = 15        # seconds

# Leveraged ETF suffixes — excluded structurally
_LEVERAGED_RE = re.compile(r"(UP|DOWN|BULL|BEAR)USDT$")

# Pegged / stable assets — excluded structurally (DC-COV-02)
_STABLE_PAIRS = frozenset({
    "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT",
    "BUSDUSDT", "EURUSDT", "GBPUSDT", "AUDUSDT", "USTUSDT",
    "USD1USDT",    # probe_01: pegged asset with artificial vol
    "RLUSDUSDT",   # probe_01: pegged asset with artificial vol
})

# ---------------------------------------------------------------------------
# Internal HTTP helper
# ---------------------------------------------------------------------------

def _get(url: str, params: dict = None) -> Optional[list]:
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[CoverageUniverse] GET error {url}: {e}")
        return None

# ---------------------------------------------------------------------------
# Core build logic
# ---------------------------------------------------------------------------

def _build(extra_stable_pairs: frozenset = frozenset()) -> List[str]:
    """
    Fetches Binance exchangeInfo + ticker/24hr and applies inclusion/exclusion criteria.
    Returns sorted list of symbols. Returns empty list on fetch failure.
    """
    stable_pairs = _STABLE_PAIRS | extra_stable_pairs

    # Step 1: get TRADING symbols from exchangeInfo (authoritative status filter)
    ei_data = _get(BINANCE_REST + "/api/v3/exchangeInfo")
    if not ei_data:
        print("[CoverageUniverse] exchangeInfo fetch failed — returning empty universe")
        return []
    trading_usdt = {
        s["symbol"] for s in ei_data.get("symbols", [])
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
    }

    # Step 2: get volume/trade counts from ticker/24hr
    ticker_data = _get(BINANCE_REST + "/api/v3/ticker/24hr")
    if not ticker_data:
        print("[CoverageUniverse] ticker/24hr fetch failed — returning empty universe")
        return []

    counters = {
        "total":       len(trading_usdt),
        "leveraged":   0,
        "stable":      0,
        "low_vol":     0,
        "low_trades":  0,
        "accepted":    0,
    }

    ticker_map = {t.get("symbol", ""): t for t in ticker_data}
    result = []

    for sym in trading_usdt:
        if _LEVERAGED_RE.search(sym):
            counters["leveraged"] += 1
            continue

        if sym in stable_pairs:
            counters["stable"] += 1
            continue

        t = ticker_map.get(sym)
        if not t:
            continue

        try:
            quote_vol   = float(t.get("quoteVolume", 0))
            trade_count = int(t.get("count", 0))
        except (ValueError, TypeError):
            continue

        if quote_vol < MIN_QUOTE_VOL_24H:
            counters["low_vol"] += 1
            continue

        if trade_count < MIN_TRADE_COUNT:
            counters["low_trades"] += 1
            continue

        # Exclude non-ASCII symbols — Bento YAML parser rejects them
        if not sym.isascii():
            continue
        result.append(sym)
        counters["accepted"] += 1

    result.sort()
    print(
        f"[CoverageUniverse] build: "
        f"trading_usdt={counters['total']} "
        f"leveraged={counters['leveraged']} "
        f"stable={counters['stable']} "
        f"low_vol={counters['low_vol']} "
        f"low_trades={counters['low_trades']} "
        f"accepted={counters['accepted']}"
    )
    return result
# ---------------------------------------------------------------------------
# CoverageUniverseBuilder
# ---------------------------------------------------------------------------

class CoverageUniverseBuilder:
    """
    Maintains a refreshed Coverage Universe in the background.
    Thread-safe. Uses Event.wait for sleeping — no busy-loop.
    """

    def __init__(
        self,
        min_quote_vol_24h_usd: float = MIN_QUOTE_VOL_24H,
        min_trade_count_24h:   int   = MIN_TRADE_COUNT,
        rebuild_interval_h:    float = REBUILD_INTERVAL_S / 3600,
        extra_stable_pairs:    list  = None,
    ):
        self._min_vol      = min_quote_vol_24h_usd
        self._min_trades   = min_trade_count_24h
        self._interval_s   = rebuild_interval_h * 3600
        self._extra_stable = frozenset(extra_stable_pairs or [])

        self._universe:   List[str] = []
        self._signature:  str       = ""
        self._last_build: float     = 0.0
        self._lock        = threading.RLock()

        self._thread: Optional[threading.Thread] = None
        self._stop    = threading.Event()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def refresh(self) -> None:
        """Synchronous rebuild. Blocks until complete. Call at boot."""
        universe = _build(self._extra_stable)
        sig = self._compute_signature(universe)
        with self._lock:
            self._universe   = universe
            self._signature  = sig
            self._last_build = time.time()
        print(
            f"[CoverageUniverse] refresh done — "
            f"{len(universe)} symbols, sig={sig[:12]}..."
        )

    def start(self) -> None:
        """
        Starts background refresh thread. Call after initial refresh().
        Safe to call multiple times — no-op if already running.
        """
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="CoverageUniverseBuilder"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def get_coverage_universe(self) -> List[str]:
        """Returns current universe. Never blocks. Returns [] if not yet built."""
        with self._lock:
            return list(self._universe)

    def get_coverage_signature(self) -> str:
        """Returns sha256 hex of current sorted universe. Empty string if not built."""
        with self._lock:
            return self._signature

    def get_meta(self) -> dict:
        """Diagnostic info for logging."""
        with self._lock:
            return {
                "universe_size": len(self._universe),
                "signature":     self._signature[:12] + "..." if self._signature else "",
                "last_build_s":  round(time.time() - self._last_build, 1) if self._last_build else None,
                "interval_h":    self._interval_s / 3600,
            }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_signature(universe: List[str]) -> str:
        payload = ",".join(sorted(universe)).encode()
        return hashlib.sha256(payload).hexdigest()

    def _loop(self) -> None:
        while not self._stop.wait(timeout=self._interval_s):
            try:
                self.refresh()
            except Exception as e:
                print(f"[CoverageUniverse] background refresh error: {e}")

# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cub = CoverageUniverseBuilder()
    cub.refresh()

    meta = cub.get_meta()
    print(f"\nMeta: {meta}")

    universe = cub.get_coverage_universe()
    print(f"\nFirst 20: {universe[:20]}")
    print(f"Signature: {cub.get_coverage_signature()}")

    # Validation checklist:
    # [ ] universe size ~210-220
    # [ ] BTCUSDT, ETHUSDT absent (have perps — but wait, perp filter NOT here)
    # [ ] USDCUSDT, USD1USDT absent (stables)
    # [ ] BTCUPUSDT absent (leveraged)
    # [ ] accepted count matches universe size
