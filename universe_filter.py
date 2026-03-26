"""
universe_filter.py — Capa 1 (F1) — KTS Trade Seeker

Builds and refreshes a filtered watchlist of ~50-100 USDT spot pairs
with the "pumpeable" profile:

    vol_24h_usdt in [min_vol, max_vol]   -- Binance ticker/24hr
    tiene_perp   == False                -- Binance fapi + dapi
    market_cap   < max_mcap              -- CoinGecko (with graceful fallback)
    dormancia    == True                 -- vol_24h < pct25 of ALL USDT pairs

Design principles:
- All data is cached locally; no blocking calls in the hot loop.
- CoinGecko failure -> fallback to cache, then Binance-only (mcap filter skipped).
- Thread-safe: get_universe() can be called from any thread at any time.
- Refresh runs in a background daemon thread using Event.wait() (no busy-loop).

Usage:
    from universe_filter import UniverseFilter

    uf = UniverseFilter()
    uf.start()                    # launches background refresh thread
    syms = uf.get_universe()      # returns current filtered set (list)
"""

import time
import threading
import json
import os
import requests
from typing import List, Optional, Set

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_REST  = "https://api.binance.com"
BINANCE_FAPI  = "https://fapi.binance.com"
BINANCE_DAPI  = "https://dapi.binance.com"
COINGECKO_API = "https://api.coingecko.com/api/v3"

# Watchlist criteria
DEFAULT_MIN_VOL_24H  =    100_000   # USDT -- minimum liquidity
DEFAULT_MAX_VOL_24H  =  5_000_000   # USDT -- above this -> too liquid / large cap
DEFAULT_MAX_MCAP     = 50_000_000   # USD  -- CoinGecko market cap ceiling
DORMANCY_PERCENTILE  = 25           # vol_24h < pct25 of ALL USDT universe -> dormant

# Refresh cadences (seconds)
REFRESH_EXCHANGE_INFO_S = 6 * 3600  # 6h
REFRESH_TICKER_S        =      900  # 15 min
REFRESH_MCAP_S          =     3600  # 60 min
REFRESH_UNIVERSE_S      =     1800  # 30 min

# CoinGecko rate limit: 30 req/min free tier -> 1 req every 2s is safe
CG_REQUEST_DELAY_S = 3.0
CG_LIST_TIMEOUT_S  = 20    # /coins/list is ~15K entries, needs more time
CG_MCAP_CACHE_PATH = "/tmp/kts_cg_mcap_cache.json"

TIMEOUT = 10  # default HTTP timeout

# CoinGecko API key (Demo tier -- free, permanent)
CG_API_KEY = os.getenv("KTS_CG_API_KEY", "")

# CoinGecko API key (Demo tier — free, permanent)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(url: str, params: dict = None, timeout: int = TIMEOUT) -> Optional[dict]:
    """Simple GET with timeout. Returns parsed JSON or None on error."""
    try:
        headers = {"x-cg-demo-api-key": CG_API_KEY} if CG_API_KEY and "coingecko" in url else {}
        r = requests.get(url, params=params, timeout=timeout, headers=headers)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[UniverseFilter] GET error {url}: {e}")
        return None


def _percentile(data: List[float], pct: int) -> float:
    """
    Returns the pct-th percentile of data (nearest-rank method).
    Returns 0.0 on empty input.
    """
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = max(0, int(len(sorted_data) * pct / 100) - 1)
    return sorted_data[idx]


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_spot_usdt_symbols() -> Set[str]:
    """Returns all TRADING USDT spot pairs from Binance exchangeInfo."""
    data = _get(BINANCE_REST + "/api/v3/exchangeInfo")
    if not data:
        return set()
    return {
        s["symbol"]
        for s in data.get("symbols", [])
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
    }


def fetch_perp_symbols() -> Set[str]:
    """
    Returns all active perpetual base symbols mapped to spot USDT pairs.
    Sources: Binance fapi (USD-M) + dapi (COIN-M).
    """
    perps: Set[str] = set()

    # USD-M futures -- symbols already match spot (e.g. BTCUSDT)
    data = _get(BINANCE_FAPI + "/fapi/v1/exchangeInfo")
    if data:
        for s in data.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
                perps.add(s["symbol"])

    # COIN-M futures -- symbols like BTCUSD_PERP; map base -> spot
    data = _get(BINANCE_DAPI + "/dapi/v1/exchangeInfo")
    if data:
        for s in data.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
                base = s.get("baseAsset", "")
                if base:
                    perps.add(base + "USDT")

    return perps


def fetch_ticker_24h() -> dict:
    """
    Returns dict[symbol] -> {quoteVolume: float, lastPrice: float}
    for ALL USDT pairs. Full universe kept so dormancy pct25 uses complete market reference.
    """
    data = _get(BINANCE_REST + "/api/v3/ticker/24hr")
    if not data:
        return {}
    result = {}
    for t in data:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        try:
            result[sym] = {
                "quoteVolume": float(t.get("quoteVolume", 0)),
                "lastPrice":   float(t.get("lastPrice", 0)),
            }
        except (ValueError, TypeError):
            continue
    return result


def fetch_coingecko_mcap(spot_symbols: Set[str]) -> dict:
    """
    Returns dict[symbol_usdt] -> market_cap_usd for as many symbols as possible.

    Strategy:
    1. Load /coins/list (extended timeout) to build ticker -> coin_id mapping.
    2. Paginate /coins/markets (250/page) sorted by mcap desc.
    3. Persist cache on success.
    4. On any failure: return cached data (stale is better than nothing).
    """
    cached = _load_mcap_cache()

    try:
        # Step 1: full coin list -- use extended timeout
        coins_list = _get(COINGECKO_API + "/coins/list", timeout=CG_LIST_TIMEOUT_S)
        if not coins_list:
            print("[UniverseFilter] CoinGecko /coins/list failed -- using cache")
            return cached

        time.sleep(CG_REQUEST_DELAY_S)

        # Build lowercase base -> [coin_id, ...] map
        bases = {sym[:-4].lower() for sym in spot_symbols if sym.endswith("USDT")}
        symbol_to_ids: dict = {}
        for coin in coins_list:
            cid     = coin.get("id", "")
            csymbol = coin.get("symbol", "").lower()
            if csymbol in bases:
                symbol_to_ids.setdefault(csymbol, []).append(cid)

        # Step 2: fetch mcap only for our candidates using ids param (1 request max)
        # Collect all coin_ids relevant to our universe
        all_ids = []
        for base in bases:
            all_ids.extend(symbol_to_ids.get(base, []))
        all_ids = list(set(all_ids))  # deduplicate

        mcap_by_id: dict = {}
        # Step 2: fetch mcap only for candidate coin_ids (chunked, 200 ids/request)
        # Only map ids for our spot_symbols -- avoids 414 URI Too Long
        candidate_bases = {sym[:-4].lower() for sym in spot_symbols if sym.endswith('USDT')}
        all_ids = []
        for base in candidate_bases:
            all_ids.extend(symbol_to_ids.get(base, []))
        all_ids = list(set(all_ids))

        mcap_by_id: dict = {}
        n_requests = 0
        chunk_size = 200
        for i in range(0, max(len(all_ids), 1), chunk_size):
            chunk = all_ids[i:i + chunk_size]
            if not chunk:
                break
            data = _get(
                COINGECKO_API + '/coins/markets',
                params={
                    'vs_currency': 'usd',
                    'ids':         ','.join(chunk),
                    'per_page':    250,
                    'sparkline':   'false',
                },
            )
            n_requests += 1
            if data:
                for coin in data:
                    cid  = coin.get('id')
                    mcap = coin.get('market_cap')
                    if cid and mcap is not None:
                        mcap_by_id[cid] = mcap
            if i + chunk_size < len(all_ids):
                time.sleep(CG_REQUEST_DELAY_S)

        # Step 3: map back to XYZUSDT symbols
        # Multiple ids can share a ticker (e.g. "usdc") -- take highest mcap
        result: dict = {}
        for sym in spot_symbols:
            if not sym.endswith("USDT"):
                continue
            base = sym[:-4].lower()
            ids  = symbol_to_ids.get(base, [])
            if not ids:
                continue
            best_mcap = max((mcap_by_id.get(cid, 0) for cid in ids), default=0)
            if best_mcap > 0:
                result[sym] = best_mcap

        _save_mcap_cache(result)
        print(f"[UniverseFilter] CoinGecko: {len(result)} market caps fetched ({n_requests} requests)")
        return result

    except Exception as e:
        print(f"[UniverseFilter] CoinGecko fetch error: {e} -- using cache ({len(cached)} entries)")
        return cached


def _load_mcap_cache() -> dict:
    try:
        if os.path.exists(CG_MCAP_CACHE_PATH):
            with open(CG_MCAP_CACHE_PATH, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_mcap_cache(data: dict):
    try:
        with open(CG_MCAP_CACHE_PATH, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"[UniverseFilter] cache write error: {e}")


# ---------------------------------------------------------------------------
# Core filter logic
# ---------------------------------------------------------------------------

def build_universe(
    spot_symbols: Set[str],
    perp_symbols: Set[str],
    ticker:       dict,
    mcap:         dict,
    min_vol:      float = DEFAULT_MIN_VOL_24H,
    max_vol:      float = DEFAULT_MAX_VOL_24H,
    max_mcap:     float = DEFAULT_MAX_MCAP,
    use_mcap:     bool  = True,
) -> List[str]:
    """
    Applies all filters and returns the final universe list.

    Filter pipeline (each stage logged for validation):
        1. Spot-only (exclude perps)
        2. Volume range [min_vol, max_vol]
        3. Dormancy: vol_24h < pct25 of ALL USDT pairs in ticker
           (reference = full market, not just the vol-filtered subset)
        4. Market cap < max_mcap (CoinGecko; gracefully skipped if unavailable)
    """
    counters = {
        "spot_base":     len(spot_symbols),
        "after_perp":    0,
        "after_vol":     0,
        "after_dormant": 0,
        "after_mcap":    0,
        "no_mcap_data":  0,
    }

    # Step 1: exclude perps
    candidates = spot_symbols - perp_symbols
    counters["after_perp"] = len(candidates)

    # Step 2: volume range filter
    vol_filtered = []
    for sym in candidates:
        t = ticker.get(sym)
        if not t:
            continue
        vol = t["quoteVolume"]
        if min_vol <= vol <= max_vol:
            vol_filtered.append((sym, vol))
    counters["after_vol"] = len(vol_filtered)

    if not vol_filtered:
        print(f"[UniverseFilter] build_universe: {counters} -> EMPTY (no vol candidates)")
        return []

    # Step 3: dormancy removed -- vol_range already defines the profile

    counters["after_dormant"] = len(vol_filtered)  # dormancy removed

    # Step 4: market cap filter
    if use_mcap and mcap:
        final = []
        for sym, _ in vol_filtered:
            sym_mcap = mcap.get(sym)
            if sym_mcap is None:
                counters["no_mcap_data"] += 1
                final.append(sym)  # include: don't exclude blindly without data
            elif sym_mcap < max_mcap:
                final.append(sym)
        counters["after_mcap"] = len(final)
    else:
        final = [sym for sym, _ in vol_filtered]
        counters["after_mcap"] = len(final)
        if use_mcap and not mcap:
            print("[UniverseFilter] WARNING: mcap filter skipped (CoinGecko unavailable)")

    print(
        f"[UniverseFilter] build_universe: "
        f"spot={counters['spot_base']} "
        f"-> -perp={counters['after_perp']} "
        f"-> vol_range={counters['after_vol']} "
        f"-> dormant={counters['after_dormant']} "
        f"-> mcap={counters['after_mcap']} "
        f"(no_mcap_data={counters['no_mcap_data']})"
    )

    final.sort()
    return final


# ---------------------------------------------------------------------------
# UniverseFilter -- main class
# ---------------------------------------------------------------------------

class UniverseFilter:
    """
    Maintains a refreshed filtered universe in the background.
    Thread-safe for concurrent reads from the fast loop.
    Uses Event.wait() for sleeping -- no busy-loop, responsive to stop().
    """

    def __init__(
        self,
        min_vol:  float = DEFAULT_MIN_VOL_24H,
        max_vol:  float = DEFAULT_MAX_VOL_24H,
        max_mcap: float = DEFAULT_MAX_MCAP,
    ):
        self.min_vol  = min_vol
        self.max_vol  = max_vol
        self.max_mcap = max_mcap

        self._spot_symbols:  Set[str]  = set()
        self._perp_symbols:  Set[str]  = set()
        self._ticker:        dict      = {}
        self._mcap:          dict      = {}
        self._universe:      List[str] = []
        self._mcap_is_stale: bool      = False

        self._last_exchange_info = 0.0
        self._last_ticker        = 0.0
        self._last_mcap          = 0.0
        self._last_universe      = 0.0

        self._lock   = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._stop   = threading.Event()

    def start(self):
        """
        Builds the universe synchronously (blocking) then starts background thread.
        Call once at bot startup before entering the fast loop.
        """
        if self._thread and self._thread.is_alive():
            return
        print("[UniverseFilter] initial build starting (blocking)...")
        self._refresh_all()
        print(f"[UniverseFilter] ready -- {len(self._universe)} symbols in universe")

        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="UniverseFilter"
        )
        self._thread.start()

    def stop(self):
        """Signals the background thread to exit cleanly."""
        self._stop.set()

    def get_universe(self) -> List[str]:
        """Returns the current filtered universe. Never blocks."""
        with self._lock:
            return list(self._universe)

    def stats(self) -> dict:
        """Returns diagnostic info for logging/Grafana."""
        with self._lock:
            return {
                "universe_size": len(self._universe),
                "spot_total":    len(self._spot_symbols),
                "perp_excluded": len(self._perp_symbols),
                "mcap_coverage": len(self._mcap),
                "mcap_is_stale": self._mcap_is_stale,
                "last_refresh":  self._last_universe,
            }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _loop(self):
        """Background refresh loop. Event.wait() blocks cleanly until next cycle."""
        while not self._stop.wait(timeout=REFRESH_UNIVERSE_S):
            try:
                self._refresh_all()
            except Exception as e:
                print(f"[UniverseFilter] refresh error: {e}")

    def _refresh_all(self):
        now = time.time()

        # Exchange info -- every 6h
        if now - self._last_exchange_info > REFRESH_EXCHANGE_INFO_S:
            spot  = fetch_spot_usdt_symbols()
            perps = fetch_perp_symbols()
            if spot:
                with self._lock:
                    self._spot_symbols       = spot
                    self._perp_symbols       = perps
                    self._last_exchange_info = now
                print(
                    f"[UniverseFilter] exchangeInfo: "
                    f"{len(spot)} spot, {len(perps)} perps excluded"
                )

        # Ticker 24h -- every 15 min
        if now - self._last_ticker > REFRESH_TICKER_S:
            ticker = fetch_ticker_24h()
            if ticker:
                with self._lock:
                    self._ticker      = ticker
                    self._last_ticker = now

        # Market cap -- every 60 min
        if now - self._last_mcap > REFRESH_MCAP_S:
            with self._lock:
                spot_snap = set(self._spot_symbols)
            mcap = fetch_coingecko_mcap(spot_snap)
            # Detect stale: CG returned empty but cache exists
            stale = (len(mcap) == 0 and bool(_load_mcap_cache()))
            with self._lock:
                self._mcap          = mcap
                self._last_mcap     = now
                self._mcap_is_stale = stale
            if stale:
                print("[UniverseFilter] WARNING: using stale mcap cache (CoinGecko down)")

        # Rebuild universe from latest cached data
        with self._lock:
            spot   = set(self._spot_symbols)
            perps  = set(self._perp_symbols)
            ticker = dict(self._ticker)
            mcap   = dict(self._mcap)

        universe = build_universe(
            spot_symbols = spot,
            perp_symbols = perps,
            ticker       = ticker,
            mcap         = mcap,
            min_vol      = self.min_vol,
            max_vol      = self.max_vol,
            max_mcap     = self.max_mcap,
            use_mcap     = True,
        )

        with self._lock:
            self._universe      = universe
            self._last_universe = time.time()


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uf = UniverseFilter()
    uf.start()

    s = uf.stats()
    print(f"\nStats: {s}")

    universe = uf.get_universe()
    print(f"\nUniverse ({len(universe)} symbols):")
    for sym in universe[:30]:
        print(f"  {sym}")
    if len(universe) > 30:
        print(f"  ... and {len(universe) - 30} more")

    # Validation checklist:
    # [ ] universe size between 40 and 120
    # [ ] no symbol with known perp (BTCUSDT, ETHUSDT, SOLUSDT) appears
    # [ ] build_universe log shows each filter stage with decreasing counts
    # [ ] mcap_is_stale == False on clean run
