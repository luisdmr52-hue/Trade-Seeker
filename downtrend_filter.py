"""
downtrend_filter.py — Capa 2: Downtrend Filter
Kryptos Trade Seeker · F1

Detecta si un símbolo está en downtrend estructural y debe ser bloqueado
antes de abrir el stream aggTrade.

Lógica (DC-C2-02):
    downtrend = EMA(20) < EMA(50) AND price_now < EMA(50)

Fuente de datos (DC-C2-01):
    metrics_ext_15m, campo avg_price, columna de tiempo: bucket.
    Historia: últimas 14h (~56 velas de 15m) para convergencia de EMA(50).

Contrato:
    is_downtrend(symbol) -> True  = BLOQUEAR
                         -> False = DEJAR PASAR

Fail-open (DC-C2-04):
    Cualquier error (DB caída, datos insuficientes, timeout) retorna False.
    El símbolo NO se bloquea por fallo de infraestructura.

Thread-safe: caché protegido con Lock. Contadores con Lock separado.

Caché (DC-C2-03):
    TTL nominal: 180s (resultado válido).
    TTL corto:    30s (datos insuficientes — puede mejorar pronto).
    gc(active_symbols): llamar periódicamente para limpiar entradas muertas.
    invalidate(symbol): llamar desde UniverseFilter cuando un símbolo
                        re-entra al universo para evitar decisiones con
                        caché stale.

Conexión:
    Conexión nueva por query (infrecuente gracias al caché).
    statement_timeout=4000ms para evitar que una query lenta congele el thread.
    connect_timeout=5s para el handshake.
"""

import time
import threading
from typing import Optional

import psycopg2

from utils import log

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# DSN — mismo host/credenciales que ts_feed.py. Definido localmente para
# evitar acoplamiento frágil entre módulos. Si cambia, actualizar aquí también.
_DSN = "host=localhost port=5432 dbname=tsdb user=postgres password=postgres"

# statement_timeout en milisegundos — evita que la query cuelgue el thread
_STATEMENT_TIMEOUT_MS = 4000

# Timeout de conexión en segundos (solo handshake)
_CONNECT_TIMEOUT_S = 5

_CACHE_TTL_S: int       = 180   # TTL normal: resultado válido
_CACHE_TTL_SHORT_S: int = 30    # TTL corto: datos insuficientes
_MIN_PRICES: int        = 50    # mínimo de velas para calcular EMA(50)
_EMA_FAST: int          = 20
_EMA_SLOW: int          = 50

# Query — usa 'bucket' (nombre real de la columna en metrics_ext_15m)
_QUERY = """
    SELECT avg_price
    FROM metrics_ext_15m
    WHERE symbol = %s
      AND bucket >= NOW() - INTERVAL '14 hours'
    ORDER BY bucket ASC;
"""


# ---------------------------------------------------------------------------
# EMA calculation
# ---------------------------------------------------------------------------

def _compute_ema(prices: list, period: int) -> float:
    """
    EMA recursiva sobre lista de precios en orden ASC.
    Inicializa con prices[0] como semilla.
    Con >=50 puntos el sesgo inicial es marginal para filtro estructural F1.
    """
    k = 2.0 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1.0 - k)
    return ema


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def _fetch_prices(symbol: str) -> Optional[list]:
    """
    Retorna lista de avg_price en orden ASC para las últimas 14 horas,
    o None si hay error.

    Conexión nueva por llamada — infrecuente gracias al caché.
    statement_timeout evita bloqueos indefinidos bajo carga en Timescale.
    """
    conn = None
    try:
        dsn_with_timeout = (
            f"{_DSN} "
            f"connect_timeout={_CONNECT_TIMEOUT_S} "
            f"options='-c statement_timeout={_STATEMENT_TIMEOUT_MS}'"
        )
        conn = psycopg2.connect(dsn_with_timeout)
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(_QUERY, (symbol,))
            rows = cur.fetchall()
        return [row[0] for row in rows if row[0] is not None]
    except Exception as e:
        log("C2", f"ERROR fetch_prices {symbol}: {type(e).__name__}: {e}")
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# DowntrendFilter
# ---------------------------------------------------------------------------

class DowntrendFilter:
    """
    Integración en run_fast_loop (fast_loop.py):

        # Al inicializar el loop:
        dtf = DowntrendFilter()

        # Antes de confirmer.add_candidate(sym):
        if delta >= pump_pct:
            if dtf.is_downtrend(sym):
                log("FAST", f"downtrend skip {sym}")
                continue
            confirmer.add_candidate(sym)
            ...

        # En el ciclo de cleanup periódico (_CLEANUP_EVERY):
        dtf.gc(set(uf.get_universe()))

    Integración en universe_filter.py:
        Cuando un símbolo entra al universo → dtf.invalidate(symbol)
        Evita que caché stale bloquee un símbolo que acaba de calificar.
    """

    def __init__(
        self,
        cache_ttl_s: int       = _CACHE_TTL_S,
        cache_ttl_short_s: int = _CACHE_TTL_SHORT_S,
    ):
        self.cache_ttl_s       = cache_ttl_s
        self.cache_ttl_short_s = cache_ttl_short_s

        # caché: sym → (value: bool, ts: float, ttl: int)
        self._cache: dict  = {}
        self._lock         = threading.Lock()

        # contadores de diagnóstico
        self._hits:   int = 0
        self._misses: int = 0
        self._counter_lock = threading.Lock()

    # ── Interfaz pública ─────────────────────────────────────────────────

    def is_downtrend(self, symbol: str) -> bool:
        """
        Retorna True si el símbolo está en downtrend estructural.
        Retorna False en cualquier condición de error o datos insuficientes
        (fail-open — DC-C2-04).
        """
        now = time.monotonic()

        # ── Caché ────────────────────────────────────────────────────────
        with self._lock:
            cached = self._cache.get(symbol)
            if cached is not None:
                value, ts, ttl = cached
                if now - ts < ttl:
                    with self._counter_lock:
                        self._hits += 1
                    return value

        with self._counter_lock:
            self._misses += 1

        # ── Fetch ────────────────────────────────────────────────────────
        prices = _fetch_prices(symbol)

        if prices is None:
            # Error de DB — fail-open, no cachear para reintentar pronto
            return False

        # ── Datos insuficientes ──────────────────────────────────────────
        if len(prices) < _MIN_PRICES:
            log("C2",
                f"insufficient data {symbol}: {len(prices)} velas < {_MIN_PRICES} "
                f"— fail-open (retry en {self.cache_ttl_short_s}s)")
            # Cachear con TTL corto — puede mejorar en el próximo intervalo
            with self._lock:
                self._cache[symbol] = (False, now, self.cache_ttl_short_s)
            return False

        # ── Cálculo ──────────────────────────────────────────────────────
        try:
            ema_fast  = _compute_ema(prices, _EMA_FAST)
            ema_slow  = _compute_ema(prices, _EMA_SLOW)
            price_now = prices[-1]

            result = ema_fast < ema_slow and price_now < ema_slow

        except Exception as e:
            log("C2", f"ERROR compute {symbol}: {type(e).__name__}: {e} — fail-open")
            return False

        # ── Log solo si downtrend detectado ─────────────────────────────
        if result:
            log("C2",
                f"DOWNTREND {symbol} | "
                f"ema{_EMA_FAST}={ema_fast:.6g} "
                f"ema{_EMA_SLOW}={ema_slow:.6g} "
                f"price={price_now:.6g} "
                f"n={len(prices)}")

        # ── Cachear con TTL normal ───────────────────────────────────────
        with self._lock:
            self._cache[symbol] = (result, now, self.cache_ttl_s)

        return result

    def invalidate(self, symbol: str) -> None:
        """
        Fuerza re-query en el próximo ciclo para este símbolo.
        Llamar desde UniverseFilter cuando un símbolo re-entra al universo.
        """
        with self._lock:
            self._cache.pop(symbol, None)

    def gc(self, active_symbols: set) -> None:
        """
        Elimina entradas del caché para símbolos que ya no están en el universo.
        Llamar periódicamente desde el ciclo de cleanup de fast_loop.py.

        Ejemplo:
            dtf.gc(set(uf.get_universe()))
        """
        with self._lock:
            dead = [s for s in self._cache if s not in active_symbols]
            for s in dead:
                del self._cache[s]
        if dead:
            log("C2", f"gc: removed {len(dead)} stale entries — {dead}")

    def cache_size(self) -> int:
        with self._lock:
            return len(self._cache)

    def cache_stats(self) -> dict:
        """
        Retorna hits/misses acumulados desde el inicio.
        Útil para logs de diagnóstico periódico.
        """
        with self._counter_lock:
            return {"hits": self._hits, "misses": self._misses}
