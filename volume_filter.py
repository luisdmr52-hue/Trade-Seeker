"""
volume_filter.py — Capa 3: Volumen Relativo
Kryptos Trade Seeker · F1

Calcula rel_volume = vol_now_30s / vol_base_30s para un símbolo
en el momento de confirmación de señal PUMP_FAST.

vol_now:  quote volume (USDT) acumulado en la ventana aggTrade activa.
          Se obtiene desde AggTradeConfirmer via debug_state()["quote_vol_30s"].

vol_base: estimado del volumen promedio por ventana de 30s.
          F1: quote_vol_24h / 2880 (24h * 120 ventanas/h) — proxy estable
          sin necesidad de historia de klines.
          F2+: reemplazar con query a metrics_ext_15m.

Thread-safe: sí (caché protegido con Lock, sin estado mutable compartido).
Degradación: si vol_base no está disponible → retorna (0.0, "no_base").
"""

import time
import threading
from typing import Optional, Tuple

_WINDOWS_PER_DAY: int = 2880  # 24h * 60min/h * 2 ventanas/min
_CACHE_TTL_S: int = 120


class RelativeVolumeFilter:
    """
    Uso en run_fast_loop:
        rvf = RelativeVolumeFilter(min_rel_volume=3.0)

        rel_vol, reason = rvf.check(sym, quote_vol_30s, quote_vol_24h)
        if reason != "ok":
            log("FAST", f"rel_vol skip {sym}: {reason} ({rel_vol:.2f}x)")
            confirmer.remove(sym)
            continue
    """

    def __init__(self, min_rel_volume: float = 3.0):
        self.min_rel_volume = min_rel_volume
        self._cache: dict   = {}   # sym → (vol_base, monotonic_ts)
        self._lock          = threading.Lock()

    def check(
        self,
        symbol:        str,
        quote_vol_30s: float,
        quote_vol_24h: float,
    ) -> Tuple[float, str]:
        """
        Retorna (rel_volume, reason).
        reason == "ok"             → pasa el filtro
        reason == "no_vol_now"     → ventana aggTrade vacía
        reason == "no_base"        → vol_24h no disponible o cero
        reason == "low_rel_volume" → rel_volume < min_rel_volume
        """
        if quote_vol_30s <= 0:
            return 0.0, "no_vol_now"

        vol_base = self._get_vol_base(symbol, quote_vol_24h)
        if vol_base is None or vol_base <= 0:
            return 0.0, "no_base"

        rel_vol = quote_vol_30s / vol_base

        if rel_vol < self.min_rel_volume:
            return rel_vol, "low_rel_volume"

        return rel_vol, "ok"

    def rel_volume(
        self,
        symbol:        str,
        quote_vol_30s: float,
        quote_vol_24h: float,
    ) -> Optional[float]:
        """Solo el valor numérico, o None si no calculable. Para logging."""
        if quote_vol_30s <= 0 or quote_vol_24h <= 0:
            return None
        vol_base = self._get_vol_base(symbol, quote_vol_24h)
        if not vol_base:
            return None
        return quote_vol_30s / vol_base

    def invalidate(self, symbol: str):
        with self._lock:
            self._cache.pop(symbol, None)

    def cache_size(self) -> int:
        with self._lock:
            return len(self._cache)

    def _get_vol_base(self, symbol: str, quote_vol_24h: float) -> Optional[float]:
        """
        F1: vol_base = quote_vol_24h / 2880
        F2+: reemplazar con query a metrics_ext_15m escalada a ventana 30s.
        """
        now = time.monotonic()

        with self._lock:
            cached = self._cache.get(symbol)
            if cached is not None:
                vol_base, ts = cached
                if now - ts < _CACHE_TTL_S:
                    return vol_base

        if quote_vol_24h <= 0:
            return None

        vol_base = quote_vol_24h / _WINDOWS_PER_DAY

        with self._lock:
            self._cache[symbol] = (vol_base, now)

        return vol_base
