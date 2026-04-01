"""
time_gate.py — Capa 4: Time Policy Layer
Kryptos Trade Seeker · F1

Separa contexto de mercado (market_regime) de disponibilidad del operador
(operator_window). El operador es human-in-the-loop — la decisión de bloquear
depende PRIMERO del operador, no del mercado.

Interfaz pública:
    evaluate_hour(utc_hour: int) -> dict   # lógica pura (SoT)
    evaluate_now()              -> dict    # wrapper UTC

Output:
    {
        "utc_hour":        int | None,
        "local_hour":      int | None,
        "market_regime":   "aggressive" | "normal" | "caution" | "pause" | "low_activity" | "unknown",
        "operator_window": "awake" | "sleep" | "unknown",
        "blocked":         bool,
        "reason":          "operator_sleep" | "allowed_window" | "fail_open",
    }

Política F1:
    operator_window == "sleep"  -> blocked=True,  reason="operator_sleep"
    operator_window == "awake"  -> blocked=False, reason="allowed_window"
    market_regime NO bloquea en F1 — solo etiqueta para calibración futura.

Operator window (Caracas, UTC-4):
    awake: 08:00–22:59
    sleep: 23:00–07:59

Market regime (UTC):
    aggressive:   {14, 15, 16, 17}
    normal:       {8..13}
    caution:      {18, 19}
    pause:        {20, 21, 22}
    low_activity: resto

Config (config.yaml):
    time_gates:
        aggressive_hours_utc: [14, 15, 16, 17]
        normal_hours_utc:     [8, 9, 10, 11, 12, 13]
        caution_hours_utc:    [18, 19]
        pause_hours_utc:      [20, 21, 22]
        operator_awake_start: 8    # hora local Caracas (UTC-4)
        operator_awake_end:   22   # inclusive

    Si falta la key → defaults hardcodeados.
    El módulo nunca lanza excepción — fail-open siempre.

Fail-open:
    evaluate_now() captura toda excepción incluyendo fallos en _build_sets()
    o cfg(). El caller detecta reason=="fail_open" y loggea.

Thread-safe: sí (función pura, sin estado mutable).
"""

from datetime import datetime, timezone, timedelta

from utils import cfg

# ---------------------------------------------------------------------------
# Timezone — Caracas es UTC-4 fijo (sin DST)
# ---------------------------------------------------------------------------

_TZ_CARACAS = timezone(timedelta(hours=-4))

# ---------------------------------------------------------------------------
# Defaults hardcodeados
# ---------------------------------------------------------------------------

_DEFAULT_AGGRESSIVE: frozenset = frozenset({14, 15, 16, 17})
_DEFAULT_NORMAL:     frozenset = frozenset({8, 9, 10, 11, 12, 13})
_DEFAULT_CAUTION:    frozenset = frozenset({18, 19})
_DEFAULT_PAUSE:      frozenset = frozenset({20, 21, 22})

_DEFAULT_AWAKE_START: int = 8   # 08:00 Caracas
_DEFAULT_AWAKE_END:   int = 22  # 22:59 Caracas (inclusive)

_FAIL_OPEN_RESULT: dict = {
    "utc_hour":        None,
    "local_hour":      None,
    "market_regime":   "unknown",
    "operator_window": "unknown",
    "blocked":         False,
    "reason":          "fail_open",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_hour_set(key: str, default: frozenset) -> frozenset:
    """Lee lista de horas desde config. Valida rango 0-23. Fallback a default."""
    raw = cfg(f"time_gates.{key}", None)
    if raw is None:
        return default
    try:
        parsed = frozenset(int(h) for h in raw)
        if not parsed:
            return default
        if not all(0 <= h <= 23 for h in parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _load_int(key: str, default: int) -> int:
    """Lee un entero desde config. Valida rango 0-23. Fallback a default."""
    raw = cfg(f"time_gates.{key}", None)
    if raw is None:
        return default
    try:
        val = int(raw)
        if 0 <= val <= 23:
            return val
        return default
    except (TypeError, ValueError):
        return default


def _build_sets() -> tuple:
    """Construye sets de horas y parámetros de operador desde config."""
    aggressive   = _load_hour_set("aggressive_hours_utc", _DEFAULT_AGGRESSIVE)
    normal       = _load_hour_set("normal_hours_utc",     _DEFAULT_NORMAL)
    caution      = _load_hour_set("caution_hours_utc",    _DEFAULT_CAUTION)
    pause        = _load_hour_set("pause_hours_utc",      _DEFAULT_PAUSE)
    awake_start  = _load_int("operator_awake_start",      _DEFAULT_AWAKE_START)
    awake_end    = _load_int("operator_awake_end",        _DEFAULT_AWAKE_END)
    return aggressive, normal, caution, pause, awake_start, awake_end


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def evaluate_hour(utc_hour: int, local_hour: int) -> dict:
    """
    Lógica pura de clasificación. SoT del gate.
    Cualquier excepción en _build_sets() burbujea hacia evaluate_now().

    Args:
        utc_hour:   hora entera 0-23 en UTC.
        local_hour: hora entera 0-23 en Caracas (UTC-4).

    Returns:
        dict con contrato completo.
    """
    aggressive, normal, caution, pause, awake_start, awake_end = _build_sets()

    # market_regime — basado en UTC
    if utc_hour in aggressive:
        market_regime = "aggressive"
    elif utc_hour in normal:
        market_regime = "normal"
    elif utc_hour in caution:
        market_regime = "caution"
    elif utc_hour in pause:
        market_regime = "pause"
    else:
        market_regime = "low_activity"

    # operator_window — basado en hora local Caracas
    if awake_start <= local_hour <= awake_end:
        operator_window = "awake"
    else:
        operator_window = "sleep"

    # decisión de bloqueo — operator_window manda en F1
    blocked = operator_window == "sleep"
    reason  = "operator_sleep" if blocked else "allowed_window"

    return {
        "utc_hour":        utc_hour,
        "local_hour":      local_hour,
        "market_regime":   market_regime,
        "operator_window": operator_window,
        "blocked":         blocked,
        "reason":          reason,
    }


def evaluate_now() -> dict:
    """
    Wrapper UTC+Caracas. Nunca lanza — fail-open si algo falla.
    El caller detecta reason=="fail_open" y loggea.
    """
    try:
        now_utc   = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(_TZ_CARACAS)
        return evaluate_hour(now_utc.hour, now_local.hour)
    except Exception:
        return _FAIL_OPEN_RESULT
