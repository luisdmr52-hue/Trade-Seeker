"""
time_gate.py — Capa 4: Time of Day Gate
Kryptos Trade Seeker · F1

Filtra ciclos de detección según la hora UTC. Gate operativo barato
que evita abrir candidatos en ventanas de baja liquidez.

Interfaz pública:
    evaluate_hour(utc_hour: int) -> dict   # lógica pura (SoT)
    evaluate_now()              -> dict    # wrapper UTC

Output:
    {
        "utc_hour": int | None,
        "regime":   "aggressive" | "normal" | "caution" | "pause" | "low_activity" | "unknown",
        "action":   "allow" | "block",
        "blocked":  bool,
        "reason":   "allowed_window" | "pause_window" | "low_activity_window" | "fail_open",
    }

Política:
    aggressive   -> allow
    normal       -> allow
    caution      -> allow  (log de ciclo en fast_loop — no por símbolo)
    pause        -> block
    low_activity -> block

Config (config.yaml):
    time_gates:
        aggressive_hours_utc: [14, 15, 16, 17]
        normal_hours_utc:     [8, 9, 10, 11, 12, 13]
        caution_hours_utc:    [18, 19]
        pause_hours_utc:      [20, 21, 22]

    Si la key no existe o viene inválida → defaults hardcodeados.
    El módulo nunca lanza excepción hacia el caller — fail-open siempre.

Fail-open:
    evaluate_now() captura cualquier excepción, incluyendo fallos en
    _build_sets() / cfg(). El caller detecta reason=="fail_open" y loggea.

Thread-safe: sí (función pura, sin estado mutable).
"""

from datetime import datetime, timezone

from utils import cfg

# ---------------------------------------------------------------------------
# Defaults hardcodeados — fallback si config.yaml no tiene time_gates
# ---------------------------------------------------------------------------

_DEFAULT_AGGRESSIVE: frozenset = frozenset({14, 15, 16, 17})
_DEFAULT_NORMAL:     frozenset = frozenset({8, 9, 10, 11, 12, 13})
_DEFAULT_CAUTION:    frozenset = frozenset({18, 19})
_DEFAULT_PAUSE:      frozenset = frozenset({20, 21, 22})

# Resultado de fallback cuando evaluate_now() no puede operar
_FAIL_OPEN_RESULT: dict = {
    "utc_hour": None,
    "regime":   "unknown",
    "action":   "allow",
    "blocked":  False,
    "reason":   "fail_open",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_hour_set(key: str, default: frozenset) -> frozenset:
    """
    Lee una lista de horas desde config.yaml.
    Valida que todos los valores sean enteros 0-23.
    Retorna el default si la key no existe, está vacía, o contiene inválidos.
    """
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


def _build_sets() -> tuple:
    """Construye los sets de horas desde config con fallback a defaults."""
    aggressive = _load_hour_set("aggressive_hours_utc", _DEFAULT_AGGRESSIVE)
    normal     = _load_hour_set("normal_hours_utc",     _DEFAULT_NORMAL)
    caution    = _load_hour_set("caution_hours_utc",    _DEFAULT_CAUTION)
    pause      = _load_hour_set("pause_hours_utc",      _DEFAULT_PAUSE)
    return aggressive, normal, caution, pause


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def evaluate_hour(utc_hour: int) -> dict:
    """
    Lógica pura de clasificación. SoT del gate.
    Protegida internamente — si _build_sets() falla, propaga la excepción
    hacia evaluate_now() donde está el handler de fail-open.

    Args:
        utc_hour: hora entera 0-23 en UTC.

    Returns:
        dict con utc_hour, regime, action, blocked, reason.
    """
    aggressive, normal, caution, pause = _build_sets()

    if utc_hour in aggressive:
        regime = "aggressive"
    elif utc_hour in normal:
        regime = "normal"
    elif utc_hour in caution:
        regime = "caution"
    elif utc_hour in pause:
        regime = "pause"
    else:
        regime = "low_activity"

    blocked = regime in ("pause", "low_activity")
    action  = "block" if blocked else "allow"

    if regime == "pause":
        reason = "pause_window"
    elif regime == "low_activity":
        reason = "low_activity_window"
    else:
        reason = "allowed_window"

    return {
        "utc_hour": utc_hour,
        "regime":   regime,
        "action":   action,
        "blocked":  blocked,
        "reason":   reason,
    }


def evaluate_now() -> dict:
    """
    Wrapper UTC. Obtiene la hora actual en UTC y delega a evaluate_hour().

    Captura cualquier excepción — incluyendo fallos en _build_sets() o cfg()
    que burbujean desde evaluate_hour(). Retorna _FAIL_OPEN_RESULT en ese caso.
    El caller (fast_loop.py) es responsable de detectar reason=="fail_open"
    y loggear — este módulo no tiene side effects de logging.

    Returns:
        dict con el contrato estándar. Nunca lanza.
    """
    try:
        utc_hour = datetime.now(timezone.utc).hour
        return evaluate_hour(utc_hour)
    except Exception:
        return _FAIL_OPEN_RESULT
