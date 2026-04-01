"""
entry_gate.py — Capa 5 F1 — Entry gate
KTS TradeSeeker · 2026-03-31

Decisiones cerradas:
    DC-C5-01: Rule 3 (persistencia) bloqueada — feed sin resolución suficiente.
              aggTrade N mediana=6, TSFeed resolución ~60s → ambos insuficientes
              para medir persistencia intra-ventana de 30s.
    DC-C5-02: Rule 3 se reabre solo cuando:
              - >= 5 cambios de precio reales por ventana de 30s, o
              - aggTrade N >= 15 en mediana por ventana

F1 activo: solo Rule 1 (timing gate).

Rule 1 — Timing gate:
    stage_pct = (price_at_confirm - price_at_trigger) / price_at_trigger * 100
    Si stage_pct > stage_pct_max → reject "late_entry"

    Nota: stage_pct puede ser negativo si el precio retrocedió levemente entre
    trigger y confirmación. Eso es pass válido — no es entrada tardía.
    Aparecerá en logs como stage_pct negativo + pass_reason="timing_ok".

Rule 2 — buy_ratio:
    Redundante en F1. AggTradeConfirmer.is_confirmed() ya garantiza
    buy_ratio >= vol_ratio antes de llegar aquí. Desactivada.

Rule 3 — Persistencia:
    Bloqueada (DC-C5-01).

Contrato de logging (responsabilidad del caller):
    Todo path con accepted=False debe loggear rejection_reason + stage_pct.
    Los near-misses son la materia prima del outcome tracker.

CodeGuard:
    - sin IO, sin red, sin DB — función pura y determinística
    - sin except:pass
    - stage_pct siempre presente (Optional[float], None solo en error de input)
    - EntryGateResult inmutable (frozen=True)
    - stage_pct_max validado — hot-reload de config no puede silenciar todas las señales
"""

from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Threshold — v1.1 bootstrap, no calibrado con outcomes
# Revisar cuando outcome tracker tenga >= 150 observaciones (DC-C5-02)
# ---------------------------------------------------------------------------
DEFAULT_STAGE_PCT_MAX = 4.0


@dataclass(frozen=True)
class EntryGateResult:
    """
    Resultado inmutable del entry gate.

    accepted:         True si la señal pasa todas las rules activas.
    rejection_reason: razón de rechazo, o None si accepted=True.
                      Valores posibles: "late_entry" | "invalid_confirm_price"
    pass_reason:      razón de paso, o None si accepted=False.
                      Valores posibles: "timing_ok"
    stage_pct:        avance del move al momento de confirmar (%).
                      None solo si price_at_confirm era inválido.
                      Puede ser negativo (retroceso leve) — es pass válido.
    """
    accepted:         bool
    rejection_reason: Optional[str]
    pass_reason:      Optional[str]
    stage_pct:        Optional[float]


def evaluate(
    price_at_trigger: float,
    price_at_confirm: float,
    stage_pct_max:    float = DEFAULT_STAGE_PCT_MAX,
) -> EntryGateResult:
    """
    Evalúa si una señal confirmada pasa el entry gate.

    Args:
        price_at_trigger: precio cuando se detectó delta >= pump_pct.
                          Debe ser > 0 — si no, lanza ValueError.
        price_at_confirm: precio actual al momento de la confirmación.
                          Si es <= 0, retorna reject conservador sin lanzar.
        stage_pct_max:    umbral máximo de avance permitido en %.
                          Debe ser > 0 — si no, lanza ValueError.
                          Validar antes de pasar desde config hot-reload.

    Returns:
        EntryGateResult con decisión, razón y stage_pct para logging.

    Raises:
        ValueError: si price_at_trigger <= 0 o stage_pct_max <= 0.
                    El caller (fast_loop.py) debe wrappear en try/except.
    """
    # -- Validar inputs críticos ------------------------------------------
    if price_at_trigger <= 0:
        raise ValueError(
            f"price_at_trigger inválido: {price_at_trigger!r} — debe ser > 0"
        )

    if stage_pct_max <= 0:
        raise ValueError(
            f"stage_pct_max inválido: {stage_pct_max!r} — debe ser > 0"
        )

    # -- Precio de confirmación inválido → fallo conservador ---------------
    if price_at_confirm <= 0:
        return EntryGateResult(
            accepted=False,
            rejection_reason="invalid_confirm_price",
            pass_reason=None,
            stage_pct=None,
        )

    # -- Calcular stage_pct ------------------------------------------------
    stage_pct = (price_at_confirm - price_at_trigger) / price_at_trigger * 100

    # -- Rule 1: Timing gate -----------------------------------------------
    if stage_pct > stage_pct_max:
        return EntryGateResult(
            accepted=False,
            rejection_reason="late_entry",
            pass_reason=None,
            stage_pct=round(stage_pct, 3),
        )

    # -- Pass ---------------------------------------------------------------
    return EntryGateResult(
        accepted=True,
        rejection_reason=None,
        pass_reason="timing_ok",
        stage_pct=round(stage_pct, 3),
    )
