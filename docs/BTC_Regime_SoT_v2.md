# BTC Regime Foundation / Gate — SoT v2

meta: {ver:"v2.0", date:"2026-04-13", schema:"v2", mode:"shadow", validation_status:"experimental"}

---

## 1. Objetivo del módulo

La Capa 6 — BTC Regime Foundation / Gate — es una capa de contexto de mercado
basada en BTC para el sistema KTS TradeSeeker.

Su propósito en F2:

    - observar
    - medir
    - estructurar
    - persistir
    - exponer contexto utilizable para análisis posterior

No tiene autoridad operativa en F2. No existe aún como gate ni modificador.

A futuro, si demuestra valor empírico, podrá asumir dos funciones:

    - Hard Gate de stress: evitar operar en condiciones extremas de mercado
    - Modulador continuo: modificar confianza / weighting / sizing según contexto

Estas funciones NO están activas en F2 y solo podrían activarse en F3
tras validación empírica suficiente.

---

## 2. Rol dentro de TradeSeeker

BTC Regime es un side-process productor de contexto, no una dependencia
funcional del pipeline base.

    señal → contexto → decisión → evaluación

La capa pertenece exclusivamente al bloque de contexto. No puede contener
lógica oculta de decisión.

Relación con el pipeline:

    - feed de detección PUMP_FAST → no se toca
    - fast_loop → ignora regime en F2
    - entry_gate → no consume regime en F2
    - aggtrade_confirmer → no consume regime en F2
    - dataset_capture → puede enriquecer eventos con contexto BTC (read-only, PIT)
    - outcome_tracker → independiente del regime

Eliminar BTC Regime debe dejar idéntica la conducta operativa del bot.

---

## 3. Filosofía de diseño

### 3.1. Principios no negociables

1. Zero-impact en F2: nada de esta capa puede bloquear señales, modificar
   thresholds, ajustar confirm_ratio, ajustar size, alterar score final,
   o cambiar la decisión del sistema.

2. Feature-first: la capa parte de features observables → scores continuos
   por dimensión → interpretación secundaria opcional.

3. No discreto como núcleo: no se aprueba un regime discreto como output
   principal. El núcleo es el vector continuo por dimensión.

4. Separación estricta: facts ≠ features ≠ scores ≠ tags ≠ health ≠ governance.

5. PIT correctness: toda feature refleja el estado observable hasta
   window_end_ts inclusive. Sin lookahead.

6. Fail-open: si el worker falla, el pipeline base continúa sin cambio
   operativo. El fallo no puede back-propagate.

7. Compatible con el stack actual: Python, TimescaleDB, Benthos, Grafana,
   datos tipo trades / bookTicker / métricas derivadas. Sin ML complejo.
   Sin dependencias pesadas. Latencia compatible con loop ~2s.

### 3.2. Anti-patrones prohibidos

    - composite market score único
    - regime discreto como output principal
    - clasificaciones discretas tipo risk_on / risk_off como núcleo
    - threshold operativo en F2
    - decisión leakage: if btc_*_score → branching en fast_loop / entry_gate
    - JSON como núcleo del modelo de datos
    - lógica distinta offline y online
    - sign bruto de flow en stress score
    - tags multi-dimensionales
    - forward-fill silencioso de features

---

## 4. Modelo de datos conceptual

El snapshot de BTC Regime se segrega en 6 bloques semánticos obligatorios
más un bloque opcional de debug:

```
snapshot = {
  temporal:     {...},   # identidad temporal del snapshot
  facts:        {...},   # observaciones base del mercado
  features:     {...},   # transformaciones derivadas pre-score
  scores:       {...},   # outputs continuos principales — SoT
  shadow_tags:  {...},   # interpretaciones discretas secundarias
  health:       {...},   # calidad operativa del snapshot
  governance:   {...},   # contrato de uso
  debug:        {...}    # opcional
}
```

### Reglas de clasificación

    R1. Observación base → facts
    R2. Transformación analítica previa a score → features
    R3. Salida continua principal → scores
    R4. Interpretación discreta secundaria → shadow_tags
    R5. Frescura / error / degradación → health
    R6. Modo / uso / validez contractual → governance
    R7. Si no cabe claramente → no entra al snapshot principal

### Jerarquía semántica

    scores continuos = source of truth
    shadow tags = derivaciones secundarias de scores
    health → controla emisión de tags
    governance → define qué puede hacerse con el snapshot

---

## 5. Contrato temporal (CRÍTICO)

### 5.1. Principio base

    timestamp de cálculo ≠ timestamp del dato fuente ≠ fin de ventana

No se permite usar un solo campo "ts" como sustituto de todo.

### 5.2. Campos temporales obligatorios

```
snapshot_ts          → cuándo el snapshot quedó listo
source_ts            → hasta cuándo llegaban los inputs realmente usados
feature_ts           → timestamp lógico de validez de las features
window_end_ts        → final de la ventana rolling usada
window_start_ts      → inicio de la ventana más larga usada
source_lag_ms        → snapshot_ts - source_ts
compute_latency_ms   → costo de cómputo del snapshot
trade_source_ts      → último dato de trades incorporado
book_source_ts       → último dato de bookTicker incorporado
trade_source_lag_ms  → snapshot_ts - trade_source_ts
book_source_lag_ms   → snapshot_ts - book_source_ts
```

### 5.3. Reglas semánticas

    DC-TIME-01: snapshot_ts = tiempo de disponibilidad, no tiempo del mercado
    DC-TIME-02: feature_ts = window_end_ts (regla v2)
    DC-TIME-03: joins con eventos solo con snapshots donde feature_ts <= t_confirm
    DC-TIME-04: source_lag_ms mide frescura real; compute_latency_ms no sustituye eso
    DC-TIME-05: ningún snapshot puede ocultar inconsistencia temporal entre fuentes
    DC-TIME-06: si no puede garantizarse PIT → snapshot marcado degraded/stale, nunca clean

### 5.4. Integridad temporal

    window_start_ts <= window_end_ts <= source_ts <= snapshot_ts
    source_lag_ms >= 0
    compute_latency_ms >= 0
    trade_source_ts <= snapshot_ts
    book_source_ts <= snapshot_ts
    feature_ts = window_end_ts
    ningún join puede usar feature_ts > t_confirm

### 5.5. Campos mínimos al enriquecer eventos

Al adjuntar BTC Regime a candidate_events o dataset:

    btc_regime_feature_ts
    btc_regime_snapshot_ts
    btc_regime_join_lag_ms
    btc_regime_source_lag_ms
    btc_regime_snapshot_health_status
    btc_regime_freshness_status

---

## 6. Pipeline lógico del snapshot

### 6.1. Ingestion

El btc_regime_worker consume datos BTC desde la fuente de market data
oficial del sistema (Timescale / infraestructura existente). No se aprueba
una segunda fuente de verdad ad hoc.

Inputs aprobados:

    A. Trades BTC: aggTrade / trades equivalentes — count, quote vol,
       signed flow, intertrade gaps

    B. Book BTC: best bid/ask, spread, top-of-book imbalance, update rate

    C. Derivados rolling internos: returns, RV, realized range, z-scores,
       percentiles, stability / recovery

No aprobados en v2: altcoin features, outcome tracker outputs, labels,
funding / OI / liquidation feeds, news/social, features de otras capas.

### 6.2. Feature computation

El worker mantiene buffers rolling internos con ventanas: 30s, 1m, 5m, 15m, 60m.

Cada feature sigue exactamente esta secuencia:

    1. raw feature
    2. normalization (rolling z-score o rolling percentile)
    3. stabilization / transformation (clip + tanh, o direct [0,1])
    4. direction handling (positive / negative / magnitude_only)
    5. component value
    6. aggregation by group (anchor / support)
    7. final score

### 6.3. Scoring

Scores continuos calculados por dimensión. Ver Sección 8 para lógica completa.

### 6.4. Tagging

Shadow tags derivados de scores (nunca de features crudas). Ver Sección 10.

Tags solo se emiten si el score de la dimensión existe y la dimensión no
está en estado crítico de degradación.

### 6.5. Health evaluation

Health evaluado en 3 ejes separados: freshness, completeness, validity.
Ver Sección 11.

### 6.6. Snapshot finalization

    window_end_ts = latest_event_time_included
    feature_ts    = window_end_ts
    snapshot_ts   = instante de disponibilidad del snapshot

El worker persiste el snapshot en btc_regime_snapshots. Los buffers viven
solo dentro del worker y no comparten estado mutable con el detector.

Cadence objetivo: 1s–2s por ciclo.

---

## 7. Feature Layer (detallado)

### 7.1. Dimensiones aprobadas (5, cerradas)

El núcleo es de exactamente 5 dimensiones. No se aprueba una sexta ni se
permite reducir a 4.

```
1. stress      → tensión / shock / fricción
2. trend       → dirección persistente intradía
3. volatility  → amplitud de movimiento
4. liquidity   → calidad de ejecución observable
5. activity    → intensidad de participación
```

### 7.2. Separación obligatoria entre dimensiones

- Stress no absorbe volatility: volatility puede participar de forma
  secundaria, pero stress no puede depender principalmente de RV.
- Liquidity no absorbe activity: trade count y quote volume pertenecen
  a activity, no a liquidity.
- Trend no absorbe stress: trend mide dirección persistente, no shock
  instantáneo.

### 7.3. Features por dimensión

**STRESS** — tensión, shock, fricción. NO dirección.

Anchor:
```
btc_abs_r_1m            ← abs(btc_r_1m)
btc_abs_r_5m            ← abs(btc_r_5m)
btc_spread_bps_z        ← z-score rolling de btc_spread_bps
```

Support:
```
btc_tobi_extreme        ← percentile rolling de abs(btc_tobi)
btc_abs_signed_flow_30s ← abs(btc_signed_flow_30s)
                          [magnitud de imbalance — NO signo bruto]
```

**TREND** — dirección persistente intradía. Mantiene signo.

Anchor:
```
btc_r_15m               ← retorno 15m
btc_r_60m               ← retorno 60m
```

Support:
```
btc_dir_persistence_15m    ← fracción de retornos 1m del mismo signo en 15m
btc_efficiency_ratio_15m   ← abs(net_move) / sum(abs(moves)) en 15m
```

**VOLATILITY** — amplitud / expansión de movimiento.

Anchor:
```
btc_rv_1m               ← realized volatility ventana 1m
btc_rv_5m               ← realized volatility ventana 5m
```

Support:
```
btc_rv_ratio_1m_15m     ← btc_rv_1m / btc_rv_15m (expansión relativa)
                          [btc_rv_15m se computa internamente en el worker
                           pero no se persiste como columna independiente;
                           btc_rv_ratio_1m_15m es la feature persistida]
btc_realized_range_5m   ← high - low en ventana 5m
```

**LIQUIDITY** — calidad de ejecución observable.

Anchor:
```
btc_spread_bps          ← spread en basis points (dirección: negative)
btc_spread_stability_1m ← estabilidad de spread en 1m (dirección: positive)
```

Support:
```
btc_spread_recovery_30s ← velocidad de recovery del spread (dirección: positive)
btc_tobi_non_extreme    ← tobi en rango no extremo como proxy de absorción
                          [btc_tobi NO puede ser feature dominante de liquidity]
```

**ACTIVITY** — intensidad de participación.

Anchor:
```
btc_n_trades_30s        ← conteo de trades en 30s
btc_quote_vol_30s       ← volumen en quote currency en 30s
```

Support:
```
btc_trade_intensity_1m     ← trades / segundo en 1m (tasa de llegada)
btc_intertrade_gap_ms_p50  ← mediana del gap entre trades (dirección: negative)
btc_book_update_rate_30s   ← actualizaciones del book en 30s
```

---

## 8. Feature → Score Mapping v2

### 8.1. Principios del mapping

1. Scores continuos son SoT.
2. No se permite branching lógico tipo if/else para calcular el score.
3. No se permite optimización de pesos en v2.
4. La misma fórmula debe existir online y offline (paridad obligatoria).
5. Toda transformación está versionada: score_formula_version = "v2_anchor_support_fixed_mapping".
6. Ninguna feature puede tener dirección implícita — toda dirección está
   declarada en este documento.

### 8.2. Métodos de normalización aprobados

    A. rolling z-score:  z = (x - mean_rolling) / std_rolling
    B. rolling percentile rank: pct = percentile_rank(x, rolling_window)

Ventana base: 1h rolling. Ventana extendida opcional: 3h rolling.
Cada feature tiene método fijo — no se decide en runtime.

Si std_rolling < 1e-9, o ventana mínima incompleta, o feature inválida:
normalized_feature = null (nunca valor artificial).

### 8.3. Transformaciones post-normalización

    z-score    → clip(-3, +3) → tanh    → resultado ∈ [-1, +1]
    percentile → usar directo           → resultado ∈ [0, 1]

No se permite otra transformación en v2.

### 8.4. Agrupación anchor / support

Cada dimensión se divide en anchor (features estructurales) y support
(features complementarias). Los pesos no están optimizados.

```
score_dim = mean([
    mean(anchor_components),
    mean(support_components_available)
])
```

Si support group está vacío: score_dim = mean(anchor_components).
Si faltan features anchor mínimas: score_dim = null.

### 8.5. Tabla cerrada por dimensión

**STRESS SCORE** ∈ [0, 1] — tensión / shock

| Feature | Norm | Transform | Direction |
|---|---|---|---|
| btc_abs_r_1m | z-score | clip+tanh | magnitude_only |
| btc_abs_r_5m | z-score | clip+tanh | magnitude_only |
| btc_spread_bps_z | ya es z | clip+tanh | positive |
| btc_tobi_extreme | percentile | direct [0,1] | magnitude_only |
| btc_abs_signed_flow_30s | z-score o percentile | clip+tanh o direct | magnitude_only |

Anchor: btc_abs_r_1m, btc_abs_r_5m, btc_spread_bps_z
Support: btc_tobi_extreme, btc_abs_signed_flow_30s

Post-procesamiento: map_to_[0,1]

Regla crítica: stress no preserva dirección. Solo intensidad de tensión.
btc_abs_signed_flow_30s usa magnitud, NUNCA signo bruto.

**TREND SCORE** ∈ [-1, +1] — dirección persistente

| Feature | Norm | Transform | Direction |
|---|---|---|---|
| btc_r_15m | z-score | clip+tanh | positive |
| btc_r_60m | z-score | clip+tanh | positive |
| btc_dir_persistence_15m | percentile | direct [0,1] | positive |
| btc_efficiency_ratio_15m | percentile | direct [0,1] | positive |

Anchor: btc_r_15m, btc_r_60m
Support: btc_dir_persistence_15m, btc_efficiency_ratio_15m

Regla crítica: trend mantiene signo. No usar abs(). No mapear a [0,1].
Support no puede invertir el signo del anchor group por sí sola.

**VOLATILITY SCORE** ∈ [0, 1] — amplitud de movimiento

| Feature | Norm | Transform | Direction |
|---|---|---|---|
| btc_rv_1m | percentile | direct [0,1] | positive |
| btc_rv_5m | percentile | direct [0,1] | positive |
| btc_rv_ratio_1m_15m | percentile | direct [0,1] | positive |
| btc_realized_range_5m | percentile | direct [0,1] | positive |

Anchor: btc_rv_1m, btc_rv_5m
Support: btc_rv_ratio_1m_15m, btc_realized_range_5m

**LIQUIDITY SCORE** ∈ [0, 1] — calidad de ejecución

| Feature | Norm | Transform | Direction |
|---|---|---|---|
| btc_spread_bps | z-score | clip+tanh | negative |
| btc_spread_stability_1m | percentile | direct [0,1] | positive |
| btc_spread_recovery_30s | percentile | direct [0,1] | positive |
| btc_tobi_non_extreme | percentile | direct [0,1] | support_only |

Anchor: btc_spread_bps, btc_spread_stability_1m
Support: btc_spread_recovery_30s, btc_tobi_non_extreme

Implementación para dirección negative:
    spread_component = 1 - rescaled_positive_badness (o equivalente monotónico)

Regla crítica: btc_tobi NO puede ser feature dominante de liquidity.

**ACTIVITY SCORE** ∈ [0, 1] — intensidad de participación

| Feature | Norm | Transform | Direction |
|---|---|---|---|
| btc_n_trades_30s | percentile | direct [0,1] | positive |
| btc_quote_vol_30s | percentile | direct [0,1] | positive |
| btc_trade_intensity_1m | percentile | direct [0,1] | positive |
| btc_intertrade_gap_ms_p50 | percentile | direct [0,1] | negative |
| btc_book_update_rate_30s | percentile | direct [0,1] | positive |

Anchor: btc_n_trades_30s, btc_quote_vol_30s
Support: btc_trade_intensity_1m, btc_intertrade_gap_ms_p50, btc_book_update_rate_30s

Implementación para dirección negative:
    gap_component = 1 - pct_gap

### 8.6. Decisiones cerradas del mapping

    DC-SCORE-01: paridad offline/online obligatoria
    DC-SCORE-02: cada feature queda cerrada con método fijo en v2
    DC-SCORE-03: stress usa magnitud de flow, no signo bruto
    DC-SCORE-04: liquidity ancla en spread/stability/recovery; tobi es support
    DC-SCORE-05: agrupación anchor/support sin pesos optimizados
    DC-SCORE-06: sin anchor group mínimo → score de dimensión = null
    DC-SCORE-07: std casi cero o ventana insuficiente → normalized feature = null
    DC-SCORE-08: toda transformación gobernada por versionado explícito

---

## 9. Scores Layer

### 9.1. Outputs principales obligatorios

```
btc_stress_score      ∈ [0, 1]    — 0=relajado, 1=bajo tensión
btc_trend_score       ∈ [-1, +1]  — -1=sesgo bajista, 0=neutro, +1=sesgo alcista
btc_volatility_score  ∈ [0, 1]    — 0=comprimido, 1=expandido
btc_liquidity_score   ∈ [0, 1]    — 0=mala liquidez, 1=buena liquidez
btc_activity_score    ∈ [0, 1]    — 0=mercado muerto, 1=muy activo
```

### 9.2. Reglas de los scores

- Son continuos, interpretables, no normativos.
- Son el output principal de la capa.
- No se permite composite score único del mercado.
- No se permite regime discreto como output principal.
- Ningún campo normativo tipo allow/block/safe/unsafe puede existir.

### 9.3. Componentes de auditoría (opcionales)

Estos campos son opcionales pero recomendados para tuning / debug:

```
btc_stress_anchor_component
btc_stress_support_component
btc_trend_anchor_component
btc_trend_support_component
btc_volatility_anchor_component
btc_volatility_support_component
btc_liquidity_anchor_component
btc_liquidity_support_component
btc_activity_anchor_component
btc_activity_support_component
```

No son SoT. No reemplazan scores. Solo facilitan auditoría y tuning.

---

## 10. Shadow Tags v2

### 10.1. Principios no negociables

1. Scores continuos = source of truth
2. Tags = derivaciones secundarias
3. No afectan comportamiento del sistema en F2
4. No pueden usarse para filtrar dataset clean
5. No pueden introducir lógica multi-dimensional
6. Deben ser deterministas y versionadas
7. No deben degradar interpretabilidad del score

Regla clave: eliminar todos los tags no debe cambiar ningún resultado
del sistema.

### 10.2. Estructura de columnas (no JSON como núcleo)

```
btc_stress_state         TEXT   — derivado de btc_stress_score
btc_trend_state          TEXT   — derivado de btc_trend_score
btc_volatility_state     TEXT   — derivado de btc_volatility_score
btc_liquidity_state      TEXT   — derivado de btc_liquidity_score
btc_activity_state       TEXT   — derivado de btc_activity_score
shadow_tag_policy_version TEXT NOT NULL  — versionado obligatorio
btc_shadow_tags_extra    JSONB  — opcional, solo debug/extensión
```

### 10.3. Valores aprobados por tag

```
btc_stress_state      ∈ {low, medium, high}
btc_trend_state       ∈ {down, neutral, up}
btc_volatility_state  ∈ {compressed, normal, expanding}
btc_liquidity_state   ∈ {thin, normal, thick}
btc_activity_state    ∈ {low, normal, high}
```

Thresholds bootstrap (v1_bootstrap_fixed_thresholds):

```
stress:     low < 0.3 | 0.3 ≤ medium < 0.7 | high ≥ 0.7
trend:      down < -0.2 | -0.2 ≤ neutral ≤ 0.2 | up > 0.2
volatility: thresholds pendientes de calibración — usar bootstrap simétrico ∈ [0,1]
liquidity:  thresholds pendientes de calibración — usar bootstrap simétrico ∈ [0,1]
activity:   thresholds pendientes de calibración — usar bootstrap simétrico ∈ [0,1]
```

Política para dimensiones con thresholds aún no calibrados: los tags pueden
emitirse usando bootstrap simétrico provisional (low < 0.33 | 0.33 ≤ normal < 0.67
| high ≥ 0.67), siempre que shadow_tag_policy_version lo declare explícitamente.
Si no hay política cerrada → el tag de esa dimensión no se emite (null).

Todo cambio en thresholds o lógica → nueva shadow_tag_policy_version.

Valor inicial: shadow_tag_policy_version = 'v1_bootstrap_fixed_thresholds'

### 10.4. Reglas de generación

1. Tag = f(score) — solo del score de su propia dimensión
2. No usar features crudas
3. No usar múltiples dimensiones
4. No usar lógica condicional compuesta
5. No usar historial

Ejemplo prohibido: if stress_high AND liquidity_low → "bad_market"

### 10.5. Reglas de emisión

Un tag SOLO puede emitirse si:

    - score de la dimensión existe (no null)
    - validity_status != invalid
    - dimensión no está en estado crítico de degradación

Reglas adicionales:

    - si snapshot_health_status = stale → todos los tags = null
    - si dimensión no tiene features críticas → tag = null
    - si dimensión está degraded → tag puede existir pero es interpretativo débil

### 10.6. Tags prohibidos

```
good_regime / bad_market / tradable / non_tradable
risk_on / risk_off / allow_trade / deny_trade
cualquier proxy de decisión
```

Regla: si implica acción → está prohibido.

### 10.7. Relación con dataset

Permitido: análisis por bucket, segmentación de performance, visualización.

Prohibido en F2: filtrar dataset_clean_base por tags, modificar labels,
afectar selección de eventos.

### 10.8. Decisiones cerradas

    DC-TAGS-01: Tags no son SoT
    DC-TAGS-02: JSONB no es formato principal
    DC-TAGS-03: Versionado obligatorio con shadow_tag_policy_version
    DC-TAGS-04: Tags dependen de scores, no de features
    DC-TAGS-05: Tags no pueden existir sin score válido
    DC-TAGS-06: Tags no pueden influir en comportamiento en F2
    DC-TAGS-07: No se permiten tags multi-dimensionales
    DC-TAGS-08: Snapshot solo contiene display tags

---

## 11. Health / Degradation v2

### 11.1. Principios base

1. No ocultar degradación
2. No imputar silenciosamente
3. No confundir pipeline vivo con dato sano
4. No usar compute_latency como sustituto de freshness
5. No mezclar health del mercado con health del snapshot
6. Mantener fail-open en F2
7. Exponer causas explícitas de degradación

### 11.2. Tres ejes de health (separados, no colapsados)

**A. Freshness** — edad temporal real de los datos fuente

```
freshness_status ∈ {fresh, delayed, stale}

Thresholds bootstrap v2:
    trade_soft_ttl_ms    = 3000
    trade_hard_ttl_ms    = 15000
    book_soft_ttl_ms     = 3000
    book_hard_ttl_ms     = 15000
    snapshot_soft_ttl_ms = 3000
    snapshot_hard_ttl_ms = 15000

fresh   → todas las fuentes críticas <= soft TTL
delayed → alguna fuente > soft TTL pero ninguna supera hard TTL
stale   → alguna fuente supera hard TTL o falta por completo
```

**B. Completeness** — suficiencia de features por dimensión

```
completeness_status ∈ {complete, partial, insufficient}

complete     → todas las features críticas presentes
partial      → faltan no críticas o algunas no centrales
insufficient → faltan features críticas de una o más dimensiones
```

Nota: se elimina la regla plana de "50% global". La evaluación es por dimensión.

**C. Validity** — plausibilidad y consistencia de valores

```
validity_status ∈ {valid, suspicious, invalid}

Checks mínimos:
    btc_mid_price > 0
    btc_bid_price > 0 si existe
    btc_ask_price > 0 si existe
    btc_bid_price <= btc_ask_price si ambos existen
    btc_spread_bps >= 0 si existe
    btc_n_trades_30s >= 0 si existe
    btc_quote_vol_30s >= 0 si existe
    btc_intertrade_gap_ms_p50 >= 0 si existe
    source_lag_ms >= 0
    compute_latency_ms >= 0

valid      → todos los checks pasan
suspicious → valores raros pero no imposibles
invalid    → violación clara de integridad (ej: ask < bid)
```

### 11.3. Feed status por fuente

```
trade_feed_status ∈ {live, delayed, frozen, missing}
book_feed_status  ∈ {live, delayed, frozen, missing}

live    → feed actualiza dentro del soft TTL
delayed → lag > soft TTL y <= hard TTL
frozen  → técnicamente presente pero sin nuevos updates por encima del hard TTL
missing → fuente ausente / no disponible / no usable
```

Regla: frozen ≠ missing. Un feed congelado es más peligroso que uno
claramente ausente porque parece válido pero no lo es.

### 11.4. Features críticas por dimensión

```
STRESS:
    críticas: btc_abs_r_1m, btc_abs_r_5m
              + al menos una de: btc_spread_bps_z, btc_tobi_extreme, btc_abs_signed_flow_30s

TREND:
    críticas: btc_r_15m, btc_r_60m
    recomendadas: btc_dir_persistence_15m, btc_efficiency_ratio_15m

VOLATILITY:
    críticas: btc_rv_1m, btc_rv_5m
    recomendadas: btc_rv_ratio_1m_15m, btc_realized_range_5m

LIQUIDITY:
    críticas: btc_spread_bps
    recomendadas: btc_spread_stability_1m, btc_tobi_non_extreme, btc_spread_recovery_30s
                  [btc_tobi es la fact base; btc_tobi_non_extreme es la feature derivada usada en score]

ACTIVITY:
    críticas: al menos una de: btc_n_trades_30s, btc_quote_vol_30s
    recomendadas: btc_trade_intensity_1m, btc_intertrade_gap_ms_p50,
                  btc_book_update_rate_30s
```

### 11.5. Regla de score bajo faltantes

```
score calculable → features críticas de la dimensión presentes
score degraded   → faltan recomendadas pero no críticas
score null       → faltan features críticas o invalidez severa
```

### 11.6. Health status global

```
snapshot_health_status ∈ {fresh, degraded, stale}

fresh    → freshness=fresh AND completeness=complete AND validity=valid
degraded → no es stale Y al menos un eje no está en estado ideal
stale    → freshness=stale, O validity=invalid,
           O completeness=insufficient en múltiples dimensiones,
           O faltan fuentes críticas
```

El estado global resume — no reemplaza los 3 ejes.

### 11.7. Observable vs joinable

```
snapshot_observable → puede mostrarse en logs, grafana, debug, research visual
snapshot_joinable   → apto para enriquecer candidate_events sin comprometer PIT

observable = TRUE salvo invalididad severa total

joinable = TRUE solo si:
    snapshot_health_status != stale
    feature_ts <= t_confirm
    no existe invalid_value_detected
    no faltan features críticas de dimensiones persistidas como score
```

Un snapshot puede ser observable pero no joinable.

### 11.8. Degradation reasons

Todo snapshot degraded/stale/suspicious debe incluir degradation_reasons.

Valores aprobados:

```
trade_feed_delayed / trade_feed_frozen / trade_feed_missing
book_feed_delayed / book_feed_frozen / book_feed_missing
critical_feature_missing / optional_feature_missing
window_incomplete / invalid_value_detected / late_events_exceeded
partial_snapshot / join_not_recommended
```

Regla: no se permite marcar degraded/stale sin razón explícita.

### 11.9. Comportamiento ante fallas

```
Caso A: falta feature no crítica
    → completeness=partial, health=degraded, score puede seguir

Caso B: falta feature crítica de una dimensión
    → score de esa dimensión = null, health=degraded o stale

Caso C: falta trade feed completo
    → trade_feed_status=missing, stress/activity probablemente null

Caso D: falta book feed completo
    → book_feed_status=missing, liquidity probablemente degraded

Caso E: invalid value detectado
    → validity=invalid, health=stale
```

Regla general: no inventar valores, no forward-fill silencioso, no ocultar nulls.

### 11.10. Late / out-of-order data

```
late_event_count    → eventos que llegaron tarde respecto al window_end_ts
max_event_delay_ms  → máximo atraso observado entre event time y processing
```

Health debe medirse usando source/event time, no solo processing time.

Si late_event_count supera umbral o max_event_delay_ms es alto:
freshness_status no puede permanecer fresh.

### 11.11. Logging obligatorio

Deben loggearse: cambio de snapshot_health_status, cambio de feed_status,
aparición de invalid_value_detected, salto de fresh → stale, aumento
material de late_event_count. Frecuencia: un log por transición, no spam.

### 11.12. Relación con dataset clean

Prohibido: filtrar dataset_clean_base usando snapshot_health_status,
usar degraded/stale para redefinir labels, excluir eventos por mala
calidad de regime.

Permitido: adjuntar health al dataset_enriched, analizar outcomes
por health bucket.

### 11.13. Decisiones cerradas

    DC-HEALTH-01: Health separado en freshness, completeness, validity
    DC-HEALTH-02: trade_feed_status y book_feed_status obligatorios en v2
    DC-HEALTH-03: Se elimina la regla plana del 50% global
    DC-HEALTH-04: Score por dimensión depende de features críticas por dimensión
    DC-HEALTH-05: degraded/stale debe incluir degradation_reasons
    DC-HEALTH-06: snapshot_observable y snapshot_joinable separados explícitamente
    DC-HEALTH-07: source/event time manda sobre processing time para freshness
    DC-HEALTH-08: Health no puede usarse para filtrar dataset_clean_base en F2

---

## 12. Governance Layer

### 12.1. Contrato de modo

```
regime_mode ∈ {shadow, advisory, active_gate}

Valor por defecto en F2: shadow

Fail-safe: si mode inválido, ausente o inconsistente → degradar a shadow
```

Transiciones válidas:

    shadow → advisory → active_gate
    active_gate → advisory → shadow

Transición inválida sin revalidación: shadow → active_gate directo.

No se permite saltar la fase de maduración.

**shadow**: corre, calcula, registra, expone. No bloquea, no ajusta, no decide.

**advisory** (no activo): puede emitir flags interpretativos para revisión humana.
No puede imponer cambios automáticos. Solo activable con evidencia de utilidad
interpretativa, estabilidad razonable y no-duplicación de capas previas.

**active_gate** (no activo en F2): autoridad real sobre conducta del sistema.
Exige validación histórica, evidencia incremental, thresholds documentados,
rollback simple, monitoreo y auditoría del cambio.

### 12.2. Validation status

```
regime_validation_status ∈ {
    experimental,
    observed,
    validated_partial,
    validated_operational
}

Estado actual: experimental
```

### 12.3. Clasificación de consumidores

**CLASS_1 — Observacional** (TOTALMENTE PERMITIDO en F2)

    logs, grafana dashboards, debug views, CLI de inspección, usuario

**CLASS_2 — Analítico / Persistencia** (PERMITIDO con restricciones en F2)

    dataset_capture, TimescaleDB writers, queries research, notebooks offline

    Puede: adjuntar scores como feature, persistir snapshot, usar para
    segmentación offline y correlación con outcomes.

    NO puede: filtrar dataset base, decidir qué eventos se guardan o son
    válidos, modificar labeling, crear whitelist/blacklist dependiente del regime.

**CLASS_3 — Operativo** (PROHIBIDO en F2)

    fast_loop, entry_gate, aggtrade_confirmer, lógica de thresholds,
    lógica de cooldowns, lógica de size, alert routing, sistema de ejecución.

Prohibido explícito en F2:

    if btc_stress_score > x → reject
    if btc_liquidity_score < y → tighten rules
    if btc_trend_score > z → increase size
    if btc_volatility_score < w → ignore signals

### 12.4. Contrato de consumo por modo

En modo shadow, los consumidores deben tratar BTC Regime como:
observacional, no-causal, zero-impact.

```
Permitido: logging, dashboards, dataset enrichment, research offline,
           análisis por segmentos, validación histórica, auditoría

Prohibido: gating, filtering, threshold tuning, scoring adjustment,
           size adjustment, alert suppression, label manipulation
```

### 12.5. Auditoría de cambios de modo

Todo cambio de modo debe dejar rastro con: old_mode, new_mode, timestamp,
quién cambió, razón, evidencia o referencia documental.

Cambiar de mode no es un tweak técnico menor — es un cambio de gobierno.

### 12.6. Campos de governance en snapshot

```
regime_mode                  TEXT NOT NULL  — 'shadow' hoy
regime_validation_status     TEXT NOT NULL  — 'experimental' hoy
schema_version               TEXT NOT NULL  — 'v2'
feature_version              TEXT NOT NULL  — 'v1'
score_formula_version        TEXT NOT NULL  — 'v2_anchor_support_fixed_mapping'
normalization_policy_version TEXT NOT NULL  — 'v1_fixed_per_feature'
allowed_use_class            TEXT NOT NULL  — 'observational,analytical'
prohibited_use_class         TEXT NOT NULL  — 'operational'
```

---

## 13. Snapshot Schema v2 (CRÍTICO)

> **Nota de reemplazo:** este schema reemplaza completamente al Snapshot Schema v1
> (bloque 7 previo). El schema v1 queda obsoleto. Ningún campo legacy del v1 debe
> usarse: staleness_state, degraded_flag, error_flag, trade_input_stale,
> book_input_stale, stress_candidate y regime_candidate fueron eliminados del
> núcleo y reemplazados por modelos más explícitos en v2.

### 13.1. Tabla

```sql
btc_regime_snapshots
```

Una fila = un snapshot completo. PK técnica: snapshot_ts.

### 13.2. SQL completo v2

```sql
CREATE TABLE btc_regime_snapshots (

    -- BLOQUE TEMPORAL
    snapshot_ts                      TIMESTAMPTZ PRIMARY KEY,
    source_ts                        TIMESTAMPTZ NOT NULL,
    feature_ts                       TIMESTAMPTZ NOT NULL,
    window_start_ts                  TIMESTAMPTZ NOT NULL,
    window_end_ts                    TIMESTAMPTZ NOT NULL,
    trade_source_ts                  TIMESTAMPTZ NULL,
    book_source_ts                   TIMESTAMPTZ NULL,
    source_lag_ms                    DOUBLE PRECISION NOT NULL,
    trade_source_lag_ms              DOUBLE PRECISION NULL,
    book_source_lag_ms               DOUBLE PRECISION NULL,
    compute_latency_ms               DOUBLE PRECISION NOT NULL,

    -- BLOQUE FACTS — market base
    btc_mid_price                    DOUBLE PRECISION NOT NULL,
    btc_bid_price                    DOUBLE PRECISION NULL,
    btc_ask_price                    DOUBLE PRECISION NULL,
    btc_spread_bps                   DOUBLE PRECISION NULL,
    btc_tobi                         DOUBLE PRECISION NULL,

    -- BLOQUE FACTS — returns
    btc_r_1m                         DOUBLE PRECISION NULL,
    btc_r_5m                         DOUBLE PRECISION NULL,
    btc_r_15m                        DOUBLE PRECISION NULL,
    btc_r_60m                        DOUBLE PRECISION NULL,

    -- BLOQUE FACTS — volatility base
    btc_rv_1m                        DOUBLE PRECISION NULL,
    btc_rv_5m                        DOUBLE PRECISION NULL,
    btc_realized_range_5m            DOUBLE PRECISION NULL,
    -- Nota: btc_rv_15m se computa internamente en el worker para derivar
    -- btc_rv_ratio_1m_15m, pero no se persiste como columna independiente.

    -- BLOQUE FACTS — activity / flow base
    btc_n_trades_30s                 INTEGER NULL,
    btc_quote_vol_30s                DOUBLE PRECISION NULL,
    btc_signed_flow_30s              DOUBLE PRECISION NULL,
    btc_intertrade_gap_ms_p50        DOUBLE PRECISION NULL,
    btc_book_update_rate_30s         DOUBLE PRECISION NULL,

    -- BLOQUE FEATURES — stress
    btc_abs_r_1m                     DOUBLE PRECISION NULL,
    btc_abs_r_5m                     DOUBLE PRECISION NULL,
    btc_spread_bps_z                 DOUBLE PRECISION NULL,
    btc_tobi_extreme                 DOUBLE PRECISION NULL,
    btc_abs_signed_flow_30s          DOUBLE PRECISION NULL,

    -- BLOQUE FEATURES — trend
    btc_dir_persistence_15m          DOUBLE PRECISION NULL,
    btc_efficiency_ratio_15m         DOUBLE PRECISION NULL,

    -- BLOQUE FEATURES — volatility
    btc_rv_ratio_1m_15m              DOUBLE PRECISION NULL,

    -- BLOQUE FEATURES — liquidity
    btc_spread_stability_1m          DOUBLE PRECISION NULL,
    btc_spread_recovery_30s          DOUBLE PRECISION NULL,
    btc_tobi_non_extreme             DOUBLE PRECISION NULL,

    -- BLOQUE FEATURES — activity
    btc_trade_intensity_1m           DOUBLE PRECISION NULL,

    -- BLOQUE SCORES (outputs principales — SoT)
    btc_stress_score                 DOUBLE PRECISION NULL,
    btc_trend_score                  DOUBLE PRECISION NULL,
    btc_volatility_score             DOUBLE PRECISION NULL,
    btc_liquidity_score              DOUBLE PRECISION NULL,
    btc_activity_score               DOUBLE PRECISION NULL,

    -- BLOQUE COMPONENTES DE AUDITORÍA (opcionales)
    btc_stress_anchor_component      DOUBLE PRECISION NULL,
    btc_stress_support_component     DOUBLE PRECISION NULL,
    btc_trend_anchor_component       DOUBLE PRECISION NULL,
    btc_trend_support_component      DOUBLE PRECISION NULL,
    btc_volatility_anchor_component  DOUBLE PRECISION NULL,
    btc_volatility_support_component DOUBLE PRECISION NULL,
    btc_liquidity_anchor_component   DOUBLE PRECISION NULL,
    btc_liquidity_support_component  DOUBLE PRECISION NULL,
    btc_activity_anchor_component    DOUBLE PRECISION NULL,
    btc_activity_support_component   DOUBLE PRECISION NULL,

    -- BLOQUE SHADOW TAGS
    btc_stress_state                 TEXT NULL,
    btc_trend_state                  TEXT NULL,
    btc_volatility_state             TEXT NULL,
    btc_liquidity_state              TEXT NULL,
    btc_activity_state               TEXT NULL,
    shadow_tag_policy_version        TEXT NOT NULL,
    btc_shadow_tags_extra            JSONB NULL,

    -- BLOQUE HEALTH — global
    snapshot_health_status           TEXT NOT NULL,
    snapshot_observable              BOOLEAN NOT NULL DEFAULT TRUE,
    snapshot_joinable                BOOLEAN NOT NULL DEFAULT FALSE,

    -- BLOQUE HEALTH — ejes
    freshness_status                 TEXT NOT NULL,
    completeness_status              TEXT NOT NULL,
    validity_status                  TEXT NOT NULL,

    -- BLOQUE HEALTH — feed status
    trade_feed_status                TEXT NOT NULL,
    book_feed_status                 TEXT NOT NULL,

    -- BLOQUE HEALTH — degradación y faltantes
    missing_feature_count            INTEGER NOT NULL DEFAULT 0,
    missing_features_list            JSONB NULL,
    degradation_reasons              JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- BLOQUE HEALTH — late / out-of-order
    late_event_count                 INTEGER NULL,
    max_event_delay_ms               DOUBLE PRECISION NULL,

    -- BLOQUE HEALTH — worker state
    worker_status                    TEXT NOT NULL,

    -- BLOQUE GOVERNANCE
    regime_mode                      TEXT NOT NULL,
    regime_validation_status         TEXT NOT NULL,
    schema_version                   TEXT NOT NULL,
    feature_version                  TEXT NOT NULL,
    score_formula_version            TEXT NOT NULL,
    normalization_policy_version     TEXT NOT NULL,
    allowed_use_class                TEXT NOT NULL,
    prohibited_use_class             TEXT NOT NULL,

    -- BLOQUE DEBUG (opcional)
    debug_payload                    JSONB NULL,

    -- CHECKS
    -- Enforceados por SQL:
    CHECK (source_lag_ms >= 0),
    CHECK (compute_latency_ms >= 0),
    CHECK (trade_source_lag_ms IS NULL OR trade_source_lag_ms >= 0),
    CHECK (book_source_lag_ms IS NULL OR book_source_lag_ms >= 0),
    CHECK (window_start_ts <= window_end_ts),
    -- Enforceados por aplicación (semántica multi-columna no soportada como CHECK simple):
    --   feature_ts = window_end_ts
    --   window_end_ts <= source_ts <= snapshot_ts
    --   trade_source_ts <= snapshot_ts (si not null)
    --   book_source_ts <= snapshot_ts (si not null)
    --   ningún join puede usar feature_ts > t_confirm
    CHECK (btc_mid_price > 0),
    CHECK (btc_bid_price IS NULL OR btc_bid_price > 0),
    CHECK (btc_ask_price IS NULL OR btc_ask_price > 0),
    CHECK (btc_n_trades_30s IS NULL OR btc_n_trades_30s >= 0),
    CHECK (btc_quote_vol_30s IS NULL OR btc_quote_vol_30s >= 0),
    CHECK (btc_intertrade_gap_ms_p50 IS NULL OR btc_intertrade_gap_ms_p50 >= 0),
    CHECK (btc_book_update_rate_30s IS NULL OR btc_book_update_rate_30s >= 0),
    CHECK (btc_stress_score IS NULL OR (btc_stress_score >= 0 AND btc_stress_score <= 1)),
    CHECK (btc_volatility_score IS NULL OR (btc_volatility_score >= 0 AND btc_volatility_score <= 1)),
    CHECK (btc_liquidity_score IS NULL OR (btc_liquidity_score >= 0 AND btc_liquidity_score <= 1)),
    CHECK (btc_activity_score IS NULL OR (btc_activity_score >= 0 AND btc_activity_score <= 1)),
    CHECK (btc_trend_score IS NULL OR (btc_trend_score >= -1 AND btc_trend_score <= 1)),
    CHECK (btc_stress_state IN ('low','medium','high') OR btc_stress_state IS NULL),
    CHECK (btc_trend_state IN ('down','neutral','up') OR btc_trend_state IS NULL),
    CHECK (btc_volatility_state IN ('compressed','normal','expanding') OR btc_volatility_state IS NULL),
    CHECK (btc_liquidity_state IN ('thin','normal','thick') OR btc_liquidity_state IS NULL),
    CHECK (btc_activity_state IN ('low','normal','high') OR btc_activity_state IS NULL),
    CHECK (missing_feature_count >= 0),
    CHECK (late_event_count IS NULL OR late_event_count >= 0),
    CHECK (max_event_delay_ms IS NULL OR max_event_delay_ms >= 0),
    CHECK (snapshot_health_status IN ('fresh','degraded','stale')),
    CHECK (freshness_status IN ('fresh','delayed','stale')),
    CHECK (completeness_status IN ('complete','partial','insufficient')),
    CHECK (validity_status IN ('valid','suspicious','invalid')),
    CHECK (trade_feed_status IN ('live','delayed','frozen','missing')),
    CHECK (book_feed_status IN ('live','delayed','frozen','missing')),
    CHECK (worker_status IN ('warming_up','live','catching_up','degraded','stalled')),
    CHECK (regime_mode IN ('shadow','advisory','active_gate')),
    CHECK (
        regime_validation_status IN (
            'experimental',
            'observed',
            'validated_partial',
            'validated_operational'
        )
    )
);
```

### 13.3. Índices recomendados

```sql
-- Obligatorio (parte del CREATE)
PRIMARY KEY (snapshot_ts)

-- Recomendados
CREATE INDEX ON btc_regime_snapshots (feature_ts DESC);
CREATE INDEX ON btc_regime_snapshots (source_ts DESC);
CREATE INDEX ON btc_regime_snapshots (snapshot_health_status, snapshot_ts DESC);
CREATE INDEX ON btc_regime_snapshots (regime_mode, snapshot_ts DESC);
CREATE INDEX ON btc_regime_snapshots (snapshot_joinable, feature_ts DESC);
-- Si PIT joins son frecuentes:
CREATE INDEX ON btc_regime_snapshots (feature_ts);
```

### 13.4. Hypertable

```sql
SELECT create_hypertable('btc_regime_snapshots', 'snapshot_ts',
    chunk_time_interval => INTERVAL '1 day');
-- Compresión: diferida — no necesaria para primer deploy v2
```

### 13.5. Valores iniciales esperados

```
regime_mode                  = 'shadow'
regime_validation_status     = 'experimental'
schema_version               = 'v2'
feature_version              = 'v1'
score_formula_version        = 'v2_anchor_support_fixed_mapping'
normalization_policy_version = 'v1_fixed_per_feature'
shadow_tag_policy_version    = 'v1_bootstrap_fixed_thresholds'
allowed_use_class            = 'observational,analytical'
prohibited_use_class         = 'operational'
```

### 13.6. Cambios explícitos respecto a Schema v1

Agregados en v2:

```
btc_abs_signed_flow_30s, btc_tobi_non_extreme
snapshot_health_status, snapshot_observable, snapshot_joinable
freshness_status, completeness_status, validity_status
trade_feed_status, book_feed_status
missing_features_list, degradation_reasons
late_event_count, max_event_delay_ms
worker_status, shadow_tag_policy_version
normalization_policy_version
componentes anchor/support opcionales
```

Eliminados del núcleo v2:

```
stress_candidate, regime_candidate
staleness_state, degraded_flag, error_flag
trade_input_stale, book_input_stale
```

### 13.7. Decisiones cerradas del schema

    DC-SCHEMA-01: tabla única btc_regime_snapshots
    DC-SCHEMA-02: una fila = un snapshot completo
    DC-SCHEMA-03: snapshot_ts es la PK técnica en v2
    DC-SCHEMA-04: columnas explícitas para temporal + facts + features +
                  scores + shadow_tags + health + governance
    DC-SCHEMA-05: JSON no es núcleo; solo apoyo opcional para debug
    DC-SCHEMA-06: scores continuos son el output principal persistido
    DC-SCHEMA-07: shadow tags secundarios
    DC-SCHEMA-08: regime_mode y regime_validation_status obligatorios
    DC-SCHEMA-09: Snapshot Schema v2 reemplaza a Snapshot Schema v1
    DC-SCHEMA-10: schema v2 alineado con health v2, tags v2, wiring v2 y mapping v2
    DC-SCHEMA-11: solo features finales necesarias para score — no normalizaciones intermedias
    DC-SCHEMA-12: shadow tags del snapshot v2 son solo display tags versionados
    DC-SCHEMA-13: snapshot_joinable entra al núcleo v2
    DC-SCHEMA-14: worker_status entra al núcleo v2
    DC-SCHEMA-15: campos legacy ambiguos de health quedan fuera del núcleo v2
    DC-SCHEMA-16: btc_abs_signed_flow_30s reemplaza el uso implícito de signed
                  flow bruto para stress mapping

---

## 14. Wiring Contract v2

### 14.1. Arquitectura aprobada

```
BTC market data source (fuente oficial del sistema)
    → btc_regime_worker
    → btc_regime_snapshots
    → read-only consumers (CLASS_1 / CLASS_2)
```

El worker: calcula contexto BTC, persiste snapshots, no participa en el
detector, no muta estado de otras capas.

No aprobado: regime embebido dentro de fast_loop, entry_gate, confirmer,
ni helpers operativos que usen regime en tiempo real.

### 14.2. Source of truth operativo

BTC Regime debe leer desde la capa de market data ya sostenida por la
arquitectura existente. No se aprueba una segunda fuente de verdad ad hoc.

En caso de duda: priorizar simplicidad y auditabilidad sobre mínima latencia extra.

### 14.3. Responsabilidad del worker

Permitido: leer datos BTC aprobados, mantener buffers rolling, calcular
facts / features / scores / shadow tags, evaluar health, persistir snapshot,
exponer query de lectura.

Prohibido: leer candidate_events para modificar cálculo online, leer
outcomes para modificar cálculo, escribir sobre config del detector,
modificar thresholds de otras capas, suprimir alertas, mutar estado del
bot, reescribir snapshots previos.

### 14.4. Cadence y alineación temporal

Cadence objetivo: 1s–2s por ciclo fija de timer.

```
window_end_ts = latest_event_time_included
feature_ts    = window_end_ts
snapshot_ts   = instante de disponibilidad del snapshot
```

Prohibido: feature_ts = now() sin control del dato realmente incluido.

### 14.5. Worker status

```
worker_status ∈ {warming_up, live, catching_up, degraded, stalled}

warming_up  → buffers incompletos tras arranque — snapshot permitido,
              health no puede fingir fresh
live        → saludable, emitiendo dentro de contrato
catching_up → recuperándose de backlog — observable, joinable solo si health
              lo permite
degraded    → vivo pero con degradación persistente de inputs o features
stalled     → detenido — no puede presentarse como live
```

### 14.6. Buffers y ventanas

Buffers rolling viven solo dentro del btc_regime_worker. Ventanas: 30s, 1m,
5m, 15m, 60m. Deben ser deterministas, reconstruibles, sin datos futuros,
sin relleno silencioso, sin reescritura de histórico.

### 14.7. Persistencia

```
Destino: btc_regime_snapshots
Modelo: una fila por snapshot

Persistir:
    fresh    → siempre
    degraded → siempre
    stale    → si aún es interpretable y health lo marca claramente

No persistir solo si: snapshot imposible de construir,
integridad temporal rota, o estructura mínima inválida.
En esos casos: log obligatorio, no silencioso.
```

### 14.8. Readers por clase de consumidor

```
btc_regime_reader_observability → logs, grafana, debug, inspección humana
btc_regime_reader_analytics     → dataset_capture, research, queries PIT
reader operativo                → NO APROBADO EN F2
```

Prohibido: helper genérico ambiguo tipo get_btc_regime_for_decision().

### 14.9. Joins PIT con candidate_events

Regla PIT obligatoria:

    usar el snapshot más reciente tal que: feature_ts <= t_confirm_event

Si hay múltiples candidatos válidos: elegir el de feature_ts más alto.

Para joins conservadores de enrichment, adicionalmente: snapshot_joinable = TRUE.

Campos mínimos a copiar al evento enriquecido:

```
btc_regime_snapshot_ts
btc_regime_feature_ts
btc_regime_source_lag_ms
btc_regime_snapshot_health_status
btc_regime_worker_status
btc_stress_score_at_confirm
btc_trend_score_at_confirm
btc_volatility_score_at_confirm
btc_liquidity_score_at_confirm
btc_activity_score_at_confirm
```

Opcionales secundarios:

```
btc_*_state_at_confirm
btc_regime_join_lag_ms
```

Regla: scores son núcleo. Shadow tags son secundarios.
Enrichment sí. Causalidad no.

### 14.10. Integración con fast_loop / entry_gate / confirmer

```
fast_loop en F2:
    - puede ignorar totalmente regime
    - opcionalmente puede loggear regime de forma no causal

entry_gate en F2:
    - no consume regime

aggtrade_confirmer en F2:
    - no consume regime
```

Prohibido explícito:

    if btc_*_score → branching
    if btc_*_state → branching
    mutate thresholds / size / alerts using regime

Regla crítica: eliminar BTC Regime debe dejar idéntica la conducta
operativa del bot.

### 14.11. Fail-open y no back-propagation

Si el worker falla, el pipeline base continúa sin cambio operativo.

El fallo de BTC Regime NO puede back-propagate hacia: latencia del detector,
cardinalidad de candidatos, confirmaciones, rechazos, alertas, outcomes,
ni composición del dataset_clean_base.

Única consecuencia permitida: pérdida o degradación del enrichment BTC.

### 14.12. Alerting del propio worker

Se aprueba alerting mínimo cuando ocurra de forma sostenida:

    - no hay snapshots fresh por ventana superior al presupuesto
    - trade_feed_status = frozen o missing
    - book_feed_status = frozen o missing
    - snapshot_health_status = stale sostenido
    - fallan N ciclos consecutivos de persistencia
    - worker_status pasa a stalled

Alerting en F2: observacional, no acciona cambios automáticos en el detector.

### 14.13. Test de no-regresión (medible)

Sistema con BTC Regime ON vs OFF en F2.

Resultado esperado — idéntico:

    candidate_count, candidate_symbol_set, confirm_count, alert_count,
    entry_gate_decision_distribution, outcome_job_count,
    dataset_capture_row_count, orden temporal relativo de eventos base

Únicas diferencias permitidas: snapshots btc_regime_snapshots, columnas
extra en dataset_enriched, logs / dashboards / observabilidad.

Si cualquier métrica operativa base cambia: violación del wiring contract.

### 14.14. Warmup / resume / restart

Al arrancar: buffers vacíos o parciales, worker en warming_up, health
no puede fingir fresh. Durante catching_up: snapshots permitidos, health
debe reflejar atraso real. Prohibido: rellenar histórico silenciosamente,
emitir scores completos sin ventana suficiente, reescribir snapshots.

### 14.15. Decisions cerradas del wiring

    DC-WIRE-01: BTC Regime vive como proceso separado
    DC-WIRE-02: Source of truth operativo debe alinearse con la fuente
                oficial de market data del sistema
    DC-WIRE-03: Cadence es fija por timer; feature_ts = window_end_ts
    DC-WIRE-04: Se introducen worker_status explícitos
    DC-WIRE-05: Readers separados por clase de consumidor
    DC-WIRE-06: Alerting mínimo del propio worker aprobado
    DC-WIRE-07: Fallo de BTC Regime no puede back-propagate al pipeline base
    DC-WIRE-08: Test ON vs OFF es contrato medible, no solo conceptual
    DC-WIRE-09: Claude no puede cerrar decisiones abiertas por su cuenta

---

## 15. Reglas de uso por TradeSeeker

### 15.1. Estado actual: shadow

En el estado actual (regime_mode = shadow), BTC Regime:

    PUEDE:
        - calcular features y scores
        - generar y persistir snapshots
        - exponer último snapshot
        - adjuntar contexto a candidate_events
        - mostrarse en logs y Grafana
        - usarse en research histórico y correlación con outcomes

    NO PUEDE:
        - bloquear señales
        - aprobar señales
        - modificar thresholds, confirm_ratio, rel_volume_min, stage_pct_max
        - modificar cooldowns, size, routing de alertas
        - modificar labeling, outcome tracker, dataset_clean
        - filtrar eventos del research base
        - alterar scoring del detector

### 15.2. Futuro: hard gate de stress (pendiente F3)

Solo activable si se valida empíricamente que BTC Regime aporta edge real.

Requiere: evidencia incremental, criterio operativo explícito, validación
fuera de muestra, mecanismo de rollback, monitoreo explícito, auditoría y
aprobación documentada. Transición: shadow → advisory → active_gate.

### 15.3. Futuro: modifier continuo (pendiente F3)

Solo activable bajo mismas condiciones. Potenciales usos: modular confianza,
weighting, sizing según contexto.

### 15.4. Regla de anti-contaminación del dataset

BTC Regime NO puede influir en la composición del dataset en F2.

    dataset_clean_base = eventos sin influencia de BTC Regime
    dataset_enriched   = dataset_clean_base + columnas BTC Regime

dataset_enriched hereda composición de dataset_clean_base. Nunca al revés.

Test de sanidad obligatorio: correr con BTC Regime ON vs OFF y verificar
mismos eventos, mismo orden, mismos outcomes, misma distribución.

Si BTC Regime cambia qué datos existen → hay contaminación → no hay
validación válida posible.

### 15.5. Regla de validación limpia

Para que BTC Regime sea validado correctamente, debe ser evaluado sobre
datos que no influyó. La capa debe ser exógena al dataset que la evalúa.
Si no, no hay validación — solo confirmación circular.

---

## 16. Limitaciones actuales

- Diseño conceptual cerrado; calibración empírica pendiente
- Activación real no aprobada
- Thresholds de scoring son bootstrap — no han sido calibrados con datos propios
- Shadow tag thresholds son placeholders
- Ventanas de normalización aún por validar en producción
- BTC Regime no ha demostrado edge incremental todavía — esa es la hipótesis a testear
- worker_status = warming_up hasta que los buffers de ventanas largas (60m) se llenen
- Los componentes anchor/support son opcionales en v2 — no son SoT y no reemplazan scores

---

## 17. Roadmap de evolución

### F2 (actual) — Shadow / Observacional

    ✔ Arquitectura y contratos cerrados
    ✔ Schema v2 definido
    → Implementar btc_regime_worker separado
    → Conectar feeds BTC (trades + bookTicker)
    → Calcular facts, features, scores, tags, health
    → Persistir en btc_regime_snapshots
    → Enriquecer candidate_events con contexto PIT
    → Observar y acumular historia

### F3 (pendiente validación) — Advisory / Active Gate

    → Analizar correlación de scores con outcomes reales
    → Validar si aporta información incremental sobre las capas existentes
    → Si se valida: pasar a advisory (logs + dashboards operativos)
    → Si se valida con suficiente evidencia: activar hard gate de stress
    → Calibrar thresholds con datos propios antes de cualquier activación

Condición de salida de shadow: no es tiempo solamente. Se requiere
cobertura suficiente de condiciones de mercado, estabilidad de métricas,
patrones observables no-ruido, y relación medible con calidad de señales.

---

## 18. Decisiones cerradas

### Diseño y dimensiones

    DC-FEAT-01: El núcleo es de 5 dimensiones: stress, trend, volatility,
                liquidity, activity. No se aprueba una sexta ni reducir a 4.
    DC-FEAT-02: El output principal es un vector continuo por dimensión,
                no un regime discreto.
    DC-FEAT-03: Stress usa magnitud de flow (abs_signed_flow), no signo bruto.
    DC-FEAT-04: Liquidity ancla en spread/stability/recovery; tobi es support.
    DC-FEAT-05: Activity es una dimensión propia separada de liquidity y volatility.

### Outputs y segregación

    DC-OUT-01: Snapshot segregado en: facts, features, scores, shadow_tags,
               health, governance.
    DC-OUT-02: Scores es el núcleo oficial de la capa.
    DC-OUT-03: Shadow tags son secundarios y no normativos.
    DC-OUT-04: Health describe calidad operativa del snapshot, no el mercado.
    DC-OUT-05: Governance describe contrato de uso, no contexto de mercado.
    DC-OUT-06: Ningún campo normativo tipo allow/block/safe/unsafe.
    DC-OUT-07: Si un campo no puede clasificarse claramente, no entra al snapshot.

### Feature → Score Mapping

    DC-SCORE-01 a DC-SCORE-08: ver Sección 8.6

### Shadow Tags

    DC-TAGS-01 a DC-TAGS-08: ver Sección 10.8

### Health / Degradation

    DC-HEALTH-01 a DC-HEALTH-08: ver Sección 11.13

### Contrato temporal

    DC-TIME-01 a DC-TIME-06: ver Sección 5.3

### Snapshot Schema

    DC-SCHEMA-01 a DC-SCHEMA-16: ver Sección 13.7

### Wiring

    DC-WIRE-01 a DC-WIRE-09: ver Sección 14.15

### Dataset y anti-contaminación

    DC-DATA-01: BTC Regime no puede influir en la composición del dataset en F2
    DC-DATA-02: BTC Regime solo puede entrar como feature/contexto
    DC-DATA-03: dataset_clean_base es independiente del regime
    DC-DATA-04: dataset_enriched hereda dataset_clean_base sin alterarlo
    DC-DATA-05: cualquier filtrado basado en regime es una violación
    DC-DATA-06: outcome tracker es independiente del regime
    DC-DATA-07: validación de regime debe hacerse sobre dataset no influido

### Modo operativo

    DC-MODE-01: BTC Regime expone campo obligatorio regime_mode
    DC-MODE-02: Modo oficial en F2: shadow
    DC-MODE-03: Transición shadow → active_gate directo está prohibida
    DC-MODE-04: Cualquier inconsistencia de modo degrada a shadow
    DC-MODE-05: Todo cambio de modo debe ser auditable con rastro completo

---

## Nota sobre KTS_TradeSeeker_Design_v3.2

El bloque de Capa 6 que existe en `KTS_TradeSeeker_Design_v3.2.md` debe
tratarse como histórico para todo lo relacionado con BTC Regime.

En particular, los siguientes campos y estructuras que aparecen allí han
sido reemplazados por este SoT y no deben usarse como referencia activa:

    - stress_candidate / regime_candidate → eliminados del núcleo v2
    - staleness_s → reemplazado por source_lag_ms + freshness_status
    - error_flag → reemplazado por validity_status + degradation_reasons
    - el snapshot hipotético del design v3.2 → reemplazado por schema v2

El SoT autorizado para Capa 6 es este documento:
BTC_Regime_SoT_v2.md · v2.0 · 2026-04-13

---

✅ Fin — BTC Regime Foundation / Gate · SoT v2 · 2026-04-13
