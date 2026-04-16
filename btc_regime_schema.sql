-- btc_regime_schema.sql — KTS TradeSeeker · BTC Regime Foundation · Capa 6
-- Schema v2 — 2026-04-14
-- SoT: docs/BTC_Regime_SoT_v2.md
--
-- Ejecutar en VPS:
--   docker exec -i ts_timescaledb psql -U postgres -d tsdb < btc_regime_schema.sql
--
-- Idempotencia:
--   - CREATE TABLE IF NOT EXISTS → idempotente
--   - CREATE INDEX IF NOT EXISTS → idempotente
--   - create_hypertable con if_not_exists=TRUE → idempotente solo si ya fue convertida
--   - COMMENT ON TABLE/COLUMN → idempotente (reemplaza)
--   Re-ejecutar es seguro si la tabla ya existe como hypertable.
--   Si existe como tabla regular sin datos → create_hypertable la convierte.
--   Si existe como tabla regular con datos → migrate_data=TRUE la convierte igual.

-- Abortar en el primer error — evita efectos en cascada de fallos parciales (I2)
\set ON_ERROR_STOP on

-- ===========================================================================
-- TABLA PRINCIPAL
-- ===========================================================================

CREATE TABLE IF NOT EXISTS btc_regime_snapshots (

    -- -------------------------------------------------------------------------
    -- BLOQUE TEMPORAL
    -- DC-TIME-01: snapshot_ts = tiempo de disponibilidad, no tiempo del mercado
    -- DC-TIME-02: feature_ts = window_end_ts
    -- Invariantes multi-columna (enforced por aplicación, no por SQL):
    --   window_start_ts <= window_end_ts <= source_ts <= snapshot_ts
    --   trade_source_ts <= snapshot_ts (si not null)
    --   book_source_ts  <= snapshot_ts (si not null)
    --   feature_ts = window_end_ts
    --   ningún join puede usar feature_ts > t_confirm
    -- -------------------------------------------------------------------------
    snapshot_ts                      TIMESTAMPTZ         NOT NULL,
    source_ts                        TIMESTAMPTZ         NOT NULL,
    feature_ts                       TIMESTAMPTZ         NOT NULL,
    window_start_ts                  TIMESTAMPTZ         NOT NULL,
    window_end_ts                    TIMESTAMPTZ         NOT NULL,
    trade_source_ts                  TIMESTAMPTZ         NULL,
    book_source_ts                   TIMESTAMPTZ         NULL,
    source_lag_ms                    DOUBLE PRECISION    NOT NULL,
    trade_source_lag_ms              DOUBLE PRECISION    NULL,
    book_source_lag_ms               DOUBLE PRECISION    NULL,
    compute_latency_ms               DOUBLE PRECISION    NOT NULL,

    -- -------------------------------------------------------------------------
    -- BLOQUE FACTS — market base
    -- Fuente: metrics_ext (bid, ask, spread_bp, bid_qty, ask_qty)
    -- -------------------------------------------------------------------------
    btc_mid_price                    DOUBLE PRECISION    NOT NULL,
    btc_bid_price                    DOUBLE PRECISION    NULL,
    btc_ask_price                    DOUBLE PRECISION    NULL,
    btc_spread_bps                   DOUBLE PRECISION    NULL,   -- spread en basis points; fuente: metrics_ext.spread_bp; >= 0
    btc_tobi                         DOUBLE PRECISION    NULL,   -- top-of-book imbalance: (bid_qty - ask_qty) / (bid_qty + ask_qty); fuente: metrics_ext; rango [-1, +1]

    -- -------------------------------------------------------------------------
    -- BLOQUE FACTS — returns
    -- Calculados desde ventanas rolling de metrics_ext (price history)
    -- Sin bounds naturales — retornos pueden ser cualquier valor real
    -- -------------------------------------------------------------------------
    btc_r_1m                         DOUBLE PRECISION    NULL,
    btc_r_5m                         DOUBLE PRECISION    NULL,
    btc_r_15m                        DOUBLE PRECISION    NULL,
    btc_r_60m                        DOUBLE PRECISION    NULL,

    -- -------------------------------------------------------------------------
    -- BLOQUE FACTS — volatility base
    -- Nota: btc_rv_15m se computa internamente en el worker para derivar
    -- btc_rv_ratio_1m_15m, pero NO se persiste como columna independiente.
    -- -------------------------------------------------------------------------
    btc_rv_1m                        DOUBLE PRECISION    NULL,   -- realized volatility ventana 1m; >= 0
    btc_rv_5m                        DOUBLE PRECISION    NULL,   -- realized volatility ventana 5m; >= 0
    btc_realized_range_5m            DOUBLE PRECISION    NULL,   -- high - low en ventana 5m; >= 0

    -- -------------------------------------------------------------------------
    -- BLOQUE FACTS — activity / flow base
    -- Fuente: WS btcusdt@aggTrade (continuo)
    -- Invariante de aplicación (no enforeable como CHECK simple):
    --   abs(btc_signed_flow_30s) <= btc_quote_vol_30s cuando ambos presentes
    -- -------------------------------------------------------------------------
    btc_n_trades_30s                 INTEGER             NULL,   -- conteo de trades en ventana 30s; >= 0
    btc_quote_vol_30s                DOUBLE PRECISION    NULL,   -- volumen en USDT (buy + sell) en ventana 30s; >= 0
    btc_signed_flow_30s              DOUBLE PRECISION    NULL,   -- buy_vol - sell_vol en USDT; con signo; puede ser negativo
    btc_intertrade_gap_ms_p50        DOUBLE PRECISION    NULL,   -- mediana del gap entre trades consecutivos, en milisegundos; fuente: WS aggTrade; >= 0
    btc_aggtrade_msg_rate_30s        DOUBLE PRECISION    NULL,   -- proxy de intensidad: mensajes aggTrade/s en ventana 30s; NO representa updates reales del book; >= 0

    -- -------------------------------------------------------------------------
    -- BLOQUE FEATURES — stress
    -- Anchor: btc_abs_r_1m, btc_abs_r_5m, btc_spread_bps_z
    -- Support: btc_tobi_extreme, btc_abs_signed_flow_30s
    -- -------------------------------------------------------------------------
    btc_abs_r_1m                     DOUBLE PRECISION    NULL,   -- abs(btc_r_1m); >= 0
    btc_abs_r_5m                     DOUBLE PRECISION    NULL,   -- abs(btc_r_5m); >= 0
    btc_spread_bps_z                 DOUBLE PRECISION    NULL,   -- z-score rolling crudo de btc_spread_bps (pre-clip/tanh); sin bounds fijos
    btc_tobi_extreme                 DOUBLE PRECISION    NULL,   -- percentile rolling de abs(btc_tobi); rango [0, 1]
    btc_abs_signed_flow_30s          DOUBLE PRECISION    NULL,   -- abs(btc_signed_flow_30s); magnitud de imbalance, NUNCA signo bruto (DC-SCORE-03); >= 0

    -- -------------------------------------------------------------------------
    -- BLOQUE FEATURES — trend
    -- Anchor: btc_r_15m, btc_r_60m (facts, no columnas separadas aquí)
    -- Support: los dos campos siguientes
    -- -------------------------------------------------------------------------
    btc_dir_persistence_15m          DOUBLE PRECISION    NULL,   -- fracción de retornos 1m del mismo signo en 15m; rango [0, 1]
    btc_efficiency_ratio_15m         DOUBLE PRECISION    NULL,   -- abs(net_move) / sum(abs(moves)) en 15m; rango [0, 1]

    -- -------------------------------------------------------------------------
    -- BLOQUE FEATURES — volatility
    -- Anchor: btc_rv_1m, btc_rv_5m (facts)
    -- Support: el campo siguiente
    -- -------------------------------------------------------------------------
    btc_rv_ratio_1m_15m              DOUBLE PRECISION    NULL,   -- btc_rv_1m / btc_rv_15m (expansión relativa); >= 0

    -- -------------------------------------------------------------------------
    -- BLOQUE FEATURES — liquidity
    -- Anchor: btc_spread_bps (fact), btc_spread_stability_1m
    -- Support: btc_spread_recovery_30s, btc_tobi_non_extreme
    -- -------------------------------------------------------------------------
    btc_spread_stability_1m          DOUBLE PRECISION    NULL,   -- estabilidad de spread en 1m; percentile rolling; rango [0, 1]
    btc_spread_recovery_30s          DOUBLE PRECISION    NULL,   -- velocidad de recovery del spread en 30s; percentile rolling; rango [0, 1]
    btc_tobi_non_extreme             DOUBLE PRECISION    NULL,   -- tobi en rango no extremo, proxy de absorción; percentile rolling; rango [0, 1]

    -- -------------------------------------------------------------------------
    -- BLOQUE FEATURES — activity
    -- Anchor: btc_n_trades_30s, btc_quote_vol_30s (facts)
    -- Support: este campo + btc_intertrade_gap_ms_p50 (fact) + btc_aggtrade_msg_rate_30s (fact)
    -- -------------------------------------------------------------------------
    btc_trade_intensity_1m           DOUBLE PRECISION    NULL,   -- trades/s en ventana 1m (tasa de llegada); >= 0

    -- -------------------------------------------------------------------------
    -- BLOQUE SCORES — outputs principales, SoT (DC-SCORE-*)
    -- stress      ∈ [0,  1]  — 0=relajado, 1=bajo tensión
    -- trend       ∈ [-1, +1] — -1=sesgo bajista, 0=neutro, +1=sesgo alcista
    -- volatility  ∈ [0,  1]  — 0=comprimido, 1=expandido
    -- liquidity   ∈ [0,  1]  — 0=mala liquidez, 1=buena liquidez
    -- activity    ∈ [0,  1]  — 0=mercado muerto, 1=muy activo
    -- -------------------------------------------------------------------------
    btc_stress_score                 DOUBLE PRECISION    NULL,
    btc_trend_score                  DOUBLE PRECISION    NULL,
    btc_volatility_score             DOUBLE PRECISION    NULL,
    btc_liquidity_score              DOUBLE PRECISION    NULL,
    btc_activity_score               DOUBLE PRECISION    NULL,

    -- -------------------------------------------------------------------------
    -- BLOQUE COMPONENTES DE AUDITORÍA — opcionales, no SoT (Sección 9.3)
    -- No reemplazan scores. Solo para tuning / debug.
    -- Stress/Volatility/Liquidity/Activity components ∈ [0, 1]
    -- Trend components ∈ [-1, +1]
    -- -------------------------------------------------------------------------
    btc_stress_anchor_component      DOUBLE PRECISION    NULL,
    btc_stress_support_component     DOUBLE PRECISION    NULL,
    btc_trend_anchor_component       DOUBLE PRECISION    NULL,
    btc_trend_support_component      DOUBLE PRECISION    NULL,
    btc_volatility_anchor_component  DOUBLE PRECISION    NULL,
    btc_volatility_support_component DOUBLE PRECISION    NULL,
    btc_liquidity_anchor_component   DOUBLE PRECISION    NULL,
    btc_liquidity_support_component  DOUBLE PRECISION    NULL,
    btc_activity_anchor_component    DOUBLE PRECISION    NULL,
    btc_activity_support_component   DOUBLE PRECISION    NULL,

    -- -------------------------------------------------------------------------
    -- BLOQUE SHADOW TAGS — derivaciones secundarias de scores (DC-TAGS-*)
    -- Valores cerrados por dimensión:
    --   stress:     low | medium | high
    --   trend:      down | neutral | up
    --   volatility: compressed | normal | expanding
    --   liquidity:  thin | normal | thick
    --   activity:   low | normal | high
    -- Tags solo se emiten si score existe y health no es stale (DC-TAGS-05)
    -- -------------------------------------------------------------------------
    btc_stress_state                 TEXT                NULL,
    btc_trend_state                  TEXT                NULL,
    btc_volatility_state             TEXT                NULL,
    btc_liquidity_state              TEXT                NULL,
    btc_activity_state               TEXT                NULL,
    shadow_tag_policy_version        TEXT                NOT NULL,
    btc_shadow_tags_extra            JSONB               NULL,   -- solo debug/extensión; nunca usar para lógica operativa (DC-TAGS-08)

    -- -------------------------------------------------------------------------
    -- BLOQUE HEALTH — global (DC-HEALTH-*)
    -- snapshot_health_status ∈ {fresh, degraded, stale}
    -- snapshot_observable: TRUE salvo invalidez severa total
    -- snapshot_joinable: TRUE solo si cumple criterios PIT (Sección 11.7)
    -- -------------------------------------------------------------------------
    snapshot_health_status           TEXT                NOT NULL,
    snapshot_observable              BOOLEAN             NOT NULL DEFAULT TRUE,
    snapshot_joinable                BOOLEAN             NOT NULL DEFAULT FALSE,

    -- HEALTH — ejes separados (DC-HEALTH-01)
    freshness_status                 TEXT                NOT NULL,   -- fresh | delayed | stale
    completeness_status              TEXT                NOT NULL,   -- complete | partial | insufficient
    validity_status                  TEXT                NOT NULL,   -- valid | suspicious | invalid

    -- HEALTH — feed status por fuente (DC-HEALTH-02)
    trade_feed_status                TEXT                NOT NULL,   -- live | delayed | frozen | missing
    book_feed_status                 TEXT                NOT NULL,   -- live | delayed | frozen | missing

    -- HEALTH — degradación explícita (DC-HEALTH-05)
    missing_feature_count            INTEGER             NOT NULL DEFAULT 0,
    missing_features_list            JSONB               NULL,       -- array JSON de nombres de features faltantes; debe ser array si presente
    degradation_reasons              JSONB               NOT NULL DEFAULT '[]'::jsonb,  -- array JSON de códigos de razón; siempre array

    -- HEALTH — late / out-of-order data (Sección 11.10)
    late_event_count                 INTEGER             NULL,
    max_event_delay_ms               DOUBLE PRECISION    NULL,

    -- HEALTH — worker status (DC-WIRE-04)
    worker_status                    TEXT                NOT NULL,   -- warming_up | live | catching_up | degraded | stalled

    -- -------------------------------------------------------------------------
    -- BLOQUE GOVERNANCE (DC-MODE-*, DC-SCHEMA-08)
    -- Contrato de uso — ver COMMENT ON COLUMN abajo para descripción completa
    -- -------------------------------------------------------------------------
    regime_mode                      TEXT                NOT NULL,   -- shadow | advisory | active_gate
    regime_validation_status         TEXT                NOT NULL,   -- experimental | observed | validated_partial | validated_operational
    schema_version                   TEXT                NOT NULL,
    feature_version                  TEXT                NOT NULL,
    score_formula_version            TEXT                NOT NULL,
    normalization_policy_version     TEXT                NOT NULL,
    allowed_use_class                TEXT                NOT NULL,
    prohibited_use_class             TEXT                NOT NULL,

    -- -------------------------------------------------------------------------
    -- BLOQUE AUDITORÍA DE INSERT (I15)
    -- -------------------------------------------------------------------------
    created_at                       TIMESTAMPTZ         NOT NULL DEFAULT now(),  -- momento de insert en DB; puede diferir de snapshot_ts si hay backlog

    -- -------------------------------------------------------------------------
    -- BLOQUE DEBUG — opcional
    -- -------------------------------------------------------------------------
    debug_payload                    JSONB               NULL,   -- exclusivo para debug interno del worker; no usar en análisis ni lógica

    -- =========================================================================
    -- PRIMARY KEY
    -- DC-SCHEMA-03: snapshot_ts es la PK técnica en v2
    -- =========================================================================
    PRIMARY KEY (snapshot_ts),

    -- =========================================================================
    -- CHECKS — integridad de valores
    -- Los invariantes multi-columna semánticos se enforcan en la aplicación:
    --   window_end_ts <= source_ts <= snapshot_ts
    --   trade_source_ts <= snapshot_ts (si not null)
    --   book_source_ts  <= snapshot_ts (si not null)
    --   feature_ts = window_end_ts
    --   abs(btc_signed_flow_30s) <= btc_quote_vol_30s (si ambos presentes)
    -- =========================================================================

    -- Temporal
    CHECK (source_lag_ms >= 0),
    CHECK (compute_latency_ms >= 0),
    CHECK (trade_source_lag_ms IS NULL OR trade_source_lag_ms >= 0),
    CHECK (book_source_lag_ms  IS NULL OR book_source_lag_ms  >= 0),
    CHECK (window_start_ts <= window_end_ts),

    -- Facts — precios, spread, tobi
    CHECK (btc_mid_price > 0),
    CHECK (btc_bid_price IS NULL OR btc_bid_price > 0),
    CHECK (btc_ask_price IS NULL OR btc_ask_price > 0),
    CHECK (btc_spread_bps IS NULL OR btc_spread_bps >= 0),
    CHECK (btc_bid_price IS NULL OR btc_ask_price IS NULL OR btc_bid_price <= btc_ask_price),
    CHECK (btc_tobi IS NULL OR (btc_tobi >= -1 AND btc_tobi <= 1)),

    -- Facts — volatility
    CHECK (btc_rv_1m           IS NULL OR btc_rv_1m           >= 0),
    CHECK (btc_rv_5m           IS NULL OR btc_rv_5m           >= 0),
    CHECK (btc_realized_range_5m IS NULL OR btc_realized_range_5m >= 0),

    -- Facts — activity
    CHECK (btc_n_trades_30s          IS NULL OR btc_n_trades_30s          >= 0),
    CHECK (btc_quote_vol_30s         IS NULL OR btc_quote_vol_30s         >= 0),
    CHECK (btc_intertrade_gap_ms_p50 IS NULL OR btc_intertrade_gap_ms_p50 >= 0),
    CHECK (btc_aggtrade_msg_rate_30s IS NULL OR btc_aggtrade_msg_rate_30s >= 0),
    CHECK (btc_abs_signed_flow_30s   IS NULL OR btc_abs_signed_flow_30s   >= 0),

    -- Features — stress
    CHECK (btc_abs_r_1m    IS NULL OR btc_abs_r_1m    >= 0),
    CHECK (btc_abs_r_5m    IS NULL OR btc_abs_r_5m    >= 0),
    CHECK (btc_tobi_extreme IS NULL OR (btc_tobi_extreme >= 0 AND btc_tobi_extreme <= 1)),

    -- Features — trend
    CHECK (btc_dir_persistence_15m  IS NULL OR (btc_dir_persistence_15m  >= 0 AND btc_dir_persistence_15m  <= 1)),
    CHECK (btc_efficiency_ratio_15m IS NULL OR (btc_efficiency_ratio_15m >= 0 AND btc_efficiency_ratio_15m <= 1)),

    -- Features — volatility
    CHECK (btc_rv_ratio_1m_15m IS NULL OR btc_rv_ratio_1m_15m >= 0),

    -- Features — liquidity
    CHECK (btc_spread_stability_1m IS NULL OR (btc_spread_stability_1m >= 0 AND btc_spread_stability_1m <= 1)),
    CHECK (btc_spread_recovery_30s IS NULL OR (btc_spread_recovery_30s >= 0 AND btc_spread_recovery_30s <= 1)),
    CHECK (btc_tobi_non_extreme    IS NULL OR (btc_tobi_non_extreme    >= 0 AND btc_tobi_non_extreme    <= 1)),

    -- Features — activity
    CHECK (btc_trade_intensity_1m IS NULL OR btc_trade_intensity_1m >= 0),

    -- Scores — rangos por dimensión
    CHECK (btc_stress_score     IS NULL OR (btc_stress_score     >= 0  AND btc_stress_score     <= 1)),
    CHECK (btc_volatility_score IS NULL OR (btc_volatility_score >= 0  AND btc_volatility_score <= 1)),
    CHECK (btc_liquidity_score  IS NULL OR (btc_liquidity_score  >= 0  AND btc_liquidity_score  <= 1)),
    CHECK (btc_activity_score   IS NULL OR (btc_activity_score   >= 0  AND btc_activity_score   <= 1)),
    CHECK (btc_trend_score      IS NULL OR (btc_trend_score      >= -1 AND btc_trend_score      <= 1)),

    -- Componentes de auditoría — rangos por dimensión
    CHECK (btc_stress_anchor_component      IS NULL OR (btc_stress_anchor_component      >= 0  AND btc_stress_anchor_component      <= 1)),
    CHECK (btc_stress_support_component     IS NULL OR (btc_stress_support_component     >= 0  AND btc_stress_support_component     <= 1)),
    CHECK (btc_trend_anchor_component       IS NULL OR (btc_trend_anchor_component       >= -1 AND btc_trend_anchor_component       <= 1)),
    CHECK (btc_trend_support_component      IS NULL OR (btc_trend_support_component      >= -1 AND btc_trend_support_component      <= 1)),
    CHECK (btc_volatility_anchor_component  IS NULL OR (btc_volatility_anchor_component  >= 0  AND btc_volatility_anchor_component  <= 1)),
    CHECK (btc_volatility_support_component IS NULL OR (btc_volatility_support_component >= 0  AND btc_volatility_support_component <= 1)),
    CHECK (btc_liquidity_anchor_component   IS NULL OR (btc_liquidity_anchor_component   >= 0  AND btc_liquidity_anchor_component   <= 1)),
    CHECK (btc_liquidity_support_component  IS NULL OR (btc_liquidity_support_component  >= 0  AND btc_liquidity_support_component  <= 1)),
    CHECK (btc_activity_anchor_component    IS NULL OR (btc_activity_anchor_component    >= 0  AND btc_activity_anchor_component    <= 1)),
    CHECK (btc_activity_support_component   IS NULL OR (btc_activity_support_component   >= 0  AND btc_activity_support_component   <= 1)),

    -- Shadow tags — valores cerrados
    CHECK (btc_stress_state     IN ('low','medium','high')             OR btc_stress_state     IS NULL),
    CHECK (btc_trend_state      IN ('down','neutral','up')             OR btc_trend_state      IS NULL),
    CHECK (btc_volatility_state IN ('compressed','normal','expanding') OR btc_volatility_state IS NULL),
    CHECK (btc_liquidity_state  IN ('thin','normal','thick')           OR btc_liquidity_state  IS NULL),
    CHECK (btc_activity_state   IN ('low','normal','high')             OR btc_activity_state   IS NULL),

    -- Health — JSONB structure
    CHECK (missing_features_list IS NULL OR jsonb_typeof(missing_features_list) = 'array'),
    CHECK (jsonb_typeof(degradation_reasons) = 'array'),

    -- Health — counts
    CHECK (missing_feature_count >= 0),
    CHECK (late_event_count   IS NULL OR late_event_count   >= 0),
    CHECK (max_event_delay_ms IS NULL OR max_event_delay_ms >= 0),

    -- Health — enums
    CHECK (snapshot_health_status IN ('fresh','degraded','stale')),
    CHECK (freshness_status       IN ('fresh','delayed','stale')),
    CHECK (completeness_status    IN ('complete','partial','insufficient')),
    CHECK (validity_status        IN ('valid','suspicious','invalid')),
    CHECK (trade_feed_status      IN ('live','delayed','frozen','missing')),
    CHECK (book_feed_status       IN ('live','delayed','frozen','missing')),
    CHECK (worker_status          IN ('warming_up','live','catching_up','degraded','stalled')),

    -- Governance — enums
    CHECK (regime_mode IN ('shadow','advisory','active_gate')),
    CHECK (regime_validation_status IN (
        'experimental','observed','validated_partial','validated_operational'
    ))
);

-- ===========================================================================
-- HYPERTABLE (I1, I16)
-- migrate_data=TRUE: permite convertir tabla con datos preexistentes
-- if_not_exists=TRUE: no falla si ya es hypertable
-- Wrapped en DO block para logging explícito de éxito/fallo
-- ===========================================================================

DO $$
BEGIN
    PERFORM create_hypertable(
        'btc_regime_snapshots',
        'snapshot_ts',
        chunk_time_interval => INTERVAL '1 day',
        if_not_exists       => TRUE,
        migrate_data        => TRUE
    );
    RAISE NOTICE 'create_hypertable: btc_regime_snapshots OK';
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'create_hypertable failed: % — %', SQLSTATE, SQLERRM;
END;
$$;

-- ===========================================================================
-- ÍNDICES
-- ===========================================================================

-- Get-latest snapshot — patrón más frecuente del worker y enrichment PIT (I17)
CREATE INDEX IF NOT EXISTS btc_regime_snapshots_snapshot_ts_desc_idx
    ON btc_regime_snapshots (snapshot_ts DESC);

-- PIT joins con candidate_events por feature_ts
CREATE INDEX IF NOT EXISTS btc_regime_snapshots_feature_ts_idx
    ON btc_regime_snapshots (feature_ts DESC);

-- Consultas por source_ts
CREATE INDEX IF NOT EXISTS btc_regime_snapshots_source_ts_idx
    ON btc_regime_snapshots (source_ts DESC);

-- Observabilidad / Grafana — filtrar por health + tiempo
CREATE INDEX IF NOT EXISTS btc_regime_snapshots_health_ts_idx
    ON btc_regime_snapshots (snapshot_health_status, snapshot_ts DESC);

-- Governance queries — filtrar por mode + tiempo
CREATE INDEX IF NOT EXISTS btc_regime_snapshots_mode_ts_idx
    ON btc_regime_snapshots (regime_mode, snapshot_ts DESC);

-- PIT joins conservadores — snapshot_joinable = TRUE
CREATE INDEX IF NOT EXISTS btc_regime_snapshots_joinable_feature_ts_idx
    ON btc_regime_snapshots (snapshot_joinable, feature_ts DESC);

-- ===========================================================================
-- COMENTARIO DE TABLA
-- ===========================================================================

COMMENT ON TABLE btc_regime_snapshots IS
    'BTC Regime Foundation snapshots — Capa 6 KTS TradeSeeker. '
    'Schema v2. SoT: docs/BTC_Regime_SoT_v2.md. '
    'Mode: shadow (F2). Zero operational impact en F2. '
    'Una fila = un snapshot completo del contexto BTC (~1-2s cadence). '
    'Outputs principales: btc_*_score (scores continuos, SoT). '
    'Shadow tags y componentes son secundarios. '
    'regime_mode=shadow: uso CLASS_3 (operativo) prohibido.';

-- ===========================================================================
-- COMENTARIOS DE COLUMNAS — governance y campos críticos (I25)
-- ===========================================================================

COMMENT ON COLUMN btc_regime_snapshots.regime_mode IS
    'Modo operativo del sistema BTC Regime. '
    'shadow: corre, calcula, registra, expone — NO bloquea ni modifica decisiones. '
    'advisory: flags informativos para revisión humana — NO activo en F2. '
    'active_gate: autoridad real sobre el sistema — NO activo en F2. '
    'Transición directa shadow → active_gate está prohibida (DC-MODE-03). '
    'Todo cambio de modo requiere rastro de auditoría completo (DC-MODE-05).';

COMMENT ON COLUMN btc_regime_snapshots.regime_validation_status IS
    'Estado de validación empírica del módulo BTC Regime. '
    'experimental: diseño cerrado, sin evidencia de calibración (estado actual F2). '
    'observed: historia acumulada, patrones visibles. '
    'validated_partial: evidencia parcial de utilidad incremental. '
    'validated_operational: validado para uso operativo (habilita gate/modifier).';

COMMENT ON COLUMN btc_regime_snapshots.allowed_use_class IS
    'Clases de consumidores permitidos según el modo actual. '
    'CLASS_1 (observacional): logs, Grafana, debug — siempre permitido. '
    'CLASS_2 (analítico): dataset_capture, research, queries PIT — permitido con restricciones. '
    'CLASS_3 (operativo): fast_loop, entry_gate, etc. — PROHIBIDO en shadow mode.';

COMMENT ON COLUMN btc_regime_snapshots.prohibited_use_class IS
    'Clases de consumidores prohibidas en el modo actual. '
    'En shadow mode CLASS_3 (operativo) está prohibido. '
    'Prohibido explícito: gating, filtering, threshold tuning, scoring adjustment, '
    'size adjustment, alert suppression, label manipulation.';

COMMENT ON COLUMN btc_regime_snapshots.snapshot_joinable IS
    'TRUE solo si el snapshot es apto para enriquecer candidate_events sin comprometer PIT. '
    'Condiciones requeridas: health != stale, feature_ts <= t_confirm, '
    'no invalid_value_detected, no faltan features críticas de dimensiones persistidas. '
    'Un snapshot puede ser observable pero no joinable (Sección 11.7).';

COMMENT ON COLUMN btc_regime_snapshots.worker_status IS
    'Estado operativo del btc_regime_worker en el momento del snapshot. '
    'warming_up: buffers incompletos post-arranque — health no puede fingir fresh. '
    'live: saludable, emitiendo dentro de contrato. '
    'catching_up: recuperándose de backlog — joinable solo si health lo permite. '
    'degraded: vivo con degradación persistente de inputs o features. '
    'stalled: detenido — no puede presentarse como live.';

COMMENT ON COLUMN btc_regime_snapshots.created_at IS
    'Timestamp de insert en DB. Puede diferir de snapshot_ts si hay backlog de persistencia. '
    'Útil para auditoría de latencia entre cómputo y persistencia real.';
