-- ============================================================
-- KTS TradeSeeker — Dataset Foundation Schema
-- Fase 1: Event-Based Capture
-- v1.1 — 2026-04-10
-- ============================================================
-- Tablas: candidate_events, event_features_pti
-- Independientes del outcome tracker (signals / signal_outcomes / outcome_jobs).
--
-- ⚠️  ADVERTENCIA DE IDEMPOTENCIA:
-- Este script usa CREATE TABLE IF NOT EXISTS y CREATE INDEX IF NOT EXISTS.
-- Si las tablas ya existen, el script NO aplica cambios de columnas ni
-- modificaciones de schema. Para alteraciones futuras usar ALTER TABLE
-- explícito — NUNCA re-ejecutar este script esperando que actualice.
-- ============================================================

-- ── candidate_events ─────────────────────────────────────────
-- Una fila por candidato que alcanza el estado FSM "confirming".
-- Incluye candidatos rechazados por entry_gate (DC-DATASET-02).
-- t_confirm es el anchor PIT de todo el dataset.
-- ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS candidate_events (
    -- Identidad
    -- Formato garantizado: "ev_" + 32 hex lowercase (sha256 truncado)
    event_id             TEXT        NOT NULL
                         CHECK (event_id ~ '^ev_[a-f0-9]{32}$'),
    symbol               TEXT        NOT NULL,

    -- Anchor temporal (PIT correctness)
    t_event              TIMESTAMPTZ,          -- inicio del ciclo donde se detectó
    t_trigger            TIMESTAMPTZ,          -- momento en que delta >= pump_pct (NULL si FSM idle al trigger)
    t_confirm            TIMESTAMPTZ NOT NULL, -- momento de transición a confirming (anchor de todo el dataset)

    -- Estado FSM
    -- phase_at_event es siempre "confirming" en F1 — constraint refleja eso
    phase_at_event       TEXT
                         CHECK (phase_at_event IS NULL OR phase_at_event = 'confirming'),
    fsm_state_path       TEXT,                 -- trayectoria: "idle→building→triggered→confirming"

    -- Tipo y hint semántico
    type_hint            TEXT,                 -- "pump_fast_candidate" en F1

    -- Decisión operacional (DC-DATASET-02)
    entry_gate_decision  TEXT
                         CHECK (entry_gate_decision IN ('passed', 'rejected', 'unknown')),
    rejection_reason     TEXT,                 -- "late_entry" | "no_trigger_price" | NULL si passed

    -- Versionado
    -- config_hash vive SOLO aquí, no en event_features_pti.
    -- Motivo: eliminar riesgo de divergencia silenciosa entre tablas (P2).
    -- El lineage completo de features vive en event_features_pti.
    detector_version     TEXT        NOT NULL, -- = bot_version (DC-DATASET-01)
    config_hash          TEXT        NOT NULL, -- sha256[:16] del bloque rules.fast_ts activo

    -- Auditoría
    source_event_time    TIMESTAMPTZ,          -- timestamp fuente si difiere de t_confirm
    recorded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (event_id)
);

-- Cubre queries por símbolo con ORDER BY t_confirm
CREATE INDEX IF NOT EXISTS idx_candidate_events_symbol_tconfirm
    ON candidate_events (symbol, t_confirm DESC);

-- Cubre ORDER BY t_confirm DESC sobre todo el dataset sin filtro de símbolo
CREATE INDEX IF NOT EXISTS idx_candidate_events_tconfirm
    ON candidate_events (t_confirm DESC);

-- Cubre queries de análisis por decisión del gate
CREATE INDEX IF NOT EXISTS idx_candidate_events_entry_gate
    ON candidate_events (entry_gate_decision, t_confirm DESC);

-- ── event_features_pti ───────────────────────────────────────
-- Una fila por candidato — features point-in-time en t_confirm.
-- Sin lookahead. Todo lo que aquí figura era observable en t_confirm.
-- config_hash NO está aquí — vive solo en candidate_events (ver nota arriba).
-- ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS event_features_pti (
    -- FK al evento padre
    -- ON DELETE RESTRICT: protege contra borrado accidental de candidate_events
    -- que dejaría features huérfanas. Para borrar un evento, primero borrar features.
    event_id                TEXT        NOT NULL
                            REFERENCES candidate_events(event_id)
                            ON DELETE RESTRICT,

    -- Anchor temporal de las features — normalmente igual a t_confirm
    feature_timestamp       TIMESTAMPTZ NOT NULL,

    -- TimingBlock — disponibles en memoria en t_confirm desde MoveState.timing_extras()
    stage_pct               DOUBLE PRECISION,     -- % avance desde trigger a confirm (puede ser negativo — es pass válido)
    delta_pct               DOUBLE PRECISION,     -- delta que abrió la ventana aggTrade
    t_build_ms              DOUBLE PRECISION,     -- ms desde move_start hasta trigger (0 si FSM idle al trigger)
    t_confirm_ms            DOUBLE PRECISION,     -- ms desde trigger hasta confirming

    -- FSM extras — de MoveState.timing_extras()
    move_start_price        DOUBLE PRECISION,
    noise_at_start          DOUBLE PRECISION,
    peak_since_start        DOUBLE PRECISION,
    cusum_at_trigger        DOUBLE PRECISION,
    cusum_shadow_pass       BOOLEAN,

    -- FlowBlock — de AggTradeConfirmer.debug_state() en t_confirm
    rel_vol                 DOUBLE PRECISION,     -- quote_vol_30s / vol_base (F1: vol_24h/2880)
    confirm_ratio           DOUBLE PRECISION,     -- buy_vol / total_vol en ventana 30s aggTrade
    n_trades                INTEGER,

    -- ContextBlock — de time_gate.evaluate_now() en t_confirm
    utc_hour                INTEGER
                            CHECK (utc_hour IS NULL OR (utc_hour >= 0 AND utc_hour <= 23)),
    market_regime_timegate  TEXT,                 -- "aggressive"|"normal"|"caution"|"pause"|"low_activity"|"unknown"
    operator_window_state   TEXT
                            CHECK (operator_window_state IN ('awake', 'sleep', 'unknown')),

    -- Data Quality
    -- feature_latency_ms: tiempo de ejecución del proceso de captura (time.monotonic delta).
    --   NO es la diferencia entre recorded_at y t_confirm — es el costo de la operación capture().
    --   Útil para diagnosticar performance del pipeline de captura.
    -- feature_freshness_ms: diferencia en ms entre recorded_at y t_confirm.
    --   En F1 ambos valores son ~0 porque todas las features vienen de memoria en runtime.
    --   El campo tiene valor semántico en Fase 2+ cuando features puedan leerse de Timescale con lag.
    feature_latency_ms      DOUBLE PRECISION,
    feature_freshness_ms    DOUBLE PRECISION,
    stale_feature_mask      JSONB,                -- {field: "missing_critical"|"missing_optional"}
    is_degraded             BOOLEAN     NOT NULL DEFAULT FALSE,
    missing_feature_count   INTEGER     NOT NULL DEFAULT 0
                            CHECK (missing_feature_count >= 0),

    -- Versionado y lineage de features (DC-DATASET-01)
    -- config_hash NO está aquí — vive en candidate_events para evitar divergencia silenciosa.
    detector_version         TEXT        NOT NULL,  -- = bot_version
    feature_version          TEXT        NOT NULL,  -- versión del schema/semántica ("v1.0", "v1.1", ...)
    feature_set_hash         TEXT        NOT NULL,  -- sha256[:16] del conjunto de feature keys activas
    source_runtime_component TEXT,                  -- "fast_loop.confirming_snapshot"
    transformation_path      TEXT,                  -- "confirming.snapshot.v1"

    PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS idx_efpti_feature_timestamp
    ON event_features_pti (feature_timestamp DESC);

-- Auditoría de calidad: WHERE is_degraded = TRUE ORDER BY feature_timestamp DESC
CREATE INDEX IF NOT EXISTS idx_efpti_is_degraded
    ON event_features_pti (is_degraded, feature_timestamp DESC);


-- ── Queries de inspección y auditoría ────────────────────────
-- Copiar y ejecutar según necesidad.

-- Inspección rápida (últimos 20 eventos con features):
-- SELECT
--     ce.event_id, ce.symbol, ce.t_confirm,
--     ce.entry_gate_decision, ce.rejection_reason,
--     ef.stage_pct, ef.delta_pct, ef.rel_vol, ef.confirm_ratio,
--     ef.is_degraded, ef.missing_feature_count, ef.feature_latency_ms
-- FROM candidate_events ce
-- JOIN event_features_pti ef USING (event_id)
-- ORDER BY ce.t_confirm DESC
-- LIMIT 20;

-- Auditoría de calidad (data quality metrics):
-- SELECT
--     ce.event_id, ce.symbol, ce.t_confirm,
--     ce.entry_gate_decision, ce.rejection_reason,
--     ef.feature_timestamp, ef.is_degraded,
--     ef.missing_feature_count, ef.feature_latency_ms,
--     ef.feature_freshness_ms, ef.stale_feature_mask
-- FROM candidate_events ce
-- JOIN event_features_pti ef USING (event_id)
-- ORDER BY ce.t_confirm DESC
-- LIMIT 20;

-- ⚠️  AUDITORÍA DE HUÉRFANOS — ejecutar periódicamente:
-- Detecta candidate_events sin event_features_pti correspondiente.
-- El schema no puede prevenir esto por constraint (FK es unidireccional).
-- Si esta query devuelve filas, hay un bug en el pipeline de captura.
--
-- SELECT ce.event_id, ce.symbol, ce.t_confirm, ce.recorded_at
-- FROM candidate_events ce
-- LEFT JOIN event_features_pti ef USING (event_id)
-- WHERE ef.event_id IS NULL
-- ORDER BY ce.t_confirm DESC;
