-- ============================================================
-- KTS TradeSeeker — Migration: Restore BTC Enrichment Columns
-- 2026-05-08 — post DB loss recovery
-- Aplica columnas BTC PIT que dataset_capture.py v1.1 espera
-- pero que dataset_schema.sql v1.1 no incluia.
-- ============================================================

-- Metadata BTC (aplicado 2026-05-07)
ALTER TABLE event_features_pti
    ADD COLUMN IF NOT EXISTS btc_regime_snapshot_ts   TIMESTAMPTZ      NULL,
    ADD COLUMN IF NOT EXISTS btc_regime_source_lag_ms DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS btc_regime_health_status TEXT             NULL,
    ADD COLUMN IF NOT EXISTS btc_regime_worker_status TEXT             NULL,
    ADD COLUMN IF NOT EXISTS btc_regime_feature_ts    TIMESTAMPTZ      NULL,
    ADD COLUMN IF NOT EXISTS btc_regime_join_lag_ms   DOUBLE PRECISION NULL;

-- Scores y states BTC (aplicado 2026-05-08)
ALTER TABLE event_features_pti
    ADD COLUMN IF NOT EXISTS btc_stress_score_at_confirm     DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS btc_trend_score_at_confirm      DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS btc_volatility_score_at_confirm DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS btc_liquidity_score_at_confirm  DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS btc_activity_score_at_confirm   DOUBLE PRECISION NULL,
    ADD COLUMN IF NOT EXISTS btc_stress_state_at_confirm     TEXT             NULL,
    ADD COLUMN IF NOT EXISTS btc_trend_state_at_confirm      TEXT             NULL;

-- Indices PIT para research
CREATE INDEX IF NOT EXISTS idx_efpti_btc_regime_feature_ts
    ON event_features_pti (btc_regime_feature_ts DESC);

CREATE INDEX IF NOT EXISTS idx_efpti_btc_regime_health
    ON event_features_pti (btc_regime_health_status, feature_timestamp DESC);
