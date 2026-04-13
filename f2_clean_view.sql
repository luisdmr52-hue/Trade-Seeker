-- ============================================================
-- KTS TradeSeeker — F2 Clean Dataset View
-- research_signal_dataset_clean_v1
-- v1.1 — 2026-04-12
-- ============================================================
-- Vista final de cierre de F2. Entrada limpia para F3.
--
-- Decisiones de diseño cerradas (F2.4):
--   - Excluye: bad_sigma, bad_stale_data
--   - Incluye: ok, degraded_low_density, degraded_high_lag,
--              degraded_unstable_window
--   - WHERE usa whitelist positiva — no NOT IN con NULLs
--   - n_trades_bucket con rangos fijos (no NTILE dinámico)
--   - Markouts fijos a horizon_min = 30 (INNER JOIN)
--   - 1 fila por event_id garantizada por JOIN con PK(event_id, horizon_min)
--
-- IMPORTANTE — exclusión silenciosa por INNER JOIN con markouts:
--   Eventos etiquetados sin fila en event_markouts para horizon_min=30
--   quedan excluidos de esta vista sin error visible. Al momento de
--   creación (2026-04-12): labeled=293, markout_events=275 → ~18 excluidos.
--   Si se requiere incluirlos, reemplazar por LEFT JOIN con NULLs explícitos.
--
-- Política operativa (F2.4):
--   ok_only + other_degraded  → usar directamente
--   degraded_low_density      → usar con needs_downweight=true
--   n_trades sweet spot       → Q2_6_9 y Q3_10_19
--
-- Columnas derivadas vinculadas:
--   needs_downweight     derivada de dataset_bucket (no de quality_reason_code)
--   is_n_trades_sweet_spot derivada de n_trades_bucket (no de n_trades directo)
--   → ambas se actualizan automáticamente si cambia la lógica de su fuente
-- ============================================================

-- FIX-05/12: usar CTE para que columnas derivadas (needs_downweight,
-- is_n_trades_sweet_spot) se deriven de los alias ya calculados
-- (dataset_bucket, n_trades_bucket) en vez de recalcular la lógica
-- desde las columnas fuente. Evita divergencia silenciosa si se modifica
-- uno de los CASE sin actualizar el otro.
CREATE OR REPLACE VIEW research_signal_dataset_clean_v1 AS
WITH base AS (
    SELECT
        -- ── Identidad / contexto ──────────────────────────────────────────
        ce.event_id,
        ce.symbol,
        ce.t_confirm,                       -- anchor temporal canónico (F2.3/F3)

        -- ── Contexto operativo del evento ─────────────────────────────────
        -- FIX-08: entry_gate_decision y rejection_reason incluidos para
        -- permitir segmentación near-miss vs alerta en F3.
        ce.entry_gate_decision,
        ce.rejection_reason,

        -- ── Features de contexto temporal ─────────────────────────────────
        f.feature_timestamp,
        f.utc_hour,
        f.market_regime_timegate,
        f.operator_window_state,

        -- ── Features principales PIT ──────────────────────────────────────
        f.stage_pct,
        f.delta_pct,
        f.t_build_ms,
        f.t_confirm_ms,
        f.rel_vol,
        f.confirm_ratio,
        -- FIX-18: cast explícito a integer para blindar comparaciones
        -- en n_trades_bucket/is_n_trades_sweet_spot contra cambios de tipo
        -- upstream en event_features_pti.n_trades.
        f.n_trades::integer                 AS n_trades,
        f.feature_latency_ms,

        -- ── Labeling ──────────────────────────────────────────────────────
        l.label_quality_30m,
        l.quality_reason_code,
        l.tb_label_30m_vol_scaled,
        l.good_event_fixed_30m,
        l.sigma_event_30m,
        l.anchor_lag_ms,
        l.anchor_window_coverage_ms,

        -- ── Markouts fijos a 30m ──────────────────────────────────────────
        m.ret_h             AS ret_30m,
        m.mfe_h             AS mfe_30m,
        m.mae_h             AS mae_30m,
        m.forward_lag_ratio AS forward_lag_ratio_30m,
        m.point_density     AS point_density_30m,
        -- FIX-16: points_used_30m = 0 indica markout sin puntos usados
        -- → ret_30m/mfe_30m/mae_30m pueden ser NULL o inválidos en esos casos.
        -- La columna queda visible para filtrado en F3.
        m.points_used       AS points_used_30m,

        -- ── Normalización F2.4 — primer nivel ─────────────────────────────

        -- dataset_bucket: bucket normalizado por política F2.4.
        -- FIX-12: única fuente de verdad para el bucket.
        -- needs_downweight e is_n_trades_sweet_spot se derivan de aquí (outer).
        -- 'other_unknown' no puede aparecer porque el WHERE es whitelist positiva.
        -- Si aparece, indica que el WHERE fue modificado sin actualizar este CASE.
        CASE
            WHEN l.label_quality_30m = 'ok'
                THEN 'ok_only'
            WHEN l.quality_reason_code IN ('degraded_high_lag', 'degraded_unstable_window')
                THEN 'other_degraded'
            WHEN l.quality_reason_code = 'degraded_low_density'
                THEN 'degraded_low_density'
            ELSE 'other_unknown'     -- centinela: nunca debe aparecer con WHERE actual
        END                                 AS dataset_bucket,

        -- n_trades_bucket: rangos fijos derivados de F2.4.
        -- FIX-02: sin lower bound en Q1 — n_trades <= 5 incluye valores
        --   < 3 que no se han visto pero pueden aparecer en el futuro.
        --   Nombre Q1_le_5 refleja el upper bound, no un lower bound implícito.
        -- FIX-03/18: NULL explícito → OUT_OF_RANGE. Cast a integer ya aplicado
        --   en la columna fuente (f.n_trades::integer AS n_trades).
        CASE
            WHEN f.n_trades IS NULL         THEN 'OUT_OF_RANGE'
            WHEN f.n_trades::integer <= 5   THEN 'Q1_le_5'
            WHEN f.n_trades::integer <= 9   THEN 'Q2_6_9'
            WHEN f.n_trades::integer <= 19  THEN 'Q3_10_19'
            ELSE                                 'Q4_19_plus'
        END                                 AS n_trades_bucket

    FROM candidate_events ce
    JOIN event_features_pti f
        ON f.event_id = ce.event_id
    JOIN event_labels_core l
        ON l.event_id = ce.event_id
    -- INNER JOIN intencional: requiere fila en event_markouts para horizon_min=30.
    -- Eventos etiquetados sin markout a 30m quedan excluidos — ver nota en header.
    -- horizon_min = 30 es parte de la PK (event_id, horizon_min) →
    -- el JOIN produce exactamente 1 fila por event_id.
    JOIN event_markouts m
        ON m.event_id = ce.event_id
       AND m.horizon_min = 30

    -- FIX-01: whitelist positiva — no NOT IN con NULLs.
    -- Cuando label_quality_30m = 'ok', quality_reason_code es NULL.
    -- NOT IN ('bad_sigma', 'bad_stale_data') con NULL devuelve NULL, no TRUE,
    -- y silenciosamente excluiría los eventos ok.
    -- La whitelist positiva evalúa label_quality_30m = 'ok' primero,
    -- sin tocar quality_reason_code.
    -- FIX-04/13: ningún reason_code fuera de la whitelist puede entrar,
    -- incluidos degraded con quality_reason_code = NULL por bug del labeler.
    WHERE (
        l.label_quality_30m = 'ok'
        OR l.quality_reason_code IN (
            'degraded_low_density',
            'degraded_high_lag',
            'degraded_unstable_window'
        )
    )
)
SELECT
    -- Identidad
    event_id,
    symbol,
    t_confirm,

    -- Contexto operativo
    entry_gate_decision,
    rejection_reason,

    -- Features temporales
    feature_timestamp,
    utc_hour,
    market_regime_timegate,
    operator_window_state,

    -- Features PIT
    stage_pct,
    delta_pct,
    t_build_ms,
    t_confirm_ms,
    rel_vol,
    confirm_ratio,
    n_trades,
    feature_latency_ms,

    -- Labeling
    label_quality_30m,
    quality_reason_code,
    tb_label_30m_vol_scaled,
    good_event_fixed_30m,
    sigma_event_30m,
    anchor_lag_ms,
    anchor_window_coverage_ms,

    -- Markouts 30m
    ret_30m,
    mfe_30m,
    mae_30m,
    forward_lag_ratio_30m,
    point_density_30m,
    points_used_30m,

    -- ── Normalización F2.4 — segundo nivel (derivado del CTE) ────────────

    -- dataset_bucket propagado desde CTE (fuente única)
    dataset_bucket,

    -- is_usable_for_f3: contrato explícito — todo lo que pasa el WHERE es usable.
    -- FIX-11: constante TRUE; si se convierte en lógica condicional en el futuro,
    -- V-11 lo detectará.
    TRUE                                    AS is_usable_for_f3,

    -- FIX-12: needs_downweight derivado de dataset_bucket (no de quality_reason_code).
    -- Si dataset_bucket cambia, needs_downweight se actualiza automáticamente.
    CASE
        WHEN dataset_bucket = 'degraded_low_density' THEN TRUE
        ELSE FALSE
    END                                     AS needs_downweight,

    -- n_trades_bucket propagado desde CTE (fuente única)
    n_trades_bucket,

    -- FIX-05: is_n_trades_sweet_spot derivado de n_trades_bucket (no de n_trades).
    -- Si los rangos del bucket cambian, este flag se actualiza automáticamente.
    -- FIX-03: NULL ya manejado en el CTE → n_trades_bucket = 'OUT_OF_RANGE' → FALSE.
    CASE
        WHEN n_trades_bucket IN ('Q2_6_9', 'Q3_10_19') THEN TRUE
        ELSE FALSE
    END                                     AS is_n_trades_sweet_spot

FROM base;

-- FIX-17: COMMENT actualizado para documentar exclusión por INNER JOIN.
COMMENT ON VIEW research_signal_dataset_clean_v1 IS
    'F2 clean dataset. Entrada para F3. Excluye bad_sigma y bad_stale_data. '
    'Whitelist positiva en WHERE. 1 fila por event_id. Markouts fijos a 30m. '
    'INNER JOIN con event_markouts: eventos sin markout a 30m quedan excluidos. '
    'needs_downweight derivado de dataset_bucket. '
    'is_n_trades_sweet_spot derivado de n_trades_bucket. '
    'v1.1 2026-04-12. Política operativa: F2.4.';


-- ============================================================
-- VALIDACIONES POST-CREACIÓN
-- Ejecutar inmediatamente después de crear la vista.
-- Valores críticos marcados con "ASSERT" deben ser exactamente 0
-- para declarar la vista correctamente implementada.
-- ============================================================

-- V-01: conteo total y cobertura vs labeled universe
-- Informativo — el número crece con nuevos eventos.
-- La diferencia con n_labeled refleja bad excluidos + eventos sin markout 30m.
\echo ''
\echo 'V-01: conteo total y cobertura'
SELECT
    (SELECT COUNT(*) FROM research_signal_dataset_clean_v1)  AS n_clean,
    (SELECT COUNT(*) FROM event_labels_core)                  AS n_labeled,
    (SELECT COUNT(*) FROM event_labels_core)
        - (SELECT COUNT(*) FROM research_signal_dataset_clean_v1) AS n_excluded;

-- V-02: conteo por dataset_bucket
-- Esperado: ok_only, other_degraded, degraded_low_density PRESENTES.
-- ASSERT: other_unknown = 0 (ver V-06).
\echo ''
\echo 'V-02: conteo por dataset_bucket'
SELECT
    dataset_bucket,
    COUNT(*) AS n
FROM research_signal_dataset_clean_v1
GROUP BY 1
ORDER BY
    CASE dataset_bucket
        WHEN 'ok_only'              THEN 1
        WHEN 'other_degraded'       THEN 2
        WHEN 'degraded_low_density' THEN 3
        ELSE                             4   -- other_unknown si aparece
    END;

-- V-03: unicidad — ASSERT: total_rows = unique_events
\echo ''
\echo 'V-03: unicidad (ASSERT: total_rows = unique_events)'
SELECT
    COUNT(*)                 AS total_rows,
    COUNT(DISTINCT event_id) AS unique_events,
    COUNT(*) - COUNT(DISTINCT event_id) AS duplicates   -- ASSERT = 0
FROM research_signal_dataset_clean_v1;

-- V-04: sanity check de calidad por bucket
-- FIX-01: good_rate usa CASE explícito con NULL→0 para denominador correcto.
-- Referencia F2.4:
--   ok_only:              good_rate ~0.80, ret ~-0.003, mfe ~0.013
--   other_degraded:       good_rate ~0.76, ret ~-0.002, mfe ~0.018
--   degraded_low_density: good_rate ~0.54, ret ~-0.004, mfe ~0.009
\echo ''
\echo 'V-04: sanity check de calidad por bucket'
SELECT
    dataset_bucket,
    COUNT(*) AS n,
    -- FIX-01: CASE explícito — NULL cuenta como 0 en el denominador
    ROUND(AVG(CASE WHEN good_event_fixed_30m IS TRUE THEN 1.0 ELSE 0.0 END)::numeric, 4) AS good_rate,
    ROUND(AVG(ret_30m)::numeric,  4) AS avg_ret_30m,
    ROUND(AVG(mfe_30m)::numeric,  4) AS avg_mfe_30m,
    -- FIX-14: validar needs_downweight y is_n_trades_sweet_spot por bucket
    SUM(CASE WHEN needs_downweight      = TRUE  THEN 1 ELSE 0 END) AS n_needs_downweight,
    SUM(CASE WHEN is_n_trades_sweet_spot = TRUE THEN 1 ELSE 0 END) AS n_sweet_spot
FROM research_signal_dataset_clean_v1
GROUP BY 1
ORDER BY
    CASE dataset_bucket
        WHEN 'ok_only'              THEN 1
        WHEN 'other_degraded'       THEN 2
        WHEN 'degraded_low_density' THEN 3
        ELSE                             4
    END;

-- V-05: distribución por n_trades_bucket en orden lógico
-- FIX-07/19: ORDER BY CASE explícito — no orden alfabético.
-- OUT_OF_RANGE solo si hay n_trades IS NULL.
\echo ''
\echo 'V-05: distribución por n_trades_bucket (orden lógico)'
SELECT
    n_trades_bucket,
    COUNT(*) AS n,
    -- FIX-01: CASE explícito para good_rate
    ROUND(AVG(CASE WHEN good_event_fixed_30m IS TRUE THEN 1.0 ELSE 0.0 END)::numeric, 4) AS good_rate,
    ROUND(AVG(ret_30m)::numeric, 4) AS avg_ret_30m
FROM research_signal_dataset_clean_v1
GROUP BY 1
ORDER BY
    CASE n_trades_bucket
        WHEN 'Q1_le_5'    THEN 1
        WHEN 'Q2_6_9'     THEN 2
        WHEN 'Q3_10_19'   THEN 3
        WHEN 'Q4_19_plus' THEN 4
        ELSE                   5   -- OUT_OF_RANGE al final
    END;

-- V-06: ASSERT other_unknown = 0
-- Si > 0: el WHERE fue modificado sin actualizar el CASE de dataset_bucket,
-- o el labeler produjo un reason_code nuevo no contemplado.
\echo ''
\echo 'V-06: ASSERT other_unknown = 0'
SELECT COUNT(*) AS other_unknown_count   -- ASSERT = 0
FROM research_signal_dataset_clean_v1
WHERE dataset_bucket = 'other_unknown';

-- V-07: ASSERT ok_count > 0
-- FIX-03: número esperado no hardcodeado — crece con nuevos datos.
-- Protege contra regresión del FIX-01 (NOT IN con NULL excluía eventos ok).
\echo ''
\echo 'V-07: ASSERT ok_count > 0 (protección regresión FIX-01)'
SELECT COUNT(*) AS ok_count   -- ASSERT > 0
FROM research_signal_dataset_clean_v1
WHERE label_quality_30m = 'ok';

-- V-08: n_trades NULL → OUT_OF_RANGE
-- Si n_trades_null_count > 0, esos eventos están en OUT_OF_RANGE con sweet_spot=FALSE.
\echo ''
\echo 'V-08: n_trades NULL check'
SELECT COUNT(*) AS n_trades_null_count
FROM research_signal_dataset_clean_v1
WHERE n_trades IS NULL;

-- V-09: ASSERT needs_downweight coherente con dataset_bucket
-- FIX-12: needs_downweight deriva de dataset_bucket — verificar coherencia.
-- ASSERT: ambos counts = 0.
\echo ''
\echo 'V-09: ASSERT coherencia needs_downweight vs dataset_bucket (ASSERT ambos = 0)'
SELECT
    SUM(CASE
        WHEN dataset_bucket = 'degraded_low_density' AND needs_downweight = FALSE THEN 1
        ELSE 0
    END) AS low_density_without_downweight,    -- ASSERT = 0
    SUM(CASE
        WHEN dataset_bucket != 'degraded_low_density' AND needs_downweight = TRUE THEN 1
        ELSE 0
    END) AS non_low_density_with_downweight    -- ASSERT = 0
FROM research_signal_dataset_clean_v1;

-- V-10: ASSERT is_n_trades_sweet_spot coherente con n_trades_bucket
-- FIX-05: is_n_trades_sweet_spot deriva de n_trades_bucket — verificar coherencia.
-- ASSERT: ambos counts = 0.
\echo ''
\echo 'V-10: ASSERT coherencia is_n_trades_sweet_spot vs n_trades_bucket (ASSERT ambos = 0)'
SELECT
    SUM(CASE
        WHEN n_trades_bucket IN ('Q2_6_9', 'Q3_10_19') AND is_n_trades_sweet_spot = FALSE THEN 1
        ELSE 0
    END) AS sweet_spot_bucket_but_false,       -- ASSERT = 0
    SUM(CASE
        WHEN n_trades_bucket NOT IN ('Q2_6_9', 'Q3_10_19') AND is_n_trades_sweet_spot = TRUE THEN 1
        ELSE 0
    END) AS non_sweet_spot_bucket_but_true     -- ASSERT = 0
FROM research_signal_dataset_clean_v1;

-- V-11: ASSERT is_usable_for_f3 siempre TRUE
-- FIX-11: protege contra regresiones si alguien convierte la constante en lógica.
-- ASSERT = 0.
\echo ''
\echo 'V-11: ASSERT is_usable_for_f3 siempre TRUE (ASSERT = 0)'
SELECT COUNT(*) AS not_usable_count    -- ASSERT = 0
FROM research_signal_dataset_clean_v1
WHERE is_usable_for_f3 IS DISTINCT FROM TRUE;

-- V-12: NULLs en columnas de outcome (ret_30m, mfe_30m, mae_30m)
-- FIX-15: detectar eventos con markout presente pero valores NULL en outcome.
-- Informativo — no es error de la vista, pero afecta análisis en F3.
\echo ''
\echo 'V-12: NULLs en columnas de outcome (informativo)'
SELECT
    COUNT(*) FILTER (WHERE ret_30m IS NULL)  AS null_ret_30m,
    COUNT(*) FILTER (WHERE mfe_30m IS NULL)  AS null_mfe_30m,
    COUNT(*) FILTER (WHERE mae_30m IS NULL)  AS null_mae_30m,
    COUNT(*) FILTER (WHERE points_used_30m = 0) AS zero_points_used  -- FIX-16
FROM research_signal_dataset_clean_v1;

-- V-13: ASSERT dataset_bucket vs quality_reason_code coherencia cruzada
-- FIX-20: validar que las columnas derivadas son coherentes con la fuente.
-- ASSERT: todos los counts = 0.
\echo ''
\echo 'V-13: ASSERT coherencia cruzada dataset_bucket vs quality_reason_code (ASSERT todos = 0)'
SELECT
    SUM(CASE
        WHEN dataset_bucket = 'ok_only'
         AND label_quality_30m != 'ok' THEN 1
        ELSE 0
    END) AS ok_bucket_not_ok_label,           -- ASSERT = 0
    SUM(CASE
        WHEN dataset_bucket = 'degraded_low_density'
         AND quality_reason_code != 'degraded_low_density' THEN 1
        ELSE 0
    END) AS low_density_bucket_wrong_code,    -- ASSERT = 0
    SUM(CASE
        WHEN dataset_bucket = 'other_degraded'
         AND quality_reason_code NOT IN ('degraded_high_lag', 'degraded_unstable_window') THEN 1
        ELSE 0
    END) AS other_degraded_bucket_wrong_code  -- ASSERT = 0
FROM research_signal_dataset_clean_v1;

\echo ''
\echo '============================================================'
\echo 'RESUMEN DE ASSERTS CRÍTICOS:'
\echo '  V-03 duplicates          = 0'
\echo '  V-06 other_unknown       = 0'
\echo '  V-07 ok_count            > 0'
\echo '  V-09 needs_downweight    = 0, 0'
\echo '  V-10 sweet_spot          = 0, 0'
\echo '  V-11 not_usable          = 0'
\echo '  V-13 coherencia cruzada  = 0, 0, 0'
\echo 'Si cualquier ASSERT falla, la vista NO está correctamente implementada.'
\echo '============================================================'
