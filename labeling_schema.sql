-- ============================================================
-- KTS TradeSeeker — F2.2 Outcome Labeling Schema
-- v2.1 — 2026-04-11
-- ============================================================
-- Tablas:
--   event_labels_core    — label actual por evento + retry state
--   event_markouts       — markouts multi-horizonte
--   event_label_attempts — historial de cada intento del labeler
--   label_retry_policy   — catálogo de política de retry por reason_code
--
-- Arquitectura de retry (Opción C):
--   - event_labels_core    = estado actual del evento (snapshot)
--   - event_label_attempts = historial completo de intentos
--   - label_retry_policy   = fuente única de clasificación terminal/retryable
--   - labeling_worker lee label_retry_policy y actualiza worker_status/retry fields
--   - outcome_labeler escribe event_labels_core + event_label_attempts
--
-- Separación de responsabilidades:
--   outcome_labeler: mide, etiqueta, persiste resultado técnico y attempts
--   labeling_worker: decide retry, actualiza worker_status, gestiona política
--
-- ── PREREQUISITOS ──────────────────────────────────────────────
-- La tabla candidate_events debe existir antes de ejecutar este script.
-- event_labels_core tiene FK → candidate_events(event_id).
-- event_label_attempts tiene FK → candidate_events(event_id).
--
-- ── MODO DE EJECUCIÓN ──────────────────────────────────────────
-- fix A6/M2: ejecutar dentro de una transacción explícita:
--   psql -c "BEGIN; \i labeling_schema.sql; COMMIT;"
-- o en psql interactivo:
--   BEGIN;
--   \i labeling_schema.sql
--   COMMIT;
-- Si el script falla a mitad, hacer ROLLBACK para evitar estado parcial.
-- NOTA: CREATE INDEX CONCURRENTLY no puede ejecutarse dentro de una transacción.
-- Los índices de este script usan CREATE INDEX (no CONCURRENTLY) para ser
-- compatibles con el wrapper transaccional.
--
-- ── IDEMPOTENCIA ───────────────────────────────────────────────
-- CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.
-- INSERT ... ON CONFLICT DO NOTHING para datos iniciales.
-- ALTER TABLE ... ADD COLUMN IF NOT EXISTS para migraciones.
-- ADVERTENCIA: ADD COLUMN IF NOT EXISTS silencia mismatch de tipo.
--   Si una columna existe con tipo incorrecto, verificar manualmente:
--   SELECT column_name, data_type FROM information_schema.columns
--   WHERE table_name = 'event_labels_core';
--
-- ── ORDEN DE BORRADO (FK constraints) ──────────────────────────
-- fix A1: para borrar un candidate_event completo, borrar en orden:
--   1. event_markouts      (ON DELETE CASCADE desde event_labels_core)
--   2. event_labels_core   (ON DELETE RESTRICT desde candidate_events)
--   3. event_label_attempts (ON DELETE RESTRICT desde candidate_events)
--   4. candidate_events
-- Alternativa: DELETE FROM event_labels_core WHERE event_id=X
--   → event_markouts se borra automáticamente (CASCADE)
--   → luego DELETE FROM event_label_attempts WHERE event_id=X
--   → luego DELETE FROM candidate_events WHERE event_id=X
--
-- ── VERSIÓN DE POSTGRES ────────────────────────────────────────
-- Mínimo: Postgres 12 (para ADD COLUMN ... DEFAULT sin rewrite de tabla).
-- Postgres 11 o menor: ADD COLUMN NOT NULL DEFAULT puede bloquear tablas grandes.
-- ============================================================

-- ── event_labels_core ────────────────────────────────────────
-- Una fila por evento — estado actual del labeling.
-- El labeler escribe label_quality_30m y campos técnicos.
-- El worker actualiza worker_status, retry_count, next_retry_at, is_terminal, is_retryable.
-- ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS event_labels_core (

    -- Identidad
    -- ON DELETE RESTRICT: no se puede borrar candidate_events si tiene label.
    -- Ver sección "ORDEN DE BORRADO" arriba.
    event_id                  TEXT        NOT NULL
                              REFERENCES candidate_events(event_id)
                              ON DELETE RESTRICT,

    -- ── LABEL TÉCNICO (escrito por outcome_labeler) ──────────────────────────

    -- Label canónico — triple barrier vol-scaled.
    -- NULL si label_quality='bad'.
    tb_label_30m_vol_scaled   INTEGER
                              CHECK (tb_label_30m_vol_scaled IS NULL
                                     OR tb_label_30m_vol_scaled IN (-1, 0, 1)),

    -- Label auxiliar — interpretabilidad / sanity check.
    good_event_fixed_30m      BOOLEAN,

    -- Quality del label.
    --   ok       → todos los componentes ok
    --   degraded → ninguno bad, al menos uno degraded
    --   bad      → algún componente bad o excepción en el labeler
    label_quality_30m         TEXT        NOT NULL
                              CHECK (label_quality_30m IN ('ok', 'degraded', 'bad')),
    quality_reason_code       TEXT,

    -- Triple barrier metadata.
    first_touch_30m           TEXT
                              CHECK (first_touch_30m IS NULL
                                     OR first_touch_30m IN ('upper', 'lower', 'vertical', 'none')),
    time_to_barrier_ms        BIGINT
                              CHECK (time_to_barrier_ms IS NULL
                                     OR time_to_barrier_ms >= 0),

    -- Volatilidad del evento.
    sigma_event_30m           DOUBLE PRECISION
                              CHECK (sigma_event_30m IS NULL
                                     OR sigma_event_30m >= 0),

    -- Precio anchor y metadata.
    p_anchor                  DOUBLE PRECISION
                              CHECK (p_anchor IS NULL OR p_anchor > 0),
    anchor_points             INTEGER
                              CHECK (anchor_points IS NULL OR anchor_points >= 0),
    anchor_lag_ms             DOUBLE PRECISION
                              CHECK (anchor_lag_ms IS NULL OR anchor_lag_ms >= 0),
    anchor_window_coverage_ms DOUBLE PRECISION
                              CHECK (anchor_window_coverage_ms IS NULL
                                     OR anchor_window_coverage_ms >= 0),
    anchor_spread_bps         DOUBLE PRECISION,
    anchor_spread_z           DOUBLE PRECISION,
    quote_skew_ms             DOUBLE PRECISION
                              CHECK (quote_skew_ms IS NULL OR quote_skew_ms >= 0),

    -- Versionado del labeler.
    -- labeled_at: cuando se ejecutó el label técnico — NO se actualiza por el worker.
    -- Para el timestamp del último intento del worker, ver last_attempt_at.
    labeler_version           TEXT        NOT NULL,
    labeled_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- ── RETRY STATE (escrito por labeling_worker) ────────────────────────────
    --
    -- worker_status: estado operativo del evento desde la perspectiva del worker.
    --   pending       → nunca procesado (valor por defecto al insertar)
    --   done          → label ok/degraded — terminal, no se reprocesa
    --   retry_wait    → bad retryable — esperando cooldown antes de reintentar
    --   terminal      → bad terminal o retries agotados — no se reprocesa
    --   failed_internal → error interno del worker al actualizar estado
    --
    -- Regla: outcome_labeler NO toca estos campos.
    --        labeling_worker los actualiza DESPUÉS de cada label() call.
    worker_status             TEXT        NOT NULL DEFAULT 'pending'
                              CHECK (worker_status IN (
                                  'pending', 'done', 'retry_wait',
                                  'terminal', 'failed_internal'
                              )),
    retry_count               INTEGER     NOT NULL DEFAULT 0
                              CHECK (retry_count >= 0),
    last_attempt_at           TIMESTAMPTZ,
    next_retry_at             TIMESTAMPTZ,
    is_terminal               BOOLEAN     NOT NULL DEFAULT FALSE,
    is_retryable              BOOLEAN     NOT NULL DEFAULT FALSE,
    -- terminal_reason_code: reason_code que causó el estado terminal.
    -- 'retry_exhausted:<original_reason>' si se agotaron los reintentos.
    -- fix M9: máximo 200 chars — consistente con el truncado del worker.
    terminal_reason_code      TEXT
                              CHECK (terminal_reason_code IS NULL
                                     OR length(terminal_reason_code) <= 200),

    PRIMARY KEY (event_id)
);

-- Calidad de labels
CREATE INDEX IF NOT EXISTS idx_elc_quality_labeled_at
    ON event_labels_core (label_quality_30m, labeled_at DESC);

-- Queries temporales
CREATE INDEX IF NOT EXISTS idx_elc_labeled_at
    ON event_labels_core (labeled_at DESC);

-- Label canónico
CREATE INDEX IF NOT EXISTS idx_elc_tb_label
    ON event_labels_core (tb_label_30m_vol_scaled, labeled_at DESC);

-- fix A3: índice en quality_reason_code para el JOIN con label_retry_policy
CREATE INDEX IF NOT EXISTS idx_elc_quality_reason_code
    ON event_labels_core (quality_reason_code)
    WHERE quality_reason_code IS NOT NULL;

-- Worker query: eventos en retry_wait con cooldown vencido.
-- fix A3: índice parcial cubre solo filas existentes con worker_status relevante.
-- Eventos nuevos (nunca procesados) no tienen fila en esta tabla —
-- el planner accede a ellos via candidate_events con filtro elc.event_id IS NULL.
-- fix B3: definido UNA sola vez aquí — eliminada la definición duplicada del bloque ALTER.
CREATE INDEX IF NOT EXISTS idx_elc_worker_status_retry
    ON event_labels_core (worker_status, next_retry_at ASC)
    WHERE worker_status IN ('retry_wait', 'pending');


-- ── event_markouts ───────────────────────────────────────────
-- Una fila por (evento, horizonte).
-- FK → event_labels_core con ON DELETE CASCADE:
--   borrar event_labels_core → markouts se borran automáticamente.
-- ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS event_markouts (

    event_id                  TEXT        NOT NULL
                              REFERENCES event_labels_core(event_id)
                              ON DELETE CASCADE,
    horizon_min               INTEGER     NOT NULL
                              CHECK (horizon_min IN (5, 15, 30, 60, 120)),

    p_h                       DOUBLE PRECISION
                              CHECK (p_h IS NULL OR p_h > 0),
    ret_h                     DOUBLE PRECISION,
    mfe_h                     DOUBLE PRECISION,
    mae_h                     DOUBLE PRECISION,

    sampling_method           TEXT
                              CHECK (sampling_method IS NULL
                                     OR sampling_method IN
                                        ('median_forward', 'median_symmetric', 'first_after')),

    points_used               INTEGER
                              CHECK (points_used IS NULL OR points_used >= 0),
    forward_lag_ms            DOUBLE PRECISION
                              CHECK (forward_lag_ms IS NULL OR forward_lag_ms >= 0),
    forward_lag_ratio         DOUBLE PRECISION
                              CHECK (forward_lag_ratio IS NULL OR forward_lag_ratio >= 0),
    window_span_ms            DOUBLE PRECISION
                              CHECK (window_span_ms IS NULL OR window_span_ms >= 0),
    point_density             DOUBLE PRECISION
                              CHECK (point_density IS NULL OR point_density >= 0),

    spread_bps                DOUBLE PRECISION,
    spread_z                  DOUBLE PRECISION,
    mid_iqr_bps               DOUBLE PRECISION
                              CHECK (mid_iqr_bps IS NULL OR mid_iqr_bps >= 0),

    quality_flag              TEXT
                              CHECK (quality_flag IS NULL
                                     OR quality_flag IN ('ok', 'degraded', 'bad')),
    reason_code               TEXT,

    PRIMARY KEY (event_id, horizon_min)
);

CREATE INDEX IF NOT EXISTS idx_em_horizon_min
    ON event_markouts (horizon_min);

CREATE INDEX IF NOT EXISTS idx_em_event_id
    ON event_markouts (event_id);


-- ── event_label_attempts ─────────────────────────────────────
-- Historial completo de cada intento del labeler por evento.
-- Escrita por OutcomeLabeler.label() en CADA llamada, sin excepción.
-- El worker NO escribe aquí — solo lee para auditoría.
--
-- fix A7: la invariante "si existe fila en event_labels_core, existe al menos
-- una fila en event_label_attempts" NO está enforced por constraint —
-- depende del código del labeler (write_attempt usa su propia conexión).
-- Si write_attempt() falla, la invariante puede violarse. Monitorear con
-- la query de auditoría al final de este script.
--
-- finished_at=NULL indica intento interrumpido o en progreso.
-- ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS event_label_attempts (

    -- PK autoincremental — order of insertion = order of attempts globally
    attempt_id                BIGSERIAL   PRIMARY KEY,

    -- FK → candidate_events (no a event_labels_core, porque el intento puede
    -- fallar antes de que exista la fila en event_labels_core).
    -- ON DELETE RESTRICT: no borrar candidate_events mientras haya attempts.
    -- Ver sección "ORDEN DE BORRADO" en el encabezado.
    event_id                  TEXT        NOT NULL
                              REFERENCES candidate_events(event_id)
                              ON DELETE RESTRICT,

    -- Número de intento para este evento (1-based, gestionado por el labeler).
    -- Calculado como: SELECT COALESCE(MAX(attempt_n), 0) + 1
    --                 FROM event_label_attempts WHERE event_id = ?
    attempt_n                 INTEGER     NOT NULL
                              CHECK (attempt_n >= 1),

    -- Timing del intento.
    -- finished_at=NULL indica intento interrumpido o en progreso.
    started_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at               TIMESTAMPTZ,
    duration_ms               BIGINT
                              CHECK (duration_ms IS NULL OR duration_ms >= 0),

    -- Versión del labeler que ejecutó este intento.
    labeler_version           TEXT        NOT NULL,

    -- Resultado técnico del intento.
    result_quality            TEXT
                              CHECK (result_quality IS NULL
                                     OR result_quality IN ('ok', 'degraded', 'bad')),
    reason_code               TEXT,

    -- was_success: TRUE si result_quality IN ('ok', 'degraded').
    -- DEFAULT FALSE: conservador — un fallo sin resultado explícito es failure.
    was_success               BOOLEAN     NOT NULL DEFAULT FALSE,

    -- Contexto del error si fue bad/exception.
    -- error_class: nombre de la clase de excepción Python (e.g., 'OperationalError').
    -- error_message: mensaje truncado a 500 chars por el labeler.
    -- fix M5: CHECK en DB como segunda línea de defensa.
    error_class               TEXT,
    error_message             TEXT
                              CHECK (error_message IS NULL
                                     OR length(error_message) <= 500),

    -- retry_scheduled_at: reservado para uso futuro (F3).
    -- El labeling_worker actual (v2.1) NO popula este campo — queda NULL.
    -- fix A2/M6: documentado explícitamente que el worker actual no lo usa.
    retry_scheduled_at        TIMESTAMPTZ,

    UNIQUE (event_id, attempt_n)
);

-- Auditoría por evento — historial de intentos en orden
CREATE INDEX IF NOT EXISTS idx_ela_event_id_attempt
    ON event_label_attempts (event_id, attempt_n DESC);

-- Queries temporales
CREATE INDEX IF NOT EXISTS idx_ela_started_at
    ON event_label_attempts (started_at DESC);

-- Análisis de attempts por resultado
CREATE INDEX IF NOT EXISTS idx_ela_was_success
    ON event_label_attempts (was_success, started_at DESC);


-- ── label_retry_policy ───────────────────────────────────────
-- Catálogo de política de retry por reason_code.
-- Fuente única de clasificación terminal vs retryable.
-- El labeling_worker hace JOIN con esta tabla para decidir worker_status.
-- Modificable en producción sin tocar código.
-- ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS label_retry_policy (

    -- reason_code: debe coincidir exactamente con los QRC_* del outcome_labeler.
    reason_code               TEXT        PRIMARY KEY,

    -- retryable: TRUE = el worker puede reintentar este evento.
    -- fix B4: retryable y is_terminal deben ser mutuamente excluyentes.
    -- El CHECK constraint lo enforcea en DB.
    retryable                 BOOLEAN     NOT NULL,

    -- is_terminal: TRUE = no reintentar nunca.
    -- fix B4: CHECK constraint garantiza coherencia con retryable.
    is_terminal               BOOLEAN     NOT NULL,

    -- fix B4: retryable XOR is_terminal — no puede ser ambos ni ninguno.
    -- Si retryable=TRUE → is_terminal debe ser FALSE, y viceversa.
    CONSTRAINT chk_retryable_xor_terminal
        CHECK (retryable != is_terminal),

    -- max_retries: número máximo de reintentos antes de marcar como terminal.
    -- fix B5: debe ser 0 cuando retryable=FALSE (terminales no tienen retries).
    max_retries               INTEGER     NOT NULL
                              CHECK (max_retries >= 0),

    -- fix B5: coherencia entre retryable y max_retries.
    CONSTRAINT chk_max_retries_coherence
        CHECK (
            (retryable = FALSE AND max_retries = 0)
            OR
            (retryable = TRUE  AND max_retries > 0)
        ),

    -- cooldown_seconds: segundos de espera entre reintentos.
    -- fix B5: debe ser 0 cuando retryable=FALSE.
    cooldown_seconds          INTEGER     NOT NULL
                              CHECK (cooldown_seconds >= 0),

    -- fix B5: coherencia entre retryable y cooldown_seconds.
    CONSTRAINT chk_cooldown_coherence
        CHECK (
            (retryable = FALSE AND cooldown_seconds = 0)
            OR
            (retryable = TRUE  AND cooldown_seconds > 0)
        ),

    -- priority: reservado para F3 (orden de prioridad entre reason_codes).
    priority                  INTEGER     NOT NULL DEFAULT 100,

    notes                     TEXT
);

-- ── Datos iniciales — label_retry_policy ─────────────────────
-- Política inicial para F2. Calibrar thresholds en F3 con datos reales.
--
-- TERMINALES: la causa del bad es estructural — reintentar no cambiará el resultado.
--   bad_invalid_book:       book de precios inválido → estructural
--   bad_sigma:              precio sin varianza en la ventana → estructural
--   bad_time_inconsistency: inconsistencia temporal en datos → estructural
--
-- RETRYABLES: la causa del bad puede ser transitoria.
--   bad_missing_quotes:    no había datos bid/ask → metrics_ext puede tener lag
--   bad_stale_data:        datos demasiado antiguos → puede mejorar con el tiempo
--   bad_labeler_exception: excepción interna del labeler → puede ser transitoria
--
-- fix M3/L5: ON CONFLICT DO NOTHING (sintaxis Postgres) preserva customizaciones.
-- Si se necesita sobreescribir datos existentes, usar ON CONFLICT DO UPDATE.
-- Implicación: un re-run del script NO sobreescribe cambios manuales en la policy.

INSERT INTO label_retry_policy
    (reason_code, retryable, is_terminal, max_retries, cooldown_seconds, priority, notes)
VALUES
    -- TERMINALES (retryable=FALSE, is_terminal=TRUE, max_retries=0, cooldown=0)
    ('bad_invalid_book',       FALSE, TRUE,  0, 0,   10,
     'Book inválido (bid/ask). Estructural — no mejora con el tiempo.'),
    ('bad_sigma',              FALSE, TRUE,  0, 0,   10,
     'Sigma=0 o insuficiente. Precio sin varianza en la ventana. Estructural.'),
    ('bad_time_inconsistency', FALSE, TRUE,  0, 0,   10,
     'Inconsistencia temporal en datos. Estructural.'),

    -- RETRYABLES (retryable=TRUE, is_terminal=FALSE, max_retries>0, cooldown>0)
    ('bad_missing_quotes',     TRUE,  FALSE, 3, 300,  50,
     'Sin datos bid/ask en ventana anchor. Transitorio — metrics_ext puede tener lag.'),
    ('bad_stale_data',         TRUE,  FALSE, 5, 600,  50,
     'Datos demasiado antiguos. Transitorio — puede mejorar a medida que llegan datos.'),
    ('bad_labeler_exception',  TRUE,  FALSE, 3, 900,  50,
     'Excepción interna del labeler. Potencialmente transitoria (DB load, timeout, etc.).')

ON CONFLICT (reason_code) DO NOTHING;


-- ── ALTER TABLE para tablas existentes ───────────────────────
-- Ejecutar si event_labels_core ya existe sin los campos de retry.
-- Idempotentes via IF NOT EXISTS.
--
-- ADVERTENCIA (fix B2): ADD COLUMN IF NOT EXISTS silencia mismatch de tipo.
-- Verificar tipos existentes antes de ejecutar:
--   SELECT column_name, data_type, column_default
--   FROM information_schema.columns
--   WHERE table_name = 'event_labels_core'
--   ORDER BY ordinal_position;
--
-- NOTA (fix A5): Postgres 12+ soporta ADD COLUMN NOT NULL DEFAULT sin
-- rewrite de tabla. En Postgres 11 o menor, estas operaciones pueden
-- bloquear la tabla. Planificar ventana de mantenimiento si aplica.
-- ──────────────────────────────────────────────────────────────

ALTER TABLE event_labels_core
    ADD COLUMN IF NOT EXISTS worker_status TEXT NOT NULL DEFAULT 'pending';

-- El CHECK constraint de worker_status no puede agregarse con ADD COLUMN IF NOT EXISTS.
-- Para agregarlo en tablas existentes ejecutar manualmente si se desea:
-- ALTER TABLE event_labels_core
--     ADD CONSTRAINT chk_worker_status
--     CHECK (worker_status IN ('pending','done','retry_wait','terminal','failed_internal'));

ALTER TABLE event_labels_core
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE event_labels_core
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ;

ALTER TABLE event_labels_core
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;

ALTER TABLE event_labels_core
    ADD COLUMN IF NOT EXISTS is_terminal BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE event_labels_core
    ADD COLUMN IF NOT EXISTS is_retryable BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE event_labels_core
    ADD COLUMN IF NOT EXISTS terminal_reason_code TEXT;

-- fix M9: CHECK de longitud no puede agregarse con ADD COLUMN IF NOT EXISTS.
-- Para agregarlo en tablas existentes:
-- ALTER TABLE event_labels_core
--     ADD CONSTRAINT chk_terminal_reason_code_length
--     CHECK (terminal_reason_code IS NULL OR length(terminal_reason_code) <= 200);

-- fix A4: índice en quality_reason_code para el JOIN con label_retry_policy
CREATE INDEX IF NOT EXISTS idx_elc_quality_reason_code
    ON event_labels_core (quality_reason_code)
    WHERE quality_reason_code IS NOT NULL;

-- fix B3: índice definido una sola vez (no duplicado en este bloque ALTER)
-- El idx_elc_worker_status_retry ya fue creado arriba en la sección CREATE TABLE.
-- Si la tabla existía sin el índice, crearlo aquí con IF NOT EXISTS es idempotente:
CREATE INDEX IF NOT EXISTS idx_elc_worker_status_retry
    ON event_labels_core (worker_status, next_retry_at ASC)
    WHERE worker_status IN ('retry_wait', 'pending');

-- Agregar reason_code a event_markouts si no existe
ALTER TABLE event_markouts
    ADD COLUMN IF NOT EXISTS reason_code TEXT;


-- ── Queries de operación ─────────────────────────────────────

-- Worker query actualizada (fix M7 — alineada con labeling_worker.py v2.1):
-- SELECT ce.event_id, ce.symbol,
--        COALESCE(elc.retry_count, 0) AS retry_count,
--        elc.quality_reason_code
-- FROM candidate_events ce
-- LEFT JOIN event_labels_core elc ON elc.event_id = ce.event_id
-- LEFT JOIN label_retry_policy lrp ON lrp.reason_code = elc.quality_reason_code
-- WHERE ce.t_confirm < now() - interval '120 minutes'
--   AND (
--         elc.event_id IS NULL
--     OR (
--             elc.worker_status = 'retry_wait'
--         AND elc.is_retryable  = TRUE
--         AND elc.next_retry_at <= now()
--         AND elc.retry_count   <  COALESCE(lrp.max_retries, 0)
--       )
--   )
--   AND (elc.worker_status IS NULL
--        OR elc.worker_status NOT IN ('done', 'terminal'))
-- ORDER BY ce.t_confirm ASC
-- LIMIT 50;

-- Health check de retry state:
-- SELECT worker_status, COUNT(*) AS n, AVG(retry_count) AS avg_retries
-- FROM event_labels_core
-- GROUP BY worker_status ORDER BY worker_status;

-- Health check de label quality:
-- SELECT
--     label_quality_30m,
--     COUNT(*) AS n,
--     ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER (), 4) AS ratio
-- FROM event_labels_core
-- GROUP BY label_quality_30m ORDER BY label_quality_30m;
-- Objetivo: ok_ratio >= 0.90, bad_ratio <= 0.02 (SoT Sección 11).

-- Auditoría de attempts por evento:
-- SELECT ela.attempt_n, ela.started_at, ela.duration_ms,
--        ela.result_quality, ela.reason_code, ela.was_success
-- FROM event_label_attempts ela
-- WHERE ela.event_id = '<event_id>'
-- ORDER BY ela.attempt_n ASC;

-- Eventos bloqueados en retry_wait con cooldown vencido (diagnóstico):
-- SELECT elc.event_id, elc.quality_reason_code, elc.retry_count,
--        elc.next_retry_at, lrp.max_retries
-- FROM event_labels_core elc
-- JOIN label_retry_policy lrp ON lrp.reason_code = elc.quality_reason_code
-- WHERE elc.worker_status = 'retry_wait'
--   AND elc.next_retry_at <= now()
-- ORDER BY elc.next_retry_at ASC;

-- fix A7: Auditoría de violación de invariante (attempts sin label):
-- SELECT ela.event_id, ela.attempt_n, ela.started_at, ela.result_quality
-- FROM event_label_attempts ela
-- WHERE NOT EXISTS (
--     SELECT 1 FROM event_labels_core elc WHERE elc.event_id = ela.event_id
-- )
-- ORDER BY ela.started_at DESC;

-- Verificar tipos de columnas después de ALTER TABLE (fix B2):
-- SELECT column_name, data_type, is_nullable, column_default
-- FROM information_schema.columns
-- WHERE table_name = 'event_labels_core'
-- ORDER BY ordinal_position;
