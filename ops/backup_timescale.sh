#!/usr/bin/env bash
# KTS — backup_timescale.sh v1.1
# SoT: KTS_Cron_Limpieza_Backups_SoT_v1.1

set -euo pipefail

LOG="/var/log/kts_backup.log"
STATUS="/var/lib/kts/backup_timescale.status"
LOCK_GLOBAL="/var/lock/kts_maintenance.lock"
LOCK_SPECIFIC="/var/lock/kts_backup_timescale.lock"
BACKUP_ROOT="/root/backups/timescale"
ENV_FILE="/root/trade-seeker/.env"
CONTAINER="ts_timescaledb"
DB="tsdb"
DB_USER="postgres"

RETENTION_DAYS=7
RECENT_OK_MAX_AGE_HOURS=36
MIN_OK_BACKUPS_TO_KEEP=2

CRITICAL_TABLES=(
    signals
    signal_outcomes
    outcome_jobs
    candidate_events
    event_features_pti
    event_labels_core
    event_label_attempts
    event_markouts
    btc_regime_snapshots
    label_retry_policy
)

declare -A ALLOW_EMPTY
ALLOW_EMPTY[signals]=0
ALLOW_EMPTY[signal_outcomes]=0
ALLOW_EMPTY[outcome_jobs]=1
ALLOW_EMPTY[candidate_events]=0
ALLOW_EMPTY[event_features_pti]=0
ALLOW_EMPTY[event_labels_core]=1
ALLOW_EMPTY[event_label_attempts]=1
ALLOW_EMPTY[event_markouts]=1
ALLOW_EMPTY[btc_regime_snapshots]=1
ALLOW_EMPTY[label_retry_policy]=1

exec >> "$LOG" 2>&1

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
RUN_TS="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
RUN_DATE="$(date -u '+%Y-%m-%d')"
HOSTNAME_VAL="$(hostname)"
START_EPOCH=$(date +%s)
BACKUP_DIR="${BACKUP_ROOT}/${RUN_DATE}"
MANIFEST="${BACKUP_DIR}/manifest.txt"

# ── helpers ────────────────────────────────────────────────────────────────────

tg_send() {
    local msg="$1"
    local token="" chat_id=""
    if [[ -f "$ENV_FILE" ]]; then
        token="$(grep '^TELEGRAM_BOT_TOKEN=' "$ENV_FILE" | head -1 | cut -d= -f2-)"
        chat_id="$(grep '^TELEGRAM_CHAT_ID=' "$ENV_FILE" | head -1 | cut -d= -f2-)"
    fi
    if [[ -z "$token" || -z "$chat_id" ]]; then
        echo "[WARN] telegram_config_missing — skipping alert"
        return 0
    fi
    local truncated
    truncated="$(echo "$msg" | head -c 3900)"
    local attempt resp retry_after
    for attempt in 1 2 3; do
        resp="$(curl -s --max-time 8 \
            -X POST "https://api.telegram.org/bot${token}/sendMessage" \
            -d "chat_id=${chat_id}" \
            --data-urlencode "text=${truncated}" 2>&1)" || true
        if echo "$resp" | grep -q '"ok":true'; then
            echo "[INFO] telegram_sent attempt=${attempt}"
            return 0
        fi
        echo "[WARN] telegram_attempt_failed attempt=${attempt} resp=${resp}"
        retry_after="$(echo "$resp" | grep -o '"retry_after":[0-9]*' | grep -o '[0-9]*$' || true)"
        if [[ -n "$retry_after" ]]; then
            if (( retry_after >= 1 && retry_after <= 10 )); then
                sleep "$retry_after"
            else
                echo "[WARN] telegram_skipped_retry_after_gt_cap retry_after=${retry_after}"
            fi
        else
            sleep $(( attempt * 2 ))
        fi
    done
    echo "[WARN] telegram_all_attempts_failed"
    return 0
}

write_status() {
    local state="$1" run_status="${2:-}" extra="${3:-}"
    local ended_at duration_s
    ended_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    duration_s=$(( $(date +%s) - START_EPOCH ))
    local tmp="${STATUS}.tmp.$$"
    {
        echo "state=${state}"
        echo "pid=$$"
        echo "run_id=${RUN_ID}"
        echo "started_at=${RUN_TS}"
        echo "ended_at=${ended_at}"
        echo "duration_s=${duration_s}"
        echo "run_status=${run_status}"
        [[ -n "$extra" ]] && echo "$extra"
    } > "$tmp"
    mv -f "$tmp" "$STATUS"
}

append_manifest() {
    echo "$1" >> "$MANIFEST"
}

# ── stale check ────────────────────────────────────────────────────────────────

PREVIOUS_STATE="none"
if [[ -f "$STATUS" ]]; then
    prev_state="$(grep '^state=' "$STATUS" | cut -d= -f2-)"
    if [[ "$prev_state" == "running" ]]; then
        prev_pid="$(grep '^pid=' "$STATUS" | cut -d= -f2-)"
        pid_alive=0
        cmd_match=0
        if kill -0 "$prev_pid" 2>/dev/null; then
            pid_alive=1
            cmdline="$(tr '\0' ' ' < /proc/${prev_pid}/cmdline 2>/dev/null || true)"
            if echo "$cmdline" | grep -q 'backup_timescale'; then
                cmd_match=1
            fi
        fi
        if (( pid_alive == 0 )) || (( cmd_match == 0 )); then
            PREVIOUS_STATE="stale_interrupted"
            echo "[WARN] previous_state=stale_interrupted pid=${prev_pid} pid_alive=${pid_alive} cmd_match=${cmd_match}"
        fi
    fi
fi

# ── locks ──────────────────────────────────────────────────────────────────────

exec 9>"$LOCK_GLOBAL"
if ! flock -n 9; then
    echo "[INFO] maintenance_lock_busy — SKIPPED_LOCKED"
    write_status "finished" "SKIPPED_LOCKED" \
        "backup_dir=none
manifest=none
failed_tables=none
empty_tables=none
semantic_empty_failed_tables=none
previous_state=${PREVIOUS_STATE}
pid_alive_but_cmd_mismatch=0"
    tg_send "[WARN][KTS Timescale Backup]
host=${HOSTNAME_VAL}
ts=${RUN_TS}
run_status=SKIPPED_LOCKED
reason=maintenance_lock_busy_or_backup_lock_busy
action=check_/var/lib/kts/backup_timescale.status"
    exit 0
fi

exec 8>"$LOCK_SPECIFIC"
if ! flock -n 8; then
    echo "[INFO] backup_lock_busy — SKIPPED_LOCKED"
    write_status "finished" "SKIPPED_LOCKED" \
        "backup_dir=none
manifest=none
failed_tables=none
empty_tables=none
semantic_empty_failed_tables=none
previous_state=${PREVIOUS_STATE}
pid_alive_but_cmd_mismatch=0"
    tg_send "[WARN][KTS Timescale Backup]
host=${HOSTNAME_VAL}
ts=${RUN_TS}
run_status=SKIPPED_LOCKED
reason=maintenance_lock_busy_or_backup_lock_busy
action=check_/var/lib/kts/backup_timescale.status"
    exit 0
fi

# ── status running ─────────────────────────────────────────────────────────────

local_tmp="${STATUS}.tmp.$$"
{
    echo "state=running"
    echo "pid=$$"
    echo "run_id=${RUN_ID}"
    echo "started_at=${RUN_TS}"
    echo "previous_state=${PREVIOUS_STATE}"
} > "$local_tmp"
mv -f "$local_tmp" "$STATUS"

echo "========================================================"
echo "[INFO] backup_timescale run_id=${RUN_ID} started_at=${RUN_TS}"
echo "========================================================"

# ── validar contenedor ────────────────────────────────────────────────────────

if ! docker inspect "$CONTAINER" --format '{{.State.Running}}' 2>/dev/null | grep -q 'true'; then
    echo "[ERROR] container ${CONTAINER} not running — aborting"
    write_status "finished" "FAILED_ALL" \
        "backup_dir=none
manifest=none
failed_tables=all
empty_tables=none
semantic_empty_failed_tables=none
previous_state=${PREVIOUS_STATE}
pid_alive_but_cmd_mismatch=0"
    tg_send "[ERROR][KTS Timescale Backup]
host=${HOSTNAME_VAL}
ts=${RUN_TS}
run_status=FAILED_ALL
backup_dir=none
manifest=none
failed_tables=container_not_running
action=check_/var/log/kts_backup.log"
    exit 1
fi

if ! docker exec "$CONTAINER" sh -lc 'command -v pg_dump && command -v pg_restore && command -v psql' > /dev/null 2>&1; then
    echo "[ERROR] pg tools missing in container — aborting"
    write_status "finished" "FAILED_ALL" \
        "backup_dir=none
manifest=none
failed_tables=all
empty_tables=none
semantic_empty_failed_tables=none
previous_state=${PREVIOUS_STATE}
pid_alive_but_cmd_mismatch=0"
    exit 1
fi

# ── crear directorio y manifest ───────────────────────────────────────────────

mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"
chown root:root "$BACKUP_DIR"

{
    echo "# KTS Timescale backup manifest"
    echo "schema=v1.1"
    echo "host=${HOSTNAME_VAL}"
    echo "run_id=${RUN_ID}"
    echo "started_at=${RUN_TS}"
    echo "container=${CONTAINER}"
    echo "db=${DB}"
    echo "strict_names=true"
    echo "retention_days=${RETENTION_DAYS}"
    echo "recent_ok_max_age_hours=${RECENT_OK_MAX_AGE_HOURS}"
    echo "min_ok_backups_to_keep=${MIN_OK_BACKUPS_TO_KEEP}"
} > "$MANIFEST"
chmod 640 "$MANIFEST"

# ── limpiar /tmp residuos ────────────────────────────────────────────────────

docker exec "$CONTAINER" sh -lc 'rm -f /tmp/kts_*.dump' || true

# ── loop de tablas ────────────────────────────────────────────────────────────

FAILED_TABLES=()
EMPTY_TABLES=()
SEMANTIC_EMPTY_FAILED=()
OK_COUNT=0
FAIL_COUNT=0

for TABLE in "${CRITICAL_TABLES[@]}"; do
    echo "[INFO] --- backing up table: ${TABLE} ---"
    TABLE_STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    DUMP_FILE="${BACKUP_DIR}/${TABLE}.dump"
    TMP_NAME="kts_${RUN_ID}_${TABLE}.dump"
    ALLOW_EMPTY_VAL="${ALLOW_EMPTY[$TABLE]:-1}"

    # defaults
    EXISTS=false
    ROWS=na
    DUMP_OK=false
    DUMP_TIMEOUT=false
    COPY_OK=false
    COPY_TIMEOUT=false
    HOST_SIZE=0
    CONT_SIZE=0
    HOST_SHA=na
    CONT_SHA=na
    SHA_MATCH=false
    RESTORE_LIST_OK=false
    RESTORE_TIMEOUT=false
    TABLE_ENTRY_OK=false
    TABLE_DATA_ENTRY_OK=false
    CID_BEFORE=na
    CID_AFTER=na
    CID_STABLE=false
    TMP_AVAIL=na
    TABLE_STATUS=failed

    # 1. tabla existe
    exists_result="$(docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB" -Atc \
        "SELECT to_regclass('public.${TABLE}');" 2>/dev/null || true)"
    if [[ "$exists_result" == "public.${TABLE}" || "$exists_result" == "${TABLE}" ]]; then
        EXISTS=true
    else
        echo "[ERROR] table ${TABLE} not found — failed_missing_table"
        TABLE_STATUS=failed_missing_table
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        FAILED_TABLES+=("$TABLE")
        TABLE_ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        append_manifest "table=${TABLE} exists=false allow_empty=${ALLOW_EMPTY_VAL} rows_at_dump_time=na table_started_at=${TABLE_STARTED_AT} table_ended_at=${TABLE_ENDED_AT} dump_ok=false dump_timeout=false copy_ok=false copy_timeout=false host_size_bytes=0 container_size_bytes=0 host_sha256=na container_sha256=na sha256_match=false restore_list_ok=false restore_timeout=false table_entry_ok=false table_data_entry_ok=false container_id_before=na container_id_after=na container_id_stable=false tmp_avail_kb_before=na table_status=${TABLE_STATUS}"
        continue
    fi

    # 2. contar filas
    ROWS="$(docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB" -Atc \
        "SELECT COUNT(*) FROM public.${TABLE};" 2>/dev/null || echo "na")"
    echo "[INFO] table=${TABLE} rows=${ROWS}"

    # 3. container_id antes
    CID_BEFORE="$(docker inspect -f '{{.Id}}' "$CONTAINER" 2>/dev/null || echo na)"

    # 4. limpiar tmp
    docker exec "$CONTAINER" sh -lc "rm -f /tmp/kts_*.dump" 2>/dev/null || true

    # 5. tmp avail
    TMP_AVAIL="$(docker exec "$CONTAINER" sh -lc "df -P /tmp | awk 'NR==2{print \$4}'" 2>/dev/null || echo na)"

    # 6. pg_dump
    dump_rc=0
    if ! timeout 1800 docker exec "$CONTAINER" \
        pg_dump -U "$DB_USER" -d "$DB" -Fc --strict-names \
        -t "public.${TABLE}" -f "/tmp/${TMP_NAME}" 2>&1; then
        dump_rc=$?
        if (( dump_rc == 124 )); then
            echo "[ERROR] pg_dump timeout table=${TABLE}"
            DUMP_TIMEOUT=true
            TABLE_STATUS=failed_timeout
        else
            echo "[ERROR] pg_dump failed rc=${dump_rc} table=${TABLE}"
            TABLE_STATUS=failed
        fi
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        FAILED_TABLES+=("$TABLE")
        TABLE_ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        append_manifest "table=${TABLE} exists=${EXISTS} allow_empty=${ALLOW_EMPTY_VAL} rows_at_dump_time=${ROWS} table_started_at=${TABLE_STARTED_AT} table_ended_at=${TABLE_ENDED_AT} dump_ok=false dump_timeout=${DUMP_TIMEOUT} copy_ok=false copy_timeout=false host_size_bytes=0 container_size_bytes=0 host_sha256=na container_sha256=na sha256_match=false restore_list_ok=false restore_timeout=false table_entry_ok=false table_data_entry_ok=false container_id_before=${CID_BEFORE} container_id_after=na container_id_stable=false tmp_avail_kb_before=${TMP_AVAIL} table_status=${TABLE_STATUS}"
        continue
    fi
    DUMP_OK=true

    # 7. docker cp al host
    cp_rc=0
    if ! timeout 300 docker cp "${CONTAINER}:/tmp/${TMP_NAME}" "$DUMP_FILE" 2>&1; then
        cp_rc=$?
        if (( cp_rc == 124 )); then
            echo "[ERROR] docker cp timeout table=${TABLE}"
            COPY_TIMEOUT=true
            TABLE_STATUS=failed_timeout
        else
            echo "[ERROR] docker cp failed rc=${cp_rc} table=${TABLE}"
            TABLE_STATUS=failed
        fi
        docker exec "$CONTAINER" rm -f "/tmp/${TMP_NAME}" 2>/dev/null || true
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        FAILED_TABLES+=("$TABLE")
        TABLE_ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        append_manifest "table=${TABLE} exists=${EXISTS} allow_empty=${ALLOW_EMPTY_VAL} rows_at_dump_time=${ROWS} table_started_at=${TABLE_STARTED_AT} table_ended_at=${TABLE_ENDED_AT} dump_ok=${DUMP_OK} dump_timeout=${DUMP_TIMEOUT} copy_ok=false copy_timeout=${COPY_TIMEOUT} host_size_bytes=0 container_size_bytes=0 host_sha256=na container_sha256=na sha256_match=false restore_list_ok=false restore_timeout=false table_entry_ok=false table_data_entry_ok=false container_id_before=${CID_BEFORE} container_id_after=na container_id_stable=false tmp_avail_kb_before=${TMP_AVAIL} table_status=${TABLE_STATUS}"
        continue
    fi
    COPY_OK=true

    # 8. size host
    if [[ -s "$DUMP_FILE" ]]; then
        HOST_SIZE="$(stat -c%s "$DUMP_FILE" 2>/dev/null || echo 0)"
        chmod 600 "$DUMP_FILE"
        chown root:root "$DUMP_FILE"
    else
        echo "[ERROR] dump file empty on host table=${TABLE}"
        docker exec "$CONTAINER" rm -f "/tmp/${TMP_NAME}" 2>/dev/null || true
        TABLE_STATUS=failed
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        FAILED_TABLES+=("$TABLE")
        TABLE_ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        append_manifest "table=${TABLE} exists=${EXISTS} allow_empty=${ALLOW_EMPTY_VAL} rows_at_dump_time=${ROWS} table_started_at=${TABLE_STARTED_AT} table_ended_at=${TABLE_ENDED_AT} dump_ok=${DUMP_OK} dump_timeout=false copy_ok=${COPY_OK} copy_timeout=false host_size_bytes=0 container_size_bytes=0 host_sha256=na container_sha256=na sha256_match=false restore_list_ok=false restore_timeout=false table_entry_ok=false table_data_entry_ok=false container_id_before=${CID_BEFORE} container_id_after=na container_id_stable=false tmp_avail_kb_before=${TMP_AVAIL} table_status=${TABLE_STATUS}"
        continue
    fi

    # 9. sha256 host
    HOST_SHA="$(sha256sum "$DUMP_FILE" | awk '{print $1}')"

    # 10. size y sha256 contenedor
    CONT_SIZE="$(docker exec "$CONTAINER" stat -c%s "/tmp/${TMP_NAME}" 2>/dev/null || echo 0)"
    CONT_SHA="$(docker exec "$CONTAINER" sha256sum "/tmp/${TMP_NAME}" 2>/dev/null | awk '{print $1}' || echo na)"

    if [[ "$HOST_SHA" == "$CONT_SHA" ]]; then
        SHA_MATCH=true
    else
        echo "[ERROR] sha256 mismatch table=${TABLE} host=${HOST_SHA} container=${CONT_SHA}"
        docker exec "$CONTAINER" rm -f "/tmp/${TMP_NAME}" 2>/dev/null || true
        TABLE_STATUS=failed
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        FAILED_TABLES+=("$TABLE")
        TABLE_ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        append_manifest "table=${TABLE} exists=${EXISTS} allow_empty=${ALLOW_EMPTY_VAL} rows_at_dump_time=${ROWS} table_started_at=${TABLE_STARTED_AT} table_ended_at=${TABLE_ENDED_AT} dump_ok=${DUMP_OK} dump_timeout=false copy_ok=${COPY_OK} copy_timeout=false host_size_bytes=${HOST_SIZE} container_size_bytes=${CONT_SIZE} host_sha256=${HOST_SHA} container_sha256=${CONT_SHA} sha256_match=false restore_list_ok=false restore_timeout=false table_entry_ok=false table_data_entry_ok=false container_id_before=${CID_BEFORE} container_id_after=na container_id_stable=false tmp_avail_kb_before=${TMP_AVAIL} table_status=${TABLE_STATUS}"
        continue
    fi

    # 11. pg_restore -l
    restore_list_output=""
    restore_rc=0
    if ! restore_list_output="$(timeout 300 docker exec "$CONTAINER" \
        pg_restore -l "/tmp/${TMP_NAME}" 2>&1)"; then
        restore_rc=$?
        if (( restore_rc == 124 )); then
            echo "[ERROR] pg_restore -l timeout table=${TABLE}"
            RESTORE_TIMEOUT=true
            TABLE_STATUS=failed_timeout
        else
            echo "[ERROR] pg_restore -l failed rc=${restore_rc} table=${TABLE}"
            TABLE_STATUS=failed
        fi
        docker exec "$CONTAINER" rm -f "/tmp/${TMP_NAME}" 2>/dev/null || true
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        FAILED_TABLES+=("$TABLE")
        TABLE_ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        append_manifest "table=${TABLE} exists=${EXISTS} allow_empty=${ALLOW_EMPTY_VAL} rows_at_dump_time=${ROWS} table_started_at=${TABLE_STARTED_AT} table_ended_at=${TABLE_ENDED_AT} dump_ok=${DUMP_OK} dump_timeout=false copy_ok=${COPY_OK} copy_timeout=false host_size_bytes=${HOST_SIZE} container_size_bytes=${CONT_SIZE} host_sha256=${HOST_SHA} container_sha256=${CONT_SHA} sha256_match=${SHA_MATCH} restore_list_ok=false restore_timeout=${RESTORE_TIMEOUT} table_entry_ok=false table_data_entry_ok=false container_id_before=${CID_BEFORE} container_id_after=na container_id_stable=false tmp_avail_kb_before=${TMP_AVAIL} table_status=${TABLE_STATUS}"
        continue
    fi
    RESTORE_LIST_OK=true

    # 12. validar entradas en restore list
    if echo "$restore_list_output" | grep -q "TABLE public ${TABLE}"; then
        TABLE_ENTRY_OK=true
    fi
    if echo "$restore_list_output" | grep -q "TABLE DATA public ${TABLE}"; then
        TABLE_DATA_ENTRY_OK=true
    fi

    # 13. container_id después
    CID_AFTER="$(docker inspect -f '{{.Id}}' "$CONTAINER" 2>/dev/null || echo na)"
    if [[ "$CID_BEFORE" == "$CID_AFTER" ]]; then
        CID_STABLE=true
    else
        echo "[ERROR] container_id changed during verify table=${TABLE}"
        TABLE_STATUS=failed_container_changed
        docker exec "$CONTAINER" rm -f "/tmp/${TMP_NAME}" 2>/dev/null || true
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        FAILED_TABLES+=("$TABLE")
        TABLE_ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        append_manifest "table=${TABLE} exists=${EXISTS} allow_empty=${ALLOW_EMPTY_VAL} rows_at_dump_time=${ROWS} table_started_at=${TABLE_STARTED_AT} table_ended_at=${TABLE_ENDED_AT} dump_ok=${DUMP_OK} dump_timeout=false copy_ok=${COPY_OK} copy_timeout=false host_size_bytes=${HOST_SIZE} container_size_bytes=${CONT_SIZE} host_sha256=${HOST_SHA} container_sha256=${CONT_SHA} sha256_match=${SHA_MATCH} restore_list_ok=${RESTORE_LIST_OK} restore_timeout=false table_entry_ok=${TABLE_ENTRY_OK} table_data_entry_ok=${TABLE_DATA_ENTRY_OK} container_id_before=${CID_BEFORE} container_id_after=${CID_AFTER} container_id_stable=false tmp_avail_kb_before=${TMP_AVAIL} table_status=${TABLE_STATUS}"
        continue
    fi

    # 14. limpiar tmp contenedor
    docker exec "$CONTAINER" rm -f "/tmp/${TMP_NAME}" 2>/dev/null || true

    TABLE_ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

    # 15. determinar table_status
    if [[ "$TABLE_ENTRY_OK" == "true" && "$TABLE_DATA_ENTRY_OK" == "true" && \
          "$SHA_MATCH" == "true" && "$CID_STABLE" == "true" ]]; then
        if [[ "$ROWS" == "0" ]]; then
            if (( ALLOW_EMPTY_VAL == 1 )); then
                TABLE_STATUS=ok_empty
                EMPTY_TABLES+=("$TABLE")
                echo "[INFO] table=${TABLE} table_status=ok_empty"
            else
                TABLE_STATUS=failed_semantic_empty
                SEMANTIC_EMPTY_FAILED+=("$TABLE")
                FAILED_TABLES+=("$TABLE")
                FAIL_COUNT=$(( FAIL_COUNT + 1 ))
                echo "[WARN] table=${TABLE} table_status=failed_semantic_empty"
            fi
        else
            TABLE_STATUS=ok
            OK_COUNT=$(( OK_COUNT + 1 ))
            echo "[INFO] table=${TABLE} table_status=ok rows=${ROWS}"
        fi
    else
        TABLE_STATUS=failed
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
        FAILED_TABLES+=("$TABLE")
        echo "[ERROR] table=${TABLE} table_status=failed entry_ok=${TABLE_ENTRY_OK} data_ok=${TABLE_DATA_ENTRY_OK}"
    fi

    append_manifest "table=${TABLE} exists=${EXISTS} allow_empty=${ALLOW_EMPTY_VAL} rows_at_dump_time=${ROWS} table_started_at=${TABLE_STARTED_AT} table_ended_at=${TABLE_ENDED_AT} dump_ok=${DUMP_OK} dump_timeout=${DUMP_TIMEOUT} copy_ok=${COPY_OK} copy_timeout=${COPY_TIMEOUT} host_size_bytes=${HOST_SIZE} container_size_bytes=${CONT_SIZE} host_sha256=${HOST_SHA} container_sha256=${CONT_SHA} sha256_match=${SHA_MATCH} restore_list_ok=${RESTORE_LIST_OK} restore_timeout=${RESTORE_TIMEOUT} table_entry_ok=${TABLE_ENTRY_OK} table_data_entry_ok=${TABLE_DATA_ENTRY_OK} container_id_before=${CID_BEFORE} container_id_after=${CID_AFTER} container_id_stable=${CID_STABLE} tmp_avail_kb_before=${TMP_AVAIL} table_status=${TABLE_STATUS}"

done

# ── run status final ──────────────────────────────────────────────────────────

TOTAL=${#CRITICAL_TABLES[@]}
FAILED_CSV="none"
EMPTY_CSV="none"
SEM_EMPTY_CSV="none"

(( ${#FAILED_TABLES[@]} > 0 )) && FAILED_CSV="$(IFS=,; echo "${FAILED_TABLES[*]}")"
(( ${#EMPTY_TABLES[@]} > 0 )) && EMPTY_CSV="$(IFS=,; echo "${EMPTY_TABLES[*]}")"
(( ${#SEMANTIC_EMPTY_FAILED[@]} > 0 )) && SEM_EMPTY_CSV="$(IFS=,; echo "${SEMANTIC_EMPTY_FAILED[*]}")"

if (( FAIL_COUNT == TOTAL )); then
    RUN_STATUS="FAILED_ALL"
elif (( FAIL_COUNT > 0 )); then
    RUN_STATUS="PARTIAL_FAILED"
elif (( ${#EMPTY_TABLES[@]} > 0 )); then
    RUN_STATUS="OK_WITH_EMPTY_TABLES"
else
    RUN_STATUS="OK_ALL"
fi

ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
DURATION_S=$(( $(date +%s) - START_EPOCH ))

{
    echo "RUN_STATUS=${RUN_STATUS}"
    echo "failed_tables=${FAILED_CSV}"
    echo "empty_tables=${EMPTY_CSV}"
    echo "semantic_empty_failed_tables=${SEM_EMPTY_CSV}"
    echo "ended_at=${ENDED_AT}"
    echo "duration_s=${DURATION_S}"
} >> "$MANIFEST"

echo "[INFO] backup finished run_status=${RUN_STATUS} failed=${FAIL_COUNT}/${TOTAL} duration_s=${DURATION_S}"

# ── alertas ───────────────────────────────────────────────────────────────────

if [[ "$RUN_STATUS" == "FAILED_ALL" || "$RUN_STATUS" == "PARTIAL_FAILED" ]]; then
    tg_send "[ERROR][KTS Timescale Backup]
host=${HOSTNAME_VAL}
ts=${ENDED_AT}
run_status=${RUN_STATUS}
backup_dir=${BACKUP_DIR}
manifest=${MANIFEST}
failed_tables=${FAILED_CSV}
semantic_empty_failed_tables=${SEM_EMPTY_CSV}
action=check_/var/log/kts_backup.log"
elif [[ "$RUN_STATUS" == "OK_WITH_EMPTY_TABLES" ]]; then
    tg_send "[WARN][KTS Timescale Backup]
host=${HOSTNAME_VAL}
ts=${ENDED_AT}
run_status=OK_WITH_EMPTY_TABLES
empty_tables=${EMPTY_CSV}
backup_dir=${BACKUP_DIR}
manifest=${MANIFEST}"
fi

# ── retención protegida ───────────────────────────────────────────────────────

if [[ "$RUN_STATUS" == "OK_ALL" || "$RUN_STATUS" == "OK_WITH_EMPTY_TABLES" ]]; then
    echo "[INFO] running retention cleanup"

    # identificar backups OK (tienen RUN_STATUS=OK_ALL u OK_WITH_EMPTY_TABLES en manifest)
    mapfile -t ALL_BACKUP_DIRS < <(find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d | sort -r)
    OK_DIRS=()
    for d in "${ALL_BACKUP_DIRS[@]}"; do
        mf="${d}/manifest.txt"
        if [[ -f "$mf" ]]; then
            rs="$(grep '^RUN_STATUS=' "$mf" | cut -d= -f2- || true)"
            if [[ "$rs" == "OK_ALL" || "$rs" == "OK_WITH_EMPTY_TABLES" ]]; then
                OK_DIRS+=("$d")
            fi
        fi
    done

    # verificar que newest OK no sea demasiado viejo
    NEWEST_OK_AGE_OK=false
    if (( ${#OK_DIRS[@]} > 0 )); then
        newest_ok="${OK_DIRS[0]}"
        newest_mtime="$(stat -c%Y "$newest_ok" 2>/dev/null || echo 0)"
        now_epoch=$(date +%s)
        age_hours=$(( (now_epoch - newest_mtime) / 3600 ))
        if (( age_hours <= RECENT_OK_MAX_AGE_HOURS )); then
            NEWEST_OK_AGE_OK=true
        else
            echo "[WARN] newest OK backup is ${age_hours}h old — skipping retention to protect last good backup"
        fi
    fi

    if [[ "$NEWEST_OK_AGE_OK" == "true" ]] && (( ${#OK_DIRS[@]} > MIN_OK_BACKUPS_TO_KEEP )); then
        # proteger los últimos MIN_OK_BACKUPS_TO_KEEP backups OK
        PROTECTED=("${OK_DIRS[@]:0:$MIN_OK_BACKUPS_TO_KEEP}")

        while IFS= read -r -d '' dir; do
            # no borrar protegidos
            protected=false
            for p in "${PROTECTED[@]}"; do
                if [[ "$dir" == "$p" ]]; then
                    protected=true
                    break
                fi
            done
            if [[ "$protected" == "true" ]]; then
                echo "[INFO] retention: protecting ${dir}"
                continue
            fi
            echo "[INFO] retention: removing ${dir}"
            rm -rf "$dir"
        done < <(find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -mtime "+${RETENTION_DAYS}" -print0)
    else
        echo "[INFO] retention: skipped (not enough OK backups or newest too old)"
    fi
fi

# ── status final ──────────────────────────────────────────────────────────────

write_status "finished" "$RUN_STATUS" \
"backup_dir=${BACKUP_DIR}
manifest=${MANIFEST}
failed_tables=${FAILED_CSV}
empty_tables=${EMPTY_CSV}
semantic_empty_failed_tables=${SEM_EMPTY_CSV}
previous_state=${PREVIOUS_STATE}
pid_alive_but_cmd_mismatch=0"

if [[ "$RUN_STATUS" == "FAILED_ALL" || "$RUN_STATUS" == "PARTIAL_FAILED" ]]; then
    exit 1
fi
exit 0
