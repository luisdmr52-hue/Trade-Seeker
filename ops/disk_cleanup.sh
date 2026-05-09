#!/usr/bin/env bash
# KTS — disk_cleanup.sh v1.1
# SoT: KTS_Cron_Limpieza_Backups_SoT_v1.1

set -euo pipefail

LOG="/var/log/kts_disk_cleanup.log"
STATUS="/var/lib/kts/disk_cleanup.status"
LOCK_GLOBAL="/var/lock/kts_maintenance.lock"
LOCK_SPECIFIC="/var/lock/kts_disk_cleanup.lock"
ENV_FILE="/root/trade-seeker/.env"

exec >> "$LOG" 2>&1

RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
RUN_TS="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
HOSTNAME="$(hostname)"
START_EPOCH=$(date +%s)

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

get_max_fs_pct() {
    local max_pct=0 max_path="/"
    local paths=("/" "/var/lib/docker" "/var/lib/containerd"
                 "/root/kts-lab/data/pg" "/root/backups/timescale" "/var/log")
    local p pct
    for p in "${paths[@]}"; do
        [[ -e "$p" ]] || continue
        pct="$(df -P "$p" 2>/dev/null | awk 'NR==2{gsub(/%/,"",$5); print $5}')" || continue
        [[ -z "$pct" ]] && continue
        if (( pct > max_pct )); then
            max_pct=$pct
            max_path="$p"
        fi
    done
    echo "${max_pct} ${max_path}"
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
            if echo "$cmdline" | grep -q 'disk_cleanup'; then
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
    write_status "finished" "skipped_locked" \
        "previous_state=${PREVIOUS_STATE}
pid_alive_but_cmd_mismatch=0"
    tg_send "[WARN][KTS Disk Cleanup]
host=${HOSTNAME}
ts=${RUN_TS}
run_status=SKIPPED_LOCKED
reason=maintenance_lock_busy
action=check_/var/lib/kts/disk_cleanup.status"
    exit 0
fi

exec 8>"$LOCK_SPECIFIC"
if ! flock -n 8; then
    echo "[INFO] disk_cleanup_lock_busy — SKIPPED_LOCKED"
    write_status "finished" "skipped_locked" \
        "previous_state=${PREVIOUS_STATE}
pid_alive_but_cmd_mismatch=0"
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
echo "[INFO] disk_cleanup run_id=${RUN_ID} started_at=${RUN_TS}"
echo "========================================================"

# ── métricas antes ────────────────────────────────────────────────────────────

echo "[INFO] === BEFORE ==="
df -P /
df -P /var/lib/containerd /var/lib/docker /root/kts-lab/data/pg \
   /root/backups/timescale /var/log 2>/dev/null || true
findmnt / /var/lib/containerd /var/lib/docker /root/kts-lab/data/pg \
   /root/backups/timescale /var/log 2>/dev/null || true
docker system df -v 2>/dev/null || true
docker ps -a 2>/dev/null || true
docker images 2>/dev/null || true
du -sh /var/lib/containerd /var/lib/docker /var/log \
   /root/backups/timescale 2>/dev/null || true
journalctl --disk-usage 2>/dev/null || true

read -r DISK_BEFORE_PCT _ <<< "$(get_max_fs_pct)"

# ── docker prune ──────────────────────────────────────────────────────────────

FAILED=0
CLEANUP_TIMEOUT=0

echo "[INFO] running docker system prune -f"
if ! timeout 900 docker system prune -f; then
    rc=$?
    if (( rc == 124 )); then
        echo "[ERROR] docker_prune_timeout"
        CLEANUP_TIMEOUT=1
    else
        echo "[ERROR] docker_prune_failed rc=${rc}"
    fi
    FAILED=1
fi

# ── journald rotate ───────────────────────────────────────────────────────────

echo "[INFO] running journalctl --rotate --vacuum-time=30d"
if ! timeout 120 journalctl --rotate --vacuum-time=30d; then
    rc=$?
    if (( rc == 124 )); then
        echo "[WARN] journalctl_vacuum_timeout"
    else
        echo "[WARN] journalctl_vacuum_failed rc=${rc}"
    fi
fi

# ── métricas después ──────────────────────────────────────────────────────────

echo "[INFO] === AFTER ==="
df -P /
df -P /var/lib/containerd /var/lib/docker /root/kts-lab/data/pg \
   /root/backups/timescale /var/log 2>/dev/null || true
docker system df -v 2>/dev/null || true
du -sh /var/lib/containerd /var/lib/docker /var/log \
   /root/backups/timescale 2>/dev/null || true
journalctl --disk-usage 2>/dev/null || true

read -r DISK_AFTER_PCT _ <<< "$(get_max_fs_pct)"
read -r MAX_PCT MAX_PATH <<< "$(get_max_fs_pct)"

# ── run status ────────────────────────────────────────────────────────────────

if (( FAILED == 1 )); then
    RUN_STATUS="failed"
else
    RUN_STATUS="ok"
fi

# ── alertas disco ─────────────────────────────────────────────────────────────

if (( MAX_PCT > 90 )); then
    echo "[CRIT] max_fs_used_pct=${MAX_PCT} max_fs_path=${MAX_PATH}"
    tg_send "[CRITICAL][KTS Disk Cleanup]
host=${HOSTNAME}
ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
max_fs_used_pct=${MAX_PCT}
max_fs_path=${MAX_PATH}
action=docker_system_prune_f+journalctl_rotate_vacuum_30d
status=manual_intervention_required"
elif (( MAX_PCT > 80 )); then
    echo "[WARN] max_fs_used_pct=${MAX_PCT} max_fs_path=${MAX_PATH}"
    tg_send "[WARN][KTS Disk Cleanup]
host=${HOSTNAME}
ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
max_fs_used_pct=${MAX_PCT}
max_fs_path=${MAX_PATH}
action=docker_system_prune_f+journalctl_rotate_vacuum_30d
status=watch"
else
    echo "[INFO] max_fs_used_pct=${MAX_PCT} max_fs_path=${MAX_PATH} — OK"
fi

# ── status final ──────────────────────────────────────────────────────────────

write_status "finished" "$RUN_STATUS" \
"disk_before_pct=${DISK_BEFORE_PCT}
disk_after_pct=${DISK_AFTER_PCT}
max_fs_used_pct=${MAX_PCT}
max_fs_path=${MAX_PATH}
cleanup_timeout=${CLEANUP_TIMEOUT}
previous_state=${PREVIOUS_STATE}
pid_alive_but_cmd_mismatch=0"

echo "[INFO] disk_cleanup finished run_status=${RUN_STATUS} duration_s=$(( $(date +%s) - START_EPOCH ))"

if (( FAILED == 1 )); then
    exit 1
fi
exit 0
