# KTS — SoT Cron de Limpieza de Disco y Backups TimescaleDB

meta: {
  ver: "v1.1",
  fecha: "2026-05-08",
  estado: "cerrado_para_implementacion",
  prioridad: "alta",
  contexto: "post-incidente disco lleno y perdida de 5 semanas de datos",
  owner: "Luis",
  implementador: "Claude",
  rol_chatgpt: "diseno_y_auditoria",
  cambio_vs_v1_0: "hardening adversarial: strict-names, retencion protegida formal, status atomico, worst filesystem alert, timeouts, lock alert, politica de tablas vacias"
}

---

## 1. Propósito

Este documento define el Source of Truth operativo para:

1. Limpieza automática de disco en el VPS KTS.
2. Backup diario de tablas críticas de TimescaleDB/PostgreSQL.
3. Rotación del log principal del bot.
4. Alertas Telegram ante fallos o riesgo de disco.
5. Verificación semántica mínima de backups.
6. Retención segura de backups locales.
7. Detección de ejecuciones interrumpidas o saltadas por lock.
8. Prevención de falsa seguridad tras fallos consecutivos.

El objetivo es evitar repetir el incidente donde el disco llegó al 99%, Postgres crasheó, la DB se reinicializó vacía y se perdieron aproximadamente 5 semanas de datos analíticos.

---

## 2. Contexto del incidente

El 2026-05-07 el disco del VPS llegó al 99% por acumulación de capas/imágenes bajo:

    /var/lib/containerd

Estado observado:

    /var/lib/containerd   ~88GB antes de limpieza
    /var/lib/containerd   ~9.4GB post-limpieza
    /var/log              ~3.3GB
    /var/lib/docker       ~2.2GB
    disco total           96GB
    uso post-limpieza     19GB / 96GB (~20%)

Impacto:

    Postgres/TimescaleDB crasheó
    DB reinicializada vacía
    pérdida aproximada: 5 semanas de datos de análisis

Causa raíz:

    No existía limpieza automática de Docker/containerd.
    No existían alertas de disco.
    No existían backups locales verificables de tablas críticas.
    No existía rotación de /var/log/tradeseeker.log.

---

## 3. Sistema objetivo

### 3.1 VPS

    host:      vmi2776932
    ip:        158.220.117.121
    os:        Ubuntu 24
    timezone:  America/Caracas operacional
    cron:      UTC
    disco:     96GB

### 3.2 Stack Docker relevante

    ts_timescaledb   — PostgreSQL/TimescaleDB
    ts_benthos_spot  — ingesta de precios
    ts_n8n           — orquestación
    ts_grafana       — observabilidad

### 3.3 Bind mount crítico

    /root/kts-lab/data/pg

Este path contiene los datos de TimescaleDB.

Regla crítica:

    Nunca borrar manualmente.
    Nunca incluir en limpieza.
    Nunca usar docker prune con --volumes.
    Nunca hacer rm -rf sobre este path.

---

## 4. Alcance

Este SoT cubre:

    /root/kts-lab/ops/disk_cleanup.sh
    /root/kts-lab/ops/backup_timescale.sh
    /etc/logrotate.d/tradeseeker
    crontab root
    /root/backups/timescale/
    /var/lib/kts/*.status
    /var/log/kts_backup.log
    /var/log/kts_disk_cleanup.log

No cubre:

    backup remoto externo
    restore automático
    restore test diario
    cambio de compose
    cambio de imagen Docker
    cambio de Postgres/TimescaleDB
    alertas Grafana
    Prometheus exporters
    herramientas externas tipo docker-gc
    limpieza manual de /var/lib/containerd

---

## 5. Stack permitido

Permitido:

    bash
    docker CLI
    cron root
    logrotate
    curl
    timeout
    pg_dump dentro de ts_timescaledb
    pg_restore dentro de ts_timescaledb
    psql dentro de ts_timescaledb
    sha256sum
    flock
    find
    journalctl
    df
    du
    findmnt
    stat
    awk
    sed
    grep

Prohibido para esta implementación:

    jq
    Python
    scripts externos de terceros
    docker-gc
    limpieza manual de /var/lib/containerd
    pg_dump instalado en host
    pg_restore instalado en host
    /etc/cron.d para esta instalación
    docker system prune -a
    docker system prune --volumes
    restore automático sobre tsdb
    DROP
    TRUNCATE
    DELETE
    ALTER destructivo
    VACUUM FULL automático

---

## 6. Principio de seguridad de datos

Este SoT no autoriza ningún comando que borre datos de tablas.

El backup usa solo lectura/exportación:

    pg_dump -Fc --strict-names -t public.<tabla>

El cleanup usa solo limpieza Docker segura:

    docker system prune -f

Quedan prohibidos:

    docker system prune --volumes
    docker system prune -a
    rm -rf /root/kts-lab/data/pg
    rm -rf /var/lib/containerd/*
    rm -rf /var/lib/docker/*
    DROP TABLE
    TRUNCATE TABLE
    DELETE FROM
    restore automático contra tsdb

Confirmación:

    Este diseño no borra filas ni tablas.
    Este diseño no toca directamente los datafiles de TimescaleDB.
    Este diseño no restaura encima de la base activa.
    Este diseño no limpia volúmenes Docker.
    Este diseño no limpia bind mounts.

---

## 7. Directorios y archivos

### 7.1 Scripts

    /root/kts-lab/ops/disk_cleanup.sh
    /root/kts-lab/ops/backup_timescale.sh

Permisos:

    owner: root:root
    mode:  750

### 7.2 Backups

    /root/backups/timescale/YYYY-MM-DD/

Permisos:

    /root/backups/timescale        700 root:root
    /root/backups/timescale/date   700 root:root
    *.dump                         600 root:root
    manifest.txt                   640 root:root

### 7.3 Logs

    /var/log/kts_backup.log
    /var/log/kts_disk_cleanup.log
    /var/log/tradeseeker.log

Permisos recomendados:

    /var/log/kts_backup.log         640 root:root
    /var/log/kts_disk_cleanup.log   640 root:root

### 7.4 Status files

    /var/lib/kts/backup_timescale.status
    /var/lib/kts/disk_cleanup.status

Directorio:

    /var/lib/kts
    mode: 700 root:root

### 7.5 Locks

    /var/lock/kts_maintenance.lock
    /var/lock/kts_backup_timescale.lock
    /var/lock/kts_disk_cleanup.lock

---

## 8. Tablas críticas

Estas tablas NO son reconstituibles desde el feed y deben respaldarse:

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

Regla:

    Si una tabla crítica falta, el backup de esa tabla falla.
    El fallo de una tabla NO detiene el loop.
    El run completo queda PARTIAL_FAILED o FAILED_ALL.
    Debe alertar por Telegram.

---

## 9. Tablas no críticas

Estas tablas son reconstituibles y NO entran en el backup local selectivo:

    metrics_ext
    metrics_ext_15m

Justificación:

    metrics_ext es feed de precios reconstituible.
    metrics_ext_15m es CAGG/materialización derivada.

Regla:

    No se respaldan en este script.
    No se borran en este script.
    No se alteran en este script.

---

## 10. Política de tablas vacías

Una tabla vacía puede ser técnicamente válida pero semánticamente sospechosa.

Por eso existe política por tabla.

### 10.1 Allow-empty

    signals                    ALLOW_EMPTY=0
    signal_outcomes            ALLOW_EMPTY=0
    outcome_jobs               ALLOW_EMPTY=1
    candidate_events           ALLOW_EMPTY=0
    event_features_pti         ALLOW_EMPTY=0
    event_labels_core          ALLOW_EMPTY=1
    event_label_attempts       ALLOW_EMPTY=1
    event_markouts             ALLOW_EMPTY=1
    btc_regime_snapshots       ALLOW_EMPTY=1
    label_retry_policy         ALLOW_EMPTY=1

### 10.2 Interpretación

Si rows_at_dump_time = 0 y ALLOW_EMPTY = 1:

    table_status=ok_empty

Si rows_at_dump_time = 0 y ALLOW_EMPTY = 0:

    table_status=failed_semantic_empty

Si una o más tablas quedan failed_semantic_empty:

    RUN_STATUS=PARTIAL_FAILED
    o
    RUN_STATUS=FAILED_ALL

según el número total de fallos.

---

## 11. Conexión DB

    contenedor: ts_timescaledb
    host:       localhost desde host, pero no usado para pg_dump
    port:       5432
    dbname:     tsdb
    user:       postgres
    password:   postgres

Regla:

    pg_dump, pg_restore y psql corren dentro del contenedor ts_timescaledb.
    No se instala cliente PostgreSQL en host.
    No se conecta desde host con pg_dump local.

---

## 12. Decisiones cerradas

### DC-CRON-01 — Limpieza Docker

Decisión:

    Usar docker system prune -f.
    No usar -a.
    No usar --volumes.

Justificación:

    docker system prune -f elimina objetos no usados sin borrar volúmenes por defecto.

Caso edge:

    Puede liberar poco si el peso está en objetos no pruneables.

Ajuste:

    Medir antes/después con docker system df -v, du, findmnt y df.
    Alertar si el peor filesystem queda >80% o >90%.

---

### DC-CRON-02 — Prohibida limpieza manual de containerd

Decisión:

    Nunca borrar manualmente carpetas bajo /var/lib/containerd o /var/lib/docker.

Justificación:

    Manipular storage interno puede dejar Docker/containerd inconsistente.

Caso edge:

    Prune no libera suficiente.

Ajuste:

    Alertar y hacer revisión forense manual.
    No automatizar rm en storage interno.

---

### DC-CRON-03 — Backup selectivo

Decisión:

    Backup por tabla crítica usando pg_dump -Fc --strict-names -t public.<tabla>.

Justificación:

    Reduce tamaño y evita respaldar tablas reconstituibles.
    --strict-names fuerza fallo si el patrón no matchea objetos.

Caso edge:

    Tabla crítica nueva no incluida.

Ajuste:

    Probe de tablas antes de instalar.
    Mantener lista manualmente en este SoT.

---

### DC-CRON-04 — pg_dump dentro del contenedor

Decisión:

    Ejecutar pg_dump/pg_restore/psql dentro de ts_timescaledb.

Justificación:

    No hay pg_dump instalado en host.
    Evita drift de versión cliente-servidor.

Caso edge:

    Si el contenedor no arranca, no hay backup.

Ajuste:

    Script falla temprano, registra ERROR y alerta Telegram.

---

### DC-CRON-05 — Verificación semántica estricta del backup

Decisión:

El backup de cada tabla solo queda OK si pasa:

    1. tabla existe en public
    2. rows_at_dump_time registrado
    3. pg_dump --strict-names exit code = 0
    4. dump local existe
    5. dump local size > 0
    6. sha256 host calculado
    7. container_id capturado antes de docker cp
    8. /tmp del contenedor limpiado de residuos kts_*.dump
    9. docker cp al contenedor exit code = 0
    10. dump existe dentro del contenedor
    11. dump dentro del contenedor size > 0
    12. sha256 contenedor == sha256 host
    13. pg_restore -l exit code = 0
    14. pg_restore -l contiene TABLE public <tabla>
    15. pg_restore -l contiene TABLE DATA public <tabla>
    16. container_id después de verify == container_id antes
    17. tmp dump eliminado del contenedor

Caso edge:

    Tabla vacía legítima.

Ajuste:

    rows_at_dump_time=0 no falla automáticamente si ALLOW_EMPTY=1.
    La tabla queda table_status=ok_empty.
    Si ALLOW_EMPTY=0, queda failed_semantic_empty.
    El run final queda OK_WITH_EMPTY_TABLES si todo lo demás está bien pero hay tablas vacías permitidas.
    El run final no queda OK_ALL si hay tablas vacías.

---

### DC-CRON-06 — Manifest obligatorio

Decisión:

    Cada corrida genera manifest.txt con estado por tabla y estado global del run.

Justificación:

    Permite auditar qué se respaldó, qué falló y con qué hash.

Caso edge:

    Manifest existe pero corrida fue interrumpida.

Ajuste:

    Status file anti-stale + RUN_STATUS final obligatorio.
    Si falta RUN_STATUS, el directorio no se considera backup OK.

---

### DC-CRON-07 — Lock global + lock específico

Decisión:

Usar lock global:

    /var/lock/kts_maintenance.lock

Y locks específicos:

    /var/lock/kts_backup_timescale.lock
    /var/lock/kts_disk_cleanup.lock

Orden obligatorio:

    1. lock global
    2. lock específico

Justificación:

    Evita simultaneidad backup-cleanup y evita dos backups o dos cleanups a la vez.

Caso edge:

    Backup tarda más de 1 hora y cleanup semanal intenta correr.

Ajuste:

    Cleanup sale con SKIPPED_LOCKED.
    Backup sale con SKIPPED_LOCKED si no obtiene lock.
    Backup saltado por lock debe intentar Telegram WARN.

---

### DC-CRON-08 — Telegram robusto mínimo

Decisión:

    curl --max-time 8
    3 intentos
    validar "ok":true
    extraer retry_after si aparece
    truncar mensaje a 3900 bytes
    cap de sleep por retry_after

Sin jq.

Justificación:

    HTTP 200 no garantiza que Telegram aceptó el mensaje.
    Telegram sendMessage tiene límite de longitud.
    retry_after largo no debe mantener lock demasiado tiempo.

Caso edge:

    Flood control con retry_after alto.

Ajuste:

    Si retry_after ∈ [1,10], dormir retry_after.
    Si retry_after > 10, loggear telegram_skipped_retry_after_gt_cap y no dormir más.
    Si no hay retry_after, backoff fijo 2s/4s/6s.

---

### DC-CRON-09 — Fallo Telegram no bloquea scripts

Decisión:

    Si Telegram falla, se loggea.
    No cambia el exit code del backup/cleanup.

Justificación:

    La alerta externa no debe impedir mantenimiento ni backup.

Caso edge:

    Fallo crítico no llega al teléfono.

Ajuste:

    Logs + status files siguen siendo fuente local de verdad.

---

### DC-CRON-10 — Journald rotate + vacuum

Decisión:

    journalctl --rotate --vacuum-time=30d

Justificación:

    --rotate archiva journal activo antes de aplicar vacuum.

Caso edge:

    journalctl falla o libera poco.

Ajuste:

    WARN, no fallo total.
    El umbral real se evalúa por worst filesystem después del cleanup.

---

### DC-CRON-11 — Cron root

Decisión:

    Usar crontab root.
    No usar /etc/cron.d en esta instalación.

Justificación:

    Menos puntos de fallo de formato/permisos/newline/campo usuario.

Caso edge:

    Menos auditable que archivo versionable.

Ajuste:

    Guardar salida de crontab -l en handoff/auditoría.

---

### DC-CRON-12 — Status anti-stale

Decisión:

Cada script escribe status con:

    state
    pid
    run_id
    started_at
    ended_at
    duration_s
    run_status

Al inicio, si el status previo está en running:

    verificar si PID vive
    verificar /proc/<pid>/cmdline
    si PID no vive → previous_state=stale_interrupted
    si PID vive pero cmdline no coincide → previous_state=stale_interrupted + pid_alive_but_cmd_mismatch=1

Caso edge:

    PID reciclado.

Ajuste:

    No basta kill -0.
    Validar cmdline del proceso.
    run_id y timestamps reducen ambigüedad.

---

### DC-CRON-13 — Logrotate copytruncate

Decisión:

    Usar copytruncate para /var/log/tradeseeker.log.

Justificación:

    No está verificado que tradeseeker soporte reopen de descriptor.

Caso edge:

    Pérdida de líneas durante ventana copy→truncate.

Ajuste:

    Riesgo aceptado porque tradeseeker.log no es fuente primaria del dataset.
    Manifest y status son la fuente primaria de auditoría del backup/cleanup.

---

### DC-CRON-14 — Retención protegida formal

Decisión:

    No ejecutar retención si la corrida actual falla.
    Solo ejecutar retención si existe backup OK reciente.
    Nunca borrar los últimos N backups OK.
    Proteger backups OK aunque sean viejos si no hay suficientes OK recientes.

Constantes:

    RECENT_OK_MAX_AGE_HOURS=36
    MIN_OK_BACKUPS_TO_KEEP=2
    RETENTION_DAYS=7

Backups OK aceptados:

    OK_ALL
    OK_WITH_EMPTY_TABLES

Backups no OK:

    PARTIAL_FAILED
    FAILED_ALL
    INTERRUPTED_OR_STALE
    SKIPPED_LOCKED

Caso edge:

    8 días consecutivos de backups fallidos.

Ajuste:

    No borrar último backup bueno.
    Se acepta crecimiento temporal de disco antes que perder recovery point.

---

### DC-CRON-15 — Fallo por tabla no detiene el loop

Decisión:

    Si una tabla falla, se registra y continúa con las demás.

Justificación:

    Maximiza recuperación parcial.

Caso edge:

    Todas fallan.

Ajuste:

    RUN_STATUS=FAILED_ALL y exit 1.

---

### DC-CRON-16 — Logging único

Decisión:

    El script es dueño del log usando exec >> "$LOG" 2>&1.
    Cron no redirige al mismo log.

Justificación:

    Evita duplicación e interleaving por doble escritura.

Caso edge:

    Si el script no arranca por permisos, cron puede mandar output por mail/syslog.

Ajuste:

    Verificación post-instalación ejecuta scripts manualmente.
    Status file confirma éxito real.

---

### DC-CRON-17 — Worst filesystem alert

Decisión:

La alerta de disco usa el peor filesystem medido entre:

    /
    /var/lib/docker
    /var/lib/containerd
    /root/kts-lab/data/pg
    /root/backups/timescale
    /var/log

Campos:

    max_fs_used_pct
    max_fs_path

Justificación:

    / puede estar sano mientras Docker, PG data, backups o logs están en otro mount lleno.

Caso edge:

    Path no existe o hereda filesystem padre.

Ajuste:

    Registrar df/findmnt.
    Si path no existe, no fallar; log WARN opcional.
    La alerta usa el máximo disponible.

---

### DC-CRON-18 — Timeouts operativos

Decisión:

Usar timeout en comandos largos:

    docker system prune -f              timeout 900s
    journalctl --rotate --vacuum-time   timeout 120s
    pg_dump                             timeout 1800s por tabla
    docker cp                           timeout 300s
    pg_restore -l                       timeout 300s
    curl Telegram                       timeout 8s

Justificación:

    Evita locks retenidos indefinidamente.

Caso edge:

    Timeout puede cortar una corrida parcial.

Ajuste:

    Marcar failed/timeout en manifest/status.
    No ocultar fallo.

---

### DC-CRON-19 — Status atómico

Decisión:

    Status files se escriben a tmp y luego mv -f.

Patrón:

    tmp="${STATUS}.tmp.$$"
    escribir tmp
    mv -f "$tmp" "$STATUS"

Justificación:

    Evita status parcial si el proceso muere durante múltiples echo.

Caso edge:

    Muere antes del mv.

Ajuste:

    Puede quedar .tmp residual.
    Próxima corrida puede loggear status_tmp_leftover_count y continuar.

---

### DC-CRON-20 — Backup skipped lock alert

Decisión:

    Si backup no corre porque el lock global o específico está ocupado:
      state=finished
      run_status=SKIPPED_LOCKED
      exit 0
      intentar Telegram WARN

Justificación:

    No es error de script, pero sí es pérdida de backup esperado.

Caso edge:

    Telegram falla.

Ajuste:

    Queda en status/log.

---

### DC-CRON-21 — Container ID estable durante verify

Decisión:

Durante verificación de cada dump:

    capturar container_id antes
    copiar/verificar dump
    capturar container_id después
    si cambia → table_status=failed

Justificación:

    Si el contenedor fue recreado durante la verificación, el /tmp validado puede no representar el mismo entorno.

Caso edge:

    Restart sin recreate no cambia ID.

Ajuste:

    docker exec fallaría si hay interrupción.
    Riesgo residual aceptado.

---

### DC-CRON-22 — /tmp del contenedor no es storage de backup

Decisión:

    /tmp dentro del contenedor solo se usa como staging temporal para pg_restore -l.
    No se dejan dumps persistentes allí.
    Antes de copiar, eliminar /tmp/kts_*.dump.
    Registrar df -P /tmp.

Justificación:

    Writes dentro del contenedor consumen writable layer.
    No se debe acumular basura dentro del contenedor.

Caso edge:

    Crash antes de rm.

Ajuste:

    Próxima corrida limpia /tmp/kts_*.dump antes de empezar.

---

### DC-CRON-23 — Consistencia cross-table no garantizada

Decisión:

    El backup es selectivo por tabla, secuencial.
    No garantiza snapshot lógico global exacto entre tablas.
    Sí garantiza dump consistente por tabla.

Justificación:

    KTS aceptó backup por tabla para reducir tamaño y enfocarse en tablas críticas.

Caso edge:

    candidate_events y event_features_pti pueden representar instantes ligeramente distintos.

Ajuste:

    Registrar table_started_at y table_ended_at por tabla.
    Riesgo residual aceptado para disaster recovery local.
    Futuro: dump global o restore test si se requiere consistencia analítica perfecta.

---

## 13. Estados del backup

### 13.1 Estado por tabla

    ok                       — tabla existe, rows > 0, dump/verificación completa OK
    ok_empty                 — tabla existe, rows = 0, ALLOW_EMPTY=1, dump/verificación completa OK
    failed                   — cualquier check técnico crítico falla
    failed_semantic_empty    — tabla existe, rows = 0, ALLOW_EMPTY=0
    failed_missing_table     — to_regclass no encuentra la tabla
    failed_timeout           — timeout en pg_dump, docker cp o pg_restore
    failed_container_changed — container_id cambió durante verify

### 13.2 Estado global del run

    OK_ALL                — todas las tablas ok y ninguna vacía
    OK_WITH_EMPTY_TABLES  — todas verificadas, pero una o más ok_empty
    PARTIAL_FAILED        — una o más fallaron, pero no todas
    FAILED_ALL            — todas fallaron
    SKIPPED_LOCKED        — no corrió porque maintenance lock o job lock estaba ocupado
    INTERRUPTED_OR_STALE  — estado detectado por status previo running con PID muerto o cmdline mismatch

### 13.3 Exit code

    OK_ALL                exit 0
    OK_WITH_EMPTY_TABLES  exit 0 + Telegram WARN opcional
    PARTIAL_FAILED        exit 1 + Telegram ERROR
    FAILED_ALL            exit 1 + Telegram ERROR
    SKIPPED_LOCKED        exit 0 + Telegram WARN si es backup
    INTERRUPTED_OR_STALE  no es estado final de la corrida actual; se registra como previous_state

---

## 14. Manifest schema

Archivo:

    /root/backups/timescale/YYYY-MM-DD/manifest.txt

Cabecera:

    # KTS Timescale backup manifest
    schema=v1.1
    host=<hostname>
    run_id=<YYYYMMDDTHHMMSSZ>
    started_at=<iso_utc>
    container=ts_timescaledb
    db=tsdb
    strict_names=true
    retention_days=7
    recent_ok_max_age_hours=36
    min_ok_backups_to_keep=2

Línea por tabla:

    table=<name> exists=<true|false> allow_empty=<0|1> rows_at_dump_time=<n|na> table_started_at=<iso_utc> table_ended_at=<iso_utc> dump_ok=<true|false> dump_timeout=<true|false> copy_ok=<true|false> copy_timeout=<true|false> host_size_bytes=<n> container_size_bytes=<n> host_sha256=<sha|na> container_sha256=<sha|na> sha256_match=<true|false> restore_list_ok=<true|false> restore_timeout=<true|false> table_entry_ok=<true|false> table_data_entry_ok=<true|false> container_id_before=<id|na> container_id_after=<id|na> container_id_stable=<true|false> tmp_avail_kb_before=<n|na> table_status=<ok|ok_empty|failed|failed_semantic_empty|failed_missing_table|failed_timeout|failed_container_changed>

Footer:

    RUN_STATUS=<OK_ALL|OK_WITH_EMPTY_TABLES|PARTIAL_FAILED|FAILED_ALL|SKIPPED_LOCKED>
    failed_tables=<csv|none>
    empty_tables=<csv|none>
    semantic_empty_failed_tables=<csv|none>
    ended_at=<iso_utc>
    duration_s=<n>

Regla:

    Un directorio sin RUN_STATUS válido no se considera backup OK.
    Un directorio con PARTIAL_FAILED, FAILED_ALL o SKIPPED_LOCKED no se considera backup OK.
    Un directorio con OK_ALL u OK_WITH_EMPTY_TABLES sí se considera backup OK para retención.

---

## 15. Status file schema

### 15.1 Backup status

Archivo:

    /var/lib/kts/backup_timescale.status

Campos:

    state=<running|finished>
    pid=<pid>
    run_id=<YYYYMMDDTHHMMSSZ>
    started_at=<iso_utc>
    ended_at=<iso_utc>
    duration_s=<n>
    run_status=<OK_ALL|OK_WITH_EMPTY_TABLES|PARTIAL_FAILED|FAILED_ALL|SKIPPED_LOCKED>
    backup_dir=<path>
    manifest=<path>
    failed_tables=<csv|none>
    empty_tables=<csv|none>
    semantic_empty_failed_tables=<csv|none>
    previous_state=<none|stale_interrupted>
    pid_alive_but_cmd_mismatch=<0|1>

### 15.2 Cleanup status

Archivo:

    /var/lib/kts/disk_cleanup.status

Campos:

    state=<running|finished>
    pid=<pid>
    run_id=<YYYYMMDDTHHMMSSZ>
    started_at=<iso_utc>
    ended_at=<iso_utc>
    duration_s=<n>
    run_status=<ok|failed|skipped_locked>
    disk_before_pct=<n>
    disk_after_pct=<n>
    max_fs_used_pct=<n>
    max_fs_path=<path>
    cleanup_timeout=<0|1>
    previous_state=<none|stale_interrupted>
    pid_alive_but_cmd_mismatch=<0|1>

### 15.3 Escritura atómica

Toda escritura de status debe usar:

    tmp="${STATUS}.tmp.$$"
    {
      echo "state=running"
      echo "pid=$$"
      echo "run_id=$RUN_ID"
      echo "started_at=$RUN_TS"
    } > "$tmp"
    mv -f "$tmp" "$STATUS"

Regla:

    Nunca escribir status final directamente con múltiples echo > "$STATUS".
    Siempre tmp + mv.

---

## 16. Telegram

### 16.1 Config esperada

Archivo:

    /root/trade-seeker/config.yaml

Estructura esperada:

    telegram:
      token: "<bot_token>"
      chat_id: "<chat_id>"

Extractor permitido:

    awk simple limitado a la estructura anterior.

Regla:

    Si token/chat_id no se pueden extraer, log WARN telegram_config_missing.
    No fallar el script.
    Registrar telegram_config_ok=0.

### 16.2 Mensaje backup ERROR

    [ERROR][KTS Timescale Backup]
    host=<hostname>
    ts=<iso_utc>
    run_status=<PARTIAL_FAILED|FAILED_ALL>
    backup_dir=<path>
    manifest=<path>
    failed_tables=<csv>
    semantic_empty_failed_tables=<csv>
    action=check_/var/log/kts_backup.log

### 16.3 Mensaje backup WARN — OK_WITH_EMPTY_TABLES

    [WARN][KTS Timescale Backup]
    host=<hostname>
    ts=<iso_utc>
    run_status=OK_WITH_EMPTY_TABLES
    empty_tables=<csv>
    backup_dir=<path>
    manifest=<path>

### 16.4 Mensaje backup WARN — SKIPPED_LOCKED

    [WARN][KTS Timescale Backup]
    host=<hostname>
    ts=<iso_utc>
    run_status=SKIPPED_LOCKED
    reason=maintenance_lock_busy_or_backup_lock_busy
    action=check_/var/lib/kts/backup_timescale.status

### 16.5 Mensaje cleanup WARN/CRIT

    [WARN][KTS Disk Cleanup]
    host=<hostname>
    ts=<iso_utc>
    max_fs_used_pct=<n>
    max_fs_path=<path>
    action=docker_system_prune_f+journalctl_rotate_vacuum_30d
    status=watch

    [CRITICAL][KTS Disk Cleanup]
    host=<hostname>
    ts=<iso_utc>
    max_fs_used_pct=<n>
    max_fs_path=<path>
    action=docker_system_prune_f+journalctl_rotate_vacuum_30d
    status=manual_intervention_required

### 16.6 Telegram retry policy

    attempts: 3
    timeout:  8s
    success:  response contains "ok":true
    max_message_bytes: 3900
    max_retry_sleep_s: 10

Reglas:

    Si retry_after está presente y 1 <= retry_after <= 10:
      sleep retry_after

    Si retry_after > 10:
      log telegram_skipped_retry_after_gt_cap=1
      no dormir más de 10s

    Si no hay retry_after:
      sleep 2s, 4s, 6s

Fallo de Telegram:

    No bloquea script.
    No cambia exit code.
    Debe quedar en log.

---

## 17. Cleanup script contract

Archivo:

    /root/kts-lab/ops/disk_cleanup.sh

### 17.1 Obligatorio

Debe:

    usar exec >> "$LOG" 2>&1
    usar lock global /var/lock/kts_maintenance.lock
    usar lock específico /var/lock/kts_disk_cleanup.lock
    escribir status running al inicio con tmp+mv
    detectar previous stale_interrupted
    validar PID por /proc/<pid>/cmdline
    medir filesystem antes
    medir Docker antes
    medir journald antes
    ejecutar docker system prune -f con timeout 900
    ejecutar journalctl --rotate --vacuum-time=30d con timeout 120
    medir filesystem después
    medir Docker después
    medir journald después
    calcular max_fs_used_pct y max_fs_path
    alertar si max_fs_used_pct >80 o >90
    escribir status final con tmp+mv

### 17.2 Métricas a registrar antes/después

    df -P /
    df -P /var/lib/containerd /var/lib/docker /root/kts-lab/data/pg /root/backups/timescale /var/log 2>/dev/null || true
    findmnt / /var/lib/containerd /var/lib/docker /root/kts-lab/data/pg /root/backups/timescale /var/log 2>/dev/null || true
    docker system df -v
    docker ps -a
    docker images
    du -sh /var/lib/containerd /var/lib/docker /var/log /root/backups/timescale 2>/dev/null || true
    journalctl --disk-usage

### 17.3 Umbral de alerta

Fuente de verdad para alerta:

    max_fs_used_pct entre:
      /
      /var/lib/docker
      /var/lib/containerd
      /root/kts-lab/data/pg
      /root/backups/timescale
      /var/log

Reglas:

    max_fs_used_pct > 90 → Telegram CRITICAL
    max_fs_used_pct > 80 → Telegram WARN
    max_fs_used_pct <= 80 → no alerta

### 17.4 Fallos

    docker system prune falla o timeout → FAILED=1
    journalctl falla o timeout → WARN, no FAILED
    Telegram falla → WARN, no FAILED
    lock ocupado → SKIPPED_LOCKED, exit 0

---

## 18. Backup script contract

Archivo:

    /root/kts-lab/ops/backup_timescale.sh

### 18.1 Obligatorio

Debe:

    usar exec >> "$LOG" 2>&1
    usar lock global /var/lock/kts_maintenance.lock
    usar lock específico /var/lock/kts_backup_timescale.lock
    si lock ocupado → SKIPPED_LOCKED + Telegram WARN
    escribir status running al inicio con tmp+mv
    detectar previous stale_interrupted
    validar PID por /proc/<pid>/cmdline
    validar contenedor ts_timescaledb activo
    validar pg_dump/pg_restore/psql dentro del contenedor
    crear /root/backups/timescale/YYYY-MM-DD/
    crear manifest.txt schema v1.1
    limpiar /tmp/kts_*.dump dentro del contenedor antes de verificar
    registrar df -P /tmp dentro del contenedor
    iterar todas las tablas críticas
    no detener loop por fallo de una tabla
    usar pg_dump -Fc --strict-names
    usar timeout por tabla
    calcular RUN_STATUS
    alertar según RUN_STATUS
    ejecutar retención protegida solo si RUN_STATUS OK
    escribir status final con tmp+mv

### 18.2 Verificación por tabla

Para cada tabla:

    1. table_started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    2. to_regclass('public.<tabla>') IS NOT NULL
    3. SELECT COUNT(*) FROM public.<tabla>
    4. docker inspect -f '{{.Id}}' ts_timescaledb → container_id_before
    5. docker exec ts_timescaledb rm -f /tmp/kts_*.dump
    6. docker exec ts_timescaledb df -P /tmp → tmp_avail_kb_before
    7. timeout 1800 docker exec ... pg_dump -Fc --strict-names -t public.<tabla> > file.dump
    8. test -s file.dump
    9. sha256sum file.dump
    10. timeout 300 docker cp file.dump ts_timescaledb:/tmp/kts_<run_id>_<tabla>.dump
    11. docker exec test -s /tmp/kts_<run_id>_<tabla>.dump
    12. docker exec stat -c%s /tmp/kts_<run_id>_<tabla>.dump
    13. docker exec sha256sum /tmp/kts_<run_id>_<tabla>.dump
    14. comparar sha256 host vs contenedor
    15. timeout 300 docker exec pg_restore -l /tmp/kts_<run_id>_<tabla>.dump
    16. grep TABLE public <tabla>
    17. grep TABLE DATA public <tabla>
    18. docker inspect -f '{{.Id}}' ts_timescaledb → container_id_after
    19. validar container_id_before == container_id_after
    20. docker exec rm -f /tmp/kts_<run_id>_<tabla>.dump
    21. table_ended_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

### 18.3 Tabla vacía

Regla:

    rows_at_dump_time=0
    si ALLOW_EMPTY=1 y todas las verificaciones pasan:
      table_status=ok_empty

    si ALLOW_EMPTY=0:
      table_status=failed_semantic_empty

### 18.4 Run status final

Reglas:

    si todas las tablas failed:
      RUN_STATUS=FAILED_ALL

    si una o más tablas failed pero no todas:
      RUN_STATUS=PARTIAL_FAILED

    si ninguna falla y una o más ok_empty:
      RUN_STATUS=OK_WITH_EMPTY_TABLES

    si ninguna falla y ninguna ok_empty:
      RUN_STATUS=OK_ALL

### 18.5 Retención protegida

Solo correr si:

    RUN_STATUS=OK_ALL
    o
    RUN_STATUS=OK_WITH_EMPTY_TABLES

No correr si:

    PARTIAL_FAILED
    FAILED_ALL
    SKIPPED_LOCKED

Constantes:

    RETENTION_DAYS=7
    RECENT_OK_MAX_AGE_HOURS=36
    MIN_OK_BACKUPS_TO_KEEP=2

Reglas:

    1. identificar backups OK por manifest con RUN_STATUS=OK_ALL u OK_WITH_EMPTY_TABLES
    2. no borrar los últimos MIN_OK_BACKUPS_TO_KEEP backups OK
    3. no borrar nada si newest OK tiene más de RECENT_OK_MAX_AGE_HOURS
    4. solo borrar directorios con mtime +RETENTION_DAYS que NO estén protegidos
    5. directorios sin manifest o con manifest failed pueden borrarse solo si no comprometen los últimos backups OK protegidos

Comando base permitido, después de aplicar protección:

    find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -mtime +7 -print -exec rm -rf {} \;

---

## 19. Cron

Editar root crontab:

    crontab -e

Líneas exactas:

    # KTS — Timescale critical tables backup
    0 2 * * * /root/kts-lab/ops/backup_timescale.sh

    # KTS — Docker/containerd disk cleanup
    0 3 * * 0 /root/kts-lab/ops/disk_cleanup.sh

Regla:

    No redirigir stdout/stderr en cron.
    Los scripts son dueños del logging con exec >> "$LOG" 2>&1.

Verificación:

    crontab -l | grep -E 'backup_timescale|disk_cleanup'

Verificación de ejecución:

    cat /var/lib/kts/backup_timescale.status
    cat /var/lib/kts/disk_cleanup.status

Cron logs:

    journalctl -u cron --since "24 hours ago" --no-pager | grep -E 'backup_timescale|disk_cleanup' || true
    grep -E 'backup_timescale|disk_cleanup|CRON' /var/log/syslog 2>/dev/null | tail -100 || true

---

## 20. Logrotate

Archivo:

    /etc/logrotate.d/tradeseeker

Contenido:

    /var/log/tradeseeker.log {
        daily
        rotate 14
        compress
        delaycompress
        missingok
        notifempty
        copytruncate
    }

Verificación:

    logrotate -d /etc/logrotate.d/tradeseeker

Prueba forzada:

    logrotate -f /etc/logrotate.d/tradeseeker
    ls -lh /var/log/tradeseeker*

Justificación:

    copytruncate se acepta porque no está verificado que tradeseeker soporte reopen de descriptor.
    La pérdida de líneas durante la ventana copy→truncate es riesgo residual aceptado.
    Manifest y status son fuente primaria de auditoría para backup/cleanup.

---

## 21. Probes obligatorios pre-instalación

### Probe A — Peso real Docker/containerd

    du -sh /var/lib/docker /var/lib/containerd 2>/dev/null || true
    docker system df -v
    df -h /

Acción:

    Si Docker/containerd pesan mucho → instalar cleanup.
    Si Docker no muestra reclaimable pero du pesa mucho → instalar igual, con riesgo residual.

---

### Probe B — Bind mount real

    docker inspect ts_timescaledb --format '{{json .Mounts}}'

Acción:

    Debe aparecer Type=bind y Source=/root/kts-lab/data/pg.
    Si no aparece → parar instalación.

---

### Probe C — Tablas críticas existen

    docker exec ts_timescaledb psql -U postgres -d tsdb -Atc "
    SELECT tablename
    FROM pg_tables
    WHERE schemaname='public'
    ORDER BY tablename;
    "

Acción:

    Si faltan tablas críticas → decidir si reconstruir schema antes o aceptar alertas ERROR.
    Por defecto: si faltan tablas críticas, el backup quedará PARTIAL_FAILED/FAILED_ALL.

---

### Probe D — pg_dump/pg_restore disponibles

    docker exec ts_timescaledb sh -lc 'command -v pg_dump; command -v pg_restore; command -v psql; pg_dump --version; pg_restore --version'

Acción:

    Si falta alguno → no instalar backup.

---

### Probe E — Telegram config

    grep -nE '^[[:space:]]*telegram:|^[[:space:]]*(token|chat_id):' /root/trade-seeker/config.yaml

Acción:

    Si no existe → scripts pueden instalarse, pero alertas externas no funcionarán.
    Debe quedar telegram_config_ok=0 en logs si falla.

---

### Probe F — Filesystems específicos

    df -P /
    df -P /var/lib/containerd /var/lib/docker /root/kts-lab/data/pg /root/backups/timescale /var/log 2>/dev/null || true
    findmnt / /var/lib/containerd /var/lib/docker /root/kts-lab/data/pg /root/backups/timescale /var/log 2>/dev/null || true

Acción:

    Si Docker, DB, backups o logs están en filesystem separado, registrar en auditoría.
    El umbral de alerta usa max_fs_used_pct, no solo /.

---

### Probe G — pg_dump tabla inexistente

    docker exec ts_timescaledb sh -lc '
    pg_dump -U postgres -d tsdb -Fc --strict-names -t public.__no_existe__ >/tmp/kts_no_existe_test.dump
    echo "rc=$?"
    ls -lh /tmp/kts_no_existe_test.dump 2>/dev/null || true
    rm -f /tmp/kts_no_existe_test.dump
    '

Acción:

    Registrar comportamiento real.
    El script no depende solo de pg_dump: primero usa to_regclass().
    --strict-names debe hacer fallar patrones sin match.

---

### Probe H — /tmp del contenedor

    docker exec ts_timescaledb sh -lc 'df -P /tmp; rm -f /tmp/kts_*.dump; df -P /tmp'

Acción:

    Si /tmp está lleno o no escribible → no instalar backup hasta resolver.

---

### Probe I — Cron activo

    systemctl status cron --no-pager
    journalctl -u cron --since "24 hours ago" --no-pager | tail -100 || true

Acción:

    Si cron no está activo → habilitar/iniciar cron antes de instalar crontab.

---

## 22. Setup inicial

    mkdir -p /root/kts-lab/ops
    mkdir -p /root/backups/timescale
    mkdir -p /var/lib/kts

    touch /var/log/kts_backup.log
    touch /var/log/kts_disk_cleanup.log

    chmod 700 /root/kts-lab/ops
    chmod 700 /root/backups/timescale
    chmod 700 /var/lib/kts

    chmod 640 /var/log/kts_backup.log
    chmod 640 /var/log/kts_disk_cleanup.log

    chown root:root /root/kts-lab/ops
    chown root:root /root/backups/timescale
    chown root:root /var/lib/kts
    chown root:root /var/log/kts_backup.log
    chown root:root /var/log/kts_disk_cleanup.log

---

## 23. Verificación post-instalación

### 23.1 Backup manual

    /root/kts-lab/ops/backup_timescale.sh
    echo "exit_code=$?"
    cat /var/lib/kts/backup_timescale.status
    tail -160 /var/log/kts_backup.log
    ls -lh /root/backups/timescale/$(date -u '+%Y-%m-%d')/
    cat /root/backups/timescale/$(date -u '+%Y-%m-%d')/manifest.txt

Criterio de aceptación:

    exit_code=0
    RUN_STATUS=OK_ALL o OK_WITH_EMPTY_TABLES
    todas las tablas table_status=ok u ok_empty
    ninguna tabla failed_semantic_empty
    sha256_match=true
    restore_list_ok=true
    table_entry_ok=true
    table_data_entry_ok=true
    container_id_stable=true

Si RUN_STATUS=OK_WITH_EMPTY_TABLES:

    revisar empty_tables
    validar que todas están permitidas por ALLOW_EMPTY=1

---

### 23.2 Cleanup manual

    /root/kts-lab/ops/disk_cleanup.sh
    echo "exit_code=$?"
    cat /var/lib/kts/disk_cleanup.status
    tail -160 /var/log/kts_disk_cleanup.log
    df -h /

Criterio de aceptación:

    exit_code=0
    run_status=ok o skipped_locked si lock ocupado
    max_fs_used_pct registrado
    max_fs_path registrado
    si max_fs_used_pct >80 debe haber intento Telegram WARN/CRIT

---

### 23.3 Cron

    crontab -l | grep -E 'backup_timescale|disk_cleanup'

Debe mostrar:

    0 2 * * * /root/kts-lab/ops/backup_timescale.sh
    0 3 * * 0 /root/kts-lab/ops/disk_cleanup.sh

---

### 23.4 Logrotate

    logrotate -d /etc/logrotate.d/tradeseeker
    logrotate -f /etc/logrotate.d/tradeseeker
    ls -lh /var/log/tradeseeker*

---

## 24. Criterios de aceptación final

La implementación queda aceptada solo si:

    [ ] Probes A-I ejecutados y resultados registrados.
    [ ] /root/kts-lab/ops existe con permisos 700.
    [ ] /root/backups/timescale existe con permisos 700.
    [ ] /var/lib/kts existe con permisos 700.
    [ ] disk_cleanup.sh existe, root:root, 750.
    [ ] backup_timescale.sh existe, root:root, 750.
    [ ] backup manual termina exit 0.
    [ ] manifest contiene schema=v1.1.
    [ ] manifest contiene RUN_STATUS.
    [ ] manifest tiene línea por cada tabla crítica.
    [ ] cada dump tiene size > 0.
    [ ] sha256 host == sha256 contenedor.
    [ ] pg_restore -l pasa por cada dump.
    [ ] table_entry_ok=true por cada dump aceptado.
    [ ] table_data_entry_ok=true por cada dump aceptado.
    [ ] container_id_stable=true por cada dump aceptado.
    [ ] status file backup queda state=finished.
    [ ] status file backup contiene run_status.
    [ ] cleanup manual termina exit 0 o skipped_locked justificado.
    [ ] status file cleanup queda state=finished.
    [ ] status file cleanup contiene max_fs_used_pct y max_fs_path.
    [ ] crontab root contiene dos líneas exactas.
    [ ] /etc/logrotate.d/tradeseeker existe.
    [ ] logrotate -d no falla.
    [ ] No existe uso de --volumes.
    [ ] No existe uso de docker system prune -a.
    [ ] No existe rm -rf sobre /root/kts-lab/data/pg.
    [ ] No existe DROP/TRUNCATE/DELETE automático.

---

## 25. Runbook de fallos

### 25.1 Backup PARTIAL_FAILED

Acción:

    cat /var/lib/kts/backup_timescale.status
    cat /root/backups/timescale/$(date -u '+%Y-%m-%d')/manifest.txt
    tail -200 /var/log/kts_backup.log
    docker ps --format 'table {{.Names}}\t{{.Status}}'
    docker exec ts_timescaledb psql -U postgres -d tsdb -c "\dt public.*"

Interpretación:

    Una o más tablas fallaron.
    No se ejecutó retención.
    Se conserva último backup bueno.

---

### 25.2 Backup FAILED_ALL

Acción:

    docker ps
    docker logs ts_timescaledb --tail 100
    docker exec ts_timescaledb psql -U postgres -d tsdb -c "SELECT now();"
    df -h /
    df -h /root/backups/timescale /root/kts-lab/data/pg /var/lib/containerd /var/lib/docker 2>/dev/null || true

Interpretación:

    Probable problema de contenedor, DB, disco, permisos o herramientas pg faltantes.

---

### 25.3 Backup OK_WITH_EMPTY_TABLES

Acción:

    grep '^table=' /root/backups/timescale/$(date -u '+%Y-%m-%d')/manifest.txt | grep 'table_status=ok_empty'
    cat /root/backups/timescale/$(date -u '+%Y-%m-%d')/manifest.txt | grep '^RUN_STATUS='

Interpretación:

    El backup es técnicamente verificable, pero una o más tablas permitidas están vacías.
    Validar si es esperado por fase del sistema.

---

### 25.4 Backup failed_semantic_empty

Acción:

    grep 'failed_semantic_empty' /root/backups/timescale/$(date -u '+%Y-%m-%d')/manifest.txt
    docker exec ts_timescaledb psql -U postgres -d tsdb -c "SELECT COUNT(*) FROM signals;"
    docker exec ts_timescaledb psql -U postgres -d tsdb -c "SELECT COUNT(*) FROM candidate_events;"
    docker exec ts_timescaledb psql -U postgres -d tsdb -c "SELECT COUNT(*) FROM event_features_pti;"

Interpretación:

    Tabla que no debería estar vacía está en cero.
    Esto puede indicar reinicialización, truncado o schema recién reconstruido sin datos.

---

### 25.5 Backup SKIPPED_LOCKED

Acción:

    cat /var/lib/kts/backup_timescale.status
    ps aux | grep -E 'backup_timescale|disk_cleanup' | grep -v grep
    tail -100 /var/log/kts_backup.log
    tail -100 /var/log/kts_disk_cleanup.log

Interpretación:

    El backup no corrió porque otro mantenimiento tenía el lock global o específico.
    Si ocurre repetidamente, revisar duración de cleanup/backup.

---

### 25.6 Cleanup deja disco >80%

Acción:

    df -h /
    df -h /var/lib/containerd /var/lib/docker /root/kts-lab/data/pg /root/backups/timescale /var/log 2>/dev/null || true
    du -sh /var/lib/containerd /var/lib/docker /var/log /root/backups/timescale 2>/dev/null
    docker system df -v
    journalctl --disk-usage

Interpretación:

    Prune no liberó suficiente o el peso está en backups/logs/datos activos.
    No borrar containerd manualmente sin revisión.

---

### 25.7 Status queda running

Acción:

    cat /var/lib/kts/backup_timescale.status
    cat /var/lib/kts/disk_cleanup.status
    ps -p <pid>
    tr '\0' ' ' < /proc/<pid>/cmdline 2>/dev/null || true

Interpretación:

    Si PID no vive o cmdline no coincide, la próxima corrida debe registrar stale_interrupted.

---

## 26. Riesgos residuales aceptados

### R1 — No hay restore real diario

El backup se verifica por archive/listado/checksum y metadata semántica, pero no se restaura diariamente en DB temporal.

Aceptación:

    Aceptado por simplicidad y costo operativo.
    Pendiente futuro: restore test semanal o backup remoto.

---

### R2 — Backup local no protege contra pérdida total del VPS

Aceptación:

    Este SoT protege contra truncados, errores locales y reinicialización parcial.
    No protege contra pérdida total de disco/VPS.
    Pendiente futuro: backup externo.

---

### R3 — copytruncate puede perder líneas

Aceptación:

    Riesgo aceptado porque tradeseeker.log no es fuente primaria del dataset.
    Manifest y status son fuente primaria de auditoría.

---

### R4 — Extractor YAML es limitado

Aceptación:

    Se acepta porque no se permite jq/Python.
    Si config cambia, se loggea telegram_config_missing.
    Formato soportado queda documentado.

---

### R5 — Retención protegida puede permitir crecimiento de disco

Aceptación:

    Preferimos preservar último backup bueno antes que liberar espacio ciegamente.
    Cleanup alertará si disco supera umbrales.

---

### R6 — Backup por tabla no garantiza snapshot global cross-table

Aceptación:

    Cada tabla tiene dump consistente, pero el conjunto es secuencial.
    Para disaster recovery local es aceptable.
    Para reconstrucción analítica estricta puede requerir reconciliación.

---

### R7 — Docker prune puede afectar velocidad de rollback

Aceptación:

    docker system prune -f puede borrar build cache e imágenes dangling.
    No borra contenedores activos ni volúmenes.
    Riesgo aceptado frente al riesgo mayor de disco lleno.

---

## 27. Handoff para Claude

Implementar exactamente este SoT.

Orden:

    1. Ejecutar probes A-I.
    2. Reportar resultados.
    3. Crear directorios/permisos.
    4. Crear disk_cleanup.sh según contrato v1.1.
    5. Crear backup_timescale.sh según contrato v1.1.
    6. Instalar crontab root exacto.
    7. Crear /etc/logrotate.d/tradeseeker.
    8. Ejecutar backup manual.
    9. Ejecutar cleanup manual.
    10. Verificar status files, manifests, logs y crontab.
    11. No modificar decisiones sin reportar conflicto.

No permitido:

    no usar docker system prune -a
    no usar --volumes
    no instalar pg_dump en host
    no usar jq
    no usar Python
    no limpiar /var/lib/containerd manualmente
    no cambiar a /etc/cron.d
    no borrar backups si la corrida actual falla
    no borrar los últimos 2 backups OK
    no hacer restore automático
    no ejecutar DROP/TRUNCATE/DELETE

---

## 28. Estado final

    SoT listo para implementación.
    Diseño cerrado.
    Puntos abiertos: ninguno.
    Puntos que requieren probe antes de instalar: A-I.

✅ Fin — KTS Cron Limpieza + Backups TimescaleDB SoT v1.1