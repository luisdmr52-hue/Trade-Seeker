"""
proxy_pool.py — NordVPN SOCKS5 Multi-Slot Proxy Pool
Kryptos Trade Seeker · F1 · v2.0

Evolución de proxy_watchdog.py v1.2 hacia un pool de N proxies simultáneos.

Responsabilidades:
  1. Mantiene N slots de proxy activos y validados simultáneamente.
  2. Cada slot es sticky por consumidor (benthos, confirmer, btc_regime).
  3. Dos standbys pre-validados para failover en ~0ms.
  4. IP directa del VPS como fallback soberano con cooldown de ban.
  5. Health score continuo por slot — failover proactivo antes del crash.
  6. Ban tracking global — hosts baneados por Binance nunca se reasignan.
  7. Circuit breaker por slot con backoff exponencial independiente.
  8. NordVPN API real-time para selección por menor carga.
  9. Garantía de no colisión: ningún slot usa el mismo host que otro.
 10. Hereda validación SOCKS5 TCP handshake, write atómico, estado persistente.

API pública:
    pool = ProxyPool(nord_user=..., nord_pass=..., env_file=..., ...)
    pool.start()
    pool.enable_direct_ip()          # activar IP directa del VPS

    url = pool.acquire("confirmer")  # "socks5://u:p@host:1080" o None
    pool.release("confirmer", success=True)
    pool.report_ban("confirmer")     # Binance rechazó esta IP

Fixes heredados de v1.2: #1–#14
Fixes nuevos v2.0: #15 (slots independientes), #16 (ban tracking),
                   #17 (score decay), #18 (direct IP cooldown),
                   #19 (standby pre-validation), #20 (lock granular)
"""

import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set

import requests as _requests_module

# ---------------------------------------------------------------------------
# Constantes de configuración
# ---------------------------------------------------------------------------

EURO_COUNTRIES: List[str] = [
    "Netherlands", "Germany", "Belgium", "Switzerland", "France",
    "Sweden", "Denmark", "Austria", "Luxembourg", "Norway",
    "Finland", "Czechia", "Poland", "United Kingdom",
]

TOP_N_SERVERS:              int   = 20    # pool de candidatos desde NordVPN API
STALE_THRESHOLD_S:          int   = 60    # segundos sin crecimiento = Benthos muerto
MONITOR_INTERVAL_S:         int   = 30    # frecuencia del loop principal
SERVER_REFRESH_INTERVAL_S:  int   = 300   # refresh NordVPN API cada 5min
SERVER_LIST_MAX_AGE_S:      int   = 1800  # stale si >30min sin refresh (loggea WARN)
MAX_ROTATIONS_BEFORE_OPEN:  int   = 5     # rotaciones antes de abrir circuit breaker
CIRCUIT_BREAK_BASE_S:       int   = 600   # 10min base
CIRCUIT_BREAK_MAX_S:        int   = 3600  # 1h máximo
PROXY_VALIDATE_TIMEOUT_S:   float = 5.0   # timeout handshake SOCKS5
MAX_VALIDATE_ATTEMPTS:      int   = TOP_N_SERVERS
BENTHOS_VERIFY_TIMEOUT_S:   int   = 45
BENTHOS_VERIFY_POLL_S:      int   = 5
DOCKER_TIMEOUT_S:           int   = 60

# Health score
SCORE_INIT:         float = 1.0
SCORE_ON_FAILURE:   float = -0.3   # penalización por fallo
SCORE_ON_SUCCESS:   float = 0.1    # recuperación por éxito
SCORE_PROACTIVE_FAILOVER: float = 0.4  # umbral para failover proactivo

# IP directa del VPS
DIRECT_BAN_CONSECUTIVE_FAILURES: int = 3     # fallos en ventana para detectar ban
DIRECT_BAN_WINDOW_S:             int = 60    # ventana de detección
DIRECT_BAN_COOLDOWN_S:           int = 259200  # 72h de cooldown tras ban detectado

# Ban tracking global (hosts baneados por Binance)
BAN_EXPIRY_S: int = 86400  # 24h antes de intentar reusar un host baneado

NORD_API_URL: str = (
    "https://api.nordvpn.com/v1/servers"
    "?filters[servers_technologies][identifier]=socks"
    "&filters[servers_groups][identifier]=legacy_socks5_proxy"
    "&limit=200"
)

_METRICS_RE = re.compile(
    r'^input_received\{[^}]*path="root\.input"[^}]*\}\s+(\d+)',
    re.MULTILINE,
)

# Estados del circuit breaker
_CB_CLOSED    = "closed"
_CB_OPEN      = "open"
_CB_HALF_OPEN = "half_open"

# Labels de standbys internos — no expuestos vía acquire()
_STANDBY_LABELS = ("_standby_0", "_standby_1")

_STATE_FILE = "/tmp/kts_proxy_state.json"


# ---------------------------------------------------------------------------
# SlotState — estado de un slot individual
# ---------------------------------------------------------------------------

@dataclass
class SlotState:
    """Estado completo de un slot de proxy."""

    label:      str
    slot_type:  str   # "nordvpn" | "direct" | "standby"

    # Host activo (None = sin proxy asignado)
    host:       Optional[str] = None

    # Health score [0.0 – 1.0]
    score:      float = SCORE_INIT

    # Circuit breaker propio del slot
    cb_state:           str   = _CB_CLOSED
    cb_open_until:      float = 0.0
    cb_break_duration:  int   = CIRCUIT_BREAK_BASE_S
    cb_consecutive:     int   = 0

    # Función de restart (solo slot "benthos")
    restart_fn: Optional[Callable] = field(default=None, repr=False)

    # Monitor de Benthos (solo slot "benthos")
    last_input_received:    int   = -1
    last_input_ts:          float = 0.0
    benthos_ever_received:  bool  = False

    # IP directa: tracking de fallos recientes para detección de ban
    direct_failure_times: List[float] = field(default_factory=list)
    direct_banned_until:  float       = 0.0

    # Lock propio — operaciones sobre este slot no bloquean otros
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


# ---------------------------------------------------------------------------
# ProxyPool
# ---------------------------------------------------------------------------

class ProxyPool:
    """
    Pool de proxies NordVPN SOCKS5 con slots sticky por consumidor.

    Uso básico:
        pool = ProxyPool(
            nord_user="service_user",
            nord_pass="service_pass",
            env_file="/root/kts-lab/compose/.env",
            benthos_metrics_url="http://localhost:4199/metrics",
            compose_dir="/root/kts-lab/compose",
            log_fn=log,
        )
        pool.start()
        pool.enable_direct_ip()

        # En aggtrade_confirmer al abrir cada WS:
        url = pool.acquire("confirmer")
        # ... usar url ...
        pool.release("confirmer", success=True)

        # Si Binance rechaza explícitamente la IP:
        pool.report_ban("confirmer")
    """

    def __init__(
        self,
        nord_user:           str,
        nord_pass:           str,
        env_file:            str,
        benthos_metrics_url: str  = "http://localhost:4199/metrics",
        compose_dir:         str  = "/root/kts-lab/compose",
        log_fn                    = None,
        base_yaml_path:      str  = "/root/kts-lab/compose/config/benthos_spot_base.yaml",
        runtime_yaml_path:   str  = "/root/kts-lab/compose/config/benthos_spot.yaml",
    ):
        self._user = nord_user
        self._pass = nord_pass
        self.env_file            = env_file
        self.benthos_metrics_url = benthos_metrics_url
        self.compose_dir         = compose_dir
        self._log                = log_fn or self._default_log
        self._base_yaml_path     = base_yaml_path
        self._runtime_yaml_path  = runtime_yaml_path

        # Lock exclusivo para _server_list — independiente de slots
        self._server_list_lock:  threading.Lock = threading.Lock()
        self._server_list:       List[Dict]     = []
        self._last_server_refresh: float        = 0.0

        # Slots activos — keyed by label
        # Benthos y standbys se crean en __init__; confirmer y btc_regime
        # se crean al primer acquire(); direct se crea vía enable_direct_ip()
        self._slots: Dict[str, SlotState] = {}
        self._slots_lock = threading.Lock()  # protege el dict mismo (no el contenido)

        # Ban tracking global: host → expiry_timestamp
        self._banned_hosts:      Dict[str, float] = {}
        self._banned_hosts_lock: threading.Lock   = threading.Lock()

        # Hosts actualmente asignados a algún slot (para garantía de no colisión)
        # Se mantiene sincronizado con _slots
        self._assigned_hosts: Set[str] = set()

        # Docker
        self._docker_cmd: Optional[List[str]] = None

        # Contadores públicos
        self.rotation_count: int = 0

        # Flag de arranque
        self._started = False

        # Inicializar slot benthos con restart_fn
        self._init_slot("benthos", "nordvpn", restart_fn=self._do_benthos_restart)

        # Inicializar standbys
        for label in _STANDBY_LABELS:
            self._init_slot(label, "standby")

        # Cargar estado persistido
        self._load_state()

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def start(self):
        """Arranca el pool como daemon thread. Idempotente."""
        if self._started:
            return
        self._started = True
        t = threading.Thread(target=self._run, daemon=True, name="proxy-pool")
        t.start()
        self._log("POOL", "proxy pool v2.0 started")

    def acquire(self, label: str) -> Optional[str]:
        """
        Retorna la URL del proxy para el label dado.
        Formato: "socks5://user:pass@host:1080"
        Retorna None si no hay proxy disponible — el caller conecta directo.

        Si el label no existe, lo crea y asigna un host en background.
        Las credenciales nunca aparecen en logs.
        """
        with self._slots_lock:
            if label not in self._slots:
                self._init_slot(label, "nordvpn")

        slot = self._slots[label]
        with slot.lock:
            if not slot.host:
                return None
            if slot.slot_type == "direct":
                return None  # IP directa: None = conexión directa sin proxy
            return f"socks5://{self._user}:{self._pass}@{slot.host}:1080"

    def release(self, label: str, success: bool):
        """
        Reporta el resultado de usar el proxy del slot.
        success=True  → incrementa score
        success=False → decrementa score, failover proactivo si score < umbral
        """
        with self._slots_lock:
            if label not in self._slots:
                return

        slot = self._slots[label]
        with slot.lock:
            if success:
                slot.score = min(SCORE_INIT, slot.score + SCORE_ON_SUCCESS)
                # Éxito también resetea contador del circuit breaker del slot
                slot.cb_consecutive = 0
            else:
                slot.score = max(0.0, slot.score + SCORE_ON_FAILURE)
                slot.cb_consecutive += 1
                self._log(
                    "POOL",
                    f"slot [{label}] failure — score={slot.score:.2f} "
                    f"cb_consec={slot.cb_consecutive}"
                )

            needs_failover = (
                not success and
                slot.score < SCORE_PROACTIVE_FAILOVER and
                slot.slot_type != "direct"
            )

        if needs_failover:
            self._log("POOL", f"slot [{label}] score below threshold — proactive failover")
            threading.Thread(
                target=self._failover,
                args=(label,),
                daemon=True,
                name=f"failover-{label}",
            ).start()

    def report_ban(self, label: str):
        """
        El consumidor detectó que Binance rechazó esta IP específicamente.
        Marca el host como baneado 24h globalmente y hace failover inmediato.
        A diferencia de release(success=False), un ban es definitivo para ese host.
        """
        with self._slots_lock:
            if label not in self._slots:
                return

        slot = self._slots[label]
        with slot.lock:
            host = slot.host

        if not host:
            return

        expiry = time.time() + BAN_EXPIRY_S
        with self._banned_hosts_lock:
            self._banned_hosts[host] = expiry

        self._log("POOL", f"slot [{label}] BAN reported for host={host} — banned for {BAN_EXPIRY_S//3600}h")

        # Failover inmediato en thread separado
        threading.Thread(
            target=self._failover,
            args=(label,),
            daemon=True,
            name=f"ban-failover-{label}",
        ).start()

    def enable_direct_ip(self):
        """
        Activa el slot de IP directa del VPS.
        Llamar cuando el ban de Binance haya expirado o desde el principio
        si la IP nunca fue baneada.

        El slot "direct" usa None como host — acquire("direct") retorna None,
        lo cual significa conexión directa sin proxy. El pool trackea su salud
        de forma independiente.
        """
        with self._slots_lock:
            if "direct" in self._slots:
                self._log("POOL", "direct IP slot already enabled")
                return
            self._init_slot("direct", "direct")

        self._log("POOL", "direct IP slot enabled — VPS IP active in pool")

    def disable_direct_ip(self, reason: str = ""):
        """Desactiva el slot de IP directa (ban detectado externamente)."""
        with self._slots_lock:
            if "direct" not in self._slots:
                return
            del self._slots["direct"]

        self._log("POOL", f"direct IP slot disabled — reason: {reason or 'manual'}")

    def get_status(self) -> dict:
        """Estado completo del pool para logging/debug. Sin credenciales."""
        with self._slots_lock:
            slot_labels = list(self._slots.keys())

        slots_status = {}
        for label in slot_labels:
            slot = self._slots[label]
            with slot.lock:
                slots_status[label] = {
                    "host":       slot.host,
                    "type":       slot.slot_type,
                    "score":      round(slot.score, 2),
                    "cb_state":   slot.cb_state,
                    "cb_consec":  slot.cb_consecutive,
                }

        with self._server_list_lock:
            pool_size     = len(self._server_list)
            last_refresh  = self._last_server_refresh
            list_age_s    = time.time() - last_refresh if last_refresh else None

        with self._banned_hosts_lock:
            active_bans = sum(
                1 for exp in self._banned_hosts.values()
                if exp > time.time()
            )

        return {
            "slots":          slots_status,
            "server_pool":    pool_size,
            "list_age_s":     round(list_age_s, 0) if list_age_s else None,
            "active_bans":    active_bans,
            "rotation_count": self.rotation_count,
        }

    # -----------------------------------------------------------------------
    # Internal — inicialización de slots
    # -----------------------------------------------------------------------

    def _init_slot(
        self,
        label:      str,
        slot_type:  str,
        restart_fn: Optional[Callable] = None,
    ):
        """Crea un SlotState vacío para el label dado. No asigna host aún."""
        slot = SlotState(
            label=label,
            slot_type=slot_type,
            restart_fn=restart_fn,
        )
        # Ya estamos dentro de _slots_lock cuando se llama desde __init__,
        # pero _init_slot también se llama desde acquire() con el lock tomado.
        self._slots[label] = slot

    # -----------------------------------------------------------------------
    # Internal — ciclo principal
    # -----------------------------------------------------------------------

    def _run(self):
        """Thread principal del pool."""
        self._docker_cmd = self._detect_docker_cmd()
        self._log("POOL", f"docker command: {' '.join(self._docker_cmd)}")

        # Construir pool inicial desde NordVPN API
        self._refresh_server_list()

        # Asignar host a benthos y activar
        self._assign_slot("benthos")
        slot = self._slots["benthos"]
        with slot.lock:
            has_host = bool(slot.host)

        if has_host:
            self._do_benthos_restart()
            ok = self._verify_benthos_up()
            if not ok:
                self._log("POOL", "WARNING: Benthos did not come up after initial start")
        else:
            self._log("POOL", "WARNING: no valid proxy on startup — Benthos running without proxy")

        # Pre-validar standbys en background
        threading.Thread(
            target=self._assign_standbys,
            daemon=True,
            name="standby-init",
        ).start()

        # Loop principal
        while True:
            try:
                time.sleep(MONITOR_INTERVAL_S)
                self._monitor_tick()
            except Exception as e:
                self._log("POOL", f"pool loop error: {type(e).__name__}: {e}")
                time.sleep(10)

    def _monitor_tick(self):
        """Una iteración del loop de monitoreo."""
        now = time.time()

        # Refresh periódico del server_list
        with self._server_list_lock:
            last_refresh = self._last_server_refresh
            list_age_s   = now - last_refresh if last_refresh else float("inf")

        if list_age_s > SERVER_REFRESH_INTERVAL_S:
            self._refresh_server_list()
        elif list_age_s > SERVER_LIST_MAX_AGE_S:
            self._log("POOL", f"WARNING: server list stale {list_age_s:.0f}s — API may be unreachable")

        # Limpiar bans expirados
        self._purge_expired_bans()

        # Chequear cada slot activo de forma independiente
        with self._slots_lock:
            labels = [
                l for l in self._slots
                if not l.startswith("_standby")
            ]

        for label in labels:
            try:
                self._check_slot(label)
            except Exception as e:
                self._log("POOL", f"slot [{label}] check error: {type(e).__name__}: {e}")

        # Verificar que los standbys siguen pre-validados
        self._maintain_standbys()

    def _check_slot(self, label: str):
        """
        Verifica el estado de un slot individual.
        Para "benthos": usa métricas Prometheus.
        Para "direct": detecta ban por acumulación de fallos.
        Para el resto: no hay monitoreo activo — depende de release().
        """
        slot = self._slots.get(label)
        if not slot:
            return

        with slot.lock:
            cb_state   = slot.cb_state
            cb_until   = slot.cb_open_until
            slot_type  = slot.slot_type

        # Circuit breaker del slot
        if cb_state == _CB_OPEN:
            if time.time() >= cb_until:
                with slot.lock:
                    slot.cb_state = _CB_HALF_OPEN
                self._log("POOL", f"slot [{label}] circuit → half_open")
            else:
                remaining = cb_until - time.time()
                self._log("POOL", f"slot [{label}] circuit OPEN — {remaining:.0f}s remaining")
            return

        if cb_state == _CB_HALF_OPEN:
            self._log("POOL", f"slot [{label}] circuit half_open — attempting recovery")
            self._failover(label)
            return

        # Monitoreo específico por tipo
        if label == "benthos":
            if not self._benthos_is_alive(slot):
                self._log("POOL", "Benthos stale — initiating rotation")
                self._failover("benthos")

        elif slot_type == "direct":
            self._check_direct_slot(slot)

    def _check_direct_slot(self, slot: SlotState):
        """
        Detecta ban de Binance en la IP directa por acumulación de fallos
        recientes. Si se detecta, desactiva el slot automáticamente.
        """
        now = time.time()
        with slot.lock:
            # Limpiar fallos fuera de la ventana
            slot.direct_failure_times = [
                t for t in slot.direct_failure_times
                if now - t < DIRECT_BAN_WINDOW_S
            ]
            failures_in_window = len(slot.direct_failure_times)
            banned_until = slot.direct_banned_until

        if banned_until > now:
            remaining = banned_until - now
            self._log(
                "POOL",
                f"direct IP banned — {remaining/3600:.1f}h remaining"
            )
            return

        if failures_in_window >= DIRECT_BAN_CONSECUTIVE_FAILURES:
            ban_until = now + DIRECT_BAN_COOLDOWN_S
            with slot.lock:
                slot.direct_banned_until = ban_until
                slot.direct_failure_times.clear()

            self._log(
                "POOL",
                f"direct IP ban suspected ({failures_in_window} failures "
                f"in {DIRECT_BAN_WINDOW_S}s) — cooling down "
                f"{DIRECT_BAN_COOLDOWN_S//3600}h"
            )

    # -----------------------------------------------------------------------
    # Internal — gestión de slots y failover
    # -----------------------------------------------------------------------

    def _assign_slot(self, label: str) -> bool:
        """
        Busca y asigna el mejor host disponible al slot indicado.
        Garantiza que no colisiona con hosts ya asignados ni baneados.
        Retorna True si asignó un host, False si no encontró ninguno válido.
        """
        slot = self._slots.get(label)
        if not slot:
            return False

        # Si el slot es "direct", no necesita host NordVPN
        with slot.lock:
            if slot.slot_type == "direct":
                slot.host  = None  # None = conexión directa
                slot.score = SCORE_INIT
                return True

        host = self._next_available_host(exclude_label=label)
        if not host:
            self._log("POOL", f"slot [{label}] no valid host available")
            return False

        with slot.lock:
            old_host   = slot.host
            slot.host  = host
            slot.score = SCORE_INIT
            slot.cb_state      = _CB_CLOSED
            slot.cb_consecutive = 0

        # Actualizar assigned_hosts
        with self._slots_lock:
            if old_host:
                self._assigned_hosts.discard(old_host)
            self._assigned_hosts.add(host)

        # Si es benthos, actualizar .env
        if label == "benthos":
            proxy_url = f"socks5://{self._user}:{self._pass}@{host}:1080"
            self._update_env(proxy_url)

        self._log("POOL", f"slot [{label}] → host={host}")
        self._save_state()
        return True

    def _failover(self, label: str):
        """
        Ejecuta failover para un slot:
        1. Intenta promover un standby (failover ~0ms para NordVPN).
        2. Si no hay standby, busca directamente en el pool.
        3. Si el slot tiene restart_fn, la llama.
        4. Actualiza contadores y circuit breaker del slot.
        """
        slot = self._slots.get(label)
        if not slot:
            return

        self._log("POOL", f"slot [{label}] failover initiated")

        # Intentar promover standby primero
        promoted = self._promote_standby(label)

        if not promoted:
            # Sin standby disponible — asignar directamente desde pool
            self._log("POOL", f"slot [{label}] no standby available — assigning from pool")
            promoted = self._assign_slot(label)

        if not promoted:
            # Nada disponible — abrir circuit breaker del slot
            self._log("POOL", f"slot [{label}] all options exhausted — opening circuit breaker")
            self._open_slot_circuit(label)
            # Reponer standbys igualmente — pueden validarse más tarde
            threading.Thread(
                target=self._assign_standbys,
                daemon=True,
                name=f"standby-replenish-{label}",
            ).start()
            return

        # Ejecutar restart_fn si existe (solo benthos)
        with slot.lock:
            restart_fn = slot.restart_fn

        if restart_fn:
            try:
                restart_fn()
                ok = self._verify_benthos_up()
                if ok:
                    with slot.lock:
                        slot.cb_consecutive = 0
                    self.rotation_count += 1
                    self._log("POOL", f"slot [{label}] failover successful")
                else:
                    self._log("POOL", f"slot [{label}] restart_fn ran but service did not come up")
                    self._open_slot_circuit(label)
                    return
            except Exception as e:
                self._log("POOL", f"slot [{label}] restart_fn error: {type(e).__name__}: {e}")
                self._open_slot_circuit(label)
                return
        else:
            self.rotation_count += 1
            self._log("POOL", f"slot [{label}] failover successful (no restart needed)")

        # Reponer el standby consumido en background
        threading.Thread(
            target=self._assign_standbys,
            daemon=True,
            name=f"standby-replenish-{label}",
        ).start()

        self._save_state()

    def _assign_standbys(self):
        """
        Valida y pre-asigna hosts a los slots de standby vacíos.
        Corre en background — no bloquea ningún slot activo.
        """
        for label in _STANDBY_LABELS:
            slot = self._slots.get(label)
            if not slot:
                continue

            with slot.lock:
                has_host = bool(slot.host)

            if not has_host:
                self._assign_slot(label)

    def _maintain_standbys(self):
        """
        Verifica que los standbys siguen siendo válidos.
        Si un standby tiene host asignado pero el TCP handshake falla, lo limpia.
        """
        for label in _STANDBY_LABELS:
            slot = self._slots.get(label)
            if not slot:
                continue

            with slot.lock:
                host = slot.host

            if not host:
                # Standby vacío — intentar asignar
                threading.Thread(
                    target=self._assign_slot,
                    args=(label,),
                    daemon=True,
                    name=f"standby-fill-{label}",
                ).start()
                continue

            # Re-validar el host del standby
            if not self._validate_proxy(host):
                self._log("POOL", f"standby [{label}] host={host} no longer valid — clearing")
                with slot.lock:
                    slot.host = None
                with self._slots_lock:
                    self._assigned_hosts.discard(host)
                threading.Thread(
                    target=self._assign_slot,
                    args=(label,),
                    daemon=True,
                    name=f"standby-replace-{label}",
                ).start()

    def _promote_standby(self, target_label: str) -> bool:
        """
        Mueve el host de un standby al slot target.
        Retorna True si pudo promover, False si no hay standbys disponibles.
        """
        target_slot = self._slots.get(target_label)
        if not target_slot:
            return False

        for standby_label in _STANDBY_LABELS:
            standby = self._slots.get(standby_label)
            if not standby:
                continue

            with standby.lock:
                standby_host = standby.host

            if not standby_host:
                continue

            # Promover: mover host de standby a target
            with target_slot.lock:
                old_host         = target_slot.host
                target_slot.host = standby_host
                target_slot.score          = SCORE_INIT
                target_slot.cb_consecutive = 0
                target_slot.cb_state       = _CB_CLOSED

            with standby.lock:
                standby.host = None

            with self._slots_lock:
                if old_host:
                    self._assigned_hosts.discard(old_host)
                # standby_host ya estaba en assigned_hosts — no cambiar

            # Actualizar .env si es benthos
            if target_label == "benthos":
                proxy_url = f"socks5://{self._user}:{self._pass}@{standby_host}:1080"
                self._update_env(proxy_url)

            self._log(
                "POOL",
                f"standby [{standby_label}] promoted to [{target_label}] "
                f"host={standby_host}"
            )
            self._save_state()
            return True

        return False

    def _open_slot_circuit(self, label: str):
        """Abre el circuit breaker del slot con backoff exponencial."""
        slot = self._slots.get(label)
        if not slot:
            return

        with slot.lock:
            duration             = slot.cb_break_duration
            slot.cb_state        = _CB_OPEN
            slot.cb_open_until   = time.time() + duration
            slot.cb_consecutive  = 0
            slot.cb_break_duration = min(duration * 2, CIRCUIT_BREAK_MAX_S)

        self._log(
            "POOL",
            f"slot [{label}] circuit OPEN — {duration}s "
            f"(next={min(duration*2, CIRCUIT_BREAK_MAX_S)}s)"
        )
        self._save_state()

    # -----------------------------------------------------------------------
    # Internal — gestión del pool de servidores
    # -----------------------------------------------------------------------

    def _refresh_server_list(self):
        """
        Carga la lista estatica de hostnames SOCKS5 de NordVPN.

        NordVPN depreco el endpoint legacy_socks5_proxy de su API publica —
        ya no devuelve servidores SOCKS5. El acceso SOCKS5 se hace via
        hostnames estaticos oficiales documentados en support.nordvpn.com.

        Se usan 4 hostnames EU unicamente. IPs US (165.231.x, 185.184.x)
        geolocalizan en EEUU ante Binance, lo que puede causar redireccion
        a stream.binance.us. El fallback soberano es el slot "direct" (IP VPS).

        Orden de preferencia: NL especifico > NL generico > SE especifico > SE generico.
        La "carga" es estatica (0) — NordVPN balancea internamente.
        """
        static_servers = [
            {"hostname": "amsterdam.nl.socks.nordhold.net", "load": 0, "country": "Netherlands", "name": "NordVPN NL Amsterdam"},
            {"hostname": "nl.socks.nordhold.net",           "load": 0, "country": "Netherlands", "name": "NordVPN NL"},
            {"hostname": "stockholm.se.socks.nordhold.net", "load": 0, "country": "Sweden",      "name": "NordVPN SE Stockholm"},
            {"hostname": "se.socks.nordhold.net",           "load": 0, "country": "Sweden",      "name": "NordVPN SE"},
        ]

        with self._server_list_lock:
            self._server_list         = static_servers
            self._last_server_refresh = time.time()

        self._log(
            "POOL",
            f"server list loaded: {len(static_servers)} static EU hostnames "
            f"({', '.join(s['hostname'] for s in static_servers)})"
        )

    def _next_available_host(self, exclude_label: str = "") -> Optional[str]:
        """
        Retorna el host NordVPN de menor carga que:
        - No esté baneado (banned_hosts)
        - No esté asignado a otro slot (assigned_hosts)
        - Pase la validación SOCKS5 TCP handshake

        exclude_label: si el slot ya tenía un host, se libera antes de buscar.
        """
        # Liberar host actual del slot si existe
        slot = self._slots.get(exclude_label)
        if slot:
            with slot.lock:
                old_host = slot.host
            if old_host:
                with self._slots_lock:
                    self._assigned_hosts.discard(old_host)

        now = time.time()
        with self._server_list_lock:
            candidates = list(self._server_list)

        with self._banned_hosts_lock:
            banned = {h for h, exp in self._banned_hosts.items() if exp > now}

        with self._slots_lock:
            assigned = set(self._assigned_hosts)

        for server in candidates:
            host = server["hostname"]
            if host in banned:
                continue
            if host in assigned:
                continue
            if self._validate_proxy(host):
                with self._slots_lock:
                    self._assigned_hosts.add(host)
                return host
            else:
                self._log("POOL", f"validation failed: {host} (load={server['load']}%) — skipping")

        self._log("POOL", "WARNING: no valid unassigned host found in pool")
        return None

    def _validate_proxy(self, host: str) -> bool:
        """
        Validación activa via SOCKS5 TCP handshake mínimo (RFC 1928).
        No envía credenciales — solo verifica que el servidor responde.
        """
        try:
            sock = socket.create_connection((host, 1080), timeout=PROXY_VALIDATE_TIMEOUT_S)
            try:
                sock.sendall(b'\x05\x01\x02')
                resp = sock.recv(2)
            finally:
                sock.close()

            if len(resp) < 2:
                return False
            if resp[0] != 0x05:
                return False
            if resp[1] == 0xFF:
                return False
            return resp[1] in (0x00, 0x02)

        except Exception:
            return False

    def _purge_expired_bans(self):
        """Limpia entradas de ban expiradas del registro global."""
        now = time.time()
        with self._banned_hosts_lock:
            expired = [h for h, exp in self._banned_hosts.items() if exp <= now]
            for h in expired:
                del self._banned_hosts[h]
            if expired:
                self._log("POOL", f"purged {len(expired)} expired ban(s): {expired}")

    # -----------------------------------------------------------------------
    # Internal — Benthos (herencia de v1.2)
    # -----------------------------------------------------------------------

    def _do_benthos_restart(self):
        """
        1. Escribe benthos_spot.yaml (con o sin proxy_url segun slot activo).
        2. Reinicia ts_benthos_spot via Docker stop + up.

        proxy_url se inyecta ONLY si el slot benthos tiene host NordVPN activo.
        Si el slot esta en fallback directo (host=None), se usa la base sin cambios.
        Campo proxy_url vacio rompe gorilla/websocket — se omite completamente.
        """
        self._write_benthos_yaml()

        cmd = self._docker_cmd or ["docker", "compose"]
        try:
            self._log("POOL", "stopping ts_benthos_spot...")
            stop = subprocess.run(
                cmd + ["stop", "ts_benthos_spot"],
                cwd=self.compose_dir,
                capture_output=True,
                text=True,
                timeout=DOCKER_TIMEOUT_S,
            )
            if stop.returncode != 0:
                self._log("POOL", f"docker stop warning: {stop.stderr.strip()[:300]}")

            self._log("POOL", "starting ts_benthos_spot with new proxy...")
            up = subprocess.run(
                cmd + ["up", "-d", "--no-deps", "ts_benthos_spot"],
                cwd=self.compose_dir,
                capture_output=True,
                text=True,
                timeout=DOCKER_TIMEOUT_S,
            )
            if up.returncode == 0:
                self._log("POOL", "ts_benthos_spot started")
            else:
                self._log("POOL", f"docker up failed (rc={up.returncode}): {up.stderr.strip()[:300]}")

        except subprocess.TimeoutExpired:
            self._log("POOL", f"ERROR: docker command timed out ({DOCKER_TIMEOUT_S}s)")
        except Exception as e:
            self._log("POOL", f"docker restart error: {type(e).__name__}: {e}")

    def _write_benthos_yaml(self):
        """
        Genera benthos_spot.yaml desde la plantilla base.

        Si el slot benthos tiene host NordVPN activo, inserta:
            proxy_url: socks5://user:pass@host:1080
        inmediatamente despues de la linea que contiene 'url: wss://'.

        Si no hay proxy activo (fallback directo), copia la base sin modificar.
        Write atomico: tempfile -> os.replace para evitar YAML corrupto.
        """
        slot = self._slots.get("benthos")
        proxy_host = None
        if slot:
            with slot.lock:
                h  = slot.host
                st = slot.slot_type
            if h and st == "nordvpn":
                proxy_host = h

        try:
            with open(self._base_yaml_path, "r") as f:
                lines = f.readlines()
        except Exception as e:
            self._log("POOL", f"ERROR: cannot read base yaml {self._base_yaml_path}: {e}")
            return

        out_lines = []
        injected = False
        for line in lines:
            out_lines.append(line)
            if not injected and "url: wss://" in line:
                if proxy_host:
                    indent = len(line) - len(line.lstrip())
                    proxy_line = " " * indent + f"proxy_url: socks5://{self._user}:{self._pass}@{proxy_host}:1080\n"
                    out_lines.append(proxy_line)
                    injected = True

        if proxy_host and not injected:
            self._log("POOL", "WARNING: 'url: wss://' not found in base yaml — proxy_url NOT injected")

        runtime_dir = os.path.dirname(os.path.abspath(self._runtime_yaml_path))
        fd, tmp_path = tempfile.mkstemp(dir=runtime_dir, prefix=".benthos_spot_tmp_")
        try:
            with os.fdopen(fd, "w") as tmp_f:
                tmp_f.writelines(out_lines)
                tmp_f.flush()
                os.fsync(tmp_f.fileno())
            os.chmod(tmp_path, 0o644)
            os.replace(tmp_path, self._runtime_yaml_path)
            mode = f"proxy={proxy_host}" if proxy_host else "direct (no proxy_url)"
            self._log("POOL", f"benthos_spot.yaml written [{mode}]")
        except Exception as e:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            self._log("POOL", f"ERROR: failed to write runtime yaml: {type(e).__name__}: {e}")

    def _verify_benthos_up(self) -> bool:
        """
        Espera hasta BENTHOS_VERIFY_TIMEOUT_S verificando que input_received
        empieza a crecer. Retorna True si Benthos levantó correctamente.
        """
        self._log("POOL", f"verifying Benthos up (timeout={BENTHOS_VERIFY_TIMEOUT_S}s)...")
        deadline = time.time() + BENTHOS_VERIFY_TIMEOUT_S
        baseline = self._get_input_received()

        while time.time() < deadline:
            time.sleep(BENTHOS_VERIFY_POLL_S)
            current = self._get_input_received()

            if current is None:
                continue
            if baseline is None:
                baseline = current
                continue
            if current > baseline:
                self._log("POOL", f"Benthos up — input_received growing ({baseline}→{current})")
                slot = self._slots.get("benthos")
                if slot:
                    with slot.lock:
                        slot.last_input_received   = current
                        slot.last_input_ts         = time.time()
                        slot.benthos_ever_received = True
                return True

        self._log("POOL", f"Benthos did NOT come up within {BENTHOS_VERIFY_TIMEOUT_S}s")
        return False

    def _benthos_is_alive(self, slot: SlotState) -> bool:
        """
        Verifica si Benthos está recibiendo datos.
        Lógica heredada de v1.2 — sin evidencia = asumir vivo (no rotar).
        """
        try:
            resp = _requests_module.get(self.benthos_metrics_url, timeout=5)
            resp.raise_for_status()

            match = _METRICS_RE.search(resp.text)
            if not match:
                self._log("POOL", "WARNING: input_received not found in Benthos metrics")
                return True

            current = int(match.group(1))
            now     = time.time()

            with slot.lock:
                last    = slot.last_input_received
                last_ts = slot.last_input_ts
                ever    = slot.benthos_ever_received

            if current > last:
                with slot.lock:
                    slot.last_input_received   = current
                    slot.last_input_ts         = now
                    slot.benthos_ever_received = True
                    slot.cb_consecutive        = 0
                return True

            if last_ts == 0.0:
                with slot.lock:
                    slot.last_input_ts       = now
                    slot.last_input_received = current
                return True

            stale_s = now - last_ts
            if not ever and stale_s < STALE_THRESHOLD_S:
                return True
            if stale_s > STALE_THRESHOLD_S:
                self._log(
                    "POOL",
                    f"Benthos stale {stale_s:.0f}s | "
                    f"input_received={current} | ever_received={ever}"
                )
                return False

            return True

        except Exception as e:
            self._log("POOL", f"benthos metrics error: {type(e).__name__}: {e}")
            return True

    def _get_input_received(self) -> Optional[int]:
        """Lee el valor actual de input_received desde las métricas de Benthos."""
        try:
            resp = _requests_module.get(self.benthos_metrics_url, timeout=5)
            resp.raise_for_status()
            match = _METRICS_RE.search(resp.text)
            return int(match.group(1)) if match else None
        except Exception:
            return None

    # -----------------------------------------------------------------------
    # Internal — Docker
    # -----------------------------------------------------------------------

    def _detect_docker_cmd(self) -> List[str]:
        """Detecta docker compose (v2) o docker-compose (v1)."""
        if shutil.which("docker"):
            try:
                result = subprocess.run(
                    ["docker", "compose", "version"],
                    capture_output=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    self._log("POOL", "detected: docker compose (v2)")
                    return ["docker", "compose"]
            except Exception:
                pass

        if shutil.which("docker-compose"):
            self._log("POOL", "detected: docker-compose (v1)")
            return ["docker-compose"]

        self._log("POOL", "WARNING: could not detect docker — defaulting to 'docker compose'")
        return ["docker", "compose"]

    # -----------------------------------------------------------------------
    # Internal — .env y persistencia (herencia de v1.2)
    # -----------------------------------------------------------------------

    def _update_env(self, proxy_url: str):
        """
        Actualiza NORDVPN_PROXY en el .env del compose.
        Write atómico: tempfile → os.replace. El .env nunca queda corrupto.
        """
        try:
            try:
                with open(self.env_file, "r") as f:
                    lines = f.readlines()
            except FileNotFoundError:
                lines = []

            new_line  = f"NORDVPN_PROXY={proxy_url}\n"
            found     = False
            new_lines: List[str] = []

            for line in lines:
                if line.startswith("NORDVPN_PROXY="):
                    new_lines.append(new_line)
                    found = True
                else:
                    new_lines.append(line)

            if not found:
                new_lines.append(new_line)

            env_dir = os.path.dirname(os.path.abspath(self.env_file))
            fd, tmp_path = tempfile.mkstemp(dir=env_dir, prefix=".env_tmp_")
            try:
                with os.fdopen(fd, "w") as tmp_f:
                    tmp_f.writelines(new_lines)
                    tmp_f.flush()
                    os.fsync(tmp_f.fileno())
                os.replace(tmp_path, self.env_file)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

        except Exception as e:
            self._log("POOL", f"env update error: {type(e).__name__}: {e}")

    def _save_state(self):
        """
        Persiste el estado del pool en JSON.
        Write atómico. El pool sobrevive reinicios del bot.
        """
        try:
            with self._slots_lock:
                slot_labels = list(self._slots.keys())

            slots_data = {}
            for label in slot_labels:
                slot = self._slots[label]
                with slot.lock:
                    slots_data[label] = {
                        "host":              slot.host,
                        "slot_type":         slot.slot_type,
                        "score":             slot.score,
                        "cb_state":          slot.cb_state,
                        "cb_open_until":     slot.cb_open_until,
                        "cb_break_duration": slot.cb_break_duration,
                        "cb_consecutive":    slot.cb_consecutive,
                    }

            with self._banned_hosts_lock:
                banned_snapshot = dict(self._banned_hosts)

            state = {
                "slots":          slots_data,
                "banned_hosts":   banned_snapshot,
                "rotation_count": self.rotation_count,
                "saved_at":       time.time(),
            }

            state_dir = os.path.dirname(_STATE_FILE)
            fd, tmp_path = tempfile.mkstemp(dir=state_dir, prefix=".kts_proxy_state_tmp_")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(state, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, _STATE_FILE)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

        except Exception as e:
            self._log("POOL", f"state save error: {type(e).__name__}: {e}")

    def _load_state(self):
        """
        Carga el estado persistido al iniciar.
        Estado >2h se ignora. Circuit breakers expirados se cierran.
        """
        try:
            with open(_STATE_FILE, "r") as f:
                state = json.load(f)

            saved_at = state.get("saved_at", 0)
            age_s    = time.time() - saved_at

            if age_s > 7200:
                self._log("POOL", f"state file too old ({age_s/3600:.1f}h) — starting fresh")
                return

            # Restaurar ban tracking
            banned = state.get("banned_hosts", {})
            now = time.time()
            with self._banned_hosts_lock:
                for host, expiry in banned.items():
                    if expiry > now:
                        self._banned_hosts[host] = expiry

            # Restaurar contadores
            self.rotation_count = int(state.get("rotation_count", 0))

            # Restaurar circuit breakers de slots existentes
            # No restauramos hosts — se reasignarán con validación fresca
            slots_data = state.get("slots", {})
            for label, data in slots_data.items():
                slot = self._slots.get(label)
                if not slot:
                    continue
                with slot.lock:
                    slot.cb_state          = str(data.get("cb_state", _CB_CLOSED))
                    slot.cb_open_until     = float(data.get("cb_open_until", 0.0))
                    slot.cb_break_duration = int(data.get("cb_break_duration", CIRCUIT_BREAK_BASE_S))
                    slot.cb_consecutive    = int(data.get("cb_consecutive", 0))
                    # Cerrar circuit breakers expirados
                    if slot.cb_state == _CB_OPEN and now >= slot.cb_open_until:
                        slot.cb_state = _CB_HALF_OPEN

            active_bans = sum(1 for exp in self._banned_hosts.values() if exp > now)
            self._log(
                "POOL",
                f"state loaded: rotations={self.rotation_count} | "
                f"active_bans={active_bans} | age={age_s:.0f}s"
            )

        except FileNotFoundError:
            self._log("POOL", "no previous state — starting fresh")
        except Exception as e:
            self._log("POOL", f"state load error: {type(e).__name__}: {e} — starting fresh")

    # -----------------------------------------------------------------------
    # Internal — utilidades
    # -----------------------------------------------------------------------

    def _default_log(self, tag: str, msg: str):
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        print(f"{ts} [{tag}] {msg}", flush=True)
