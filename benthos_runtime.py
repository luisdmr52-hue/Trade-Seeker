"""
benthos_runtime.py — Capa 0 (F1) — KTS Trade Seeker

Generates benthos_spot.yaml at runtime from benthos_spot_base.yaml template,
restarts ts_benthos_spot, and verifies the feed is live.

Responsibilities:
- Read base YAML template (benthos_spot_base.yaml)
- Inject input.websocket.url with /stream?streams= URL built from coverage universe
- Inject input.websocket.proxy_url only if proxy is active (absent = direct IP)
- Write benthos_spot.yaml atomically (mkstemp + chmod 644 + os.replace)
- Restart ts_benthos_spot via docker compose
- Verify feed is live by polling /metrics until input_received > 0
- Anti-churn: skip restart if runtime signature has not changed

Design:
- apply_if_changed() is the main entry point — idempotent.
- All docker compose calls use subprocess with timeout.
- Failure in apply_if_changed() returns status dict — never raises.
- Caller decides whether to abort or continue in degraded mode (DC-FEED-08).
"""

import hashlib
import os
import stat
import subprocess
import tempfile
import time
from typing import Callable, List, Optional

import requests
import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_WS_BASE    = "wss://stream.binance.com:9443/stream?streams="
STREAM_SUFFIX      = "@ticker"
PLACEHOLDER_URL    = "PLACEHOLDER_URL"

METRICS_POLL_INTERVAL_S = 3
SUBPROCESS_TIMEOUT_S    = 30

# ---------------------------------------------------------------------------
# BenthosRuntime
# ---------------------------------------------------------------------------

class BenthosRuntime:
    """
    Generates benthos_spot.yaml and manages ts_benthos_spot lifecycle.
    """

    def __init__(
        self,
        compose_dir:               str,
        base_yaml_path:            str,
        runtime_yaml_path:         str,
        metrics_url:               str,
        log_fn:                    Callable,
        get_coverage_universe_fn:  Callable[[], List[str]],
        get_coverage_signature_fn: Optional[Callable[[], str]] = None,
        proxy_url:                 Optional[str] = None,
        verify_timeout_s:          int = 60,
    ):
        self._compose_dir        = compose_dir
        self._base_yaml_path     = os.path.join(compose_dir, base_yaml_path)
        self._runtime_yaml_path  = os.path.join(compose_dir, runtime_yaml_path)
        self._metrics_url        = metrics_url
        self._log                = log_fn
        self._get_universe       = get_coverage_universe_fn
        self._get_sig            = get_coverage_signature_fn
        self._proxy_url          = proxy_url
        self._verify_timeout_s   = verify_timeout_s

        self._last_applied_sig: str = ""

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def apply_if_changed(self) -> dict:
        """
        Main entry point. Idempotent.

        Boot behavior: if feed is already live, skip restart entirely.
        Periodic behavior (6h): restart only if signature changed.

        Returns:
            {"status": "ok"|"skipped"|"failed", "reason": str, ...}
        """
        try:
            universe = self._get_universe()
            if not universe:
                return {"status": "failed", "reason": "empty_coverage_universe"}

            # If feed is already live — skip restart regardless of signature
            if self._is_feed_live():
                runtime_sig = self._compute_runtime_signature(universe)
                self._last_applied_sig = runtime_sig
                self._log("BenthosRuntime", "feed already live — skipping restart")
                return {"status": "skipped", "reason": "feed_already_live",
                        "signature": runtime_sig[:12]}

            runtime_sig = self._compute_runtime_signature(universe)

            if runtime_sig == self._last_applied_sig:
                return {"status": "skipped", "reason": "signature_unchanged",
                        "signature": runtime_sig[:12]}

            self._log("BenthosRuntime", f"applying — {len(universe)} symbols, sig={runtime_sig[:12]}...")

            self.build_runtime_yaml(universe)
            self.restart_service()
            ok = self.verify_up(timeout_s=self._verify_timeout_s)

            if ok:
                self._last_applied_sig = runtime_sig
                self._log("BenthosRuntime", "feed verified live")
                return {"status": "ok", "symbols": len(universe), "signature": runtime_sig[:12]}
            else:
                return {"status": "failed", "reason": "verify_timeout",
                        "symbols": len(universe)}

        except Exception as e:
            self._log("BenthosRuntime", f"apply_if_changed error: {type(e).__name__}: {e}")
            return {"status": "failed", "reason": str(e)}

    def build_runtime_yaml(self, universe: List[str]) -> None:
        """
        Reads base YAML, injects URL (and proxy_url if set), writes runtime YAML atomically.
        """
        # Read base template
        with open(self._base_yaml_path, "r") as f:
            content = f.read()

        # Build stream URL
        streams = "/".join(sym.lower() + STREAM_SUFFIX for sym in universe)
        url = BINANCE_WS_BASE + streams

        # Replace placeholder URL
        if PLACEHOLDER_URL not in content:
            raise ValueError(
                f"PLACEHOLDER_URL not found in {self._base_yaml_path} — "
                "base template may be outdated"
            )
        content = content.replace(PLACEHOLDER_URL, url)

        # Inject proxy_url after the url line (only if proxy is active)
        if self._proxy_url:
            content = content.replace(
                f"    url: {url}",
                f"    url: {url}\n    proxy_url: {self._proxy_url}",
            )

        # Atomic write: mkstemp → chmod 644 → os.replace
        runtime_dir = os.path.dirname(self._runtime_yaml_path)
        fd, tmp_path = tempfile.mkstemp(dir=runtime_dir, suffix=".yaml.tmp")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
            os.chmod(tmp_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)  # 644
            os.replace(tmp_path, self._runtime_yaml_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        self._log("BenthosRuntime",
                  f"runtime YAML written — {len(universe)} symbols, "
                  f"proxy={'yes' if self._proxy_url else 'no (direct IP)'}")

    def restart_service(self) -> None:
        """Restarts ts_benthos_spot via docker compose."""
        self._log("BenthosRuntime", "restarting ts_benthos_spot...")
        result = subprocess.run(
            ["docker", "compose", "restart", "ts_benthos_spot"],
            cwd=self._compose_dir,
            capture_output=True,
            text=True,
            timeout=SUBPROCESS_TIMEOUT_S,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"docker compose restart failed (rc={result.returncode}): {result.stderr.strip()}"
            )
        self._log("BenthosRuntime", "ts_benthos_spot restarted")

    def verify_up(self, timeout_s: int = 60) -> bool:
        """
        Polls /metrics until input_received > 0 or timeout.
        Returns True if feed is live, False on timeout.
        """
        deadline = time.time() + timeout_s
        self._log("BenthosRuntime", f"verifying feed (timeout={timeout_s}s)...")

        while time.time() < deadline:
            try:
                r = requests.get(self._metrics_url, timeout=5)
                if r.status_code == 200:
                    received = self._parse_input_received(r.text)
                    if received > 0:
                        self._log("BenthosRuntime",
                                  f"feed live — input_received={received}")
                        return True
            except Exception:
                pass
            time.sleep(METRICS_POLL_INTERVAL_S)

        self._log("BenthosRuntime",
                  f"verify_up timed out after {timeout_s}s — feed not confirmed")
        return False

    def current_runtime_signature(self) -> str:
        """Returns the signature of the last successfully applied runtime."""
        return self._last_applied_sig

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _is_feed_live(self) -> bool:
        """Quick check — returns True if input_received > 0 right now."""
        try:
            r = requests.get(self._metrics_url, timeout=5)
            if r.status_code == 200:
                return self._parse_input_received(r.text) > 0
        except Exception:
            pass
        return False

    def _compute_runtime_signature(self, universe: List[str]) -> str:
        proxy_component = self._proxy_url or "direct"
        try:
            with open(self._base_yaml_path, "rb") as f:
                base_hash = hashlib.sha256(f.read()).hexdigest()[:16]
        except OSError:
            base_hash = "nobase"

        payload = ",".join(sorted(universe)) + "|" + proxy_component + "|" + base_hash
        return hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _parse_input_received(metrics_text: str) -> int:
        """
        Parses Prometheus text format to extract input_received counter.
        Returns 0 if not found or unparseable.
        """
        for line in metrics_text.splitlines():
            if line.startswith("input_received") and not line.startswith("#"):
                try:
                    return int(float(line.split()[-1]))
                except (ValueError, IndexError):
                    pass
        return 0


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    def _log(tag, msg):
        print(f"[{tag}] {msg}")

    # Minimal smoke test — builds YAML only, no restart
    COMPOSE_DIR = "/root/kts-lab/compose"
    BASE_YAML   = "config/benthos_spot_base.yaml"
    RUNTIME_YAML = "config/benthos_spot_test.yaml"  # safe — no overwrite of live file

    # Fake universe
    test_universe = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

    rt = BenthosRuntime(
        compose_dir               = COMPOSE_DIR,
        base_yaml_path            = BASE_YAML,
        runtime_yaml_path         = RUNTIME_YAML,
        metrics_url               = "http://localhost:4199/metrics",
        log_fn                    = _log,
        get_coverage_universe_fn  = lambda: test_universe,
        proxy_url                 = None,  # test without proxy
    )

    print("=== Building runtime YAML (no proxy) ===")
    rt.build_runtime_yaml(test_universe)

    print("\n=== Generated YAML (first 30 lines) ===")
    with open(os.path.join(COMPOSE_DIR, RUNTIME_YAML)) as f:
        for i, line in enumerate(f):
            if i >= 30: break
            print(line, end="")

    print("\n\n=== Signature ===")
    sig = rt._compute_runtime_signature(test_universe)
    print(f"sig={sig}")

    # Cleanup test file
    try:
        os.unlink(os.path.join(COMPOSE_DIR, RUNTIME_YAML))
    except OSError:
        pass
    print("\nDone — smoke test passed")
