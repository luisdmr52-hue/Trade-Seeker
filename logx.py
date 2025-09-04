#!/usr/bin/env python3

# logx.py — minimal, marker-based logging for Trade Seeker

# Purpose: Uniform, scannable logs with markers [BOOT][CFG][RULE][HTTP][ERR], UTC timestamps,

#          compact JSON kv; tiny HTTP wrapper with retries+jitter; guard for exceptions.

# Impact:  Better observability with negligible overhead; no behavior change.

# Confidence: High — isolated helper; safe add.



from __future__ import annotations



from dataclasses import dataclass

from datetime import datetime, timezone

from typing import Protocol, runtime_checkable, Union, Dict, List, Optional

import json, random, time, traceback



JSONScalar = Union[str, int, float, bool, None]

JSONObject = Dict[str, "JSONValue"]

JSONArray = List["JSONValue"]

JSONValue = Union[JSONScalar, JSONObject, JSONArray]



def _ts() -> str:

    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")



def _emit(marker: str, msg: str, **kv: JSONValue) -> None:

    line = f"{_ts()} [{marker}] {msg}"

    if kv:

        try:

            line += " | " + json.dumps(kv, separators=(",",":"), ensure_ascii=False)

        except Exception:

            line += " | " + str(kv)

    print(line, flush=True)



def boot(msg: str, **kv: JSONValue) -> None: _emit("BOOT", msg, **kv)

def cfg(msg: str,  **kv: JSONValue) -> None: _emit("CFG",  msg, **kv)

def rule(msg: str, **kv: JSONValue) -> None: _emit("RULE", msg, **kv)

def http(msg: str, **kv: JSONValue) -> None: _emit("HTTP", msg, **kv)

def err(msg: str,  **kv: JSONValue) -> None: _emit("ERR",  msg, **kv)



@runtime_checkable

class _Response(Protocol):

    status_code: int

    text: str

    def raise_for_status(self) -> None: ...

    def json(self) -> JSONValue: ...



@runtime_checkable

class _RequestsLike(Protocol):

    def get(self, url: str, timeout: float) -> _Response: ...



@dataclass

class HttpResult:

    status: int

    text: str

    json: Optional[JSONValue]

    ms: int



def http_get(url: str, *, requests_mod: _RequestsLike, timeout: float = 10.0, max_retries: int = 2) -> HttpResult:

    retries = 0

    while True:

        t0 = time.perf_counter()

        try:

            r = requests_mod.get(url, timeout=timeout)

            ms = int((time.perf_counter() - t0) * 1000)

            http("GET", url=url, status=int(getattr(r,"status_code",-1)), ms=ms, retries=retries)

            r.raise_for_status()

            try:

                j: Optional[JSONValue] = r.json()

            except Exception:

                j = None

            return HttpResult(status=int(r.status_code), text=str(r.text), json=j, ms=ms)

        except Exception as e:

            ms = int((time.perf_counter() - t0) * 1000)

            status = int(getattr(getattr(e,"response",None),"status_code",-1)) if hasattr(e,"response") else -1

            http("GET_FAIL", url=url, status=status, ms=ms, retries=retries)

            if retries >= max_retries:

                raise

            retries += 1

            time.sleep(0.4 + random.random() * 0.4)



def guarded(fn_name: str):

    def _wrap(fn):

        def inner(*args, **kwargs):

            try:

                return fn(*args, **kwargs)

            except Exception as e:  # intentional broad catch at top-level loop

                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))

                snippet = tb[-800:] if len(tb) > 800 else tb

                err(f"{fn_name} crash", exc=str(e), traceback=snippet)

                return None

        return inner

    return _wrap

