"""
Opt-in usage telemetry (demo build).

Enabled only when ``MOSAICO_TELEMETRY_FILE`` is set: every public SDK call, every
Flight RPC, every query and every read/write stream appends one JSON object per line
to that file. When unset, :func:`install` does nothing and the SDK behaves exactly as
upstream.

Correlation with the server and with the caller:

- ``MOSAICO_TRACE_CONTEXT`` (inline JSON) and/or ``MOSAICO_TRACE_CONTEXT_FILE`` (path to
  a JSON file, re-read when it changes) provide caller context, e.g.
  ``{"agent_id": ..., "team": ..., "ticket_id": ...}``. It is attached to every event and
  sent to the server in the ``x-mosaico-trace`` header together with ``session_id`` and
  the current ``call_id`` (server telemetry copies the header into its events).
- ``session_id`` identifies this Python process; ``call_id`` identifies one outermost
  public API call (nested SDK calls are attributed to the outermost one).
"""

import atexit
import contextvars
import functools
import json
import os
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

import pyarrow.flight as fl

ENV_FILE = "MOSAICO_TELEMETRY_FILE"
ENV_CONTEXT = "MOSAICO_TRACE_CONTEXT"
ENV_CONTEXT_FILE = "MOSAICO_TRACE_CONTEXT_FILE"
TRACE_HEADER = "x-mosaico-trace"

_MAX_STR = 2000
_PKG_DIR = os.path.dirname(os.path.abspath(__file__))

_session_id = uuid.uuid4().hex[:12]
_lock = threading.Lock()
_fh = None
_installed = False

_current_call: contextvars.ContextVar[Optional["_Call"]] = contextvars.ContextVar(
    "mosaico_telemetry_call", default=None
)


def enabled() -> bool:
    return bool(os.environ.get(ENV_FILE))


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------

_ctx_cache: Dict[str, Any] = {"mtime": None, "path": None, "value": {}}


def _file_context() -> Dict[str, Any]:
    path = os.environ.get(ENV_CONTEXT_FILE)
    if not path:
        return {}
    try:
        mtime = os.stat(path).st_mtime_ns
    except OSError:
        return {}
    if _ctx_cache["path"] != path or _ctx_cache["mtime"] != mtime:
        try:
            with open(path) as f:
                value = json.load(f)
            _ctx_cache.update(path=path, mtime=mtime, value=value if isinstance(value, dict) else {})
        except (OSError, ValueError):
            _ctx_cache.update(path=path, mtime=mtime, value={})
    return _ctx_cache["value"]


def context() -> Dict[str, Any]:
    """Caller-provided trace context (inline env JSON overridden by the context file)."""
    ctx: Dict[str, Any] = {}
    inline = os.environ.get(ENV_CONTEXT)
    if inline:
        try:
            value = json.loads(inline)
            if isinstance(value, dict):
                ctx.update(value)
        except ValueError:
            ctx["raw"] = inline[:_MAX_STR]
    ctx.update(_file_context())
    return ctx


def trace_header() -> str:
    call = _current_call.get()
    payload = dict(context())
    payload["session_id"] = _session_id
    if call is not None:
        payload["call_id"] = call.call_id
        payload["api"] = call.name
    return json.dumps(payload, separators=(",", ":"), default=str)


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------


def _open():
    global _fh
    if _fh is None:
        path = os.environ[ENV_FILE]
        if path.startswith("file://"):
            path = path[len("file://") :]
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        _fh = open(path, "a", buffering=1)
        atexit.register(_fh.close)
    return _fh


def emit(event: str, **fields: Any) -> None:
    """Append one event. Never raises."""
    if not enabled():
        return
    try:
        rec: Dict[str, Any] = {
            "source": "sdk",
            "event": event,
            "ts": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            "session_id": _session_id,
            "pid": os.getpid(),
            "trace": context(),
        }
        call = _current_call.get()
        if call is not None and "call_id" not in fields:
            rec["call_id"] = call.call_id
        rec.update(fields)
        line = json.dumps(rec, default=str)
        with _lock:
            _open().write(line + "\n")
    except Exception:
        pass


def _trim(v: Any) -> Any:
    if isinstance(v, str) and len(v) > _MAX_STR:
        return v[:_MAX_STR] + "..."
    return v


def _caller() -> Optional[str]:
    """First stack frame outside the SDK: identifies the user script/line."""
    for frame in reversed(traceback.extract_stack()[:-2]):
        if not os.path.abspath(frame.filename).startswith(_PKG_DIR):
            return f"{frame.filename}:{frame.lineno}"
    return None


# ---------------------------------------------------------------------------
# Public API call tracking
# ---------------------------------------------------------------------------


class _Call:
    __slots__ = ("call_id", "name", "fields")

    def __init__(self, name: str):
        self.call_id = uuid.uuid4().hex[:16]
        self.name = name
        self.fields: Dict[str, Any] = {}


def annotate(**fields: Any) -> None:
    """Attach fields to the current outermost API call event."""
    call = _current_call.get()
    if call is not None:
        call.fields.update(fields)


def current_call_id() -> Optional[str]:
    call = _current_call.get()
    return call.call_id if call is not None else None


def api(name: str) -> Callable:
    """Decorator recording an `api_call` event for the outermost public SDK call."""

    def deco(fn: Callable) -> Callable:
        if getattr(fn, "__mosaico_telemetry__", False):
            return fn

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not enabled() or _current_call.get() is not None:
                return fn(*args, **kwargs)
            call = _Call(name)
            token = _current_call.set(call)
            started = time.perf_counter()
            err: Optional[BaseException] = None
            try:
                return fn(*args, **kwargs)
            except BaseException as e:
                err = e
                raise
            finally:
                fields: Dict[str, Any] = {
                    "api": name,
                    "call_id": call.call_id,
                    "duration_ms": (time.perf_counter() - started) * 1e3,
                    "ok": err is None,
                    "caller": _caller(),
                    "script": sys.argv[0] if sys.argv else None,
                }
                if err is not None:
                    fields["error_type"] = type(err).__name__
                    fields["error"] = _trim(str(err))
                fields.update(call.fields)
                emit("api_call", **fields)
                _current_call.reset(token)

        wrapper.__mosaico_telemetry__ = True  # type: ignore[attr-defined]
        return wrapper

    return deco


def _wrap_methods(cls: type, names, prefix: str) -> None:
    for n in names:
        attr = cls.__dict__.get(n)
        if attr is None:
            continue
        label = f"{prefix}.{n}"
        if isinstance(attr, classmethod):
            setattr(cls, n, classmethod(api(label)(attr.__func__)))
        elif isinstance(attr, staticmethod):
            setattr(cls, n, staticmethod(api(label)(attr.__func__)))
        elif callable(attr):
            setattr(cls, n, api(label)(attr))


# ---------------------------------------------------------------------------
# Query rendering
# ---------------------------------------------------------------------------


def _pb_value(v: Any) -> Any:
    kind = v.WhichOneof("kind")
    if kind is None:
        return None
    val = getattr(v, kind)
    return list(val.values) if kind.endswith("_array") else val


def _pb_condition(c: Any) -> Dict[str, Any]:
    from .proto.v1 import query_pb2

    op = query_pb2.Operator.Name(c.op).removeprefix("OPERATOR_").lower()
    return {f"${op}": _pb_value(c.value) if c.HasField("value") else None}


def query_dict(filter_pb: Any) -> Dict[str, Any]:
    """A query `Filter` message as the builder->field->{"$op": value} JSON that the SDK
    sent before the protobuf wire format, so query shapes stay comparable over time.
    Ontology predicates with an aggregator are keyed `<field>|<aggregator>`."""
    from .proto.v1 import query_pb2

    try:
        out: Dict[str, Any] = {}
        for builder, fields in (
            ("sequence", ("name", "created_at_ns")),
            ("topic", ("name", "created_at_ns", "ontology_tag", "serialization_format")),
        ):
            if not filter_pb.HasField(builder):
                continue
            msg = getattr(filter_pb, builder)
            b: Dict[str, Any] = {f: _pb_condition(getattr(msg, f)) for f in fields if msg.HasField(f)}
            if msg.user_metadata:
                b["user_metadata"] = {k: _pb_condition(c) for k, c in msg.user_metadata.items()}
            out[builder] = b
        if filter_pb.HasField("ontology"):
            o: Dict[str, Any] = {}
            for p in filter_pb.ontology.predicates:
                key = p.field
                if p.HasField("aggregator"):
                    key += "|" + query_pb2.Aggregator.Name(p.aggregator).removeprefix("AGGREGATOR_").lower()
                o[key] = _pb_condition(p.condition)
            out["ontology"] = o
        return out
    except Exception as e:  # never break a query for telemetry
        return {"_render_error": str(e)}


# ---------------------------------------------------------------------------
# Flight middleware
# ---------------------------------------------------------------------------


class TelemetryMiddleware(fl.ClientMiddleware):
    def __init__(self, method: str):
        super().__init__()
        self._method = method
        self._started = time.perf_counter()
        self._call_id = current_call_id()

    def sending_headers(self):
        return {TRACE_HEADER: trace_header().encode()}

    def received_headers(self, headers):
        pass

    def call_completed(self, exception):
        fields: Dict[str, Any] = {
            "method": self._method,
            "latency_ms": (time.perf_counter() - self._started) * 1e3,
            "ok": exception is None,
        }
        if self._call_id is not None:
            fields["call_id"] = self._call_id
        if exception is not None:
            fields["error_type"] = type(exception).__name__
            fields["error"] = _trim(str(exception))
        emit("rpc", **fields)


class TelemetryMiddlewareFactory(fl.ClientMiddlewareFactory):
    def start_call(self, info: fl.CallInfo) -> TelemetryMiddleware:
        method = getattr(info.method, "name", str(info.method))
        return TelemetryMiddleware(method)


# ---------------------------------------------------------------------------
# Stream counters
# ---------------------------------------------------------------------------


class StreamStats:
    """Row/byte counters for one read or write stream, reported once on close."""

    def __init__(self, event: str, **fields: Any):
        self.event = event
        self.fields = fields
        self.call_id = current_call_id()
        self.rows = 0
        self.batches = 0
        self.bytes = 0
        self.started = time.perf_counter()
        self._done = False

    def add(self, rows: int, nbytes: int) -> None:
        self.rows += rows
        self.batches += 1
        self.bytes += nbytes

    def finish(self, **extra: Any) -> None:
        if self._done or not enabled():
            return
        self._done = True
        emit(
            self.event,
            call_id=self.call_id,
            rows=self.rows,
            batches=self.batches,
            bytes=self.bytes,
            duration_ms=(time.perf_counter() - self.started) * 1e3,
            **self.fields,
            **extra,
        )


# ---------------------------------------------------------------------------
# Installation
# ---------------------------------------------------------------------------


def install() -> None:
    """Wrap the public SDK surface. Idempotent; no-op when telemetry is disabled."""
    global _installed
    if _installed or not enabled():
        return
    _installed = True

    from .comm.mosaico_client import MosaicoClient
    from .handlers.sequence_handler import SequenceHandler
    from .handlers.topic_handler import TopicHandler
    from .ml.data_frame_extractor import DataFrameExtractor
    from .ml.sync_transformer import SyncTransformer
    from .query import response as qr

    _wrap_methods(
        MosaicoClient,
        [
            "connect", "from_env", "sequence_handler", "sequence_exists", "topic_handler",
            "topic_exists", "sequence_create", "sequence_update", "sequence_delete",
            "session_delete", "list_sequences", "list_sequence_notifications",
            "clear_sequence_notifications", "list_topic_notifications",
            "clear_topic_notifications", "query", "info",
        ],
        "client",
    )
    _wrap_methods(SequenceHandler, ["get_data_streamer", "get_topic_handler", "update", "reload"], "sequence")
    _wrap_methods(TopicHandler, ["get_data_streamer"], "topic")
    _wrap_methods(DataFrameExtractor, ["to_pandas_chunks"], "ml.dataframe")
    _wrap_methods(SyncTransformer, ["fit", "transform"], "ml.sync")
    for cls_name in dir(qr):
        cls = getattr(qr, cls_name)
        if isinstance(cls, type) and cls.__module__ == qr.__name__:
            _wrap_methods(
                cls,
                ["clusterize", "intersect", "clusterize_all", "to_query_sequence", "to_query_topic"],
                f"query.{cls_name}",
            )

    try:
        from .bridges.ros.injector import RosbagInjector

        _wrap_methods(RosbagInjector, ["run"], "bridge.ros")
    except Exception:
        pass

    emit("session_start", argv=sys.argv[:8], python=sys.version.split()[0])
    # atexit runs LIFO and the sink registered its close() on first emit, so this runs first.
    atexit.register(lambda: emit("session_end"))
