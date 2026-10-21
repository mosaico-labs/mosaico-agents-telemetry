//! Opt-in usage telemetry (demo build).
//!
//! When `MOSAICOD_TELEMETRY_FILE` is set, the server appends one JSON object per line
//! to that file for every gRPC request (`rpc`), every DoAction (`action`), every query
//! (`query`) and every DoGet / DoPut stream (`do_get` / `do_put`). When the variable is
//! unset every function in this module is a no-op.
//!
//! Correlation: clients may send an opaque `x-mosaico-trace` header (the Python SDK
//! sends a JSON object). It is copied verbatim into every event of that request
//! together with a server-generated `request_id`.
//!
//! Events are pushed to a bounded channel and written by a background thread, so
//! request handling never blocks on disk I/O. If the channel is full events are dropped.

use serde_json::{Map, Value, json};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
        mpsc::{SyncSender, TrySendError, sync_channel},
    },
    task::{Context, Poll},
    time::Instant,
};
use tower::{Layer, Service};

pub const TRACE_HEADER: &str = "x-mosaico-trace";
const ENV_FILE: &str = "MOSAICOD_TELEMETRY_FILE";
const CHANNEL_CAPACITY: usize = 16 * 1024;
const MAX_TRACE_LEN: usize = 4096;

static SINK: OnceLock<Option<SyncSender<Value>>> = OnceLock::new();

fn sink() -> Option<&'static SyncSender<Value>> {
    SINK.get_or_init(|| {
        let path = std::env::var(ENV_FILE).ok().filter(|p| !p.is_empty())?;
        let file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("unable to open telemetry file `{}`: {}", path, e);
                return None;
            }
        };
        let (tx, rx) = sync_channel::<Value>(CHANNEL_CAPACITY);
        std::thread::Builder::new()
            .name("mosaicod-telemetry".into())
            .spawn(move || {
                let mut w = BufWriter::new(file);
                while let Ok(ev) = rx.recv() {
                    let _ = serde_json::to_writer(&mut w, &ev);
                    let _ = w.write_all(b"\n");
                    // Drain what is already queued, then flush so the file stays tailable.
                    while let Ok(next) = rx.try_recv() {
                        let _ = serde_json::to_writer(&mut w, &next);
                        let _ = w.write_all(b"\n");
                    }
                    let _ = w.flush();
                }
            })
            .ok()?;
        tracing::warn!("usage telemetry enabled, writing to `{}`", path);
        Some(tx)
    })
    .as_ref()
}

/// Returns true if telemetry is enabled for this process.
pub fn enabled() -> bool {
    sink().is_some()
}

/// Per-request telemetry context, stored in request extensions and in a task-local
/// for the duration of the handler.
#[derive(Clone, Debug)]
pub struct RequestCtx {
    pub request_id: u64,
    pub trace: Option<Value>,
    pub path: Arc<str>,
}

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

tokio::task_local! {
    static CURRENT: RequestCtx;
}

/// Runs `fut` with `ctx` as the current request context.
pub async fn scope<F: std::future::Future>(ctx: Option<RequestCtx>, fut: F) -> F::Output {
    match ctx {
        Some(ctx) => CURRENT.scope(ctx, fut).await,
        None => fut.await,
    }
}

/// Returns the current request context, if any.
pub fn current() -> Option<RequestCtx> {
    CURRENT.try_with(|c| c.clone()).ok()
}

/// Extracts the telemetry context from a tonic request.
pub fn request_ctx<T>(req: &tonic::Request<T>) -> Option<RequestCtx> {
    req.extensions().get::<RequestCtx>().cloned()
}

/// Emits an event of kind `event`, tagged with the given request context.
pub fn emit_with(ctx: Option<&RequestCtx>, event: &str, fields: Value) {
    let Some(tx) = sink() else { return };

    let mut obj = match fields {
        Value::Object(m) => m,
        other => {
            let mut m = Map::new();
            m.insert("value".into(), other);
            m
        }
    };
    obj.insert("source".into(), json!("server"));
    obj.insert("event".into(), json!(event));
    obj.insert(
        "ts".into(),
        json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
    );
    if let Some(ctx) = ctx {
        obj.insert("request_id".into(), json!(ctx.request_id));
        obj.insert("rpc".into(), json!(ctx.path.as_ref()));
        if let Some(trace) = &ctx.trace {
            obj.insert("trace".into(), trace.clone());
        }
    }

    match tx.try_send(Value::Object(obj)) {
        Ok(()) | Err(TrySendError::Full(_)) => {}
        Err(TrySendError::Disconnected(_)) => {}
    }
}

/// Emits an event using the task-local request context.
pub fn emit(event: &str, fields: Value) {
    emit_with(current().as_ref(), event, fields);
}

/// Row/byte counters shared between a streaming endpoint and the guard that reports
/// them when the stream ends (or is dropped by the client).
#[derive(Default, Debug)]
pub struct StreamStats {
    pub rows: AtomicU64,
    pub batches: AtomicU64,
    pub bytes: AtomicU64,
}

impl StreamStats {
    pub fn add_batch(&self, rows: usize, bytes: usize) {
        self.rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn to_json(&self) -> Value {
        json!({
            "rows": self.rows.load(Ordering::Relaxed),
            "batches": self.batches.load(Ordering::Relaxed),
            "bytes": self.bytes.load(Ordering::Relaxed),
        })
    }
}

tokio::task_local! {
    static STREAM_STATS: Arc<StreamStats>;
}

/// Runs `fut` with `stats` available via [`stream_stats`].
pub async fn with_stream_stats<F: std::future::Future>(stats: Arc<StreamStats>, fut: F) -> F::Output {
    STREAM_STATS.scope(stats, fut).await
}

/// Returns the stream counters installed by [`with_stream_stats`], if any.
pub fn stream_stats() -> Option<Arc<StreamStats>> {
    STREAM_STATS.try_with(|s| s.clone()).ok()
}

/// Emits `event` with the final stream counters when dropped.
pub struct StreamGuard {
    ctx: Option<RequestCtx>,
    event: &'static str,
    started: Instant,
    stats: Arc<StreamStats>,
    extra: Value,
    error: Option<String>,
}

impl StreamGuard {
    pub fn new(ctx: Option<RequestCtx>, event: &'static str, stats: Arc<StreamStats>, extra: Value) -> Self {
        Self {
            ctx,
            event,
            started: Instant::now(),
            stats,
            extra,
            error: None,
        }
    }

    pub fn set_error(&mut self, err: impl ToString) {
        self.error = Some(err.to_string());
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let mut fields = self.stats.to_json();
        if let (Value::Object(f), Value::Object(e)) = (&mut fields, &self.extra) {
            for (k, v) in e {
                f.insert(k.clone(), v.clone());
            }
            f.insert(
                "duration_ms".into(),
                json!(self.started.elapsed().as_secs_f64() * 1e3),
            );
            f.insert("ok".into(), json!(self.error.is_none()));
            if let Some(err) = &self.error {
                f.insert("error".into(), json!(err));
            }
        }
        emit_with(self.ctx.as_ref(), self.event, fields);
    }
}

/// Tower layer that assigns a request id, captures the `x-mosaico-trace` header and
/// emits one `rpc` event per gRPC call with its latency (time to response headers)
/// and status.
#[derive(Clone, Default)]
pub struct TelemetryLayer;

impl<S> Layer<S> for TelemetryLayer {
    type Service = TelemetryMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        TelemetryMiddleware { inner: service }
    }
}

#[derive(Clone)]
pub struct TelemetryMiddleware<S> {
    inner: S,
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

fn parse_trace(raw: &str) -> Value {
    let raw = &raw[..raw.len().min(MAX_TRACE_LEN)];
    serde_json::from_str(raw).unwrap_or_else(|_| json!(raw))
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for TelemetryMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        if !enabled() {
            return Box::pin(inner.call(req));
        }

        let ctx = RequestCtx {
            request_id: NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
            trace: req
                .headers()
                .get(TRACE_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(parse_trace),
            path: Arc::from(req.uri().path()),
        };
        req.extensions_mut().insert(ctx.clone());

        Box::pin(async move {
            let started = Instant::now();
            let res = inner.call(req).await;
            let latency_ms = started.elapsed().as_secs_f64() * 1e3;
            match &res {
                Ok(resp) => {
                    // A grpc-status header is present only for "trailers-only" responses,
                    // i.e. errors returned before any message was streamed.
                    let grpc_status = resp
                        .headers()
                        .get("grpc-status")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<i32>().ok())
                        .unwrap_or(0);
                    let grpc_message = resp
                        .headers()
                        .get("grpc-message")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    emit_with(
                        Some(&ctx),
                        "rpc",
                        json!({
                            "latency_ms": latency_ms,
                            "http_status": resp.status().as_u16(),
                            "grpc_status": grpc_status,
                            "grpc_message": grpc_message,
                        }),
                    );
                }
                Err(_) => emit_with(
                    Some(&ctx),
                    "rpc",
                    json!({ "latency_ms": latency_ms, "transport_error": true }),
                ),
            }
            res
        })
    }
}
