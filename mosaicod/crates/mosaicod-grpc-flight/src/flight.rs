use crate::endpoint;
use arrow_flight::{
    Action as FlightAction, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightDataDecoder, flight_service_server::FlightService,
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use mosaicod_config::params;
use mosaicod_core as core;
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_grpc_common::{
    self as grpc_common, PublicErrorGrpcExt, ToStatusExt, middleware, telemetry,
};
use mosaicod_marshal as marshal;
use mosaicod_query as query;
use mosaicod_store as store;
use serde_json::json;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use tracing::error;

pub struct Service {
    store: store::StoreRef,
    db: db::Database,
    ts_gw: query::TimeseriesEngineRef,
    api_key_management: bool,
    /// Semaphore used to controll the maximum number of concurrent writers.
    concurrent_writes_semaphore: Arc<tokio::sync::Semaphore>,
}

impl Service {
    pub fn try_new(store: store::StoreRef, db: db::Database) -> std::result::Result<Self, String> {
        let ts_gw = Arc::new(
            query::TimeseriesEngine::try_new(
                store.clone(),
                params::params().query_engine_memory_pool_size.value,
            )
            .map_err(|e| e.to_string())?,
        );

        Ok(Service {
            store,
            db,
            ts_gw,
            api_key_management: false,
            concurrent_writes_semaphore: Arc::new(tokio::sync::Semaphore::new(
                params::params().max_concurrent_writes.value.get(),
            )),
        })
    }

    pub fn enable_api_key_manegement(&mut self) {
        self.api_key_management = true;
    }

    pub fn context(&self) -> facade::Context {
        facade::Context::new(self.store.clone(), self.db.clone(), self.ts_gw.clone())
    }
}

type HandshakeStream = BoxStream<'static, std::result::Result<HandshakeResponse, Status>>;
type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, Status>>;
type DoGetStream = BoxStream<'static, std::result::Result<FlightData, Status>>;
type DoPutStream = BoxStream<'static, std::result::Result<PutResult, Status>>;
pub type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, Status>>;
type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, Status>>;
type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

pub trait IntoStream {
    fn into_stream(self) -> grpc_common::Result<DoActionStream>;
}

impl IntoStream for marshal::ActionResponse {
    /// Wraps a single ActionResponse into a one-item
    /// DoActionStream, as expected by Arrow Flight's do_action endpoint.
    ///
    /// Use this when the handler produces a single payload rather than a
    /// stream of results.
    fn into_stream(self) -> grpc_common::Result<DoActionStream> {
        let bytes = self.bytes();
        Ok(Box::pin(futures::stream::once(async move {
            Ok(arrow_flight::Result::new(bytes))
        })))
    }
}

impl Service {
    async fn impl_get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> grpc_common::Result<Response<FlightInfo>> {
        let auth_ctx = middleware::auth_context(&request)?;

        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized(
                "provided API key does not have READ permissions.".to_string(),
            ))?;
        }

        let tctx = telemetry::request_ctx(&request);
        let desc = request.into_inner();
        if tctx.is_none() {
            let info = endpoint::get_flight_info(&self.context(), desc).await?;
            return Ok(Response::new(info));
        }

        // Telemetry: every sequence/topic handler lookup, including misses (wrong names)
        let target = match marshal::flight::get_flight_info_cmd(&desc.cmd) {
            Ok(c) => json!({
                "locator": c.locator,
                "timestamp_range": c.timestamp_range.map(|r| r.to_string()),
            }),
            Err(_) => json!({}),
        };
        let started = std::time::Instant::now();
        let result = endpoint::get_flight_info(&self.context(), desc).await;
        let mut fields = target;
        fields["duration_ms"] = json!(started.elapsed().as_secs_f64() * 1e3);
        fields["ok"] = json!(result.is_ok());
        match &result {
            Ok(info) => fields["endpoints"] = json!(info.endpoint.len()),
            Err(e) => fields["error"] = json!(e.as_ref().to_string()),
        }
        telemetry::emit_with(tctx.as_ref(), "flight_info", fields);
        Ok(Response::new(result?))
    }

    async fn impl_get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> grpc_common::Result<Response<SchemaResult>> {
        let auth_ctx = middleware::auth_context(&request)?;

        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized(
                "provided API key does not have READ permissions.".to_string(),
            ))?;
        }

        let desc = request.into_inner();
        let schema_result = endpoint::get_schema(&self.context(), desc).await?;
        Ok(Response::new(schema_result))
    }

    async fn impl_list_flights(
        &self,
        request: Request<Criteria>,
    ) -> grpc_common::Result<Response<ListFlightsStream>> {
        let auth_ctx = middleware::auth_context(&request)?;

        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized(
                "provided API key does not have READ permissions.".to_string(),
            ))?;
        }

        let tctx = telemetry::request_ctx(&request);
        let criteria = request.into_inner();
        if tctx.is_none() {
            let stream = endpoint::list_flights(&self.context(), criteria).await?;
            let stream = stream.map(|item| item.log_to_status());
            return Ok(Response::new(Box::pin(stream)));
        }

        // Telemetry: sequence listing pages; `rows` counts the sequences returned
        let extra = match marshal::flight::list_flights_cmd(&criteria.expression) {
            Ok(c) => json!({ "start": format!("{:?}", c.start), "limit": format!("{:?}", c.limit) }),
            Err(_) => json!({}),
        };
        let stats = Arc::new(telemetry::StreamStats::default());
        let mut guard = telemetry::StreamGuard::new(tctx, "list_flights", stats.clone(), extra);
        let stream = match endpoint::list_flights(&self.context(), criteria).await {
            Ok(stream) => stream,
            Err(e) => {
                guard.set_error(e.as_ref().to_string());
                return Err(e);
            }
        };

        // Convert the returned stream inner result error to tonis::Status
        let mut guard = Some(guard);
        let stream = stream.map(|item| item.log_to_status()).inspect(move |item| {
            match item {
                Ok(_) => stats.add_batch(1, 0),
                Err(e) => {
                    if let Some(g) = guard.as_mut() {
                        g.set_error(e.message());
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn impl_do_get(
        &self,
        request: Request<Ticket>,
    ) -> grpc_common::Result<Response<DoGetStream>> {
        let auth_ctx = middleware::auth_context(&request)?;
        if !auth_ctx.permissions().can_read() {
            Err(core::Error::unauthorized(
                "provided API key does not have READ permissions.".to_string(),
            ))?;
        }

        let tctx = telemetry::request_ctx(&request);
        let ticket = request.into_inner();

        if tctx.is_none() {
            let data_stream = endpoint::do_get(&self.context(), ticket).await?;
            let out_stream = data_stream
                .inspect_err(|e| error!("flight encoding error: {}", e))
                .map_err(|e| Status::internal(format!("flight encoding error: {}", e)));
            return Ok(Response::new(Box::pin(out_stream)));
        }

        // Telemetry: report rows/bytes streamed once the stream ends or is dropped
        let extra = match marshal::flight::ticket_topic_from_binary(&ticket.ticket) {
            Ok(t) => json!({
                "locator": t.locator.to_string(),
                "timestamp_range": t.timestamp_range.map(|r| r.to_string()),
            }),
            Err(_) => json!({}),
        };
        let stats = Arc::new(telemetry::StreamStats::default());
        let data_stream = telemetry::scope(
            tctx.clone(),
            telemetry::with_stream_stats(stats.clone(), endpoint::do_get(&self.context(), ticket)),
        )
        .await?;
        let mut guard = Some(telemetry::StreamGuard::new(tctx, "do_get", stats, extra));

        // Map data stream error (flight error) to a tonic one
        let out_stream = data_stream
            .inspect_err(|e| error!("flight encoding error: {}", e))
            .map_err(|e| Status::internal(format!("flight encoding error: {}", e)))
            .inspect(move |item| {
                if let (Err(e), Some(g)) = (item, guard.as_mut()) {
                    g.set_error(e.message());
                }
            });

        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn impl_do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> grpc_common::Result<Response<DoPutStream>> {
        let auth_ctx = middleware::auth_context(&request)?;
        if !auth_ctx.permissions().can_write() {
            Err(core::Error::unauthorized(
                "provided API key does not have WRITE permissions.".to_string(),
            ))?;
        }

        let tctx = telemetry::request_ctx(&request);
        let stream = request.into_inner();
        let mut decoder = FlightDataDecoder::new(stream.map_err(Into::into));

        let ctx = endpoint::DoPutContext {
            inner: self.context(),
            concurrent_writes_semaphore: self.concurrent_writes_semaphore.clone(),
        };

        telemetry::scope(tctx, endpoint::do_put(ctx, &mut decoder)).await?;

        Ok(Response::new(Box::pin(futures::stream::empty())))
    }

    async fn impl_do_action(
        &self,
        request: Request<FlightAction>,
    ) -> grpc_common::Result<Response<DoActionStream>> {
        let auth_ctx = middleware::auth_context(&request)?;

        let tctx = telemetry::request_ctx(&request);
        let action = request.into_inner();
        let action_name = action.r#type.clone();
        let body_bytes = action.body.len();
        let started = std::time::Instant::now();

        let result = telemetry::scope(tctx.clone(), async {
            let action = marshal::ActionRequest::try_new(action.r#type.as_str(), &action.body)?;
            endpoint::do_action(&self.context(), action, auth_ctx.permissions()).await
        })
        .await;

        if tctx.is_some() {
            telemetry::emit_with(
                tctx.as_ref(),
                "action",
                json!({
                    "action": action_name,
                    "body_bytes": body_bytes,
                    "duration_ms": started.elapsed().as_secs_f64() * 1e3,
                    "ok": result.is_ok(),
                    "error": result.as_ref().err().map(|e| e.as_ref().to_string()),
                    "grpc_code": result.as_ref().err().map(|e| e.grpc_code() as i32),
                }),
            );
        }

        let stream = result?;

        // Create the stream from the flight result
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Map impl methods to FlightService.
#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = HandshakeStream;
    type ListFlightsStream = ListFlightsStream;
    type DoGetStream = DoGetStream;
    type DoPutStream = DoPutStream;
    type DoActionStream = DoActionStream;
    type ListActionsStream = ListActionsStream;
    type DoExchangeStream = DoExchangeStream;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(core::Error::unimplemented()
            .to_public_error()
            .log_to_status())
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        let resp = self.impl_list_flights(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let resp = self.impl_get_flight_info(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(core::Error::unimplemented()
            .to_public_error()
            .log_to_status())
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let resp = self.impl_get_schema(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let resp = self.impl_do_get(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        let resp = self.impl_do_put(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn do_action(
        &self,
        request: Request<FlightAction>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let resp = self.impl_do_action(request).await.log_to_status()?;
        Ok(resp)
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(
            "list_actions is currently unimplemented",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "do_exchange is currently unimplemented",
        ))
    }
}
