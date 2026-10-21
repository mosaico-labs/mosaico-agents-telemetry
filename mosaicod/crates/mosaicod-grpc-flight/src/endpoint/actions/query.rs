//! Query-related actions.

use mosaicod_core::types::{SequenceTopicGroup, SequenceTopicGroupSet};
use mosaicod_facade as facade;
use mosaicod_grpc_common::{self as grpc_common, telemetry};
use mosaicod_marshal::{self as marshal, ActionResponse};
use serde_json::{Value, json};
use tracing::{info, trace};

/// Executes a query and returns matching groups.
pub async fn execute(
    ctx: &facade::Context,
    query: marshal::requests::QueryFilter,
) -> grpc_common::Result<ActionResponse> {
    info!("performing a query");

    let started = std::time::Instant::now();
    let fields = telemetry::enabled().then(|| query_fields(&query));

    let result = run(ctx, query).await;

    if let Some(mut fields) = fields {
        fields["duration_ms"] = json!(started.elapsed().as_secs_f64() * 1e3);
        fields["ok"] = json!(result.is_ok());
        match &result {
            Ok(groups) => {
                fields["result_sequences"] = json!(groups.len());
                fields["result_topics"] = json!(groups.iter().map(|g| g.topics.len()).sum::<usize>());
            }
            Err(e) => fields["error"] = json!(e.as_ref().to_string()),
        }
        telemetry::emit("query", fields);
    }

    Ok(ActionResponse::query(SequenceTopicGroupSet::from(result?)))
}

async fn run(
    ctx: &facade::Context,
    query: marshal::requests::QueryFilter,
) -> grpc_common::Result<Vec<SequenceTopicGroup>> {
    let filter = marshal::query_filter_from_proto(query)?;

    trace!("query filter: {:?}", filter);

    let groups =
        facade::Query::query(filter, ctx.timeseries_querier.clone(), ctx.db.clone()).await?;

    trace!("groups found: {:?}", groups);

    Ok(groups.into())
}

/// Which builders, fields and metadata keys a query uses (no values): the query itself
/// is recorded client-side by the SDK; this keeps non-SDK clients visible too.
fn query_fields(q: &marshal::requests::QueryFilter) -> Value {
    let mut used: Vec<String> = Vec::new();
    if let Some(s) = &q.sequence {
        for (name, set) in [("name", s.name.is_some()), ("created_at_ns", s.created_at_ns.is_some())] {
            if set {
                used.push(format!("sequence:{name}"));
            }
        }
        used.extend(s.user_metadata.keys().map(|k| format!("sequence:user_metadata.{k}")));
    }
    if let Some(t) = &q.topic {
        for (name, set) in [
            ("name", t.name.is_some()),
            ("created_at_ns", t.created_at_ns.is_some()),
            ("ontology_tag", t.ontology_tag.is_some()),
            ("serialization_format", t.serialization_format.is_some()),
        ] {
            if set {
                used.push(format!("topic:{name}"));
            }
        }
        used.extend(t.user_metadata.keys().map(|k| format!("topic:user_metadata.{k}")));
    }
    if let Some(o) = &q.ontology {
        used.extend(o.predicates.iter().map(|p| match p.aggregator {
            Some(agg) => format!("ontology:{}|{}", p.field, agg),
            None => format!("ontology:{}", p.field),
        }));
    }
    used.sort();
    json!({ "fields": used })
}
