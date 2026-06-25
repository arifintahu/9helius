//! Metrics recorder setup, metric-name vocabulary, and the management endpoints
//! (`/health`, `/metrics`, `/stats`).

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::state::SharedState;

/// Metric names recorded across the codebase.
pub mod names {
    pub const REQUESTS_TOTAL: &str = "ninehelius_requests_total";
    pub const CREDITS_CONSUMED_TOTAL: &str = "ninehelius_credits_consumed_total";
    pub const CREDITS_REMAINING: &str = "ninehelius_credits_remaining";
    pub const RATE_LIMIT_HITS_TOTAL: &str = "ninehelius_rate_limit_hits_total";
    pub const UPSTREAM_ERRORS_TOTAL: &str = "ninehelius_upstream_errors_total";
    pub const INFLIGHT: &str = "ninehelius_inflight";
    pub const RPC_METHOD_TOTAL: &str = "ninehelius_rpc_method_total";
    pub const ALL_EXHAUSTED_TOTAL: &str = "ninehelius_all_exhausted_total";
    pub const REQUEST_DURATION_SECONDS: &str = "ninehelius_request_duration_seconds";
}

/// Install the global Prometheus recorder and return a handle for rendering.
pub fn init_recorder() -> anyhow::Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| anyhow::anyhow!("failed to install prometheus recorder: {e}"))?;
    Ok(handle)
}

/// `GET /health` — 200 while the process is up. Capacity-aware readiness is
/// refined in a later milestone once cooldown/quota state is wired in.
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, axum::Json(serde_json::json!({ "status": "ok" })))
}

/// `GET /metrics` — Prometheus text exposition.
pub async fn prometheus(State(state): State<SharedState>) -> impl IntoResponse {
    let body = state.prom.render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

/// `GET /stats` — operator-friendly JSON snapshot.
pub async fn stats(State(state): State<SharedState>) -> impl IntoResponse {
    let upstreams = state.pool.stats(crate::ratelimit::now_ms());
    let body = serde_json::json!({
        "uptime_secs": state.uptime_secs(),
        "gateway_bind": state.config.gateway.bind,
        "upstreams": upstreams,
    });
    (StatusCode::OK, axum::Json(body))
}
