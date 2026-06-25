//! 9helius — a transparent Helius RPC load balancer.
//!
//! Library root. The `ninehelius` binary is a thin wrapper around [`router`] and
//! [`state::AppState`]; exposing them here lets integration tests build the app.

pub mod config;
pub mod credits;
pub mod error;
pub mod metrics;
pub mod persistence;
pub mod proxy;
pub mod ratelimit;
pub mod state;
pub mod stats;
pub mod upstream;

use axum::routing::get;
use axum::Router;

use crate::state::SharedState;

/// Build the axum application: reserved management routes plus the transparent
/// proxy fallback that captures everything else.
pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(metrics::health))
        .route("/metrics", get(metrics::prometheus))
        .route("/stats", get(metrics::stats))
        .route("/stats/history", get(metrics::stats_history))
        .fallback(proxy::handle)
        .with_state(state)
}
