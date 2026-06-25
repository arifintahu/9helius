//! 9helius — a transparent Helius RPC load balancer.
//!
//! Library root. The `ninehelius` binary is a thin wrapper around [`router`] and
//! [`state::AppState`]; exposing them here lets integration tests build the app.

// Some items are introduced a milestone before their first use; this is removed
// during the final hardening pass.
#![allow(dead_code)]

pub mod config;
pub mod credits;
pub mod error;
pub mod metrics;
pub mod proxy;
pub mod state;
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
        .fallback(proxy::handle)
        .with_state(state)
}
