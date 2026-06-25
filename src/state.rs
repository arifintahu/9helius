//! Shared application state.
//!
//! Held behind an `Arc` and cloned into every request handler. Per-upstream
//! mutable state (added in later milestones) lives in atomics so the hot path
//! stays lock-free.

use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusHandle;
use time::OffsetDateTime;

use crate::config::Config;

/// Process-wide shared state.
pub struct AppState {
    pub config: Config,
    pub prom: PrometheusHandle,
    pub started_at: OffsetDateTime,
}

pub type SharedState = Arc<AppState>;

impl AppState {
    pub fn new(config: Config, prom: PrometheusHandle) -> SharedState {
        Arc::new(AppState {
            config,
            prom,
            started_at: OffsetDateTime::now_utc(),
        })
    }

    /// Seconds the process has been running.
    pub fn uptime_secs(&self) -> i64 {
        (OffsetDateTime::now_utc() - self.started_at).whole_seconds()
    }
}
