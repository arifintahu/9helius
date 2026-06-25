//! Shared application state.
//!
//! Held behind an `Arc` and cloned into every request handler. Per-upstream
//! mutable state (added in later milestones) lives in atomics so the hot path
//! stays lock-free.

use std::sync::Arc;
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusHandle;
use time::OffsetDateTime;
use url::Url;

use crate::config::Config;
use crate::upstream::Pool;

/// Process-wide shared state.
pub struct AppState {
    pub config: Config,
    pub prom: PrometheusHandle,
    pub started_at: OffsetDateTime,
    /// Shared, connection-pooling client used for all upstream calls.
    pub http: reqwest::Client,
    /// Pre-parsed upstream base URL (only the api-key query param is rewritten).
    pub upstream_base: Url,
    /// The pool of upstream keys with round-robin selection.
    pub pool: Pool,
}

pub type SharedState = Arc<AppState>;

impl AppState {
    pub fn new(config: Config, prom: PrometheusHandle) -> anyhow::Result<SharedState> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.gateway.request_timeout_ms))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;
        let upstream_base = Url::parse(&config.gateway.upstream_base)?;
        let pool = Pool::from_config(&config.upstreams);

        Ok(Arc::new(AppState {
            config,
            prom,
            started_at: OffsetDateTime::now_utc(),
            http,
            upstream_base,
            pool,
        }))
    }

    /// Seconds the process has been running.
    pub fn uptime_secs(&self) -> i64 {
        (OffsetDateTime::now_utc() - self.started_at).whole_seconds()
    }
}
