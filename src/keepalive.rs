//! Upstream connection keep-alive.
//!
//! Periodically sends a cheap `getHealth` to each enabled key so its pooled
//! Helius connection (TCP + TLS) stays warm, cutting the cold-connection tail
//! latency that round-robin selection would otherwise hit when a key's
//! connection has gone idle.
//!
//! Each ping is a real Helius call (~1 credit per key), so it counts against the
//! key's quota and is reflected in credit metrics. Disabled when
//! `gateway.keepalive_secs == 0`.

use std::time::Duration;

use serde_json::json;
use tracing::debug;

use crate::metrics::names;
use crate::ratelimit;
use crate::state::SharedState;

/// Spawn the keep-alive ticker if enabled.
pub fn spawn(state: SharedState) {
    let secs = state.config.gateway.keepalive_secs;
    if secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(secs));
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            ping_all(&state).await;
        }
    });
}

/// Ping every eligible key once. Sequential — keep-alive isn't latency-critical
/// and a handful of fast calls is cheap.
async fn ping_all(state: &SharedState) {
    let cost = state.costs.cost_of("getHealth") as u64;
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "getHealth"});

    for up in &state.pool.upstreams {
        let now = ratelimit::now_ms();
        if !up.is_enabled() || up.in_cooldown(now) || !up.has_quota_for(cost) {
            continue;
        }

        let mut url = state.upstream_base.clone();
        url.query_pairs_mut()
            .clear()
            .append_pair("api-key", up.api_key.expose());

        match state.http.post(url).json(&body).send().await {
            Ok(resp) if resp.status().as_u16() == 429 => {
                up.trip_cooldown(now);
                up.record_rate_limited();
            }
            Ok(_) => {
                let used = up.add_credits(cost);
                metrics::counter!(names::KEEPALIVE_TOTAL, "upstream" => up.name.clone())
                    .increment(1);
                metrics::counter!(names::CREDITS_CONSUMED_TOTAL, "upstream" => up.name.clone())
                    .increment(cost);
                metrics::gauge!(names::CREDITS_REMAINING, "upstream" => up.name.clone())
                    .set(up.credit_cap.saturating_sub(used) as f64);
            }
            Err(e) => debug!(upstream = %up.name, error = %e, "keep-alive ping failed"),
        }
    }
}
