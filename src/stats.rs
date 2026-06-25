//! Process-wide statistics that persist across restarts.
//!
//! Holds the global counters (all-exhausted, per-method tallies) and the monthly
//! usage history, and coordinates the UTC month rollover. Together with the
//! per-key lifetime counters on [`crate::upstream::Upstream`], this is the
//! durable source of truth; on boot it is restored and replayed into the
//! Prometheus recorder so `/metrics` resumes from where it left off.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::credits::{self, Parsed};
use crate::metrics::names;
use crate::upstream::Pool;

/// Per-key credit usage for a completed month.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonthlyUpstream {
    pub name: String,
    pub credits_used: u64,
}

/// A closed monthly accounting period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonthlyRecord {
    /// The month that ended, encoded `YYYYMM`.
    pub month: u32,
    pub total_credits: u64,
    pub upstreams: Vec<MonthlyUpstream>,
}

/// Global, durable statistics.
#[derive(Debug, Default)]
pub struct Stats {
    /// Active accounting month (`YYYYMM`); 0 until initialized at boot.
    pub current_month: AtomicU32,
    /// Times every key was unavailable for a request.
    pub all_exhausted: AtomicU64,
    /// Lifetime per-method request counts.
    pub methods: Mutex<BTreeMap<String, u64>>,
    /// Closed monthly periods, oldest first.
    pub history: Mutex<Vec<MonthlyRecord>>,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_exhausted(&self) {
        self.all_exhausted.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(names::ALL_EXHAUSTED_TOTAL).increment(1);
    }

    /// Tally the method(s) of a request (batch expands) in memory and Prometheus.
    pub fn record_methods(&self, parsed: &Parsed) {
        let mut map = self.methods.lock().unwrap();
        for method in credits::methods(parsed) {
            *map.entry(method.to_string()).or_insert(0) += 1;
            metrics::counter!(names::RPC_METHOD_TOTAL, "method" => method.to_string()).increment(1);
        }
    }

    /// If the UTC month has changed, close out the previous month into history
    /// and reset every key's monthly counter. The winner of the CAS does the
    /// work, so concurrent callers can't double-roll.
    pub fn roll_month_if_changed(&self, pool: &Pool, cur_yyyymm: u32) {
        let prev = self.current_month.load(Ordering::Acquire);
        if prev == cur_yyyymm {
            return;
        }
        if self
            .current_month
            .compare_exchange(prev, cur_yyyymm, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if prev != 0 {
            self.history.lock().unwrap().push(capture_month(pool, prev));
        }
        for up in &pool.upstreams {
            up.reset_monthly(cur_yyyymm);
            metrics::gauge!(names::CREDITS_REMAINING, "upstream" => up.name.clone())
                .set(up.remaining_credits() as f64);
        }
    }

    /// Snapshot of the in-progress month plus all closed periods (for `/stats/history`).
    pub fn history_view(&self, pool: &Pool) -> serde_json::Value {
        let cur = self.current_month.load(Ordering::Acquire);
        let current = capture_month(pool, cur);
        serde_json::json!({
            "current_month": cur,
            "current": current,
            "history": *self.history.lock().unwrap(),
        })
    }
}

/// Build a [`MonthlyRecord`] from the pool's current monthly counters.
pub fn capture_month(pool: &Pool, month: u32) -> MonthlyRecord {
    let upstreams: Vec<MonthlyUpstream> = pool
        .upstreams
        .iter()
        .map(|u| MonthlyUpstream {
            name: u.name.clone(),
            credits_used: u.credits_used(),
        })
        .collect();
    let total_credits = upstreams.iter().map(|u| u.credits_used).sum();
    MonthlyRecord {
        month,
        total_credits,
        upstreams,
    }
}

/// After restoring durable state, replay the persisted totals into the Prometheus
/// recorder so `/metrics` counters continue rather than restarting at zero.
pub fn replay_to_prometheus(pool: &Pool, stats: &Stats) {
    for up in &pool.upstreams {
        let name = up.name.clone();
        increment_nonzero(names::CREDITS_CONSUMED_TOTAL, &name, up.credits_total());
        metrics::counter!(names::REQUESTS_TOTAL, "upstream" => name.clone(), "outcome" => "ok")
            .increment(up.requests_ok());
        metrics::counter!(names::REQUESTS_TOTAL, "upstream" => name.clone(), "outcome" => "rate_limited")
            .increment(up.requests_rate_limited());
        metrics::counter!(names::REQUESTS_TOTAL, "upstream" => name.clone(), "outcome" => "error")
            .increment(up.requests_error());
        metrics::counter!(names::RATE_LIMIT_HITS_TOTAL, "upstream" => name.clone())
            .increment(up.rate_limit_hits());
        metrics::gauge!(names::CREDITS_REMAINING, "upstream" => name.clone())
            .set(up.remaining_credits() as f64);
    }
    for (method, count) in stats.methods.lock().unwrap().iter() {
        metrics::counter!(names::RPC_METHOD_TOTAL, "method" => method.clone()).increment(*count);
    }
    let all = stats.all_exhausted.load(Ordering::Acquire);
    if all > 0 {
        metrics::counter!(names::ALL_EXHAUSTED_TOTAL).increment(all);
    }
}

fn increment_nonzero(metric: &'static str, upstream: &str, value: u64) {
    if value > 0 {
        metrics::counter!(metric, "upstream" => upstream.to_string()).increment(value);
    }
}
