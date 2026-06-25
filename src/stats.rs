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

/// Per-key credit usage within an accounting period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsagePoint {
    pub name: String,
    pub credits_used: u64,
}

/// A closed monthly accounting period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonthlyRecord {
    /// The month that ended, encoded `YYYYMM`.
    pub month: u32,
    pub total_credits: u64,
    pub upstreams: Vec<UsagePoint>,
}

/// A closed daily accounting period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyRecord {
    /// The day that ended, encoded `YYYYMMDD`.
    pub day: u32,
    pub total_credits: u64,
    pub upstreams: Vec<UsagePoint>,
}

/// Global, durable statistics.
#[derive(Debug, Default)]
pub struct Stats {
    /// Active accounting month (`YYYYMM`); 0 until initialized at boot.
    pub current_month: AtomicU32,
    /// Active accounting day (`YYYYMMDD`); 0 until initialized at boot.
    pub current_day: AtomicU32,
    /// Times every key was unavailable for a request.
    pub all_exhausted: AtomicU64,
    /// Lifetime per-method request counts.
    pub methods: Mutex<BTreeMap<String, u64>>,
    /// Closed monthly periods, oldest first.
    pub history: Mutex<Vec<MonthlyRecord>>,
    /// Closed daily periods, oldest first (bounded by retention).
    pub daily_history: Mutex<Vec<DailyRecord>>,
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

    /// If the UTC day has changed, close the previous day into the daily history
    /// (trimmed to `retention_days`) and re-baseline each key's daily counter.
    pub fn roll_day_if_changed(&self, pool: &Pool, cur_yyyymmdd: u32, retention_days: usize) {
        let prev = self.current_day.load(Ordering::Acquire);
        if prev == cur_yyyymmdd {
            return;
        }
        if self
            .current_day
            .compare_exchange(prev, cur_yyyymmdd, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if prev != 0 {
            self.push_daily(capture_day(pool, prev), retention_days);
        }
        for up in &pool.upstreams {
            up.start_new_day();
        }
    }

    /// Append a closed day to the history, trimmed to `retention_days`.
    pub fn push_daily(&self, record: DailyRecord, retention_days: usize) {
        let mut h = self.daily_history.lock().unwrap();
        h.push(record);
        trim_front(&mut h, retention_days);
    }

    /// Snapshot of the in-progress periods plus all closed history (for `/stats/history`).
    pub fn history_view(&self, pool: &Pool) -> serde_json::Value {
        let month = self.current_month.load(Ordering::Acquire);
        let day = self.current_day.load(Ordering::Acquire);
        serde_json::json!({
            "current_month": month,
            "current": capture_month(pool, month),
            "history": *self.history.lock().unwrap(),
            "current_day": day,
            "today": capture_day(pool, day),
            "daily_history": *self.daily_history.lock().unwrap(),
        })
    }
}

/// Keep only the most recent `max` entries (drop from the front).
fn trim_front<T>(v: &mut Vec<T>, max: usize) {
    if max > 0 && v.len() > max {
        let drop = v.len() - max;
        v.drain(0..drop);
    }
}

/// Build a [`MonthlyRecord`] from the pool's current monthly counters.
pub fn capture_month(pool: &Pool, month: u32) -> MonthlyRecord {
    let upstreams = usage_points(pool, |u| u.credits_used());
    let total_credits = upstreams.iter().map(|u| u.credits_used).sum();
    MonthlyRecord {
        month,
        total_credits,
        upstreams,
    }
}

/// Build a [`DailyRecord`] from the pool's current daily counters.
pub fn capture_day(pool: &Pool, day: u32) -> DailyRecord {
    let upstreams = usage_points(pool, |u| u.daily_used());
    let total_credits = upstreams.iter().map(|u| u.credits_used).sum();
    DailyRecord {
        day,
        total_credits,
        upstreams,
    }
}

fn usage_points(pool: &Pool, used: impl Fn(&crate::upstream::Upstream) -> u64) -> Vec<UsagePoint> {
    pool.upstreams
        .iter()
        .map(|u| UsagePoint {
            name: u.name.clone(),
            credits_used: used(u),
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RpsConfig, UpstreamConfig};

    fn cfg(name: &str) -> UpstreamConfig {
        UpstreamConfig {
            name: name.into(),
            api_key: "k".into(),
            credit_cap: 1_000_000,
            enabled: true,
        }
    }

    fn pool() -> Pool {
        Pool::from_config(&[cfg("a"), cfg("b")], &RpsConfig::default())
    }

    #[test]
    fn day_rollover_records_and_rebaselines() {
        let pool = pool();
        pool.upstreams[0].add_credits(30);
        pool.upstreams[1].add_credits(12);
        let stats = Stats::new();
        stats.current_day.store(20260101, Ordering::Release);

        stats.roll_day_if_changed(&pool, 20260102, 90);

        {
            let h = stats.daily_history.lock().unwrap();
            assert_eq!(h.len(), 1);
            assert_eq!(h[0].day, 20260101);
            assert_eq!(h[0].total_credits, 42); // 30 + 12
        }
        // The new day starts fresh; lifetime is untouched.
        assert_eq!(pool.upstreams[0].daily_used(), 0);
        pool.upstreams[0].add_credits(5);
        assert_eq!(pool.upstreams[0].daily_used(), 5);
        assert_eq!(pool.upstreams[0].credits_total(), 35);
    }

    #[test]
    fn daily_history_trims_to_retention() {
        let pool = pool();
        let stats = Stats::new();
        for d in 1..=5 {
            stats.push_daily(capture_day(&pool, 20260100 + d), 3);
        }
        let h = stats.daily_history.lock().unwrap();
        assert_eq!(h.len(), 3);
        assert_eq!(h[0].day, 20260103); // oldest retained
        assert_eq!(h[2].day, 20260105);
    }
}
