//! Upstream key pool and selection.
//!
//! Each [`Upstream`] holds the runtime state for one Helius api-key. All mutable
//! state is atomic (plus lock-free governor limiters) so the hot path needs no
//! locks. [`Pool::select`] performs round-robin selection that skips keys which
//! are disabled, over monthly quota, on rate-limit cooldown, or out of RPS
//! tokens for the request's method class.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use serde::Serialize;

use crate::config::{RpsConfig, UpstreamConfig};
use crate::credits::MethodClass;
use crate::ratelimit::{self, Limiter};

/// Current UTC month encoded as `YYYYMM` (e.g. June 2026 → 202606).
pub fn current_yyyymm() -> u32 {
    let now = time::OffsetDateTime::now_utc();
    now.year() as u32 * 100 + u8::from(now.month()) as u32
}

/// Current UTC day encoded as `YYYYMMDD` (e.g. 2026-06-25 → 20260625).
pub fn current_yyyymmdd() -> u32 {
    let now = time::OffsetDateTime::now_utc();
    now.year() as u32 * 10_000 + u8::from(now.month()) as u32 * 100 + now.day() as u32
}

/// A string that never reveals itself in `Debug`/`Display` output.
#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        SecretString(s)
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"<redacted>\"")
    }
}

/// Runtime state for a single upstream Helius key.
pub struct Upstream {
    pub name: String,
    pub api_key: SecretString,
    pub credit_cap: u64,

    /// Credits consumed this UTC month (authoritative, atomic).
    pub credits_used: AtomicU64,
    /// `YYYYMM` epoch guarding the lazy monthly reset.
    pub epoch_yyyymm: AtomicU32,

    /// Unix-millis until which this key is on rate-limit cooldown (0 = available).
    pub cooldown_until: AtomicU64,
    /// Current exponential-backoff exponent.
    pub backoff_step: AtomicU32,

    /// In-flight requests currently routed to this key.
    pub in_flight: AtomicU32,

    pub enabled: AtomicBool,

    // ---- lifetime counters: persisted, never reset (basis for /metrics + history) ----
    /// Total credits ever charged to this key (across all months).
    pub credits_total: AtomicU64,
    pub requests_ok: AtomicU64,
    pub requests_rate_limited: AtomicU64,
    pub requests_error: AtomicU64,
    pub rate_limit_hits: AtomicU64,
    /// Lifetime `credits_total` as of the start of the current day; daily usage
    /// is `credits_total - day_start_total`.
    pub day_start_total: AtomicU64,

    /// Proactive RPS token buckets, one per method class.
    limiters: HashMap<MethodClass, Limiter>,
}

impl Upstream {
    pub fn from_config(c: &UpstreamConfig, rps: &RpsConfig) -> Self {
        Upstream {
            name: c.name.clone(),
            api_key: c.api_key.clone().into(),
            credit_cap: c.credit_cap,
            credits_used: AtomicU64::new(0),
            epoch_yyyymm: AtomicU32::new(0),
            cooldown_until: AtomicU64::new(0),
            backoff_step: AtomicU32::new(0),
            in_flight: AtomicU32::new(0),
            enabled: AtomicBool::new(c.enabled),
            credits_total: AtomicU64::new(0),
            requests_ok: AtomicU64::new(0),
            requests_rate_limited: AtomicU64::new(0),
            requests_error: AtomicU64::new(0),
            rate_limit_hits: AtomicU64::new(0),
            day_start_total: AtomicU64::new(0),
            limiters: ratelimit::build_limiters(rps),
        }
    }

    pub fn credits_used(&self) -> u64 {
        self.credits_used.load(Ordering::Acquire)
    }

    pub fn credits_total(&self) -> u64 {
        self.credits_total.load(Ordering::Acquire)
    }
    pub fn requests_ok(&self) -> u64 {
        self.requests_ok.load(Ordering::Acquire)
    }
    pub fn requests_rate_limited(&self) -> u64 {
        self.requests_rate_limited.load(Ordering::Acquire)
    }
    pub fn requests_error(&self) -> u64 {
        self.requests_error.load(Ordering::Acquire)
    }
    pub fn rate_limit_hits(&self) -> u64 {
        self.rate_limit_hits.load(Ordering::Acquire)
    }
    pub fn day_start_total(&self) -> u64 {
        self.day_start_total.load(Ordering::Acquire)
    }
    /// Credits used so far today (derived from the monotonic lifetime total).
    pub fn daily_used(&self) -> u64 {
        self.credits_total().saturating_sub(self.day_start_total())
    }
    /// Re-baseline daily usage to the current lifetime total (day rollover).
    pub fn start_new_day(&self) {
        self.day_start_total
            .store(self.credits_total(), Ordering::Release);
    }
    /// Restore the day baseline on boot.
    pub fn restore_day_start(&self, value: u64) {
        self.day_start_total.store(value, Ordering::Release);
    }

    /// Record a successful (serviced) request.
    pub fn record_ok(&self) {
        self.requests_ok.fetch_add(1, Ordering::Relaxed);
    }
    /// Record a rate-limited response (429 / -32005).
    pub fn record_rate_limited(&self) {
        self.requests_rate_limited.fetch_add(1, Ordering::Relaxed);
        self.rate_limit_hits.fetch_add(1, Ordering::Relaxed);
    }
    /// Record a transient upstream error.
    pub fn record_error(&self) {
        self.requests_error.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remaining_credits(&self) -> u64 {
        self.credit_cap.saturating_sub(self.credits_used())
    }

    /// True if charging `cost` more credits would stay within the cap.
    pub fn has_quota_for(&self, cost: u64) -> bool {
        self.credits_used().saturating_add(cost) <= self.credit_cap
    }

    /// Commit `cost` credits against this key, returning the new monthly total.
    /// Bumps both the monthly counter and the lifetime total.
    pub fn add_credits(&self, cost: u64) -> u64 {
        self.credits_total.fetch_add(cost, Ordering::AcqRel);
        self.credits_used.fetch_add(cost, Ordering::AcqRel) + cost
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Try to consume one RPS token for `class`. Unlimited classes always succeed.
    pub fn try_acquire(&self, class: MethodClass) -> bool {
        match self.limiters.get(&class) {
            Some(l) => l.check().is_ok(),
            None => true,
        }
    }

    pub fn in_cooldown(&self, now_ms: u64) -> bool {
        self.cooldown_until.load(Ordering::Acquire) > now_ms
    }

    pub fn cooldown_remaining_ms(&self, now_ms: u64) -> u64 {
        self.cooldown_until
            .load(Ordering::Acquire)
            .saturating_sub(now_ms)
    }

    /// Put this key on cooldown after a rate-limit response, growing the backoff.
    pub fn trip_cooldown(&self, now_ms: u64) {
        let step = self.backoff_step.fetch_add(1, Ordering::AcqRel);
        let dur = ratelimit::backoff_with_jitter(step, now_ms);
        self.cooldown_until.store(now_ms + dur, Ordering::Release);
    }

    /// Reset the backoff after a successful response.
    pub fn note_success(&self) {
        if self.backoff_step.load(Ordering::Acquire) != 0 {
            self.backoff_step.store(0, Ordering::Release);
        }
    }

    /// Reset the monthly credit counter and set the month epoch (rollover/boot).
    /// Lifetime totals are preserved.
    pub fn reset_monthly(&self, cur_yyyymm: u32) {
        self.epoch_yyyymm.store(cur_yyyymm, Ordering::Release);
        self.credits_used.store(0, Ordering::Release);
    }

    /// Restore monthly usage and the month epoch on boot.
    pub fn restore_monthly(&self, cur_yyyymm: u32, credits_used: u64) {
        self.epoch_yyyymm.store(cur_yyyymm, Ordering::Release);
        self.credits_used.store(credits_used, Ordering::Release);
    }

    /// Restore lifetime counters on boot.
    pub fn restore_lifetime(
        &self,
        credits_total: u64,
        requests_ok: u64,
        requests_rate_limited: u64,
        requests_error: u64,
        rate_limit_hits: u64,
    ) {
        self.credits_total.store(credits_total, Ordering::Release);
        self.requests_ok.store(requests_ok, Ordering::Release);
        self.requests_rate_limited
            .store(requests_rate_limited, Ordering::Release);
        self.requests_error.store(requests_error, Ordering::Release);
        self.rate_limit_hits.store(rate_limit_hits, Ordering::Release);
    }

    /// A point-in-time snapshot for the `/stats` endpoint.
    pub fn stat(&self, now_ms: u64) -> UpstreamStat {
        UpstreamStat {
            name: self.name.clone(),
            credits_used: self.credits_used(),
            credit_cap: self.credit_cap,
            remaining: self.remaining_credits(),
            credits_total: self.credits_total(),
            requests_ok: self.requests_ok(),
            requests_rate_limited: self.requests_rate_limited(),
            requests_error: self.requests_error(),
            rate_limit_hits: self.rate_limit_hits(),
            in_flight: self.in_flight.load(Ordering::Acquire),
            cooldown_ms_left: self.cooldown_remaining_ms(now_ms),
            enabled: self.is_enabled(),
        }
    }
}

/// Serializable per-upstream view for `/stats`.
#[derive(Debug, Serialize)]
pub struct UpstreamStat {
    pub name: String,
    /// Credits used this month (resets at the UTC month boundary).
    pub credits_used: u64,
    pub credit_cap: u64,
    pub remaining: u64,
    /// Lifetime credits ever charged (never resets).
    pub credits_total: u64,
    pub requests_ok: u64,
    pub requests_rate_limited: u64,
    pub requests_error: u64,
    pub rate_limit_hits: u64,
    pub in_flight: u32,
    pub cooldown_ms_left: u64,
    pub enabled: bool,
}

/// The pool of upstream keys plus a round-robin rotor.
pub struct Pool {
    pub upstreams: Vec<Arc<Upstream>>,
    cursor: AtomicUsize,
}

impl Pool {
    pub fn from_config(cfgs: &[UpstreamConfig], rps: &RpsConfig) -> Self {
        Pool {
            upstreams: cfgs
                .iter()
                .map(|c| Arc::new(Upstream::from_config(c, rps)))
                .collect(),
            cursor: AtomicUsize::new(0),
        }
    }

    /// True if at least one key is enabled and has monthly quota left (cooldown
    /// is transient, so it doesn't count against capacity).
    pub fn has_available_capacity(&self) -> bool {
        self.upstreams
            .iter()
            .any(|u| u.is_enabled() && u.remaining_credits() > 0)
    }

    /// Select an upstream for a request of the given `class` and `est_cost`,
    /// skipping keys already tried this request (`skip`). Round-robin order;
    /// skips disabled / over-quota / cooling-down / RPS-starved keys. Consumes
    /// one RPS token from the chosen key. Returns its index plus a handle.
    pub fn select(
        &self,
        class: MethodClass,
        est_cost: u64,
        skip: &[usize],
        now_ms: u64,
    ) -> Option<(usize, Arc<Upstream>)> {
        let n = self.upstreams.len();
        if n == 0 {
            return None;
        }
        let base = self.cursor.fetch_add(1, Ordering::Relaxed);
        for off in 0..n {
            let idx = (base + off) % n;
            if skip.contains(&idx) {
                continue;
            }
            let up = &self.upstreams[idx];
            if !up.is_enabled() || !up.has_quota_for(est_cost) || up.in_cooldown(now_ms) {
                continue;
            }
            if !up.try_acquire(class) {
                continue;
            }
            return Some((idx, up.clone()));
        }
        None
    }

    /// Seconds until the soonest key leaves cooldown (min 1), if any is cooling.
    pub fn soonest_cooldown_secs(&self, now_ms: u64) -> Option<u64> {
        self.upstreams
            .iter()
            .map(|u| u.cooldown_remaining_ms(now_ms))
            .filter(|&ms| ms > 0)
            .min()
            .map(|ms| ms.div_ceil(1000).max(1))
    }

    /// Look up an upstream by name.
    pub fn find(&self, name: &str) -> Option<&Arc<Upstream>> {
        self.upstreams.iter().find(|u| u.name == name)
    }

    pub fn stats(&self, now_ms: u64) -> Vec<UpstreamStat> {
        self.upstreams.iter().map(|u| u.stat(now_ms)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(name: &str, enabled: bool) -> UpstreamConfig {
        cfg_cap(name, enabled, 1_000_000)
    }

    fn cfg_cap(name: &str, enabled: bool, credit_cap: u64) -> UpstreamConfig {
        UpstreamConfig {
            name: name.into(),
            api_key: format!("key-{name}"),
            credit_cap,
            enabled,
        }
    }

    fn pool(cfgs: &[UpstreamConfig]) -> Pool {
        Pool::from_config(cfgs, &RpsConfig::default())
    }

    const NOW: u64 = 1_000_000_000_000;

    #[test]
    fn select_cycles_all_enabled() {
        let p = pool(&[cfg("a", true), cfg("b", true), cfg("c", true)]);
        let picks: Vec<String> = (0..3)
            .map(|_| {
                p.select(MethodClass::StandardRpc, 1, &[], NOW)
                    .unwrap()
                    .1
                    .name
                    .clone()
            })
            .collect();
        let mut sorted = picks.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "expected all keys used, got {picks:?}");
    }

    #[test]
    fn select_skips_disabled() {
        let p = pool(&[cfg("a", false), cfg("b", true), cfg("c", false)]);
        for _ in 0..5 {
            assert_eq!(
                p.select(MethodClass::StandardRpc, 1, &[], NOW).unwrap().1.name,
                "b"
            );
        }
    }

    #[test]
    fn select_skips_over_quota() {
        let p = pool(&[cfg_cap("a", true, 5), cfg_cap("b", true, 1_000_000)]);
        for _ in 0..6 {
            assert_eq!(
                p.select(MethodClass::StandardRpc, 10, &[], NOW).unwrap().1.name,
                "b"
            );
        }
    }

    #[test]
    fn select_none_when_all_over_quota() {
        let p = pool(&[cfg_cap("a", true, 5), cfg_cap("b", true, 5)]);
        assert!(p.select(MethodClass::StandardRpc, 10, &[], NOW).is_none());
    }

    #[test]
    fn select_honours_skip_set() {
        let p = pool(&[cfg("a", true), cfg("b", true)]);
        // Whatever index 0 maps to, skipping it must yield the other key.
        let (idx, up) = p.select(MethodClass::StandardRpc, 1, &[], NOW).unwrap();
        let (_, other) = p.select(MethodClass::StandardRpc, 1, &[idx], NOW).unwrap();
        assert_ne!(up.name, other.name);
    }

    #[test]
    fn select_skips_cooldown() {
        let p = pool(&[cfg("a", true), cfg("b", true)]);
        p.upstreams[0].trip_cooldown(NOW);
        p.upstreams[1].trip_cooldown(NOW);
        // Both cooling down → nothing selectable.
        assert!(p.select(MethodClass::StandardRpc, 1, &[], NOW).is_none());
        // Far in the future, cooldowns have elapsed.
        assert!(p
            .select(MethodClass::StandardRpc, 1, &[], NOW + 60_000)
            .is_some());
    }

    #[test]
    fn send_transaction_limited_to_one_rps_then_skips() {
        // Single key, sendTransaction bucket = 1 RPS. Second immediate call has
        // no token, so selection returns None.
        let p = pool(&[cfg("a", true)]);
        assert!(p
            .select(MethodClass::SendTransaction, 1, &[], NOW)
            .is_some());
        assert!(p
            .select(MethodClass::SendTransaction, 1, &[], NOW)
            .is_none());
    }

    #[test]
    fn add_credits_reduces_remaining() {
        let up = Upstream::from_config(&cfg_cap("a", true, 100), &RpsConfig::default());
        assert_eq!(up.remaining_credits(), 100);
        up.add_credits(30);
        assert_eq!(up.remaining_credits(), 70);
        assert!(up.has_quota_for(70));
        assert!(!up.has_quota_for(71));
    }

    #[test]
    fn cooldown_and_recovery() {
        let up = Upstream::from_config(&cfg("a", true), &RpsConfig::default());
        assert!(!up.in_cooldown(NOW));
        up.trip_cooldown(NOW);
        assert!(up.in_cooldown(NOW));
        up.note_success();
        // backoff reset, but the existing cooldown window still applies until it elapses
        assert!(up.in_cooldown(NOW));
        assert!(!up.in_cooldown(NOW + 60_000));
    }
}
