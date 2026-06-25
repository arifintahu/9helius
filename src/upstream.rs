//! Upstream key pool and round-robin selection.
//!
//! Each [`Upstream`] holds the runtime state for one Helius api-key. All mutable
//! state is atomic so the hot path is lock-free. M2 implements round-robin
//! selection that skips disabled keys; quota, cooldown, and RPS gating are
//! layered on in later milestones (the fields already exist here).

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use serde::Serialize;

use crate::config::UpstreamConfig;

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
#[derive(Debug)]
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
}

impl Upstream {
    pub fn from_config(c: &UpstreamConfig) -> Self {
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
        }
    }

    pub fn credits_used(&self) -> u64 {
        self.credits_used.load(Ordering::Acquire)
    }

    pub fn remaining_credits(&self) -> u64 {
        self.credit_cap.saturating_sub(self.credits_used())
    }

    /// True if charging `cost` more credits would stay within the cap.
    pub fn has_quota_for(&self, cost: u64) -> bool {
        self.credits_used().saturating_add(cost) <= self.credit_cap
    }

    /// Commit `cost` credits against this key, returning the new total.
    pub fn add_credits(&self, cost: u64) -> u64 {
        self.credits_used.fetch_add(cost, Ordering::AcqRel) + cost
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// A point-in-time snapshot for the `/stats` endpoint.
    pub fn stat(&self) -> UpstreamStat {
        UpstreamStat {
            name: self.name.clone(),
            credits_used: self.credits_used(),
            credit_cap: self.credit_cap,
            remaining: self.remaining_credits(),
            in_flight: self.in_flight.load(Ordering::Acquire),
            enabled: self.is_enabled(),
        }
    }
}

/// Serializable per-upstream view for `/stats`.
#[derive(Debug, Serialize)]
pub struct UpstreamStat {
    pub name: String,
    pub credits_used: u64,
    pub credit_cap: u64,
    pub remaining: u64,
    pub in_flight: u32,
    pub enabled: bool,
}

/// The pool of upstream keys plus a round-robin rotor.
pub struct Pool {
    pub upstreams: Vec<Arc<Upstream>>,
    cursor: AtomicUsize,
}

impl Pool {
    pub fn from_config(cfgs: &[UpstreamConfig]) -> Self {
        Pool {
            upstreams: cfgs
                .iter()
                .map(|c| Arc::new(Upstream::from_config(c)))
                .collect(),
            cursor: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.upstreams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.upstreams.is_empty()
    }

    /// Round-robin selection skipping disabled keys. Each call advances the rotor
    /// so concurrent requests fan out across keys. Returns `None` if every key is
    /// disabled.
    pub fn select_round_robin(&self) -> Option<Arc<Upstream>> {
        let n = self.upstreams.len();
        if n == 0 {
            return None;
        }
        let base = self.cursor.fetch_add(1, Ordering::Relaxed);
        for off in 0..n {
            let up = &self.upstreams[(base + off) % n];
            if up.is_enabled() {
                return Some(up.clone());
            }
        }
        None
    }

    /// Round-robin selection skipping keys that are disabled or would exceed
    /// their monthly credit cap for a request costing `est_cost`. Returns `None`
    /// if no key qualifies.
    pub fn select_for_cost(&self, est_cost: u64) -> Option<Arc<Upstream>> {
        let n = self.upstreams.len();
        if n == 0 {
            return None;
        }
        let base = self.cursor.fetch_add(1, Ordering::Relaxed);
        for off in 0..n {
            let up = &self.upstreams[(base + off) % n];
            if up.is_enabled() && up.has_quota_for(est_cost) {
                return Some(up.clone());
            }
        }
        None
    }

    pub fn stats(&self) -> Vec<UpstreamStat> {
        self.upstreams.iter().map(|u| u.stat()).collect()
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

    #[test]
    fn round_robin_cycles_all_enabled() {
        let pool = Pool::from_config(&[cfg("a", true), cfg("b", true), cfg("c", true)]);
        let picks: Vec<String> = (0..3)
            .map(|_| pool.select_round_robin().unwrap().name.clone())
            .collect();
        // Three consecutive picks should cover all three distinct keys.
        let mut sorted = picks.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "expected all keys used, got {picks:?}");
    }

    #[test]
    fn round_robin_skips_disabled() {
        let pool = Pool::from_config(&[cfg("a", false), cfg("b", true), cfg("c", false)]);
        for _ in 0..5 {
            assert_eq!(pool.select_round_robin().unwrap().name, "b");
        }
    }

    #[test]
    fn none_when_all_disabled() {
        let pool = Pool::from_config(&[cfg("a", false), cfg("b", false)]);
        assert!(pool.select_round_robin().is_none());
    }

    #[test]
    fn select_for_cost_skips_over_quota() {
        // "a" has only 5 credits of headroom; a cost-10 request must route to "b".
        let pool = Pool::from_config(&[cfg_cap("a", true, 5), cfg_cap("b", true, 1_000_000)]);
        for _ in 0..6 {
            assert_eq!(pool.select_for_cost(10).unwrap().name, "b");
        }
    }

    #[test]
    fn select_for_cost_none_when_all_over_quota() {
        let pool = Pool::from_config(&[cfg_cap("a", true, 5), cfg_cap("b", true, 5)]);
        assert!(pool.select_for_cost(10).is_none());
    }

    #[test]
    fn add_credits_reduces_remaining() {
        let up = Upstream::from_config(&cfg_cap("a", true, 100));
        assert_eq!(up.remaining_credits(), 100);
        up.add_credits(30);
        assert_eq!(up.remaining_credits(), 70);
        assert!(up.has_quota_for(70));
        assert!(!up.has_quota_for(71));
    }
}
