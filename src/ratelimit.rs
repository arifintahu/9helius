//! Rate-limit primitives: proactive per-class token buckets and the reactive
//! cooldown/backoff math.
//!
//! Proactive: each upstream key holds one [`Limiter`] per [`MethodClass`], sized
//! from the configured free-tier RPS. A request consumes a token for its class;
//! if the bucket is empty the selector skips to another key.
//!
//! Reactive: when an upstream returns 429 / JSON-RPC -32005, the key is put on a
//! cooldown that grows exponentially (1s → 32s, capped 30s) with ±25% jitter.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::{SystemTime, UNIX_EPOCH};

use governor::{Quota, RateLimiter};

use crate::config::RpsConfig;
use crate::credits::MethodClass;

/// A direct (non-keyed) in-memory token bucket.
pub type Limiter = governor::DefaultDirectRateLimiter;

/// Build one limiter per class from the configured RPS. A class with rps = 0 is
/// left unlimited (no entry).
pub fn build_limiters(rps: &RpsConfig) -> HashMap<MethodClass, Limiter> {
    let mut m = HashMap::new();
    insert(&mut m, MethodClass::StandardRpc, rps.standard_rpc);
    insert(&mut m, MethodClass::SendTransaction, rps.send_transaction);
    insert(&mut m, MethodClass::GetProgramAccounts, rps.get_program_accounts);
    insert(&mut m, MethodClass::Das, rps.das);
    insert(&mut m, MethodClass::Zk, rps.zk);
    m
}

fn insert(m: &mut HashMap<MethodClass, Limiter>, class: MethodClass, rps: u32) {
    if let Some(n) = NonZeroU32::new(rps) {
        m.insert(class, RateLimiter::direct(Quota::per_second(n)));
    }
}

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Cooldown duration (ms) for a given backoff step: `1000 * 2^step`, capped at
/// 30s, with ±25% jitter derived cheaply from `now_ms` (no RNG dependency).
pub fn backoff_with_jitter(step: u32, now_ms: u64) -> u64 {
    let base = (1000u64 << step.min(5)).min(30_000);
    let span = base / 2; // ±25% → total width base/2
    let jitter = if span == 0 {
        0
    } else {
        (now_ms % span) as i64 - (span as i64) / 2
    };
    (base as i64 + jitter).max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        // Use a fixed `now` so jitter is deterministic.
        let now = 0;
        assert_eq!(backoff_with_jitter(0, now), 1000 - 250);
        assert_eq!(backoff_with_jitter(1, now), 2000 - 500);
        // Steps beyond 5 stay capped at 30s base.
        assert_eq!(backoff_with_jitter(9, now), 30_000 - 7_500);
    }

    #[test]
    fn jitter_stays_within_band() {
        for now in [1u64, 7, 123, 999, 50_000] {
            let d = backoff_with_jitter(2, now); // base 4000, ±1000
            assert!((3000..=5000).contains(&d), "step2 now={now} -> {d}");
        }
    }

    #[test]
    fn limiters_respect_zero_as_unlimited() {
        let rps = RpsConfig {
            standard_rpc: 0,
            send_transaction: 1,
            get_program_accounts: 5,
            das: 2,
            zk: 2,
        };
        let m = build_limiters(&rps);
        assert!(!m.contains_key(&MethodClass::StandardRpc));
        assert!(m.contains_key(&MethodClass::SendTransaction));
    }
}
