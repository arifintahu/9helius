//! Durable state: an atomic JSON snapshot that survives restarts.
//!
//! The snapshot captures everything we don't want to lose on restart — per-key
//! monthly usage *and* lifetime counters, global counters, per-method tallies,
//! and the closed monthly history. It's flushed periodically and on shutdown via
//! temp-file + rename so a crash mid-write can't corrupt it.
//!
//! On boot, [`restore_into`] rehydrates the in-memory atomics and replays the
//! totals into Prometheus so `/metrics` resumes rather than restarting at zero.
//! Monthly counters are restored only if the snapshot is from the current UTC
//! month; otherwise that month is closed into history and counters start fresh.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::{PersistenceConfig, SnapshotErrorPolicy};
use crate::stats::{replay_to_prometheus, DailyRecord, MonthlyRecord, Stats, UsagePoint};
use crate::upstream::{current_yyyymm, current_yyyymmdd, Pool};

/// Current on-disk schema version.
const SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(default)]
    pub version: u32,
    /// Accounting month the `credits_used` values belong to.
    pub epoch_yyyymm: u32,
    pub saved_at_ms: u64,
    pub upstreams: Vec<UpstreamSnap>,
    #[serde(default)]
    pub all_exhausted: u64,
    #[serde(default)]
    pub methods: BTreeMap<String, u64>,
    #[serde(default)]
    pub history: Vec<MonthlyRecord>,
    /// Day the `day_start_total` baselines belong to.
    #[serde(default)]
    pub current_day: u32,
    #[serde(default)]
    pub daily_history: Vec<DailyRecord>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct UpstreamSnap {
    pub name: String,
    /// Credits used in `epoch_yyyymm`.
    pub credits_used: u64,
    #[serde(default)]
    pub credits_total: u64,
    #[serde(default)]
    pub requests_ok: u64,
    #[serde(default)]
    pub requests_rate_limited: u64,
    #[serde(default)]
    pub requests_error: u64,
    #[serde(default)]
    pub rate_limit_hits: u64,
    /// Lifetime total as of the start of `current_day`.
    #[serde(default)]
    pub day_start_total: u64,
}

impl Snapshot {
    /// Capture the full durable state from the pool and global stats.
    pub fn capture(pool: &Pool, stats: &Stats, now_ms: u64) -> Self {
        Snapshot {
            version: SCHEMA_VERSION,
            epoch_yyyymm: stats.current_month.load(Ordering::Acquire),
            saved_at_ms: now_ms,
            upstreams: pool
                .upstreams
                .iter()
                .map(|u| UpstreamSnap {
                    name: u.name.clone(),
                    credits_used: u.credits_used(),
                    credits_total: u.credits_total(),
                    requests_ok: u.requests_ok(),
                    requests_rate_limited: u.requests_rate_limited(),
                    requests_error: u.requests_error(),
                    rate_limit_hits: u.rate_limit_hits(),
                    day_start_total: u.day_start_total(),
                })
                .collect(),
            all_exhausted: stats.all_exhausted.load(Ordering::Acquire),
            methods: stats.methods.lock().unwrap().clone(),
            history: stats.history.lock().unwrap().clone(),
            current_day: stats.current_day.load(Ordering::Acquire),
            daily_history: stats.daily_history.lock().unwrap().clone(),
        }
    }
}

/// Read a snapshot. `Ok(None)` means the file is absent; `Err` means it exists
/// but is unreadable/corrupt (the caller applies the configured policy).
pub fn load(path: &Path) -> anyhow::Result<Option<Snapshot>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Atomically write a snapshot: serialize to a temp file, then rename over the
/// target (replacing any existing file).
pub fn save(path: &Path, snap: &Snapshot) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(snap)?;
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Restore durable state into the pool and stats, then replay totals into the
/// Prometheus recorder.
pub fn restore_into(pool: &Pool, stats: &Stats, cfg: &PersistenceConfig) {
    let cur_month = current_yyyymm();
    let cur_day = current_yyyymmdd();

    match load(&cfg.path) {
        Ok(Some(snap)) => apply_snapshot(pool, stats, snap, cur_month, cur_day, cfg.daily_retention_days),
        Ok(None) => {
            for up in &pool.upstreams {
                up.restore_monthly(cur_month, 0);
                up.start_new_day();
            }
        }
        Err(e) => {
            let fill_cap = cfg.on_snapshot_error == SnapshotErrorPolicy::Cap;
            warn!(error = %e, fail_closed = fill_cap, "snapshot unreadable; applying policy");
            for up in &pool.upstreams {
                let used = if fill_cap { up.credit_cap } else { 0 };
                up.restore_monthly(cur_month, used);
                up.start_new_day();
            }
        }
    }

    stats.current_month.store(cur_month, Ordering::Release);
    stats.current_day.store(cur_day, Ordering::Release);
    replay_to_prometheus(pool, stats);
}

fn apply_snapshot(
    pool: &Pool,
    stats: &Stats,
    snap: Snapshot,
    cur_month: u32,
    cur_day: u32,
    daily_retention: usize,
) {
    // Lifetime counters and global stats are restored regardless of period.
    for up in &pool.upstreams {
        if let Some(s) = snap.upstreams.iter().find(|s| s.name == up.name) {
            up.restore_lifetime(
                s.credits_total,
                s.requests_ok,
                s.requests_rate_limited,
                s.requests_error,
                s.rate_limit_hits,
            );
            up.restore_day_start(s.day_start_total);
        }
    }
    stats.all_exhausted.store(snap.all_exhausted, Ordering::Release);
    *stats.methods.lock().unwrap() = snap.methods;
    *stats.history.lock().unwrap() = snap.history;
    *stats.daily_history.lock().unwrap() = snap.daily_history;

    // --- monthly ---
    if snap.epoch_yyyymm == cur_month {
        for up in &pool.upstreams {
            let used = snap
                .upstreams
                .iter()
                .find(|s| s.name == up.name)
                .map(|s| s.credits_used)
                .unwrap_or(0);
            up.restore_monthly(cur_month, used);
        }
        info!(month = cur_month, "restored full stats from snapshot");
    } else {
        if snap.epoch_yyyymm != 0 {
            let record = MonthlyRecord {
                month: snap.epoch_yyyymm,
                total_credits: snap.upstreams.iter().map(|s| s.credits_used).sum(),
                upstreams: snap
                    .upstreams
                    .iter()
                    .map(|s| UsagePoint {
                        name: s.name.clone(),
                        credits_used: s.credits_used,
                    })
                    .collect(),
            };
            stats.history.lock().unwrap().push(record);
        }
        for up in &pool.upstreams {
            up.restore_monthly(cur_month, 0);
        }
        info!(
            prev = snap.epoch_yyyymm,
            month = cur_month,
            "snapshot from a previous month; closed into history, monthly counters reset"
        );
    }

    // --- daily ---
    if snap.current_day != cur_day {
        if snap.current_day != 0 {
            // Close the snapshot's day using the restored lifetime/day baselines.
            stats.push_daily(crate::stats::capture_day(pool, snap.current_day), daily_retention);
        }
        for up in &pool.upstreams {
            up.start_new_day();
        }
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

    fn pool(names: &[&str]) -> Pool {
        let cfgs: Vec<UpstreamConfig> = names.iter().map(|n| cfg(n)).collect();
        Pool::from_config(&cfgs, &RpsConfig::default())
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("ninehelius-test-{tag}-{}.json", std::process::id()));
        p
    }

    fn pcfg(path: std::path::PathBuf) -> PersistenceConfig {
        PersistenceConfig {
            path,
            interval_secs: 10,
            on_snapshot_error: SnapshotErrorPolicy::Zero,
            daily_retention_days: 90,
        }
    }

    #[test]
    fn save_then_load_roundtrips_all_fields() {
        let pool = pool(&["a", "b"]);
        pool.upstreams[0].add_credits(123);
        pool.upstreams[0].record_ok();
        pool.upstreams[1].add_credits(456);
        let stats = Stats::new();
        stats.current_month.store(202606, Ordering::Release);
        stats.record_exhausted();
        {
            let mut m = stats.methods.lock().unwrap();
            m.insert("getSlot".into(), 9);
        }

        let path = temp_path("roundtrip");
        save(&path, &Snapshot::capture(&pool, &stats, 999)).unwrap();

        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.version, SCHEMA_VERSION);
        assert_eq!(loaded.epoch_yyyymm, 202606);
        assert_eq!(loaded.upstreams[0].credits_used, 123);
        assert_eq!(loaded.upstreams[0].credits_total, 123);
        assert_eq!(loaded.upstreams[0].requests_ok, 1);
        assert_eq!(loaded.all_exhausted, 1);
        assert_eq!(loaded.methods.get("getSlot"), Some(&9));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_file_is_none() {
        let path = temp_path("missing-xyz");
        let _ = std::fs::remove_file(&path);
        assert!(load(&path).unwrap().is_none());
    }

    #[test]
    fn restore_same_month_restores_usage_and_lifetime() {
        let pool = pool(&["a", "b"]);
        let path = temp_path("restore-same");
        let snap = Snapshot {
            version: SCHEMA_VERSION,
            epoch_yyyymm: current_yyyymm(),
            saved_at_ms: 0,
            upstreams: vec![
                UpstreamSnap {
                    name: "a".into(),
                    credits_used: 500,
                    credits_total: 1500,
                    requests_ok: 42,
                    ..Default::default()
                },
                UpstreamSnap {
                    name: "b".into(),
                    credits_used: 7,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        save(&path, &snap).unwrap();

        let stats = Stats::new();
        restore_into(&pool, &stats, &pcfg(path.clone()));
        assert_eq!(pool.find("a").unwrap().credits_used(), 500);
        assert_eq!(pool.find("a").unwrap().credits_total(), 1500);
        assert_eq!(pool.find("a").unwrap().requests_ok(), 42);
        assert_eq!(pool.find("b").unwrap().credits_used(), 7);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn restore_old_month_keeps_lifetime_and_records_history() {
        let pool = pool(&["a"]);
        let path = temp_path("restore-old");
        let snap = Snapshot {
            version: SCHEMA_VERSION,
            epoch_yyyymm: 200001, // ancient
            saved_at_ms: 0,
            upstreams: vec![UpstreamSnap {
                name: "a".into(),
                credits_used: 500,
                credits_total: 9000,
                ..Default::default()
            }],
            ..Default::default()
        };
        save(&path, &snap).unwrap();

        let stats = Stats::new();
        restore_into(&pool, &stats, &pcfg(path.clone()));
        // monthly reset, lifetime preserved
        assert_eq!(pool.find("a").unwrap().credits_used(), 0);
        assert_eq!(pool.find("a").unwrap().credits_total(), 9000);
        assert_eq!(
            pool.find("a").unwrap().epoch_yyyymm.load(Ordering::Acquire),
            current_yyyymm()
        );
        // the ended month was closed into history
        let hist = stats.history.lock().unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].month, 200001);
        assert_eq!(hist[0].total_credits, 500);
        let _ = std::fs::remove_file(&path);
    }
}
