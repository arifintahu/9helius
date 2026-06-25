//! Credit-usage persistence: an atomic JSON snapshot that survives restarts.
//!
//! State is tiny (a few rows), single-writer, and authoritative in RAM. We flush
//! a snapshot periodically and on shutdown via a temp-file + rename so a crash
//! mid-write can't corrupt the file. On boot we restore usage only if the
//! snapshot is from the current UTC month (otherwise the month rolled over while
//! we were down, so everyone starts fresh).

use std::path::Path;
use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::{PersistenceConfig, SnapshotErrorPolicy};
use crate::upstream::{current_yyyymm, Pool};

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub epoch_yyyymm: u32,
    pub saved_at_ms: u64,
    pub upstreams: Vec<UpstreamSnap>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpstreamSnap {
    pub name: String,
    pub credits_used: u64,
}

impl Snapshot {
    /// Capture current usage from the pool.
    pub fn capture(pool: &Pool, cur_yyyymm: u32, now_ms: u64) -> Self {
        Snapshot {
            epoch_yyyymm: cur_yyyymm,
            saved_at_ms: now_ms,
            upstreams: pool
                .upstreams
                .iter()
                .map(|u| UpstreamSnap {
                    name: u.name.clone(),
                    credits_used: u.credits_used(),
                })
                .collect(),
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

/// Initialize every key's month epoch and restore prior usage where applicable.
///
/// - snapshot present & same month → restore credits by name
/// - snapshot present but older month → start at zero (month already rolled over)
/// - snapshot absent → start at zero
/// - snapshot corrupt → apply `policy` (zero, or assume each key at its cap)
pub fn restore_into(pool: &Pool, cfg: &PersistenceConfig) {
    let cur = current_yyyymm();

    match load(&cfg.path) {
        Ok(Some(snap)) if snap.epoch_yyyymm == cur => {
            for up in &pool.upstreams {
                let used = snap
                    .upstreams
                    .iter()
                    .find(|s| s.name == up.name)
                    .map(|s| s.credits_used)
                    .unwrap_or(0);
                up.restore(cur, used);
            }
            info!(month = cur, "restored credit usage from snapshot");
        }
        Ok(Some(_)) => {
            for up in &pool.upstreams {
                up.restore(cur, 0);
            }
            info!("snapshot is from a previous month; starting fresh");
        }
        Ok(None) => {
            for up in &pool.upstreams {
                up.restore(cur, 0);
            }
        }
        Err(e) => {
            let fill_cap = cfg.on_snapshot_error == SnapshotErrorPolicy::Cap;
            warn!(error = %e, fail_closed = fill_cap, "snapshot unreadable; applying policy");
            for up in &pool.upstreams {
                let used = if fill_cap { up.credit_cap } else { 0 };
                up.restore(cur, used);
                if fill_cap {
                    // restore() set credits to cap, but keep the atomic explicit
                    up.credits_used.store(up.credit_cap, Ordering::Release);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PersistenceConfig, RpsConfig, SnapshotErrorPolicy, UpstreamConfig};

    fn cfg(name: &str) -> UpstreamConfig {
        UpstreamConfig {
            name: name.into(),
            api_key: "k".into(),
            credit_cap: 1_000_000,
            enabled: true,
        }
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("ninehelius-test-{tag}-{}.json", std::process::id()));
        p
    }

    #[test]
    fn save_then_load_roundtrips() {
        let pool = Pool::from_config(&[cfg("a"), cfg("b")], &RpsConfig::default());
        pool.upstreams[0].add_credits(123);
        pool.upstreams[1].add_credits(456);
        let path = temp_path("roundtrip");
        let snap = Snapshot::capture(&pool, 202606, 999);
        save(&path, &snap).unwrap();

        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.epoch_yyyymm, 202606);
        assert_eq!(loaded.upstreams.len(), 2);
        assert_eq!(loaded.upstreams[0].credits_used, 123);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_file_is_none() {
        let path = temp_path("missing-xyz");
        let _ = std::fs::remove_file(&path);
        assert!(load(&path).unwrap().is_none());
    }

    fn pcfg(path: std::path::PathBuf) -> PersistenceConfig {
        PersistenceConfig {
            path,
            interval_secs: 10,
            on_snapshot_error: SnapshotErrorPolicy::Zero,
        }
    }

    #[test]
    fn restore_same_month_restores_usage() {
        let pool = Pool::from_config(&[cfg("a"), cfg("b")], &RpsConfig::default());
        let path = temp_path("restore-same");
        let snap = Snapshot {
            epoch_yyyymm: current_yyyymm(),
            saved_at_ms: 0,
            upstreams: vec![
                UpstreamSnap {
                    name: "a".into(),
                    credits_used: 500,
                },
                UpstreamSnap {
                    name: "b".into(),
                    credits_used: 7,
                },
            ],
        };
        save(&path, &snap).unwrap();

        restore_into(&pool, &pcfg(path.clone()));
        assert_eq!(pool.find("a").unwrap().credits_used(), 500);
        assert_eq!(pool.find("b").unwrap().credits_used(), 7);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn restore_old_month_starts_fresh() {
        let pool = Pool::from_config(&[cfg("a")], &RpsConfig::default());
        let path = temp_path("restore-old");
        let snap = Snapshot {
            epoch_yyyymm: 200001, // ancient
            saved_at_ms: 0,
            upstreams: vec![UpstreamSnap {
                name: "a".into(),
                credits_used: 500,
            }],
        };
        save(&path, &snap).unwrap();

        restore_into(&pool, &pcfg(path.clone()));
        assert_eq!(pool.find("a").unwrap().credits_used(), 0);
        // Epoch is initialized to the current month so no spurious reset later.
        assert_eq!(
            pool.find("a").unwrap().epoch_yyyymm.load(Ordering::Acquire),
            current_yyyymm()
        );
        let _ = std::fs::remove_file(&path);
    }
}
