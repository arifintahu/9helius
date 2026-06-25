//! Configuration schema and loader.
//!
//! Config is layered: `config.toml` (or a path from `NINEHELIUS_CONFIG`) is the
//! base, then environment variables prefixed `NINEHELIUS_` (with `__` as the
//! nesting separator) override individual fields.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;

/// Top-level configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub gateway: GatewayConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub costs: CostsConfig,
    #[serde(default)]
    pub rps: RpsConfig,
    pub upstreams: Vec<UpstreamConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    pub bind: SocketAddr,
    pub api_key: String,
    #[serde(default = "default_upstream_base")]
    pub upstream_base: String,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersistenceConfig {
    #[serde(default = "default_snapshot_path")]
    pub path: PathBuf,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    #[serde(default)]
    pub on_snapshot_error: SnapshotErrorPolicy,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            path: default_snapshot_path(),
            interval_secs: default_interval_secs(),
            on_snapshot_error: SnapshotErrorPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotErrorPolicy {
    /// Start every key at zero usage (default).
    #[default]
    Zero,
    /// Assume every key is at its cap (fail-closed).
    Cap,
}

/// Per-method credit cost configuration. Defaults are applied in code
/// ([`crate::credits`]); this only overrides or adds entries.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CostsConfig {
    #[serde(default)]
    pub overrides: HashMap<String, u32>,
    /// Cost charged for non-JSON-RPC REST paths when no path rule matches.
    #[serde(default = "default_rest_cost")]
    pub default_rest_cost: u32,
}

/// Per-class requests-per-second limits (Helius free tier, per key).
#[derive(Debug, Clone, Deserialize)]
pub struct RpsConfig {
    #[serde(default = "rps_standard")]
    pub standard_rpc: u32,
    #[serde(default = "rps_send")]
    pub send_transaction: u32,
    #[serde(default = "rps_gpa")]
    pub get_program_accounts: u32,
    #[serde(default = "rps_das")]
    pub das: u32,
    #[serde(default = "rps_zk")]
    pub zk: u32,
}

impl Default for RpsConfig {
    fn default() -> Self {
        Self {
            standard_rpc: rps_standard(),
            send_transaction: rps_send(),
            get_program_accounts: rps_gpa(),
            das: rps_das(),
            zk: rps_zk(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamConfig {
    pub name: String,
    pub api_key: String,
    #[serde(default = "default_credit_cap")]
    pub credit_cap: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Config {
    /// Load configuration from a TOML file plus `NINEHELIUS_` env overrides.
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let cfg: Config = Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("NINEHELIUS_").split("__"))
            .extract()?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.gateway.api_key.trim().is_empty() {
            anyhow::bail!("gateway.api_key must not be empty");
        }
        if self.gateway.api_key == "CHANGE_ME_gateway_token" {
            anyhow::bail!("gateway.api_key is still the placeholder — set a real value");
        }
        if self.upstreams.is_empty() {
            anyhow::bail!("at least one [[upstreams]] entry is required");
        }
        for up in &self.upstreams {
            if up.api_key.trim().is_empty() {
                anyhow::bail!("upstream '{}' has an empty api_key", up.name);
            }
            if up.credit_cap == 0 {
                anyhow::bail!("upstream '{}' has credit_cap = 0", up.name);
            }
        }
        if self.gateway.max_retries == 0 {
            anyhow::bail!("gateway.max_retries must be >= 1");
        }
        Ok(())
    }
}

fn default_upstream_base() -> String {
    "https://mainnet.helius-rpc.com".to_string()
}
fn default_request_timeout_ms() -> u64 {
    15_000
}
fn default_max_retries() -> u32 {
    6
}
fn default_max_body_bytes() -> usize {
    5 * 1024 * 1024
}
fn default_snapshot_path() -> PathBuf {
    PathBuf::from("state/credits.snapshot.json")
}
fn default_interval_secs() -> u64 {
    10
}
fn default_rest_cost() -> u32 {
    100
}
fn default_credit_cap() -> u64 {
    1_000_000
}
fn default_true() -> bool {
    true
}
fn rps_standard() -> u32 {
    10
}
fn rps_send() -> u32 {
    1
}
fn rps_gpa() -> u32 {
    5
}
fn rps_das() -> u32 {
    2
}
fn rps_zk() -> u32 {
    2
}
