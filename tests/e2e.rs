//! End-to-end tests.
//!
//! Unlike `proxy.rs` (which builds the router in-process), these spawn the real
//! compiled `ninehelius` binary as a child process, pointed at a mock Helius
//! upstream, and drive it over HTTP — exercising config loading, the full
//! server, background snapshot tasks, and restart recovery.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde_json::{json, Value};
use wiremock::matchers::{method, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

static SEQ: AtomicU32 = AtomicU32::new(0);

/// Kills the child process (and reaps it) when dropped, even on test panic.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn unique_dir(tag: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut p = std::env::temp_dir();
    p.push(format!("ninehelius-e2e-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Render a config TOML. `ups` is a list of (name, api_key, credit_cap).
fn config_toml(
    port: u16,
    snapshot: &str,
    upstream_base: &str,
    interval_secs: u64,
    ups: &[(&str, &str, u64)],
) -> String {
    // Forward slashes are valid in Windows paths and avoid TOML escaping.
    let snap = snapshot.replace('\\', "/");
    let mut s = format!(
        r#"
[gateway]
bind = "127.0.0.1:{port}"
api_key = "gw"
upstream_base = "{upstream_base}"
max_retries = 6

[persistence]
path = "{snap}"
interval_secs = {interval_secs}
"#
    );
    for (name, key, cap) in ups {
        s.push_str(&format!(
            "\n[[upstreams]]\nname = \"{name}\"\napi_key = \"{key}\"\ncredit_cap = {cap}\n"
        ));
    }
    s
}

fn spawn_bin(cfg_path: &Path) -> ChildGuard {
    let child = Command::new(env!("CARGO_BIN_EXE_ninehelius"))
        .env("NINEHELIUS_CONFIG", cfg_path)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn ninehelius binary");
    ChildGuard(child)
}

async fn wait_ready(base: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(400))
        .build()
        .unwrap();
    for _ in 0..60 {
        if let Ok(r) = client.get(format!("{base}/stats")).send().await {
            if r.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server at {base} did not become ready");
}

async fn stats(base: &str) -> Value {
    reqwest::get(format!("{base}/stats"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn total_credits(stats: &Value) -> u64 {
    stats["upstreams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["credits_used"].as_u64().unwrap())
        .sum()
}

/// Convenience: spin up a one-server harness with the given upstreams.
async fn start(
    tag: &str,
    upstream_base: &str,
    interval_secs: u64,
    ups: &[(&str, &str, u64)],
) -> (ChildGuard, String) {
    let dir = unique_dir(tag);
    let port = free_port();
    let snap = dir.join("snap.json");
    let cfg = config_toml(port, &snap.to_string_lossy(), upstream_base, interval_secs, ups);
    let cfg_path = dir.join("config.toml");
    std::fs::write(&cfg_path, cfg).unwrap();
    let guard = spawn_bin(&cfg_path);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base).await;
    (guard, base)
}

async fn post_rpc(base: &str, gw_key: &str, m: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/?api-key={gw_key}"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":m}))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn e2e_forwards_and_round_robins() {
    let upstream = MockServer::start().await;
    // Each key echoes its own name so we can observe the distribution.
    for k in ["k0", "k1"] {
        Mock::given(query_param("api-key", k))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": k})))
            .mount(&upstream)
            .await;
    }

    let (_g, base) = start(
        "rr",
        &upstream.uri(),
        3600,
        &[("u0", "k0", 1_000_000), ("u1", "k1", 1_000_000)],
    )
    .await;

    let mut seen = std::collections::HashSet::new();
    for _ in 0..4 {
        let body: Value = post_rpc(&base, "gw", "getSlot").await.json().await.unwrap();
        seen.insert(body["result"].as_str().unwrap().to_string());
    }
    assert_eq!(seen.len(), 2, "expected both keys used, saw {seen:?}");

    // 4 getSlot calls @ 1 credit each.
    assert_eq!(total_credits(&stats(&base).await), 4);
}

#[tokio::test]
async fn e2e_failover_on_429() {
    let upstream = MockServer::start().await;
    Mock::given(query_param("api-key", "k0"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&upstream)
        .await;
    Mock::given(query_param("api-key", "k1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": "ok"})))
        .mount(&upstream)
        .await;

    let (_g, base) = start(
        "failover",
        &upstream.uri(),
        3600,
        &[("u0", "k0", 1_000_000), ("u1", "k1", 1_000_000)],
    )
    .await;

    let resp = post_rpc(&base, "gw", "getSlot").await;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["result"], "ok");
}

#[tokio::test]
async fn e2e_rejects_bad_gateway_key() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&upstream)
        .await;

    let (_g, base) = start("auth", &upstream.uri(), 3600, &[("u0", "k0", 1_000_000)]).await;

    let resp = post_rpc(&base, "wrong-key", "getSlot").await;
    assert_eq!(resp.status(), 401);
    // Upstream must never have been contacted.
    assert!(upstream.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn e2e_health_503_when_exhausted() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": 1})))
        .mount(&upstream)
        .await;

    // Single key with a cap of exactly 1 credit.
    let (_g, base) = start("health", &upstream.uri(), 3600, &[("u0", "k0", 1)]).await;

    let client = reqwest::Client::new();
    // Healthy before the cap is consumed.
    assert_eq!(
        client.get(format!("{base}/health")).send().await.unwrap().status(),
        200
    );

    // One getSlot consumes the only credit.
    assert_eq!(post_rpc(&base, "gw", "getSlot").await.status(), 200);

    // Now exhausted → 503, and further RPC is rejected with 429.
    assert_eq!(
        client.get(format!("{base}/health")).send().await.unwrap().status(),
        503
    );
    assert_eq!(post_rpc(&base, "gw", "getSlot").await.status(), 429);
}

#[tokio::test]
async fn e2e_persists_credits_across_restart() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": 1})))
        .mount(&upstream)
        .await;

    let dir = unique_dir("persist");
    let snap = dir.join("snap.json");
    let snap_s = snap.to_string_lossy().to_string();
    let cfg_path = dir.join("config.toml");
    let ups = [("u0", "k0", 1_000_000u64), ("u1", "k1", 1_000_000)];

    // --- run 1: accumulate credits, let the 1s snapshot writer flush ---
    let port1 = free_port();
    std::fs::write(
        &cfg_path,
        config_toml(port1, &snap_s, &upstream.uri(), 1, &ups),
    )
    .unwrap();
    let g1 = spawn_bin(&cfg_path);
    let base1 = format!("http://127.0.0.1:{port1}");
    wait_ready(&base1).await;

    for _ in 0..3 {
        post_rpc(&base1, "gw", "getSlot").await;
    }
    tokio::time::sleep(Duration::from_millis(1500)).await; // periodic snapshot
    drop(g1); // kill run 1

    assert!(snap.exists(), "snapshot file should have been written");

    // --- run 2: fresh process, new port, same snapshot path ---
    let port2 = free_port();
    std::fs::write(
        &cfg_path,
        config_toml(port2, &snap_s, &upstream.uri(), 1, &ups),
    )
    .unwrap();
    let _g2 = spawn_bin(&cfg_path);
    let base2 = format!("http://127.0.0.1:{port2}");
    wait_ready(&base2).await;

    assert_eq!(
        total_credits(&stats(&base2).await),
        3,
        "credits should be restored from the snapshot"
    );
}
