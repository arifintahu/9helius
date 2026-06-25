//! Integration tests for the transparent proxy (M1).

use ninehelius::config::Config;
use ninehelius::state::AppState;
use serde_json::json;
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build an `AppState` whose upstream points at the given mock server URL.
async fn test_state(upstream_base: &str) -> ninehelius::state::SharedState {
    let toml = format!(
        r#"
        [gateway]
        bind = "127.0.0.1:0"
        api_key = "test-gw-key"
        upstream_base = "{upstream_base}"

        [[upstreams]]
        name = "u1"
        api_key = "upstream-key-1"
        "#
    );
    let config = Config::from_toml_str(&toml).expect("valid config");
    // A non-installed recorder handle is enough for tests (metrics become no-ops).
    let prom = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();
    AppState::new(config, prom).expect("state")
}

/// Build an `AppState` with several upstream keys, all pointing at `base`.
async fn test_state_keys(base: &str, keys: &[&str]) -> ninehelius::state::SharedState {
    let mut blocks = String::new();
    for (i, k) in keys.iter().enumerate() {
        blocks.push_str(&format!(
            "\n[[upstreams]]\nname = \"u{i}\"\napi_key = \"{k}\"\n"
        ));
    }
    let toml = format!(
        r#"
        [gateway]
        bind = "127.0.0.1:0"
        api_key = "test-gw-key"
        upstream_base = "{base}"
        {blocks}
        "#
    );
    let config = Config::from_toml_str(&toml).expect("valid config");
    let prom = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();
    AppState::new(config, prom).expect("state")
}

/// Spawn the app on an ephemeral port and return its base URL.
async fn spawn(state: ninehelius::state::SharedState) -> String {
    let app = ninehelius::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn forwards_and_rewrites_api_key() {
    let upstream = MockServer::start().await;
    // Only matches if the gateway rewrote api-key to the upstream key AND
    // preserved the path + JSON body verbatim.
    Mock::given(method("POST"))
        .and(path("/"))
        .and(query_param("api-key", "upstream-key-1"))
        .and(body_json(json!({"jsonrpc":"2.0","id":1,"method":"getHealth"})))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"jsonrpc":"2.0","id":1,"result":"ok"})),
        )
        .expect(1)
        .mount(&upstream)
        .await;

    let state = test_state(&upstream.uri()).await;
    let base = spawn(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/?api-key=test-gw-key"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getHealth"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["result"], "ok");
    // `.expect(1)` on the mock is verified on drop of the MockServer.
}

#[tokio::test]
async fn rejects_missing_gateway_key() {
    let upstream = MockServer::start().await;
    let state = test_state(&upstream.uri()).await;
    let base = spawn(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/?api-key=wrong-key"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getHealth"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    // Upstream must never have been contacted.
    assert!(upstream.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn tracks_credits_in_stats() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":1})))
        .mount(&upstream)
        .await;

    let state = test_state(&upstream.uri()).await;
    let base = spawn(state).await;
    let client = reqwest::Client::new();

    // getProgramAccounts costs 10 credits.
    client
        .post(format!("{base}/?api-key=test-gw-key"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getProgramAccounts"}))
        .send()
        .await
        .unwrap();

    let stats: serde_json::Value = client
        .get(format!("{base}/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let used = stats["upstreams"][0]["credits_used"].as_u64().unwrap();
    assert_eq!(used, 10, "expected 10 credits charged, stats={stats}");
}

#[tokio::test]
async fn retries_next_key_on_http_429() {
    let upstream = MockServer::start().await;
    // First key (u0 = key-1) is rate-limited; second key (u1 = key-2) succeeds.
    Mock::given(query_param("api-key", "key-1"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&upstream)
        .await;
    Mock::given(query_param("api-key", "key-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":"ok"})))
        .mount(&upstream)
        .await;

    let state = test_state_keys(&upstream.uri(), &["key-1", "key-2"]).await;
    let base = spawn(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/?api-key=test-gw-key"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getSlot"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["result"], "ok");
}

#[tokio::test]
async fn retries_next_key_on_jsonrpc_32005() {
    let upstream = MockServer::start().await;
    // HTTP 200 but JSON-RPC "Too many requests" — must be treated as rate-limited.
    Mock::given(query_param("api-key", "key-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"Too many requests"}}),
        ))
        .mount(&upstream)
        .await;
    Mock::given(query_param("api-key", "key-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":"ok"})))
        .mount(&upstream)
        .await;

    let state = test_state_keys(&upstream.uri(), &["key-1", "key-2"]).await;
    let base = spawn(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/?api-key=test-gw-key"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getSlot"}))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["result"], "ok", "should fail over past the -32005 key");
}

#[tokio::test]
async fn all_rate_limited_returns_429() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&upstream)
        .await;

    let state = test_state_keys(&upstream.uri(), &["key-1", "key-2"]).await;
    let base = spawn(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/?api-key=test-gw-key"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getSlot"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 429);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], -32005);
}

#[tokio::test]
async fn accepts_gateway_key_via_header() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(query_param("api-key", "upstream-key-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok":true})))
        .mount(&upstream)
        .await;

    let state = test_state(&upstream.uri()).await;
    let base = spawn(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/"))
        .header("x-api-key", "test-gw-key")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"getHealth"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
}
