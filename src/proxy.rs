//! The transparent proxy handler.
//!
//! Registered as the axum `fallback`, it captures any method + path that isn't a
//! reserved management route, authenticates the gateway api-key, then forwards
//! the request to an upstream Helius endpoint — rewriting only the `api-key`
//! query parameter and relaying the upstream status, headers, and body verbatim.
//!
//! Selection is round-robin across keys that have quota, aren't cooling down, and
//! have an RPS token for the request's method class. If an upstream returns a
//! rate-limit (HTTP 429 or JSON-RPC -32005) the key is put on cooldown and the
//! request is retried on the next available key.

use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, State};
use axum::http::{header, HeaderMap, HeaderName, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use tracing::{debug, warn, Instrument};
use url::Url;

use crate::credits::{self, Parsed};
use crate::error::ProxyError;
use crate::metrics::names;
use crate::ratelimit;
use crate::state::SharedState;
use crate::upstream::Upstream;

/// Headers that must not be forwarded hop-to-hop (RFC 7230 §6.1) plus `host`,
/// which reqwest sets correctly for the upstream.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
];

/// Helius rate-limit JSON-RPC error code ("Too many requests").
const RATE_LIMIT_RPC_CODE: i64 = -32005;

/// A fully-buffered upstream response (needed to inspect for -32005 and to allow
/// retrying the request body against another key).
struct Buffered {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

/// The axum fallback handler.
pub async fn handle(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let span = tracing::info_span!("request", method = %method, path = uri.path());
    let start = Instant::now();
    let result = proxy(&state, method, uri, headers, body)
        .instrument(span)
        .await;
    metrics::histogram!(names::REQUEST_DURATION_SECONDS).record(start.elapsed().as_secs_f64());
    match result {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn proxy(
    state: &SharedState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ProxyError> {
    // 1. Gateway authentication.
    check_gateway_auth(state, &uri, &headers)?;

    // 2. Body-size guard.
    if body.len() > state.config.gateway.max_body_bytes {
        return Err(ProxyError::PayloadTooLarge);
    }

    // 3. Estimate credit cost and pick the RPS class.
    let parsed = credits::parse_body(uri.path(), &body);
    let est = credits::request_cost(&parsed, &state.costs);
    let class = credits::primary_class(&parsed);

    // 4. Selection + retry-on-rate-limit loop.
    let mut skip: Vec<usize> = Vec::new();
    for _ in 0..state.config.gateway.max_retries {
        let now = ratelimit::now_ms();
        let Some((idx, upstream)) = state.pool.select(class, est, &skip, now) else {
            return Err(exhausted(state, now));
        };

        let result = forward_once(state, &upstream, &method, &uri, &headers, &body).await;

        match result {
            Ok(buf) if is_rate_limited(&buf) => {
                upstream.trip_cooldown(now);
                upstream.record_rate_limited();
                debug!(upstream = %upstream.name, "upstream rate-limited; cooling down, retrying");
                metrics::counter!(names::RATE_LIMIT_HITS_TOTAL, "upstream" => upstream.name.clone())
                    .increment(1);
                metrics::counter!(names::REQUESTS_TOTAL,
                    "upstream" => upstream.name.clone(), "outcome" => "rate_limited")
                .increment(1);
                skip.push(idx);
                continue;
            }
            Ok(buf) => {
                upstream.note_success();
                upstream.record_ok();
                commit_credits(state, &upstream, est, &parsed);
                metrics::counter!(names::REQUESTS_TOTAL,
                    "upstream" => upstream.name.clone(), "outcome" => "ok")
                .increment(1);
                return Ok(build_response(buf));
            }
            Err(e) => {
                warn!(upstream = %upstream.name, error = %e, "upstream request failed");
                upstream.record_error();
                metrics::counter!(names::UPSTREAM_ERRORS_TOTAL,
                    "upstream" => upstream.name.clone(), "kind" => "transient")
                .increment(1);
                metrics::counter!(names::REQUESTS_TOTAL,
                    "upstream" => upstream.name.clone(), "outcome" => "error")
                .increment(1);
                skip.push(idx);
                continue;
            }
        }
    }

    Err(exhausted(state, ratelimit::now_ms()))
}

/// Build the all-upstreams-exhausted error with a Retry-After hint and metric.
fn exhausted(state: &SharedState, now_ms: u64) -> ProxyError {
    state.stats.record_exhausted();
    ProxyError::AllUpstreamsExhausted {
        retry_after_secs: state.pool.soonest_cooldown_secs(now_ms),
    }
}

/// Forward the request once to a specific upstream and buffer the full response.
async fn forward_once(
    state: &SharedState,
    upstream: &Upstream,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Buffered, reqwest::Error> {
    let url = build_upstream_url(&state.upstream_base, uri, upstream.api_key.expose());
    debug!(upstream = %upstream.name, method = %method, path = uri.path(), "forwarding");

    let mut req = state.http.request(method.clone(), url);
    for (name, value) in headers {
        if is_hop_by_hop(name) || is_gateway_auth_header(name) {
            continue;
        }
        req = req.header(name, value);
    }

    upstream.in_flight.fetch_add(1, Ordering::AcqRel);
    metrics::gauge!(names::INFLIGHT, "upstream" => upstream.name.clone()).increment(1.0);

    let result = async {
        let resp = req.body(body.clone()).send().await?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.bytes().await?;
        Ok(Buffered {
            status,
            headers,
            body,
        })
    }
    .await;

    upstream.in_flight.fetch_sub(1, Ordering::AcqRel);
    metrics::gauge!(names::INFLIGHT, "upstream" => upstream.name.clone()).decrement(1.0);

    result
}

/// A rate-limit response is HTTP 429 or a JSON-RPC body with error code -32005.
fn is_rate_limited(buf: &Buffered) -> bool {
    if buf.status == StatusCode::TOO_MANY_REQUESTS {
        return true;
    }
    #[derive(Deserialize)]
    struct ErrPeek {
        error: Option<Code>,
    }
    #[derive(Deserialize)]
    struct Code {
        code: i64,
    }
    if matches!(first_non_ws(&buf.body), Some(b'{')) {
        if let Ok(peek) = serde_json::from_slice::<ErrPeek>(&buf.body) {
            return peek.error.map(|e| e.code) == Some(RATE_LIMIT_RPC_CODE);
        }
    }
    false
}

fn first_non_ws(body: &[u8]) -> Option<u8> {
    body.iter().copied().find(|b| !b.is_ascii_whitespace())
}

/// Charge credits to the chosen key and update the related metrics + tallies.
fn commit_credits(state: &SharedState, upstream: &Upstream, est: u64, parsed: &Parsed) {
    let used = upstream.add_credits(est);
    metrics::counter!(names::CREDITS_CONSUMED_TOTAL, "upstream" => upstream.name.clone())
        .increment(est);
    metrics::gauge!(names::CREDITS_REMAINING, "upstream" => upstream.name.clone())
        .set(upstream.credit_cap.saturating_sub(used) as f64);
    state.stats.record_methods(parsed);
}

/// Validate the gateway api-key, supplied as `?api-key=`, `x-api-key:`, or
/// `Authorization: Bearer`.
fn check_gateway_auth(state: &SharedState, uri: &Uri, headers: &HeaderMap) -> Result<(), ProxyError> {
    let expected = state.config.gateway.api_key.as_str();

    if let Some(q) = uri.query() {
        for (k, v) in url::form_urlencoded::parse(q.as_bytes()) {
            if k == "api-key" && v == expected {
                return Ok(());
            }
        }
    }
    if let Some(v) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        if v == expected {
            return Ok(());
        }
    }
    if let Some(v) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if v.strip_prefix("Bearer ").map(str::trim) == Some(expected) {
            return Ok(());
        }
    }
    Err(ProxyError::Unauthorized)
}

/// Build the upstream URL: base host + original path + original query with the
/// `api-key` parameter rewritten to the chosen upstream key.
fn build_upstream_url(base: &Url, uri: &Uri, upstream_key: &str) -> Url {
    let mut url = base.clone();
    url.set_path(uri.path());

    let preserved: Vec<(String, String)> = uri
        .query()
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .filter(|(k, _)| k != "api-key")
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default();

    {
        let mut qp = url.query_pairs_mut();
        qp.clear();
        for (k, v) in &preserved {
            qp.append_pair(k, v);
        }
        qp.append_pair("api-key", upstream_key);
    }
    url
}

/// Relay a buffered upstream response, stripping hop-by-hop headers.
fn build_response(buf: Buffered) -> Response {
    let mut builder = Response::builder().status(buf.status);
    for (name, value) in &buf.headers {
        if is_hop_by_hop(name) {
            continue;
        }
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from(buf.body))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    HOP_BY_HOP
        .iter()
        .any(|h| name.as_str().eq_ignore_ascii_case(h))
}

/// The gateway's own auth header must not leak to the upstream.
fn is_gateway_auth_header(name: &HeaderName) -> bool {
    let n = name.as_str();
    n.eq_ignore_ascii_case("x-api-key") || n.eq_ignore_ascii_case("authorization")
}
