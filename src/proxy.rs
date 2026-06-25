//! The transparent proxy handler.
//!
//! Registered as the axum `fallback`, it captures any method + path that isn't a
//! reserved management route, authenticates the gateway api-key, then forwards
//! the request to an upstream Helius endpoint — rewriting only the `api-key`
//! query parameter and relaying the upstream status, headers, and body verbatim.
//!
//! M1: single upstream key, no round-robin / credits / rate-limit handling yet.

use std::sync::atomic::Ordering;

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, State};
use axum::http::{header, HeaderMap, HeaderName, Method};
use axum::response::{IntoResponse, Response};
use tracing::{debug, warn};
use url::Url;

use crate::error::ProxyError;
use crate::metrics::names;
use crate::state::SharedState;

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

/// The axum fallback handler.
pub async fn handle(
    State(state): State<SharedState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match proxy(&state, method, uri, headers, body).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn proxy(
    state: &SharedState,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ProxyError> {
    // 1. Gateway authentication.
    check_gateway_auth(state, &uri, &headers)?;

    // 2. Body-size guard.
    if body.len() > state.config.gateway.max_body_bytes {
        return Err(ProxyError::PayloadTooLarge);
    }

    // 3. Pick an upstream (M2: round-robin over enabled keys).
    let upstream = state
        .pool
        .select_round_robin()
        .ok_or(ProxyError::AllUpstreamsExhausted {
            retry_after_secs: None,
        })?;

    // 4. Forward, tracking in-flight load and request outcome.
    let url = build_upstream_url(&state.upstream_base, &uri, upstream.api_key.expose())?;
    debug!(upstream = %upstream.name, method = %method, path = uri.path(), "forwarding");

    let mut req = state.http.request(method, url);
    for (name, value) in &headers {
        if is_hop_by_hop(name) || is_gateway_auth_header(name) {
            continue;
        }
        req = req.header(name, value);
    }

    upstream.in_flight.fetch_add(1, Ordering::AcqRel);
    metrics::gauge!(names::INFLIGHT, "upstream" => upstream.name.clone()).increment(1.0);

    let result = req.body(body).send().await;

    upstream.in_flight.fetch_sub(1, Ordering::AcqRel);
    metrics::gauge!(names::INFLIGHT, "upstream" => upstream.name.clone()).decrement(1.0);

    match result {
        Ok(resp) => {
            metrics::counter!(names::REQUESTS_TOTAL,
                "upstream" => upstream.name.clone(), "outcome" => "ok")
            .increment(1);
            relay_response(resp).await
        }
        Err(e) => {
            warn!(upstream = %upstream.name, error = %e, "upstream request failed");
            metrics::counter!(names::REQUESTS_TOTAL,
                "upstream" => upstream.name.clone(), "outcome" => "error")
            .increment(1);
            metrics::counter!(names::UPSTREAM_ERRORS_TOTAL,
                "upstream" => upstream.name.clone(), "kind" => "transient")
            .increment(1);
            Err(ProxyError::BadGateway(e.to_string()))
        }
    }
}

/// Validate the gateway api-key, supplied as `?api-key=`, `x-api-key:`, or
/// `Authorization: Bearer`.
fn check_gateway_auth(
    state: &SharedState,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
) -> Result<(), ProxyError> {
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
fn build_upstream_url(
    base: &Url,
    uri: &axum::http::Uri,
    upstream_key: &str,
) -> Result<Url, ProxyError> {
    let mut url = base.clone();
    url.set_path(uri.path());

    // Preserve all incoming query params except the client's api-key.
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
    Ok(url)
}

/// Convert a reqwest response into an axum response, relaying status, headers
/// (minus hop-by-hop), and body unchanged.
async fn relay_response(resp: reqwest::Response) -> Result<Response, ProxyError> {
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp
        .bytes()
        .await
        .map_err(|e| ProxyError::BadGateway(e.to_string()))?;

    let mut builder = Response::builder().status(status);
    for (name, value) in &headers {
        if is_hop_by_hop(name) {
            continue;
        }
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from(body))
        .map_err(|e| ProxyError::BadGateway(e.to_string()))
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    HOP_BY_HOP.iter().any(|h| name.as_str().eq_ignore_ascii_case(h))
}

/// The gateway's own auth header must not leak to the upstream.
fn is_gateway_auth_header(name: &HeaderName) -> bool {
    let n = name.as_str();
    n.eq_ignore_ascii_case("x-api-key") || n.eq_ignore_ascii_case("authorization")
}
