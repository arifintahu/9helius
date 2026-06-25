//! Gateway-local error type and its mapping to HTTP / JSON-RPC responses.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use thiserror::Error;

/// Errors produced by the gateway itself (not relayed from upstream).
#[derive(Debug, Error)]
pub enum ProxyError {
    /// Missing or incorrect gateway api-key.
    #[error("unauthorized")]
    Unauthorized,

    /// Request body exceeded `gateway.max_body_bytes`.
    #[error("payload too large")]
    PayloadTooLarge,

    /// Every upstream key was over quota or rate-limited.
    #[error("all upstreams exhausted")]
    AllUpstreamsExhausted {
        /// Seconds until the soonest upstream becomes available again, if known.
        retry_after_secs: Option<u64>,
    },

    /// Could not reach any upstream (connect/timeout on all tries).
    #[error("bad gateway: {0}")]
    BadGateway(String),
}

impl ProxyError {
    fn status(&self) -> StatusCode {
        match self {
            ProxyError::Unauthorized => StatusCode::UNAUTHORIZED,
            ProxyError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ProxyError::AllUpstreamsExhausted { .. } => StatusCode::TOO_MANY_REQUESTS,
            ProxyError::BadGateway(_) => StatusCode::BAD_GATEWAY,
        }
    }

    /// JSON-RPC error code, mirroring Helius conventions where relevant.
    fn rpc_code(&self) -> i64 {
        match self {
            ProxyError::Unauthorized => -32001,
            ProxyError::PayloadTooLarge => -32600,
            ProxyError::AllUpstreamsExhausted { .. } => -32005, // "Too many requests"
            ProxyError::BadGateway(_) => -32603,
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": serde_json::Value::Null,
            "error": { "code": self.rpc_code(), "message": self.to_string() },
        });

        let mut resp = (status, axum::Json(body)).into_response();
        if let ProxyError::AllUpstreamsExhausted {
            retry_after_secs: Some(secs),
        } = self
        {
            if let Ok(val) = header::HeaderValue::from_str(&secs.to_string()) {
                resp.headers_mut().insert(header::RETRY_AFTER, val);
            }
        }
        resp
    }
}
