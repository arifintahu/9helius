//! 9helius — a transparent Helius RPC load balancer.
//!
//! Combines several Helius free-tier api-keys behind one gateway URL, forwarding
//! requests in round-robin while tracking credit usage and respecting rate limits.

// Some items are introduced a milestone before their first use; this is removed
// during the final hardening pass.
#![allow(dead_code)]

mod config;
mod error;
mod metrics;
mod state;

use axum::routing::get;
use axum::Router;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::Config;
use crate::state::{AppState, SharedState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = std::env::var("NINEHELIUS_CONFIG").unwrap_or_else(|_| "config.toml".into());
    let config = Config::load(&config_path)?;
    info!(
        path = %config_path,
        upstreams = config.upstreams.len(),
        bind = %config.gateway.bind,
        "configuration loaded"
    );

    let prom = metrics::init_recorder()?;
    let bind = config.gateway.bind;
    let state = AppState::new(config, prom);

    let app = router(state.clone());

    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(%bind, "9helius listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("shutdown complete");
    Ok(())
}

fn router(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(metrics::health))
        .route("/metrics", get(metrics::prometheus))
        .route("/stats", get(metrics::stats))
        .with_state(state)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,ninehelius=debug"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}
