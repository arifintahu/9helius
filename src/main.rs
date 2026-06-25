//! 9helius binary entry point — loads config, initializes telemetry, and serves
//! the proxy. All application logic lives in the `ninehelius` library crate.

use std::time::Duration;

use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use ninehelius::config::Config;
use ninehelius::state::{AppState, SharedState};
use ninehelius::upstream::current_yyyymm;
use ninehelius::{metrics, persistence, ratelimit, router};

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
    let state = AppState::new(config, prom)?;

    // Restore credit usage from the last snapshot (current month only).
    persistence::restore_into(&state.pool, &state.config.persistence);

    spawn_snapshot_writer(state.clone());
    spawn_month_reset_ticker(state.clone());

    let app = router(state.clone());

    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(%bind, "9helius listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Persist final state on the way out.
    save_snapshot(&state);
    info!("shutdown complete");
    Ok(())
}

/// Periodically flush the credit snapshot to disk.
fn spawn_snapshot_writer(state: SharedState) {
    let interval = state.config.persistence.interval_secs.max(1);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval));
        tick.tick().await; // consume the immediate first tick
        loop {
            tick.tick().await;
            save_snapshot(&state);
        }
    });
}

/// Reset per-key monthly counters shortly after each UTC month boundary.
fn spawn_month_reset_ticker(state: SharedState) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            state.pool.reset_month_all(current_yyyymm());
        }
    });
}

fn save_snapshot(state: &SharedState) {
    let snap = persistence::Snapshot::capture(&state.pool, current_yyyymm(), ratelimit::now_ms());
    if let Err(e) = persistence::save(&state.config.persistence.path, &snap) {
        warn!(error = %e, "snapshot save failed");
    }
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
