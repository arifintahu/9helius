//! 9helius binary entry point — loads config, initializes telemetry, and serves
//! the proxy. All application logic lives in the `ninehelius` library crate.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use ninehelius::config::Config;
use ninehelius::state::{AppState, SharedState};
use ninehelius::upstream::{current_yyyymm, current_yyyymmdd};
use ninehelius::{metrics, persistence, ratelimit, router};

/// Transparent Helius RPC load balancer.
#[derive(Parser, Debug)]
#[command(name = "ninehelius", version, about, long_about = None)]
struct Cli {
    /// Path to the config file.
    /// Overrides the NINEHELIUS_CONFIG env var; defaults to ./config.toml.
    #[arg(short, long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Path to the credit/stats snapshot file.
    /// Overrides `persistence.path` from the config (use an absolute path for
    /// service deployments so it doesn't depend on the working directory).
    #[arg(short, long, value_name = "PATH")]
    state: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();

    // Config path precedence: --config flag > NINEHELIUS_CONFIG env > ./config.toml
    let config_path = cli
        .config
        .map(|p| p.to_string_lossy().into_owned())
        .or_else(|| std::env::var("NINEHELIUS_CONFIG").ok())
        .unwrap_or_else(|| "config.toml".into());

    let mut config = Config::load(&config_path)?;
    // --state overrides the snapshot path from the config file.
    if let Some(state) = cli.state {
        config.persistence.path = state;
    }
    info!(
        path = %config_path,
        state = %config.persistence.path.display(),
        upstreams = config.upstreams.len(),
        bind = %config.gateway.bind,
        "configuration loaded"
    );

    let prom = metrics::init_recorder()?;
    let bind = config.gateway.bind;
    let state = AppState::new(config, prom)?;

    // Restore all durable state (credits, lifetime counters, history) and replay
    // it into Prometheus so /metrics resumes.
    persistence::restore_into(&state.pool, &state.stats, &state.config.persistence);

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

/// Close out and reset monthly + daily counters at each UTC boundary, recording
/// the ended period into history.
fn spawn_month_reset_ticker(state: SharedState) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        let retention = state.config.persistence.daily_retention_days;
        loop {
            tick.tick().await;
            state
                .stats
                .roll_month_if_changed(&state.pool, current_yyyymm());
            state
                .stats
                .roll_day_if_changed(&state.pool, current_yyyymmdd(), retention);
        }
    });
}

fn save_snapshot(state: &SharedState) {
    let snap = persistence::Snapshot::capture(&state.pool, &state.stats, ratelimit::now_ms());
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
