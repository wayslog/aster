mod tracker;

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use axum::{
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use once_cell::sync::Lazy;
use prometheus::{
    self, opts, register_gauge, register_gauge_vec, register_histogram_vec, register_int_counter,
    register_int_counter_vec, Encoder, Gauge, GaugeVec, HistogramVec, IntCounter, IntCounterVec,
};
use tokio::{net::TcpListener, task::JoinHandle, time};
use tracing::{error, info, warn};

pub use tracker::Tracker;

static FRONT_CONNECTIONS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        opts!(
            "aster_front_connection",
            "each front nodes connections gauge"
        ),
        &["cluster"]
    )
    .expect("front connections gauge registration must succeed")
});

static FRONT_CONNECTION_INCR: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_front_connection_incr",
            "count of front connections since start"
        ),
        &["cluster"]
    )
    .expect("front connections counter registration must succeed")
});

static VERSION_GAUGE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        opts!("aster_version", "aster current running version"),
        &["version"]
    )
    .expect("version gauge registration must succeed")
});

static MEMORY_USAGE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(opts!("aster_memory_usage", "aster current memory usage"))
        .expect("memory gauge registration must succeed")
});

static CPU_USAGE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(opts!("aster_cpu_usage", "aster current cpu usage"))
        .expect("cpu gauge registration must succeed")
});

static THREAD_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(opts!("aster_thread_count", "aster thread count counter"))
        .expect("thread counter registration must succeed")
});

static GLOBAL_ERROR: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(opts!("aster_global_error", "aster global error counter"))
        .expect("global error counter registration must succeed")
});

static TOTAL_TIMER: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "aster_total_timer",
        "set up each cluster command proxy total timer",
        &["cluster"],
        vec![1_000.0, 10_000.0, 40_000.0, 100_000.0, 200_000.0]
    )
    .expect("total timer histogram registration must succeed")
});

static REMOTE_TIMER: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "aster_remote_timer",
        "set up each cluster command proxy remote timer",
        &["cluster"],
        vec![1_000.0, 10_000.0, 100_000.0]
    )
    .expect("remote timer histogram registration must succeed")
});

/// Register the running version with metrics.
pub fn register_version(version: &str) {
    VERSION_GAUGE.with_label_values(&[version]).set(1.0);
}

/// Record a new front connection.
pub fn front_conn_open(cluster: &str) {
    FRONT_CONNECTION_INCR.with_label_values(&[cluster]).inc();
    FRONT_CONNECTIONS.with_label_values(&[cluster]).inc();
}

/// Record a front connection closure.
pub fn front_conn_close(cluster: &str) {
    FRONT_CONNECTIONS.with_label_values(&[cluster]).dec();
}

/// Increment the global error counter.
pub fn global_error_incr() {
    GLOBAL_ERROR.inc();
}

/// Increment the thread counter.
pub fn thread_incr() {
    THREAD_COUNT.inc();
}

/// Create a tracker for total command latency.
pub fn total_tracker(cluster: &str) -> Tracker {
    Tracker::new(TOTAL_TIMER.with_label_values(&[cluster]))
}

/// Create a tracker for remote command latency.
pub fn remote_tracker(cluster: &str) -> Tracker {
    Tracker::new(REMOTE_TIMER.with_label_values(&[cluster]))
}

/// Spawn the metrics HTTP server and system monitor tasks.
pub fn spawn_background_tasks(port: u16) -> MetricsHandles {
    MetricsHandles {
        http: tokio::spawn(async move {
            if let Err(err) = run_http_server(port).await {
                error!(port, error = %err, "metrics HTTP server exited with error");
            }
        }),
        system: tokio::spawn(async {
            if let Err(err) = system_monitor_loop(Duration::from_secs(30)).await {
                warn!(error = %err, "system monitor exited");
            }
        }),
    }
}

/// Handle to background metrics tasks.
pub struct MetricsHandles {
    pub http: JoinHandle<()>,
    pub system: JoinHandle<()>,
}

async fn run_http_server(port: u16) -> Result<()> {
    let addr: SocketAddr = format!("0.0.0.0:{port}")
        .parse()
        .context("invalid metrics bind address")?;
    let app = Router::new().route("/metrics", get(metrics_handler));
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind metrics listener on {addr}"))?;
    info!(%addr, "metrics server listening");
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

async fn metrics_handler() -> impl axum::response::IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    if let Err(err) = encoder.encode(&metric_families, &mut buffer) {
        error!(error = %err, "failed to encode prometheus metrics");
        return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response();
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(encoder.format_type()).unwrap_or_else(|_| {
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8")
        }),
    );
    (headers, buffer).into_response()
}

async fn system_monitor_loop(interval: Duration) -> Result<()> {
    use sysinfo::{ProcessExt, System, SystemExt};

    let pid = sysinfo::get_current_pid().map_err(|err| anyhow!(err))?;
    let mut system = System::new();

    loop {
        system.refresh_process(pid);
        if let Some(process) = system.process(pid) {
            MEMORY_USAGE.set(process.memory() as f64);
            CPU_USAGE.set(process.cpu_usage() as f64);
        } else {
            warn!("process information unavailable; stopping system monitor");
            break;
        }
        time::sleep(interval).await;
    }
    Ok(())
}
