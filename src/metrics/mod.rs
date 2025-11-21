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

static BACKEND_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_backend_error_total",
            "count of backend errors by cluster/node/kind"
        ),
        &["cluster", "backend", "kind"]
    )
    .expect("backend error counter registration must succeed")
});

static BACKEND_HEARTBEATS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_backend_heartbeat_total",
            "backend heartbeat results grouped by status"
        ),
        &["cluster", "backend", "status"]
    )
    .expect("backend heartbeat counter registration must succeed")
});

static BACKEND_HEALTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        opts!(
            "aster_backend_health_status",
            "backend health state derived from heartbeat (1 healthy, 0 unhealthy)"
        ),
        &["cluster", "backend"]
    )
    .expect("backend health gauge registration must succeed")
});

static BACKEND_PROBES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_backend_probe_total",
            "count of backend probes grouped by type and result"
        ),
        &["cluster", "backend", "probe", "result"]
    )
    .expect("backend probe counter registration must succeed")
});

static BACKEND_PROBE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "aster_backend_probe_duration",
        "backend probe duration in microseconds",
        &["cluster", "backend", "probe"],
        vec![1_000.0, 10_000.0, 100_000.0, 1_000_000.0, 10_000_000.0]
    )
    .expect("backend probe duration histogram registration must succeed")
});

static FRONT_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_front_error_total",
            "count of front errors grouped by cluster and kind"
        ),
        &["cluster", "kind"]
    )
    .expect("front error counter registration must succeed")
});

static POOL_SESSIONS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        opts!(
            "aster_backend_pool_sessions",
            "current backend pool sessions grouped by kind"
        ),
        &["cluster", "backend", "kind"]
    )
    .expect("backend pool session gauge registration must succeed")
});

static POOL_EXCLUSIVE_IDLE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        opts!(
            "aster_backend_pool_exclusive_idle",
            "exclusive connections available in pool"
        ),
        &["cluster", "backend"]
    )
    .expect("backend exclusive idle gauge registration must succeed")
});

static SUBSCRIPTIONS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        opts!(
            "aster_subscription_active",
            "active subscription counts grouped by scope"
        ),
        &["cluster", "scope"]
    )
    .expect("subscription gauge registration must succeed")
});

static FRONT_COMMAND_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_front_command_total",
            "total front commands processed grouped by kind and result"
        ),
        &["cluster", "kind", "result"]
    )
    .expect("front command counter registration must succeed")
});

static CLIENT_CACHE_LOOKUP: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_client_cache_lookup_total",
            "client cache lookup results grouped by kind"
        ),
        &["cluster", "kind", "result"]
    )
    .expect("client cache lookup counter must succeed")
});

static CLIENT_CACHE_STORE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_client_cache_store_total",
            "client cache store operations grouped by kind"
        ),
        &["cluster", "kind"]
    )
    .expect("client cache store counter must succeed")
});

static CLIENT_CACHE_INVALIDATE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_client_cache_invalidate_total",
            "client cache invalidations grouped by cluster"
        ),
        &["cluster"]
    )
    .expect("client cache invalidation counter must succeed")
});

static CLIENT_CACHE_STATE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_client_cache_state_total",
            "client cache state transitions grouped by cluster"
        ),
        &["cluster", "state"]
    )
    .expect("client cache state counter must succeed")
});

static BACKEND_REQUEST_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_backend_request_total",
            "backend request results grouped by backend and outcome"
        ),
        &["cluster", "backend", "result"]
    )
    .expect("backend request counter registration must succeed")
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

static BACKUP_REQUEST_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        opts!(
            "aster_backup_request_total",
            "backup request events grouped by cluster and event"
        ),
        &["cluster", "event"]
    )
    .expect("backup request counter registration must succeed")
});

/// Register the running version with metrics.
pub fn register_version(version: &str) {
    VERSION_GAUGE.with_label_values(&[version]).set(1.0);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FrontCommandStats {
    pub read_ok: u64,
    pub read_fail: u64,
    pub write_ok: u64,
    pub write_fail: u64,
    pub other_ok: u64,
    pub other_fail: u64,
    pub invalid_fail: u64,
}

impl FrontCommandStats {
    pub fn total_ok(&self) -> u64 {
        self.read_ok + self.write_ok + self.other_ok
    }

    pub fn total_fail(&self) -> u64 {
        self.read_fail + self.write_fail + self.other_fail + self.invalid_fail
    }

    pub fn total(&self) -> u64 {
        self.total_ok() + self.total_fail()
    }
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

/// Record a backend error with the provided classification.
pub fn backend_error(cluster: &str, backend: &str, kind: &str) {
    BACKEND_ERRORS
        .with_label_values(&[cluster, backend, kind])
        .inc();
}

/// Record the outcome of a backend probe.
pub fn backend_probe_result(cluster: &str, backend: &str, probe: &str, ok: bool) {
    let result = if ok { "ok" } else { "fail" };
    BACKEND_PROBES
        .with_label_values(&[cluster, backend, probe, result])
        .inc();
    BACKEND_HEALTH
        .with_label_values(&[cluster, backend])
        .set(if ok { 1.0 } else { 0.0 });
}

/// Record the duration of a backend probe.
pub fn backend_probe_duration(cluster: &str, backend: &str, probe: &str, elapsed: Duration) {
    let micros = elapsed.as_secs_f64() * 1_000_000.0;
    BACKEND_PROBE_DURATION
        .with_label_values(&[cluster, backend, probe])
        .observe(micros);
}

/// Record a categorized frontend error.
pub fn front_error(cluster: &str, kind: &str) {
    FRONT_ERRORS.with_label_values(&[cluster, kind]).inc();
}

/// Increment session gauge when a pool session is opened.
pub fn pool_session_open(cluster: &str, backend: &str, kind: &str) {
    POOL_SESSIONS
        .with_label_values(&[cluster, backend, kind])
        .inc();
}

/// Decrement session gauge when a pool session is closed.
pub fn pool_session_close(cluster: &str, backend: &str, kind: &str) {
    POOL_SESSIONS
        .with_label_values(&[cluster, backend, kind])
        .dec();
}

/// Update the number of idle exclusive connections for a backend.
pub fn pool_exclusive_idle(cluster: &str, backend: &str, count: usize) {
    POOL_EXCLUSIVE_IDLE
        .with_label_values(&[cluster, backend])
        .set(count as f64);
}

/// Update active subscription counts.
pub fn subscription_active(cluster: &str, scope: &str, count: usize) {
    SUBSCRIPTIONS
        .with_label_values(&[cluster, scope])
        .set(count as f64);
}

/// Record a processed frontend command.
pub fn front_command(cluster: &str, kind: &str, success: bool) {
    let result = if success { "ok" } else { "fail" };
    FRONT_COMMAND_TOTAL
        .with_label_values(&[cluster, kind, result])
        .inc();
}

/// Record a client cache lookup result.
pub fn client_cache_lookup(cluster: &str, kind: &str, hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    CLIENT_CACHE_LOOKUP
        .with_label_values(&[cluster, kind, result])
        .inc();
}

/// Record a client cache store/update event.
pub fn client_cache_store(cluster: &str, kind: &str) {
    CLIENT_CACHE_STORE.with_label_values(&[cluster, kind]).inc();
}

/// Record the number of keys invalidated from the client cache.
pub fn client_cache_invalidate(cluster: &str, count: usize) {
    CLIENT_CACHE_INVALIDATE
        .with_label_values(&[cluster])
        .inc_by(count as u64);
}

/// Record a client cache state transition.
pub fn client_cache_state(cluster: &str, state: &str) {
    CLIENT_CACHE_STATE
        .with_label_values(&[cluster, state])
        .inc();
}

/// Record a backend request outcome.
pub fn backend_request_result(cluster: &str, backend: &str, result: &str) {
    BACKEND_REQUEST_TOTAL
        .with_label_values(&[cluster, backend, result])
        .inc();
}

/// Record a backup request related event for observability.
pub fn backup_event(cluster: &str, event: &str) {
    BACKUP_REQUEST_EVENTS
        .with_label_values(&[cluster, event])
        .inc();
}

/// Record the outcome of a backend heartbeat check.
pub fn backend_heartbeat(cluster: &str, backend: &str, ok: bool) {
    let status = if ok { "ok" } else { "fail" };
    backend_probe_result(cluster, backend, "heartbeat", ok);
    BACKEND_HEARTBEATS
        .with_label_values(&[cluster, backend, status])
        .inc();
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

/// Current frontend connections for a cluster.
pub fn front_connections_current(cluster: &str) -> u64 {
    let gauge = FRONT_CONNECTIONS.with_label_values(&[cluster]);
    gauge.get().max(0.0).round() as u64
}

/// Total frontend connections since proxy start for a cluster.
pub fn front_connections_total(cluster: &str) -> u64 {
    let counter = FRONT_CONNECTION_INCR.with_label_values(&[cluster]);
    counter.get().max(0) as u64
}

/// Aggregate frontend command counters for a cluster.
pub fn front_command_stats(cluster: &str) -> FrontCommandStats {
    let mut stats = FrontCommandStats::default();
    let mappings = [
        ("read", "ok"),
        ("read", "fail"),
        ("write", "ok"),
        ("write", "fail"),
        ("other", "ok"),
        ("other", "fail"),
        ("invalid", "fail"),
    ];
    for (kind, result) in mappings {
        let counter = FRONT_COMMAND_TOTAL.with_label_values(&[cluster, kind, result]);
        let value = counter.get().max(0) as u64;
        match (kind, result) {
            ("read", "ok") => stats.read_ok = value,
            ("read", "fail") => stats.read_fail = value,
            ("write", "ok") => stats.write_ok = value,
            ("write", "fail") => stats.write_fail = value,
            ("other", "ok") => stats.other_ok = value,
            ("other", "fail") => stats.other_fail = value,
            ("invalid", "fail") => stats.invalid_fail = value,
            _ => {}
        }
    }
    stats
}

/// Global error count across all clusters.
pub fn global_error_count() -> u64 {
    GLOBAL_ERROR.get().max(0) as u64
}

/// Memory usage in bytes, derived from the system monitor gauge (kB).
pub fn memory_usage_bytes() -> u64 {
    (MEMORY_USAGE.get().max(0.0) * 1024.0) as u64
}

/// CPU usage percentage reported by the system monitor gauge.
pub fn cpu_usage_percent() -> f64 {
    CPU_USAGE.get().max(0.0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn front_connections_counters_reflect_updates() {
        let cluster = "metrics-test";
        front_conn_open(cluster);
        front_conn_open(cluster);
        front_conn_close(cluster);
        assert_eq!(front_connections_current(cluster), 1);
        assert!(front_connections_total(cluster) >= 2);
    }

    #[test]
    fn front_command_stats_aggregates_totals() {
        let cluster = "metrics-stats";
        front_command(cluster, "read", true);
        front_command(cluster, "read", false);
        front_command(cluster, "invalid", false);
        let stats = front_command_stats(cluster);
        assert_eq!(stats.read_ok, 1);
        assert_eq!(stats.read_fail, 1);
        assert_eq!(stats.invalid_fail, 1);
        assert_eq!(stats.total(), 3);
    }

    #[test]
    fn global_usage_accessors_reflect_gauges() {
        MEMORY_USAGE.set(1024.0);
        CPU_USAGE.set(12.5);
        GLOBAL_ERROR.inc_by(5);
        assert_eq!(memory_usage_bytes(), 1024 * 1024);
        assert_eq!(cpu_usage_percent(), 12.5);
        assert!(global_error_count() >= 5);
    }

    #[test]
    fn backend_metrics_track_error_and_health() {
        let cluster = "metrics-backend";
        let backend = "127.0.0.1:9000";
        backend_error(cluster, backend, "timeout");
        backend_probe_result(cluster, backend, "ping", false);
        backend_heartbeat(cluster, backend, true);
        assert!(
            BACKEND_ERRORS
                .with_label_values(&[cluster, backend, "timeout"])
                .get()
                >= 1
        );
        assert_eq!(
            BACKEND_HEALTH
                .with_label_values(&[cluster, backend])
                .get(),
            1.0
        );
        assert!(
            BACKEND_PROBES
                .with_label_values(&[cluster, backend, "ping", "fail"])
                .get()
                >= 1
        );
    }

    #[test]
    fn backend_probe_duration_accumulates_samples() {
        let cluster = "metrics-probe";
        let backend = "127.0.0.1:9001";
        backend_probe_duration(
            cluster,
            backend,
            "latency",
            Duration::from_micros(1500),
        );
        let histogram = BACKEND_PROBE_DURATION.with_label_values(&[cluster, backend, "latency"]);
        assert!(histogram.get_sample_count() >= 1);
        assert!(histogram.get_sample_sum() >= 1_500.0);
    }

    #[test]
    fn client_cache_metrics_capture_states() {
        let cluster = "metrics-cache";
        client_cache_lookup(cluster, "get", true);
        client_cache_lookup(cluster, "get", false);
        client_cache_store(cluster, "set");
        client_cache_invalidate(cluster, 2);
        client_cache_state(cluster, "enabled");
        assert!(
            CLIENT_CACHE_LOOKUP
                .with_label_values(&[cluster, "get", "hit"])
                .get()
                >= 1
        );
        assert!(
            CLIENT_CACHE_LOOKUP
                .with_label_values(&[cluster, "get", "miss"])
                .get()
                >= 1
        );
        assert!(
            CLIENT_CACHE_STORE
                .with_label_values(&[cluster, "set"])
                .get()
                >= 1
        );
        assert!(
            CLIENT_CACHE_INVALIDATE
                .with_label_values(&[cluster])
                .get()
                >= 2
        );
        assert!(
            CLIENT_CACHE_STATE
                .with_label_values(&[cluster, "enabled"])
                .get()
                >= 1
        );
    }

    #[test]
    fn register_version_and_backup_events_increment_metrics() {
        register_version("9.9.9");
        backend_request_result("metrics-req", "backend-a", "ok");
        backup_event("metrics-req", "planned");
        assert_eq!(
            VERSION_GAUGE.with_label_values(&["9.9.9"]).get(),
            1.0
        );
        assert!(
            BACKEND_REQUEST_TOTAL
                .with_label_values(&["metrics-req", "backend-a", "ok"])
                .get()
                >= 1
        );
        assert!(
            BACKUP_REQUEST_EVENTS
                .with_label_values(&["metrics-req", "planned"])
                .get()
                >= 1
        );
    }
}
