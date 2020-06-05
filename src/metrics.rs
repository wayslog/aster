pub mod slowlog;
pub mod tracker;

pub use tracker::Tracker;

use crate::com::AsError;
use crate::ASTER_VERSION as VERSION;

use std::thread;
use std::time::Duration;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use prometheus::{
    self, Encoder, Gauge, GaugeVec, HistogramVec, IntCounter, IntCounterVec, TextEncoder,
};
use sysinfo::{ProcessExt, SystemExt};

lazy_static! {
    static ref ASTER_FRONT_CONNECTIONS: GaugeVec = {
        let opt = opts!(
            "aster_front_connection",
            "each front nodes connections gauge"
        );
        register_gauge_vec!(opt, &["cluster"]).unwrap()
    };
    static ref ASTER_FRONT_INCR: IntCounterVec = {
        let opt = opts!(
            "aster_front_connection_incr",
            "each front nodes connections gauge"
        );
        register_int_counter_vec!(opt, &["cluster"]).unwrap()
    };
    static ref ASTER_VERSION: GaugeVec = {
        let opt = opts!("aster_version", "aster current running version");
        register_gauge_vec!(opt, &["version"]).unwrap()
    };
    static ref ASTER_MEMORY: Gauge = {
        let opt = opts!("aster_memory_usage", "aster current memory usage");
        register_gauge!(opt).unwrap()
    };
    static ref ASTER_CPU: Gauge = {
        let opt = opts!("aster_cpu_usage", "aster current cpu usage");
        register_gauge!(opt).unwrap()
    };
    static ref ASTER_THREADS: IntCounter = {
        let opt = opts!("aster_thread_count", "aster thread count counter");
        register_int_counter!(opt).unwrap()
    };
    static ref ASTER_GLOBAL_ERROR: IntCounter = {
        let opt = opts!("aster_global_error", "aster global error counter");
        register_int_counter!(opt).unwrap()
    };
    static ref ASTER_TOTAL_TIMER: HistogramVec = {
        register_histogram_vec!(
            "aster_total_timer",
            "set up each cluster command proxy total timer",
            &["cluster"],
            vec![1_000.0, 10_000.0, 40_000.0, 100_000.0, 200_000.0]
        )
        .unwrap()
    };
    static ref ASTER_REMOTE_TIMER: HistogramVec = {
        register_histogram_vec!(
            "aster_remote_timer",
            "set up each cluster command proxy remote timer",
            &["cluster"],
            vec![1_000.0, 10_000.0, 100_000.0]
        )
        .unwrap()
    };
}

pub fn front_conn_incr(cluster: &str) {
    ASTER_FRONT_INCR.with_label_values(&[cluster]).inc();
    ASTER_FRONT_CONNECTIONS.with_label_values(&[cluster]).inc()
}

pub fn front_conn_decr(cluster: &str) {
    ASTER_FRONT_CONNECTIONS.with_label_values(&[cluster]).dec()
}

pub fn global_error_incr() {
    ASTER_GLOBAL_ERROR.inc();
}

pub fn remote_tracker(cluster: &str) -> Tracker {
    Tracker::new(ASTER_REMOTE_TIMER.with_label_values(&[cluster]))
}

pub fn total_tracker(cluster: &str) -> Tracker {
    Tracker::new(ASTER_TOTAL_TIMER.with_label_values(&[cluster]))
}

fn show_metrics() -> impl Responder {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let metric_familys = prometheus::gather();
    encoder.encode(&metric_familys[..], &mut buffer).unwrap();
    HttpResponse::Ok().body(buffer)
}

pub fn thread_incr() {
    ASTER_THREADS.inc();
}

pub fn measure_system() -> Result<(), AsError> {
    // register global thread pool with only one thread to reduce thread number
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build_global()
        .expect("rayon thread register failed");

    thread_incr();
    let pid = match sysinfo::get_current_pid() {
        Ok(pid) => pid,
        Err(err) => {
            warn!("fail get pid of current aster due {}", err);
            return Err(AsError::SystemError);
        }
    };

    let sleep_interval = Duration::from_secs(30); // 30s to sleep;
    let mut system = sysinfo::System::new();
    system.refresh_all();
    loop {
        // First we update all information of our system struct.
        if !system.refresh_process(pid) {
            return Ok(());
        }
        if let Some(process) = system.get_process(pid) {
            let cpu_usage = process.cpu_usage() as f64;
            let memory_usage = process.memory() as f64;
            ASTER_MEMORY.set(memory_usage);
            ASTER_CPU.set(cpu_usage);
            thread::sleep(sleep_interval);
        } else {
            return Ok(());
        }
    }
}

pub fn init(port: usize) -> Result<(), AsError> {
    ASTER_VERSION.with_label_values(&[VERSION]).set(1.0);
    thread_incr();
    let addr = format!("0.0.0.0:{}", port);
    info!("listen http metrics port in addr {}", port);
    HttpServer::new(|| App::new().route("/metrics", web::get().to(show_metrics)))
        .shutdown_timeout(3)
        .disable_signals()
        .workers(1)
        .bind(&addr)?
        .run()?;
    Ok(())
}
