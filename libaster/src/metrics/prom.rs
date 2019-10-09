use crate::com::AsError;
use crate::ASTER_VERSION as VERSION;

use prometheus::{self, Encoder, GaugeVec, HistogramTimer, HistogramVec, IntCounter, TextEncoder};

lazy_static! {
    pub static ref ASTER_FRONT_CONNECTIONS: GaugeVec = {
        let opt = opts!(
            "aster_front_connection",
            "each front nodes connections gauge"
        );
        register_gauge_vec!(opt, &["cluster"]).unwrap()
    };
    pub static ref ASTER_VERSION: GaugeVec = {
        let opt = opts!("aster_version", "aster current running version");
        register_gauge_vec!(opt, &["version"]).unwrap()
    };
    pub static ref ASTER_GLOBAL_ERROR: IntCounter = {
        let opt = opts!("aster_global_error", "aster global error counter");
        register_int_counter!(opt).unwrap()
    };
    pub static ref ASTER_TOTAL_TIMER: HistogramVec = {
        register_histogram_vec!(
            "aster_total_timer",
            "set up each cluster command proxy total timer",
            &["cluster"],
            vec![10_000.0, 20_000.0, 100_000.0]
        )
        .unwrap()
    };
    pub static ref ASTER_REMOTE_TIMER: HistogramVec = {
        register_histogram_vec!(
            "aster_remote_timer",
            "set up each cluster command proxy remote timer",
            &["cluster"],
            vec![1_000.0, 4_000.0, 10_000.0, 20_000.0]
        )
        .unwrap()
    };
}

pub fn front_conn_incr(cluster: &str) {
    ASTER_FRONT_CONNECTIONS.with_label_values(&[cluster]).inc()
}

pub fn global_error_incr() {
    ASTER_GLOBAL_ERROR.inc();
}

pub fn remote_timer(cluster: &str) -> HistogramTimer {
    ASTER_REMOTE_TIMER
        .with_label_values(&[cluster])
        .start_timer()
}

pub fn total_timer(cluster: &str) -> HistogramTimer {
    ASTER_TOTAL_TIMER
        .with_label_values(&[cluster])
        .start_timer()
}

pub fn init() -> Result<(), AsError> {
    ASTER_VERSION.with_label_values(&[VERSION]).set(1.0);
    Ok(())
}
