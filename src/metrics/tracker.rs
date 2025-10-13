use std::time::Instant;

use prometheus::Histogram;

/// Measures elapsed time and records it into a Prometheus histogram when dropped.
pub struct Tracker {
    start: Instant,
    histogram: Histogram,
}

impl Tracker {
    pub fn new(histogram: Histogram) -> Self {
        Self {
            start: Instant::now(),
            histogram,
        }
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let micros = elapsed.as_secs_f64() * 1_000_000.0;
        self.histogram.observe(micros);
    }
}
