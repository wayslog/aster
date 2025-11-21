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

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Histogram, HistogramOpts};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn tracker_records_elapsed_time_on_drop() {
        let histogram = Histogram::with_opts(HistogramOpts::new("tracker_test", "test")).unwrap();
        let before = histogram.get_sample_count();
        {
            let _tracker = Tracker::new(histogram.clone());
            thread::sleep(Duration::from_millis(1));
        }
        let after = histogram.get_sample_count();
        assert!(after > before);
    }
}
