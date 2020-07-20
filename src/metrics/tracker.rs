use prometheus::Histogram;

use std::time::Instant;

pub struct Tracker {
    pub start: Instant,
    hist: Histogram,
}

impl std::fmt::Debug for Tracker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "Tracker<start={:?}>", self.start)
    }
}

impl Tracker {
    pub fn new(hist: Histogram) -> Tracker {
        Self {
            start: Instant::now(),
            hist,
        }
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        let dur = self.start.elapsed();
        let micro = f64::from(dur.subsec_nanos()) / 1e3; // microseconds
        self.hist
            .observe(micro + (dur.as_secs() as f64 * 1_000_000.0));
    }
}
