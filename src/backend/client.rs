use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use crate::metrics;

/// Sequential identifier for frontend client connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

impl ClientId {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Channel size for backend dispatch queues per client.
pub const CLIENT_CHANNEL_CAPACITY: usize = 1024;

pub type RequestTx<T> = mpsc::Sender<T>;
pub type RequestRx<T> = mpsc::Receiver<T>;

pub fn client_request_channel<T>() -> (RequestTx<T>, RequestRx<T>) {
    mpsc::channel(CLIENT_CHANNEL_CAPACITY)
}

/// Helper guard that increments/decrements the connection counter.
pub struct FrontConnectionGuard<'a> {
    cluster: &'a str,
}

impl<'a> FrontConnectionGuard<'a> {
    pub fn new(cluster: &'a str) -> Self {
        metrics::front_conn_open(cluster);
        Self { cluster }
    }
}

impl<'a> Drop for FrontConnectionGuard<'a> {
    fn drop(&mut self) {
        metrics::front_conn_close(self.cluster);
    }
}
