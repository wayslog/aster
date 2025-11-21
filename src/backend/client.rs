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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics;

    #[test]
    fn client_ids_are_monotonic() {
        let a = ClientId::new();
        let b = ClientId::new();
        assert!(b.as_u64() > a.as_u64());
    }

    #[test]
    fn front_connection_guard_updates_metrics() {
        let cluster = "guard-cluster";
        let initial_current = metrics::front_connections_current(cluster);
        let initial_total = metrics::front_connections_total(cluster);

        {
            let _guard = FrontConnectionGuard::new(cluster);
            assert_eq!(
                metrics::front_connections_current(cluster),
                initial_current + 1
            );
        }

        assert_eq!(metrics::front_connections_current(cluster), initial_current);
        assert_eq!(metrics::front_connections_total(cluster), initial_total + 1);
    }
}
