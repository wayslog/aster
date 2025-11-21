use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::backend::client::ClientId;
use crate::backend::pool::{BackendNode, ConnectionPool};

use super::pool::BackendRequest;

/// Abstraction over backend request execution so it can be mocked in tests.
#[async_trait]
pub trait BackendExecutor<T>: Send + Sync
where
    T: BackendRequest,
{
    /// Dispatch a non-blocking request for the given client.
    async fn dispatch(
        &self,
        node: BackendNode,
        client_id: ClientId,
        request: T,
    ) -> Result<T::Response>;

    /// Dispatch a request that requires an exclusive backend connection.
    async fn dispatch_blocking(&self, node: BackendNode, request: T) -> Result<T::Response>;
}

/// Default executor that proxies calls through the actual connection pool.
pub struct PoolBackendExecutor<T: BackendRequest> {
    pool: Arc<ConnectionPool<T>>,
}

impl<T: BackendRequest> PoolBackendExecutor<T> {
    pub fn new(pool: Arc<ConnectionPool<T>>) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &Arc<ConnectionPool<T>> {
        &self.pool
    }
}

#[async_trait]
impl<T> BackendExecutor<T> for PoolBackendExecutor<T>
where
    T: BackendRequest,
{
    async fn dispatch(
        &self,
        node: BackendNode,
        client_id: ClientId,
        request: T,
    ) -> Result<T::Response> {
        let response_rx = self.pool.dispatch(node, client_id, request).await?;
        match response_rx.await {
            Ok(result) => result,
            Err(_) => Err(anyhow!("backend session closed unexpectedly")),
        }
    }

    async fn dispatch_blocking(&self, node: BackendNode, request: T) -> Result<T::Response> {
        let mut exclusive = self.pool.acquire_exclusive(&node);
        let response_rx = exclusive.send(request).await?;
        let outcome = response_rx.await;
        drop(exclusive);
        match outcome {
            Ok(result) => result,
            Err(_) => Err(anyhow!("backend session closed unexpectedly")),
        }
    }
}
