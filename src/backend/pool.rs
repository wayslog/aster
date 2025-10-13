use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::backend::client::{client_request_channel, ClientId, RequestTx};

/// Backend node representation (host:port string).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BackendNode(pub Arc<str>);

impl BackendNode {
    pub fn new(addr: String) -> Self {
        Self(addr.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Trait implemented by protocol-specific requests.
pub trait BackendRequest: Send + Sync + 'static {
    type Response: Send + 'static;

    fn apply_total_tracker(&mut self, cluster: &str);
    fn apply_remote_tracker(&mut self, cluster: &str);
}

/// Command sent to backend worker tasks.
pub struct SessionCommand<T: BackendRequest> {
    pub request: T,
    pub respond_to: oneshot::Sender<Result<T::Response>>,
}

/// Connector spawns per-client session workers.
#[async_trait]
pub trait Connector<T>: Send + Sync + 'static
where
    T: BackendRequest,
{
    async fn run_session(
        self: Arc<Self>,
        node: BackendNode,
        cluster: Arc<str>,
        mut rx: mpsc::Receiver<SessionCommand<T>>,
    );
}

struct SessionHandle<T: BackendRequest> {
    tx: RequestTx<SessionCommand<T>>,
    #[allow(dead_code)]
    join: JoinHandle<()>,
}

/// Connection pool mapping (backend node, client id) to session workers.
pub struct ConnectionPool<T: BackendRequest> {
    cluster: Arc<str>,
    connector: Arc<dyn Connector<T>>,
    sessions: DashMap<(BackendNode, ClientId), SessionHandle<T>>,
}

impl<T: BackendRequest> ConnectionPool<T> {
    pub fn new(cluster: Arc<str>, connector: Arc<dyn Connector<T>>) -> Self {
        Self {
            cluster,
            connector,
            sessions: DashMap::new(),
        }
    }

    pub async fn dispatch(
        &self,
        node: BackendNode,
        client_id: ClientId,
        mut request: T,
    ) -> Result<oneshot::Receiver<Result<T::Response>>> {
        let key = (node.clone(), client_id);
        let connector = self.connector.clone();
        let cluster = self.cluster.clone();

        let tx = match self.sessions.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get().tx.clone(),
            Entry::Vacant(entry) => {
                let (tx, rx) = client_request_channel();
                let join = spawn_session(connector.clone(), node.clone(), cluster.clone(), rx);
                entry.insert(SessionHandle {
                    tx: tx.clone(),
                    join,
                });
                tx
            }
        };

        request.apply_total_tracker(&self.cluster);
        request.apply_remote_tracker(&self.cluster);

        let (respond_to, response_rx) = oneshot::channel();
        if let Err(err) = tx
            .send(SessionCommand {
                request,
                respond_to,
            })
            .await
        {
            self.sessions.remove(&key);
            return Err(anyhow!("failed to enqueue backend request: {err}"));
        }
        Ok(response_rx)
    }
}

fn spawn_session<T: BackendRequest>(
    connector: Arc<dyn Connector<T>>,
    node: BackendNode,
    cluster: Arc<str>,
    rx: mpsc::Receiver<SessionCommand<T>>,
) -> JoinHandle<()> {
    tokio::spawn(async move { connector.run_session(node, cluster, rx).await })
}
