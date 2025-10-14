use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hashbrown::HashMap;
use parking_lot::RwLock;
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
    sessions: RwLock<HashMap<BackendNode, Vec<Option<SessionHandle<T>>>>>,
    slots_per_node: usize,
}

impl<T: BackendRequest> ConnectionPool<T> {
    pub fn new(cluster: Arc<str>, connector: Arc<dyn Connector<T>>) -> Self {
        Self::with_slots(cluster, connector, DEFAULT_SLOTS_PER_NODE)
    }

    pub fn with_slots(
        cluster: Arc<str>,
        connector: Arc<dyn Connector<T>>,
        slots_per_node: usize,
    ) -> Self {
        Self {
            cluster,
            connector,
            sessions: RwLock::new(HashMap::new()),
            slots_per_node: slots_per_node.max(1),
        }
    }

    pub async fn dispatch(
        &self,
        node: BackendNode,
        client_id: ClientId,
        mut request: T,
    ) -> Result<oneshot::Receiver<Result<T::Response>>> {
        let index = session_index(&node, client_id, self.slots_per_node);
        let connector = self.connector.clone();
        let cluster = self.cluster.clone();
        let tx = {
            let mut guard = self.sessions.write();
            let slots = guard.entry(node.clone()).or_insert_with(|| {
                let mut slots = Vec::with_capacity(self.slots_per_node);
                slots.resize_with(self.slots_per_node, || None);
                slots
            });
            let entry = slots.get_mut(index).expect("session index within bounds");
            let handle = entry.get_or_insert_with(|| {
                let (tx, rx) = client_request_channel();
                let join = spawn_session(connector.clone(), node.clone(), cluster.clone(), rx);
                SessionHandle {
                    tx: tx.clone(),
                    join,
                }
            });
            handle.tx.clone()
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
            let mut guard = self.sessions.write();
            if let Some(slots) = guard.get_mut(&node) {
                slots[index] = None;
                if slots.iter().all(|slot| slot.is_none()) {
                    guard.remove(&node);
                }
            }
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

const DEFAULT_SLOTS_PER_NODE: usize = 8;

fn session_index(node: &BackendNode, client_id: ClientId, slots: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    node.hash(&mut hasher);
    client_id.as_u64().hash(&mut hasher);
    (hasher.finish() as usize) % slots.max(1)
}
