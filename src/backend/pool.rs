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

struct NodeSessions<T: BackendRequest> {
    shared: Vec<Option<SessionHandle<T>>>,
    exclusive_idle: Vec<SessionHandle<T>>,
}

impl<T: BackendRequest> NodeSessions<T> {
    fn new(slots: usize) -> Self {
        let mut shared = Vec::with_capacity(slots);
        shared.resize_with(slots, || None);
        Self {
            shared,
            exclusive_idle: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.shared.iter().all(|slot| slot.is_none()) && self.exclusive_idle.is_empty()
    }
}

/// Connection pool mapping (backend node, client id) to session workers.
pub struct ConnectionPool<T: BackendRequest> {
    cluster: Arc<str>,
    connector: Arc<dyn Connector<T>>,
    sessions: RwLock<HashMap<BackendNode, NodeSessions<T>>>,
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
            let node_sessions = guard
                .entry(node.clone())
                .or_insert_with(|| NodeSessions::new(self.slots_per_node));
            let entry = node_sessions
                .shared
                .get_mut(index)
                .expect("session index within bounds");
            let handle = entry.get_or_insert_with(|| {
                new_session_handle(connector.clone(), node.clone(), cluster.clone())
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
            if let Some(node_sessions) = guard.get_mut(&node) {
                node_sessions.shared[index] = None;
                if node_sessions.is_empty() {
                    guard.remove(&node);
                }
            }
            return Err(anyhow!("failed to enqueue backend request: {err}"));
        }
        Ok(response_rx)
    }

    pub fn acquire_exclusive(&self, node: &BackendNode) -> ExclusiveConnection<'_, T> {
        let handle = {
            let mut guard = self.sessions.write();
            let node_sessions = guard
                .entry(node.clone())
                .or_insert_with(|| NodeSessions::new(self.slots_per_node));
            if let Some(handle) = node_sessions.exclusive_idle.pop() {
                handle
            } else {
                new_session_handle(self.connector.clone(), node.clone(), self.cluster.clone())
            }
        };

        ExclusiveConnection {
            pool: self,
            node: node.clone(),
            handle: Some(handle),
        }
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

fn new_session_handle<T: BackendRequest>(
    connector: Arc<dyn Connector<T>>,
    node: BackendNode,
    cluster: Arc<str>,
) -> SessionHandle<T> {
    let (tx, rx) = client_request_channel();
    let join = spawn_session(connector, node, cluster, rx);
    SessionHandle {
        tx: tx.clone(),
        join,
    }
}

fn session_index(node: &BackendNode, client_id: ClientId, slots: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    node.hash(&mut hasher);
    client_id.as_u64().hash(&mut hasher);
    (hasher.finish() as usize) % slots.max(1)
}

pub struct ExclusiveConnection<'a, T: BackendRequest> {
    pool: &'a ConnectionPool<T>,
    node: BackendNode,
    handle: Option<SessionHandle<T>>,
}

impl<'a, T: BackendRequest> ExclusiveConnection<'a, T> {
    pub async fn send(&mut self, mut request: T) -> Result<oneshot::Receiver<Result<T::Response>>> {
        request.apply_total_tracker(&self.pool.cluster);
        request.apply_remote_tracker(&self.pool.cluster);
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| anyhow!("exclusive connection has been released"))?;

        let (respond_to, response_rx) = oneshot::channel();
        if let Err(err) = handle
            .tx
            .send(SessionCommand {
                request,
                respond_to,
            })
            .await
        {
            self.handle = None;
            return Err(anyhow!("failed to enqueue backend request: {err}"));
        }
        Ok(response_rx)
    }

    pub fn into_inner(mut self) {
        self.handle.take();
    }
}

impl<'a, T: BackendRequest> Drop for ExclusiveConnection<'a, T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let mut guard = self.pool.sessions.write();
            let node_sessions = guard
                .entry(self.node.clone())
                .or_insert_with(|| NodeSessions::new(self.pool.slots_per_node));
            node_sessions.exclusive_idle.push(handle);
        }
    }
}
