use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
use rand::{seq::SliceRandom, thread_rng};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::backend::client::{ClientId, FrontConnectionGuard};
use crate::backend::pool::{BackendNode, ConnectionPool, Connector, SessionCommand};
use crate::config::ClusterConfig;
use crate::metrics;
use crate::protocol::redis::{RedisCommand, RespCodec, RespValue, SlotMap};

const FETCH_INTERVAL: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT_MS: u64 = 1_000;
const MAX_REDIRECTS: u8 = 5;
const PIPELINE_LIMIT: usize = 32;

pub struct ClusterProxy {
    cluster: Arc<str>,
    hash_tag: Option<Vec<u8>>,
    read_from_slave: bool,
    slots: Arc<watch::Sender<SlotMap>>,
    pool: Arc<ConnectionPool<RedisCommand>>,
    fetch_trigger: mpsc::UnboundedSender<()>,
}

impl ClusterProxy {
    pub async fn new(config: &ClusterConfig) -> Result<Self> {
        let cluster: Arc<str> = config.name.clone().into();
        let hash_tag = config.hash_tag.as_ref().map(|tag| tag.as_bytes().to_vec());
        let read_from_slave = config.read_from_slave.unwrap_or(false);

        let (slot_tx, _slot_rx) = watch::channel(SlotMap::new());
        let (trigger_tx, trigger_rx) = mpsc::unbounded_channel();

        let timeout_ms = config
            .read_timeout
            .or(config.write_timeout)
            .unwrap_or(REQUEST_TIMEOUT_MS);
        let connector = Arc::new(ClusterConnector::new(Duration::from_millis(timeout_ms)));
        let pool = Arc::new(ConnectionPool::new(cluster.clone(), connector.clone()));

        let proxy = Self {
            cluster: cluster.clone(),
            hash_tag,
            read_from_slave,
            slots: Arc::new(slot_tx),
            pool: pool.clone(),
            fetch_trigger: trigger_tx.clone(),
        };

        // trigger an immediate topology fetch
        trigger_tx.send(()).ok();
        tokio::spawn(fetch_topology(
            cluster,
            config.servers.clone(),
            connector,
            proxy.slots.clone(),
            trigger_rx,
        ));

        Ok(proxy)
    }

    pub async fn handle_connection(&self, socket: TcpStream) -> Result<()> {
        socket
            .set_nodelay(true)
            .context("failed to set TCP_NODELAY")?;
        let client_id = ClientId::new();
        let _guard = FrontConnectionGuard::new(&self.cluster);

        let (mut sink, stream) = Framed::new(socket, RespCodec::default()).split();
        let mut stream = stream.fuse();
        let mut pending: FuturesOrdered<BoxFuture<'static, RespValue>> = FuturesOrdered::new();
        let mut inflight = 0usize;
        let mut stream_closed = false;

        loop {
            tokio::select! {
                Some(resp) = pending.next(), if inflight > 0 => {
                    inflight -= 1;
                    sink.send(resp).await?;
                }
                frame_opt = stream.next(), if !stream_closed && inflight < PIPELINE_LIMIT => {
                    match frame_opt {
                        Some(Ok(frame)) => {
                            match RedisCommand::from_resp(frame) {
                                Ok(cmd) => {
                                    let guard = self.prepare_dispatch(client_id, cmd);
                                    pending.push_back(Box::pin(guard));
                                    inflight += 1;
                                }
                                Err(err) => {
                                    metrics::global_error_incr();
                                    let message = Bytes::from(format!("ERR {err}"));
                                    let fut = async move { RespValue::Error(message) };
                                    pending.push_back(Box::pin(fut));
                                    inflight += 1;
                                }
                            }
                        }
                        Some(Err(err)) => {
                            metrics::global_error_incr();
                            return Err(err);
                        }
                        None => {
                            stream_closed = true;
                        }
                    }
                }
                else => {
                    if stream_closed && inflight == 0 {
                        break;
                    }
                }
            }
        }

        while let Some(resp) = pending.next().await {
            sink.send(resp).await?;
        }
        sink.close().await?;
        Ok(())
    }

    fn prepare_dispatch(
        &self,
        client_id: ClientId,
        command: RedisCommand,
    ) -> BoxFuture<'static, RespValue> {
        let cluster = self.cluster.clone();
        let hash_tag = self.hash_tag.clone();
        let read_from_slave = self.read_from_slave;
        let slots = self.slots.clone();
        let pool = self.pool.clone();
        let fetch_trigger = self.fetch_trigger.clone();
        Box::pin(async move {
            match dispatch_with_context(
                cluster,
                hash_tag,
                read_from_slave,
                slots,
                pool,
                fetch_trigger,
                client_id,
                command,
            )
            .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    metrics::global_error_incr();
                    RespValue::Error(Bytes::from(format!("ERR {err}")))
                }
            }
        })
    }
}

#[derive(Debug)]
enum Redirect {
    Moved { slot: u16, address: String },
    Ask { address: String },
}

async fn dispatch_with_context(
    _cluster: Arc<str>,
    hash_tag: Option<Vec<u8>>,
    read_from_slave: bool,
    slots: Arc<watch::Sender<SlotMap>>,
    pool: Arc<ConnectionPool<RedisCommand>>,
    fetch_trigger: mpsc::UnboundedSender<()>,
    client_id: ClientId,
    command: RedisCommand,
) -> Result<RespValue> {
    let mut slot = command
        .hash_slot(hash_tag.as_deref())
        .ok_or_else(|| anyhow!("command missing key"))?;
    let mut target_override: Option<BackendNode> = None;

    for _ in 0..MAX_REDIRECTS {
        let target = if let Some(node) = target_override.clone() {
            node
        } else {
            select_node(&slots, read_from_slave, slot)?
        };

        let response_rx = pool
            .dispatch(target.clone(), client_id, command.clone())
            .await?;

        match response_rx.await {
            Ok(Ok(resp)) => match parse_redirect(resp.clone())? {
                Some(Redirect::Moved {
                    slot: new_slot,
                    address,
                }) => {
                    let _ = fetch_trigger.send(());
                    slot = new_slot;
                    target_override = Some(BackendNode::new(address));
                    continue;
                }
                Some(Redirect::Ask { address }) => {
                    target_override = Some(BackendNode::new(address));
                    continue;
                }
                None => return Ok(resp),
            },
            Ok(Err(err)) => return Err(err),
            Err(_) => return Err(anyhow!("backend session closed")),
        }
    }

    Err(anyhow!("too many cluster redirects"))
}

fn select_node(
    slots: &watch::Sender<SlotMap>,
    read_from_slave: bool,
    slot: u16,
) -> Result<BackendNode> {
    let map = slots.borrow().clone();
    if read_from_slave {
        if let Some(replica) = map.replica_for_slot(slot) {
            return Ok(BackendNode::new(replica.to_string()));
        }
    }
    if let Some(master) = map.master_for_slot(slot) {
        return Ok(BackendNode::new(master.to_string()));
    }
    bail!("slot {} not covered", slot)
}

fn parse_redirect(value: RespValue) -> Result<Option<Redirect>> {
    match value {
        RespValue::Error(err) => {
            let text = String::from_utf8_lossy(&err);
            if text.starts_with("MOVED") {
                let mut parts = text.split_whitespace();
                let _ = parts.next();
                let slot = parts
                    .next()
                    .ok_or_else(|| anyhow!("invalid MOVED response"))?
                    .parse::<u16>()?;
                let address = parts
                    .next()
                    .ok_or_else(|| anyhow!("invalid MOVED response"))?
                    .to_string();
                Ok(Some(Redirect::Moved { slot, address }))
            } else if text.starts_with("ASK") {
                let mut parts = text.split_whitespace();
                let _ = parts.next();
                let _slot = parts
                    .next()
                    .ok_or_else(|| anyhow!("invalid ASK response"))?;
                let address = parts
                    .next()
                    .ok_or_else(|| anyhow!("invalid ASK response"))?
                    .to_string();
                Ok(Some(Redirect::Ask { address }))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

#[derive(Clone)]
struct ClusterConnector {
    timeout: Duration,
}

impl ClusterConnector {
    fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    async fn open_stream(&self, address: &str) -> Result<Framed<TcpStream, RespCodec>> {
        let stream = timeout(self.timeout, TcpStream::connect(address))
            .await
            .with_context(|| format!("connection to {} timed out", address))??;
        stream
            .set_nodelay(true)
            .with_context(|| format!("failed to set TCP_NODELAY for {}", address))?;
        Ok(Framed::new(stream, RespCodec::default()))
    }

    async fn execute(
        &self,
        framed: &mut Framed<TcpStream, RespCodec>,
        command: RedisCommand,
    ) -> Result<RespValue> {
        timeout(self.timeout, framed.send(command.to_resp()))
            .await
            .context("timed out sending command")??;

        match timeout(self.timeout, framed.next()).await {
            Ok(Some(Ok(value))) => Ok(value),
            Ok(Some(Err(err))) => Err(err.into()),
            Ok(None) => Err(anyhow!("backend closed connection")),
            Err(_) => Err(anyhow!("timed out waiting for response")),
        }
    }
}

#[async_trait]
impl Connector<RedisCommand> for ClusterConnector {
    async fn run_session(
        self: Arc<Self>,
        node: BackendNode,
        cluster: Arc<str>,
        mut rx: mpsc::Receiver<SessionCommand<RedisCommand>>,
    ) {
        info!(cluster = %cluster, backend = %node.as_str(), "starting cluster backend session");
        let mut stream = match self.open_stream(node.as_str()).await {
            Ok(stream) => stream,
            Err(err) => {
                warn!(cluster = %cluster, backend = %node.as_str(), error = %err, "failed to connect backend");
                return;
            }
        };

        while let Some(cmd) = rx.recv().await {
            let result = self.execute(&mut stream, cmd.request).await;
            let is_err = result.is_err();
            let _ = cmd.respond_to.send(result);
            if is_err {
                break;
            }
        }

        debug!(cluster = %cluster, backend = %node.as_str(), "cluster backend session terminated");
    }
}

async fn fetch_topology(
    cluster: Arc<str>,
    seeds: Vec<String>,
    connector: Arc<ClusterConnector>,
    slots: Arc<watch::Sender<SlotMap>>,
    mut trigger: mpsc::UnboundedReceiver<()>,
) {
    let mut ticker = tokio::time::interval(FETCH_INTERVAL);
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            _ = trigger.recv() => {},
        }

        if let Err(err) = fetch_once(&cluster, &seeds, connector.clone(), slots.clone()).await {
            warn!(cluster = %cluster, error = %err, "failed to refresh cluster topology");
        }
    }
}

async fn fetch_once(
    cluster: &Arc<str>,
    seeds: &[String],
    connector: Arc<ClusterConnector>,
    slots: Arc<watch::Sender<SlotMap>>,
) -> Result<()> {
    let mut shuffled = seeds.to_vec();
    {
        let mut rng = thread_rng();
        shuffled.shuffle(&mut rng);
    }

    for seed in shuffled {
        match fetch_from_seed(&seed, connector.clone()).await {
            Ok(map) => {
                slots.send_replace(map.clone());
                info!(cluster = %cluster, seed = %seed, "cluster slots refreshed");
                return Ok(());
            }
            Err(err) => {
                warn!(cluster = %cluster, seed = %seed, error = %err, "failed to fetch slots from seed");
            }
        }
    }

    Err(anyhow!("all seed nodes failed"))
}

async fn fetch_from_seed(seed: &str, connector: Arc<ClusterConnector>) -> Result<SlotMap> {
    let mut stream = connector.open_stream(seed).await?;
    let command = RedisCommand::new(vec![
        Bytes::from_static(b"CLUSTER"),
        Bytes::from_static(b"SLOTS"),
    ])?;
    let response = connector.execute(&mut stream, command).await?;
    SlotMap::from_slots_response(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_moved_redirect() {
        let value = RespValue::Error(Bytes::from_static(b"MOVED 3999 127.0.0.1:6381"));
        let redirect = parse_redirect(value).unwrap().unwrap();
        match redirect {
            Redirect::Moved { slot, address } => {
                assert_eq!(slot, 3999);
                assert_eq!(address, "127.0.0.1:6381");
            }
            _ => panic!("expected MOVED"),
        }
    }
}
