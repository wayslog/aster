use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
use md5::Digest;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::backend::client::{ClientId, FrontConnectionGuard};
use crate::backend::pool::{BackendNode, ConnectionPool, Connector, SessionCommand};
use crate::config::ClusterConfig;
use crate::metrics;
use crate::protocol::redis::{MultiDispatch, RedisCommand, RedisResponse, RespCodec, RespValue};
use crate::utils::trim_hash_tag;

const DEFAULT_TIMEOUT_MS: u64 = 1_000;
const VIRTUAL_NODE_FACTOR: usize = 40;

#[derive(Clone)]
struct NodeEntry {
    backend: BackendNode,
    display: Arc<str>,
    weight: usize,
}

pub struct StandaloneProxy {
    cluster: Arc<str>,
    hash_tag: Option<Vec<u8>>,
    ring: Vec<(u64, BackendNode)>,
    pool: Arc<ConnectionPool<RedisCommand>>,
}

impl StandaloneProxy {
    pub fn new(config: &ClusterConfig) -> Result<Self> {
        let cluster: Arc<str> = config.name.clone().into();
        let hash_tag = config.hash_tag.as_ref().map(|tag| tag.as_bytes().to_vec());
        let nodes = parse_servers(&config.servers)?;
        if nodes.is_empty() {
            bail!(
                "cluster {} requires at least one backend server",
                config.name
            );
        }
        let ring = build_ring(&nodes);

        let timeout_ms = config
            .read_timeout
            .or(config.write_timeout)
            .unwrap_or(DEFAULT_TIMEOUT_MS);
        let connector = Arc::new(RedisConnector::new(Duration::from_millis(timeout_ms)));
        let pool = Arc::new(ConnectionPool::new(cluster.clone(), connector));

        Ok(Self {
            cluster,
            hash_tag,
            ring,
            pool,
        })
    }

    pub async fn dispatch(
        &self,
        client_id: ClientId,
        command: RedisCommand,
    ) -> Result<RedisResponse> {
        if let Some(multi) = command.expand_for_multi() {
            self.dispatch_multi(client_id, multi).await
        } else {
            self.dispatch_single(client_id, command).await
        }
    }

    async fn dispatch_single(
        &self,
        client_id: ClientId,
        command: RedisCommand,
    ) -> Result<RedisResponse> {
        let node = self.select_node(client_id, &command)?;
        let response_rx = self.pool.dispatch(node, client_id, command).await?;
        match response_rx.await {
            Ok(result) => result,
            Err(_) => Err(anyhow!("backend session closed unexpectedly")),
        }
    }

    async fn dispatch_multi(
        &self,
        client_id: ClientId,
        multi: MultiDispatch,
    ) -> Result<RedisResponse> {
        let mut tasks: FuturesOrdered<BoxFuture<'static, Result<(usize, RespValue)>>> =
            FuturesOrdered::new();
        for sub in multi.subcommands.into_iter() {
            let node = self.select_node(client_id, &sub.command)?;
            let pool = self.pool.clone();
            let command = sub.command.clone();
            tasks.push_back(Box::pin(async move {
                let response_rx = pool.dispatch(node, client_id, command).await?;
                match response_rx.await {
                    Ok(result) => Ok((sub.position, result?)),
                    Err(_) => Err(anyhow!("backend session closed unexpectedly")),
                }
            }));
        }

        let mut responses = Vec::new();
        while let Some(item) = tasks.next().await {
            responses.push(item?);
        }
        multi.aggregator.combine(responses)
    }

    pub async fn handle_connection(&self, socket: TcpStream) -> Result<()> {
        socket
            .set_nodelay(true)
            .context("failed to set TCP_NODELAY")?;
        let client_id = ClientId::new();
        let _guard = FrontConnectionGuard::new(&self.cluster);

        let mut framed = Framed::new(socket, RespCodec::default());

        while let Some(frame) = framed.next().await {
            let frame = match frame {
                Ok(frame) => frame,
                Err(err) => {
                    metrics::global_error_incr();
                    return Err(err.into());
                }
            };

            let command = match RedisCommand::from_resp(frame) {
                Ok(cmd) => cmd,
                Err(err) => {
                    metrics::global_error_incr();
                    let message = format!("ERR {err}");
                    let resp = RespValue::Error(Bytes::copy_from_slice(message.as_bytes()));
                    framed.send(resp).await?;
                    continue;
                }
            };

            let response = match self.dispatch(client_id, command).await {
                Ok(resp) => resp,
                Err(err) => {
                    metrics::global_error_incr();
                    let message = format!("ERR {err}");
                    RespValue::Error(Bytes::copy_from_slice(message.as_bytes()))
                }
            };

            framed.send(response).await?;
        }

        Ok(())
    }

    fn select_node(&self, client_id: ClientId, command: &RedisCommand) -> Result<BackendNode> {
        if self.ring.is_empty() {
            bail!("no backend nodes configured");
        }

        if let Some(key) = command
            .primary_key()
            .map(|key| trim_hash_tag(key, self.hash_tag.as_deref()))
        {
            let hash = hash_key(key);
            let idx = match self
                .ring
                .binary_search_by_key(&hash, |(value, _node)| *value)
            {
                Ok(idx) => idx,
                Err(idx) if idx >= self.ring.len() => 0,
                Err(idx) => idx,
            };
            Ok(self.ring[idx].1.clone())
        } else {
            let idx = (client_id.as_u64() as usize) % self.ring.len();
            Ok(self.ring[idx].1.clone())
        }
    }
}

fn parse_servers(servers: &[String]) -> Result<Vec<NodeEntry>> {
    let mut entries = Vec::new();
    for raw in servers {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let address_part = parts
            .next()
            .ok_or_else(|| anyhow!("invalid server entry: {}", raw))?;
        let alias = parts.next();

        let (address, weight) = parse_address_weight(address_part)?;
        let display = alias
            .map(|s| s.to_string())
            .unwrap_or_else(|| address.clone());

        entries.push(NodeEntry {
            backend: BackendNode::new(address),
            display: display.into(),
            weight: weight.max(1),
        });
    }
    Ok(entries)
}

fn parse_address_weight(token: &str) -> Result<(String, usize)> {
    let mut weight = 1usize;
    let mut address = token.to_string();

    if let Some(pos) = token.rfind(':') {
        let suffix = &token[pos + 1..];
        if suffix.chars().all(|c| c.is_ascii_digit()) {
            let prefix = &token[..pos];
            if prefix.rsplit_once(':').is_some() {
                if let Ok(parsed) = suffix.parse::<usize>() {
                    weight = parsed.max(1);
                    address = prefix.to_string();
                }
            }
        }
    }

    Ok((address, weight))
}

fn build_ring(entries: &[NodeEntry]) -> Vec<(u64, BackendNode)> {
    let mut ring = Vec::new();
    for entry in entries {
        let replicas = entry.weight.max(1) * VIRTUAL_NODE_FACTOR;
        for replica in 0..replicas {
            let label = format!("{}-{}", entry.display, replica);
            let hash = hash_key(label.as_bytes());
            ring.push((hash, entry.backend.clone()));
        }
    }
    ring.sort_by_key(|(hash, _)| *hash);
    ring
}

fn hash_key(data: &[u8]) -> u64 {
    let digest: Digest = md5::compute(data);
    let bytes = digest.0;
    u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

#[derive(Clone)]
struct RedisConnector {
    timeout: Duration,
    reconnect_delay: Duration,
}

impl RedisConnector {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            reconnect_delay: Duration::from_millis(100),
        }
    }

    async fn open_stream(&self, node: &BackendNode) -> Result<Framed<TcpStream, RespCodec>> {
        let connect_target = node.as_str().to_string();
        let stream = timeout(self.timeout, TcpStream::connect(&connect_target))
            .await
            .with_context(|| format!("connect to {} timed out", connect_target))??;
        stream
            .set_nodelay(true)
            .with_context(|| format!("failed to set TCP_NODELAY on {}", connect_target))?;
        Ok(Framed::new(stream, RespCodec::default()))
    }

    async fn execute_request(
        &self,
        framed: &mut Framed<TcpStream, RespCodec>,
        request: RedisCommand,
    ) -> Result<RedisResponse> {
        let frame = request.to_resp();
        timeout(self.timeout, framed.send(frame))
            .await
            .context("timed out while sending request")??;

        match timeout(self.timeout, framed.next()).await {
            Ok(Some(Ok(response))) => Ok(response),
            Ok(Some(Err(err))) => Err(err.into()),
            Ok(None) => Err(anyhow!("backend closed connection")),
            Err(_) => Err(anyhow!("timed out waiting for backend reply")),
        }
    }
}

#[async_trait]
impl Connector<RedisCommand> for RedisConnector {
    async fn run_session(
        self: Arc<Self>,
        node: BackendNode,
        cluster: Arc<str>,
        mut rx: mpsc::Receiver<SessionCommand<RedisCommand>>,
    ) {
        info!(cluster = %cluster, backend = %node.as_str(), "starting backend session");
        let mut connection: Option<Framed<TcpStream, RespCodec>> = None;

        while let Some(command) = rx.recv().await {
            if connection.is_none() {
                match self.open_stream(&node).await {
                    Ok(stream) => {
                        connection = Some(stream);
                    }
                    Err(err) => {
                        warn!(
                            cluster = %cluster,
                            backend = %node.as_str(),
                            error = %err,
                            "failed to establish backend connection"
                        );
                        let _ = command.respond_to.send(Err(err));
                        sleep(self.reconnect_delay).await;
                        continue;
                    }
                }
            }

            if let Some(ref mut framed) = connection {
                let result = self.execute_request(framed, command.request).await;
                let is_err = result.is_err();
                let _ = command.respond_to.send(result);
                if is_err {
                    connection = None;
                    sleep(self.reconnect_delay).await;
                }
            }
        }

        debug!(cluster = %cluster, backend = %node.as_str(), "backend session terminated");
    }
}
