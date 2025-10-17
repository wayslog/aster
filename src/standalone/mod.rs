use std::cmp::min;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
use md5::Digest;
#[cfg(any(unix, windows))]
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};
use tokio_util::codec::{Framed, FramedParts};
use tracing::{debug, info, warn};

use crate::auth::{AuthAction, BackendAuth, FrontendAuthenticator};
use crate::backend::client::{ClientId, FrontConnectionGuard};
use crate::backend::pool::{BackendNode, ConnectionPool, Connector, SessionCommand};
use crate::config::{ClusterConfig, ClusterRuntime, ConfigManager};
use crate::info::{InfoContext, ProxyMode};
use crate::metrics;
use crate::protocol::redis::{
    BlockingKind, MultiDispatch, RedisCommand, RedisResponse, RespCodec, RespValue, SubCommand,
    SubResponse, SubscriptionKind,
};
use crate::slowlog::Slowlog;
use crate::utils::trim_hash_tag;

const DEFAULT_TIMEOUT_MS: u64 = 1_000;
const VIRTUAL_NODE_FACTOR: usize = 40;
const FRONT_TCP_KEEPALIVE: Duration = Duration::from_secs(60);

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
    auth: Option<Arc<FrontendAuthenticator>>,
    backend_auth: Option<BackendAuth>,
    pool: Arc<ConnectionPool<RedisCommand>>,
    runtime: Arc<ClusterRuntime>,
    config_manager: Arc<ConfigManager>,
    slowlog: Arc<Slowlog>,
    listen_port: u16,
    backend_nodes: usize,
}

impl StandaloneProxy {
    pub fn new(
        config: &ClusterConfig,
        runtime: Arc<ClusterRuntime>,
        config_manager: Arc<ConfigManager>,
    ) -> Result<Self> {
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

        let backend_auth = config.backend_auth_config().map(BackendAuth::from);
        let connector = Arc::new(RedisConnector::new(
            runtime.clone(),
            DEFAULT_TIMEOUT_MS,
            backend_auth.clone(),
        ));
        let auth = config
            .frontend_auth_users()
            .map(FrontendAuthenticator::from_users)
            .transpose()?
            .map(Arc::new);
        let pool = Arc::new(ConnectionPool::new(cluster.clone(), connector));

        let backend_nodes = nodes.len();
        let listen_port = config.listen_port()?;
        let slowlog = config_manager
            .slowlog_for(&config.name)
            .ok_or_else(|| anyhow!("missing slowlog state for cluster {}", config.name))?;

        Ok(Self {
            cluster,
            hash_tag,
            ring,
            auth,
            backend_auth,
            pool,
            runtime,
            config_manager,
            slowlog,
            listen_port,
            backend_nodes,
        })
    }

    pub async fn dispatch(
        &self,
        client_id: ClientId,
        command: RedisCommand,
    ) -> Result<RedisResponse> {
        let hash_tag = self.hash_tag.as_deref();
        let ring = &self.ring;
        let command_snapshot = command.clone();
        let multi = command.expand_for_multi_with(|key| {
            if ring.is_empty() {
                return 0;
            }
            let trimmed = trim_hash_tag(key, hash_tag);
            let hash = hash_key(trimmed);
            let idx = match ring.binary_search_by_key(&hash, |(value, _node)| *value) {
                Ok(idx) => idx,
                Err(idx) if idx >= ring.len() => 0,
                Err(idx) => idx,
            };
            idx as u64
        });
        let started = Instant::now();
        let result = if let Some(multi) = multi {
            self.dispatch_multi(client_id, multi).await
        } else {
            self.dispatch_single(client_id, command).await
        };
        self.slowlog
            .maybe_record(&command_snapshot, started.elapsed());
        result
    }

    async fn dispatch_single(
        &self,
        client_id: ClientId,
        command: RedisCommand,
    ) -> Result<RedisResponse> {
        if matches!(
            command.as_subscription(),
            SubscriptionKind::Unsubscribe | SubscriptionKind::Punsub
        ) {
            return Ok(RespValue::Error(Bytes::from_static(
                b"ERR unsubscribe without active subscription",
            )));
        }
        match command.as_blocking() {
            BlockingKind::Queue { .. } | BlockingKind::Stream { .. } => {
                let node = self.select_node(client_id, &command)?;
                let mut exclusive = self.pool.acquire_exclusive(&node);
                let response_rx = exclusive.send(command).await?;
                let outcome = response_rx.await;
                drop(exclusive);
                match outcome {
                    Ok(result) => result,
                    Err(_) => Err(anyhow!("backend session closed unexpectedly")),
                }
            }
            BlockingKind::None => {
                let node = self.select_node(client_id, &command)?;
                let response_rx = self.pool.dispatch(node, client_id, command).await?;
                match response_rx.await {
                    Ok(result) => result,
                    Err(_) => Err(anyhow!("backend session closed unexpectedly")),
                }
            }
        }
    }

    async fn dispatch_multi(
        &self,
        client_id: ClientId,
        multi: MultiDispatch,
    ) -> Result<RedisResponse> {
        let mut tasks: FuturesOrdered<BoxFuture<'static, Result<SubResponse>>> =
            FuturesOrdered::new();
        for sub in multi.subcommands.into_iter() {
            let node = self.select_node(client_id, &sub.command)?;
            let pool = self.pool.clone();
            let SubCommand { positions, command } = sub;
            tasks.push_back(Box::pin(async move {
                let response_rx = pool.dispatch(node, client_id, command).await?;
                match response_rx.await {
                    Ok(result) => Ok(SubResponse {
                        positions,
                        response: result?,
                    }),
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

    async fn run_subscription(
        &self,
        parts: FramedParts<TcpStream, RespCodec>,
        client_id: ClientId,
        command: RedisCommand,
    ) -> Result<Option<FramedParts<TcpStream, RespCodec>>> {
        let node = self.select_node(client_id, &command)?;
        let mut backend = self.open_backend_stream(&node).await?;
        backend.send(command.to_resp()).await?;

        let front = Framed::from_parts(parts);
        let (mut front_sink, mut front_stream) = front.split();

        let mut continue_running = true;
        let mut channel_count: i64 = 0;
        let mut pattern_count: i64 = 0;
        metrics::subscription_active(self.cluster.as_ref(), "channel", 0);
        metrics::subscription_active(self.cluster.as_ref(), "pattern", 0);
        while continue_running {
            tokio::select! {
                backend_msg = backend.next() => {
                    match backend_msg {
                        Some(Ok(resp)) => {
                            if let Some((kind, count)) = subscription_count(&resp) {
                                match kind {
                                    SubscriptionKind::Channel | SubscriptionKind::Unsubscribe => {
                                        channel_count = count.max(0);
                                        metrics::subscription_active(
                                            self.cluster.as_ref(),
                                            "channel",
                                            channel_count as usize,
                                        );
                                    }
                                    SubscriptionKind::Pattern | SubscriptionKind::Punsub => {
                                        pattern_count = count.max(0);
                                        metrics::subscription_active(
                                            self.cluster.as_ref(),
                                            "pattern",
                                            pattern_count as usize,
                                        );
                                    }
                                    SubscriptionKind::None => {}
                                }
                                front_sink.send(resp.clone()).await?;
                                if count == 0 {
                                    continue_running = false;
                                }
                            } else {
                                front_sink.send(resp).await?;
                            }
                        }
                        Some(Err(err)) => {
                            metrics::subscription_active(self.cluster.as_ref(), "channel", 0);
                            metrics::subscription_active(self.cluster.as_ref(), "pattern", 0);
                            metrics::front_error(self.cluster.as_ref(), "subscription_stream");
                            return Err(err.into());
                        }
                        None => {
                            metrics::subscription_active(self.cluster.as_ref(), "channel", 0);
                            metrics::subscription_active(self.cluster.as_ref(), "pattern", 0);
                            return Ok(None);
                        }
                    }
                }
                front_msg = front_stream.next() => {
                    match front_msg {
                        Some(Ok(frame)) => {
                            let cmd = match RedisCommand::from_resp(frame) {
                                Ok(cmd) => cmd,
                                Err(err) => {
                                    metrics::global_error_incr();
                                    metrics::front_error(self.cluster.as_ref(), "parse");
                                    front_sink
                                        .send(RespValue::Error(Bytes::from(format!("ERR {err}"))))
                                        .await?;
                                    continue;
                                }
                            };

                            match cmd.as_subscription() {
                                SubscriptionKind::Channel | SubscriptionKind::Pattern
                                | SubscriptionKind::Unsubscribe | SubscriptionKind::Punsub => {
                                    backend.send(cmd.to_resp()).await?;
                                }
                                SubscriptionKind::None => {
                                    front_sink
                                        .send(RespValue::Error(Bytes::from_static(
                                            b"ERR only subscribe/unsubscribe allowed in subscription mode",
                                        )))
                                        .await?;
                                    metrics::front_error(self.cluster.as_ref(), "subscription_mode");
                                }
                            }
                        }
                        Some(Err(err)) => {
                            metrics::subscription_active(self.cluster.as_ref(), "channel", 0);
                            metrics::subscription_active(self.cluster.as_ref(), "pattern", 0);
                            metrics::front_error(self.cluster.as_ref(), "subscription_stream");
                            return Err(err.into());
                        }
                        None => {
                            metrics::subscription_active(self.cluster.as_ref(), "channel", 0);
                            metrics::subscription_active(self.cluster.as_ref(), "pattern", 0);
                            return Ok(None);
                        }
                    }
                }
            }
        }

        let front = front_sink.reunite(front_stream)?;
        metrics::subscription_active(
            self.cluster.as_ref(),
            "channel",
            channel_count.max(0) as usize,
        );
        metrics::subscription_active(
            self.cluster.as_ref(),
            "pattern",
            pattern_count.max(0) as usize,
        );
        Ok(Some(front.into_parts()))
    }

    async fn open_backend_stream(
        &self,
        node: &BackendNode,
    ) -> Result<Framed<TcpStream, RespCodec>> {
        let addr = node.as_str().to_string();
        let timeout_duration = self.runtime.request_timeout(DEFAULT_TIMEOUT_MS);
        let stream = timeout(timeout_duration, TcpStream::connect(&addr))
            .await
            .with_context(|| format!("connect to {} timed out", addr))??;
        stream
            .set_nodelay(true)
            .with_context(|| format!("failed to set TCP_NODELAY on {}", addr))?;
        let mut framed = Framed::new(stream, RespCodec::default());
        if let Some(auth) = &self.backend_auth {
            auth.apply_to_stream(&mut framed, timeout_duration, &addr)
                .await?;
        }
        Ok(framed)
    }

    pub async fn handle_connection(&self, socket: TcpStream) -> Result<()> {
        socket
            .set_nodelay(true)
            .context("failed to set TCP_NODELAY")?;
        #[cfg(any(unix, windows))]
        {
            let keepalive = TcpKeepalive::new()
                .with_time(FRONT_TCP_KEEPALIVE)
                .with_interval(FRONT_TCP_KEEPALIVE);
            if let Err(err) = SockRef::from(&socket).set_tcp_keepalive(&keepalive) {
                warn!(
                    cluster = %self.cluster,
                    error = %err,
                    "failed to enable frontend TCP keepalive"
                );
            }
        }
        let client_id = ClientId::new();
        let _guard = FrontConnectionGuard::new(&self.cluster);

        let mut framed = Framed::new(socket, RespCodec::default());
        let mut auth_state = self.auth.as_ref().map(|auth| auth.new_session());

        while let Some(frame) = framed.next().await {
            let frame = match frame {
                Ok(frame) => frame,
                Err(err) => {
                    metrics::global_error_incr();
                    metrics::front_error(self.cluster.as_ref(), "front_stream");
                    return Err(err.into());
                }
            };

            let command = match RedisCommand::from_resp(frame) {
                Ok(cmd) => cmd,
                Err(err) => {
                    metrics::global_error_incr();
                    metrics::front_error(self.cluster.as_ref(), "parse");
                    metrics::front_command(self.cluster.as_ref(), "invalid", false);
                    let message = format!("ERR {err}");
                    let resp = RespValue::Error(Bytes::copy_from_slice(message.as_bytes()));
                    framed.send(resp).await?;
                    continue;
                }
            };

            let mut command = command;
            if let (Some(auth), Some(state)) = (self.auth.as_ref(), auth_state.as_mut()) {
                match auth.handle_command(state, &command) {
                    AuthAction::Allow => {}
                    AuthAction::Rewrite(new_cmd) => {
                        command = new_cmd;
                    }
                    AuthAction::Reply(resp) => {
                        framed.send(resp).await?;
                        continue;
                    }
                }
            }

            let kind_label = command.kind_label();
            if matches!(
                command.as_subscription(),
                SubscriptionKind::Channel | SubscriptionKind::Pattern
            ) {
                let parts = framed.into_parts();
                let subscription = self.run_subscription(parts, client_id, command).await;
                match subscription {
                    Ok(Some(parts)) => {
                        metrics::front_command(self.cluster.as_ref(), kind_label, true);
                        framed = Framed::from_parts(parts);
                        continue;
                    }
                    Ok(None) => {
                        metrics::front_command(self.cluster.as_ref(), kind_label, true);
                        return Ok(());
                    }
                    Err(err) => {
                        metrics::front_command(self.cluster.as_ref(), kind_label, false);
                        metrics::front_error(self.cluster.as_ref(), "subscription_proxy");
                        return Err(err);
                    }
                }
            }

            if let Some(response) = self.try_handle_config(&command).await {
                let success = !response.is_error();
                metrics::front_command(self.cluster.as_ref(), kind_label, success);
                framed.send(response).await?;
                continue;
            }

            if let Some(response) = self.try_handle_info(&command) {
                metrics::front_command(self.cluster.as_ref(), kind_label, true);
                framed.send(response).await?;
                continue;
            }

            if let Some(response) = self.try_handle_slowlog(&command) {
                let success = !response.is_error();
                metrics::front_command(self.cluster.as_ref(), kind_label, success);
                framed.send(response).await?;
                continue;
            }

            let response = match self.dispatch(client_id, command).await {
                Ok(resp) => resp,
                Err(err) => {
                    metrics::global_error_incr();
                    metrics::front_error(self.cluster.as_ref(), "dispatch");
                    let message = format!("ERR {err}");
                    RespValue::Error(Bytes::copy_from_slice(message.as_bytes()))
                }
            };

            let success = !matches!(response, RespValue::Error(_));
            metrics::front_command(self.cluster.as_ref(), kind_label, success);
            framed.send(response).await?;
        }

        Ok(())
    }

    fn try_handle_slowlog(&self, command: &RedisCommand) -> Option<RespValue> {
        if !command.command_name().eq_ignore_ascii_case(b"SLOWLOG") {
            return None;
        }

        Some(crate::slowlog::handle_command(
            &self.slowlog,
            command.args(),
        ))
    }

    async fn try_handle_config(&self, command: &RedisCommand) -> Option<RespValue> {
        self.config_manager.handle_command(command).await
    }

    fn try_handle_info(&self, command: &RedisCommand) -> Option<RespValue> {
        if !command.command_name().eq_ignore_ascii_case(b"INFO") {
            return None;
        }
        let section = command
            .args()
            .get(1)
            .map(|arg| String::from_utf8_lossy(arg).to_string());
        let context = InfoContext {
            cluster: self.cluster.as_ref(),
            mode: ProxyMode::Standalone,
            listen_port: self.listen_port,
            backend_nodes: self.backend_nodes,
        };
        let payload = crate::info::render_info(context, section.as_deref());
        Some(RespValue::BulkString(payload))
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

fn subscription_count(resp: &RespValue) -> Option<(SubscriptionKind, i64)> {
    if let RespValue::Array(items) = resp {
        if items.len() >= 3 {
            let count = match &items[2] {
                RespValue::Integer(value) => Some(*value),
                RespValue::BulkString(bs) => std::str::from_utf8(bs).ok()?.parse::<i64>().ok(),
                _ => None,
            }?;

            if let RespValue::SimpleString(kind) | RespValue::BulkString(kind) = &items[0] {
                if let Some(mapped) = subscription_action_kind(kind.as_ref()) {
                    return Some((mapped, count));
                }
            }
        }
    }
    None
}

fn subscription_action_kind(kind: &[u8]) -> Option<SubscriptionKind> {
    match kind {
        b"subscribe" => Some(SubscriptionKind::Channel),
        b"psubscribe" => Some(SubscriptionKind::Pattern),
        b"unsubscribe" => Some(SubscriptionKind::Unsubscribe),
        b"punsubscribe" => Some(SubscriptionKind::Punsub),
        _ => None,
    }
}

#[derive(Clone)]
struct RedisConnector {
    runtime: Arc<ClusterRuntime>,
    default_timeout_ms: u64,
    reconnect_delay: Duration,
    max_reconnect_delay: Duration,
    heartbeat_interval: Duration,
    backend_auth: Option<BackendAuth>,
}

impl RedisConnector {
    fn new(
        runtime: Arc<ClusterRuntime>,
        default_timeout_ms: u64,
        backend_auth: Option<BackendAuth>,
    ) -> Self {
        Self {
            runtime,
            default_timeout_ms,
            reconnect_delay: Duration::from_millis(100),
            max_reconnect_delay: Duration::from_secs(2),
            heartbeat_interval: Duration::from_secs(20),
            backend_auth,
        }
    }

    async fn open_stream(&self, node: &BackendNode) -> Result<Framed<TcpStream, RespCodec>> {
        let connect_target = node.as_str().to_string();
        let timeout_duration = self.current_timeout();
        let stream = timeout(timeout_duration, TcpStream::connect(&connect_target))
            .await
            .with_context(|| format!("connect to {} timed out", connect_target))??;
        stream
            .set_nodelay(true)
            .with_context(|| format!("failed to set TCP_NODELAY on {}", connect_target))?;
        #[cfg(any(unix, windows))]
        {
            let keepalive = TcpKeepalive::new()
                .with_time(self.heartbeat_interval)
                .with_interval(self.heartbeat_interval);
            if let Err(err) = SockRef::from(&stream).set_tcp_keepalive(&keepalive) {
                warn!(
                    backend = %connect_target,
                    error = %err,
                    "failed to enable backend TCP keepalive"
                );
            }
        }
        let mut framed = Framed::new(stream, RespCodec::default());
        if let Some(auth) = &self.backend_auth {
            auth.apply_to_stream(&mut framed, timeout_duration, &connect_target)
                .await?;
        }
        Ok(framed)
    }

    async fn execute_request(
        &self,
        framed: &mut Framed<TcpStream, RespCodec>,
        request: RedisCommand,
    ) -> Result<RedisResponse> {
        let blocking = request.as_blocking();
        let frame = request.to_resp();
        let timeout_duration = self.current_timeout();
        timeout(timeout_duration, framed.send(frame))
            .await
            .context("timed out while sending request")??;

        match blocking {
            BlockingKind::Queue { .. } | BlockingKind::Stream { .. } => match framed.next().await {
                Some(Ok(response)) => Ok(response),
                Some(Err(err)) => Err(err.into()),
                None => Err(anyhow!("backend closed connection")),
            },
            BlockingKind::None => match timeout(timeout_duration, framed.next()).await {
                Ok(Some(Ok(response))) => Ok(response),
                Ok(Some(Err(err))) => Err(err.into()),
                Ok(None) => Err(anyhow!("backend closed connection")),
                Err(_) => Err(anyhow!("timed out waiting for backend reply")),
            },
        }
    }

    async fn heartbeat(&self, framed: &mut Framed<TcpStream, RespCodec>) -> Result<()> {
        use RespValue::{Array, BulkString, SimpleString};

        let ping = Array(vec![BulkString(Bytes::from_static(b"PING"))]);
        let timeout_duration = self.current_timeout();
        timeout(timeout_duration, framed.send(ping))
            .await
            .context("timed out while sending heartbeat")??;

        match timeout(timeout_duration, framed.next()).await {
            Ok(Some(Ok(resp))) => match resp {
                SimpleString(ref data) | BulkString(ref data)
                    if data.eq_ignore_ascii_case(b"PONG") =>
                {
                    Ok(())
                }
                other => Err(anyhow!("unexpected heartbeat reply: {:?}", other)),
            },
            Ok(Some(Err(err))) => Err(err.into()),
            Ok(None) => Err(anyhow!("backend closed connection during heartbeat")),
            Err(_) => Err(anyhow!("timed out waiting for heartbeat reply")),
        }
    }

    fn increase_delay(&self, current: Duration) -> Duration {
        let doubled = current
            .checked_mul(2)
            .unwrap_or_else(|| self.max_reconnect_delay);
        min(doubled, self.max_reconnect_delay)
    }

    fn current_timeout(&self) -> Duration {
        self.runtime.request_timeout(self.default_timeout_ms)
    }

    fn slow_response_threshold(&self) -> Duration {
        self.current_timeout()
            .checked_mul(4)
            .unwrap_or_else(|| Duration::from_secs(4))
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
        let mut heartbeat = interval(self.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut current_delay = self.reconnect_delay;

        loop {
            tokio::select! {
                biased;

                cmd_opt = rx.recv() => {
                    let SessionCommand { request, respond_to } = match cmd_opt {
                        Some(cmd) => cmd,
                        None => break,
                    };

                    if connection.is_none() {
                        let attempt_start = Instant::now();
                        match self.open_stream(&node).await {
                            Ok(stream) => {
                                metrics::backend_probe_duration(
                                    &cluster,
                                    node.as_str(),
                                    "connect",
                                    attempt_start.elapsed(),
                                );
                                metrics::backend_probe_result(&cluster, node.as_str(), "connect", true);
                                connection = Some(stream);
                                current_delay = self.reconnect_delay;
                            }
                            Err(err) => {
                                let elapsed = attempt_start.elapsed();
                                metrics::backend_probe_duration(
                                    &cluster,
                                    node.as_str(),
                                    "connect",
                                    elapsed,
                                );
                                metrics::backend_probe_result(&cluster, node.as_str(), "connect", false);
                                metrics::backend_error(&cluster, node.as_str(), "connect");
                                warn!(
                                    cluster = %cluster,
                                    backend = %node.as_str(),
                                    error = %err,
                                    "failed to establish backend connection"
                                );
                                let _ = respond_to.send(Err(err));
                                current_delay = self.increase_delay(current_delay);
                                sleep(current_delay).await;
                                continue;
                            }
                        }
                    }

                    if let Some(ref mut framed) = connection {
                        let slow_threshold = self.slow_response_threshold();
                        let started = Instant::now();
                        let result = self.execute_request(framed, request).await;
                        let elapsed = started.elapsed();
                        let is_slow = elapsed > slow_threshold;

                        match result {
                            Ok(resp) => {
                                let mut result_label = if matches!(resp, RespValue::Error(_)) {
                                    "resp_error"
                                } else {
                                    "ok"
                                };
                                if is_slow {
                                    metrics::backend_error(&cluster, node.as_str(), "slow");
                                    warn!(
                                        cluster = %cluster,
                                        backend = %node.as_str(),
                                        elapsed_ms = elapsed.as_millis() as u64,
                                        "standalone backend response slow"
                                    );
                                    let increased = current_delay
                                        .checked_add(self.reconnect_delay)
                                        .unwrap_or(self.max_reconnect_delay);
                                    current_delay = min(increased, self.max_reconnect_delay);
                                    result_label = "slow";
                                } else {
                                    current_delay = self.reconnect_delay;
                                }
                                metrics::backend_request_result(
                                    &cluster,
                                    node.as_str(),
                                    result_label,
                                );
                                if respond_to.send(Ok(resp)).is_err() {
                                    debug!(
                                        cluster = %cluster,
                                        backend = %node.as_str(),
                                        "standalone backend response dropped (client closed)"
                                    );
                                }
                            }
                            Err(err) => {
                                let kind = classify_backend_error(&err);
                                metrics::backend_request_result(&cluster, node.as_str(), kind);
                                metrics::backend_error(&cluster, node.as_str(), kind);
                                let _ = respond_to.send(Err(err));
                                connection = None;
                                current_delay = self.increase_delay(current_delay);
                                sleep(current_delay).await;
                            }
                        }
                    }
                }
                _ = heartbeat.tick(), if connection.is_some() => {
                    if let Some(ref mut framed) = connection {
                        let start = Instant::now();
                        let result = self.heartbeat(framed).await;
                        metrics::backend_probe_duration(
                            &cluster,
                            node.as_str(),
                            "heartbeat",
                            start.elapsed(),
                        );
                        match result {
                            Ok(()) => {
                                metrics::backend_heartbeat(&cluster, node.as_str(), true);
                                current_delay = self.reconnect_delay;
                            }
                            Err(err) => {
                                metrics::backend_error(&cluster, node.as_str(), "heartbeat");
                                metrics::backend_heartbeat(&cluster, node.as_str(), false);
                                warn!(
                                    cluster = %cluster,
                                    backend = %node.as_str(),
                                    error = %err,
                                    "standalone backend heartbeat failed"
                                );
                                connection = None;
                                current_delay = self.increase_delay(current_delay);
                            }
                        }
                    }
                }
            }
        }

        debug!(cluster = %cluster, backend = %node.as_str(), "backend session terminated");
    }
}

fn classify_backend_error(err: &anyhow::Error) -> &'static str {
    let message = err.to_string();
    if message.contains("timed out") {
        "timeout"
    } else if message.contains("closed connection") {
        "closed"
    } else if message.contains("heartbeat") {
        "heartbeat"
    } else {
        "execute"
    }
}
