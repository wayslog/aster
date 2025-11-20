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
use crate::cache::{tracker::CacheTrackerSet, ClientCache};
use crate::config::{ClusterConfig, ClusterRuntime, ConfigManager};
use crate::hotkey::Hotkey;
use crate::info::{InfoContext, ProxyMode};
use crate::metrics;
use crate::protocol::redis::{
    BlockingKind, MultiDispatch, RedisCommand, RedisResponse, RespCodec, RespValue, RespVersion,
    SubCommand, SubResponse, SubscriptionKind,
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
    hotkey: Arc<Hotkey>,
    listen_port: u16,
    backend_nodes: usize,
    client_cache: Arc<ClientCache>,
    _cache_trackers: Arc<CacheTrackerSet>,
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
            config.backend_resp_version,
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
        let hotkey = config_manager
            .hotkey_for(&config.name)
            .ok_or_else(|| anyhow!("missing hotkey state for cluster {}", config.name))?;
        let client_cache = config_manager
            .client_cache_for(&config.name)
            .ok_or_else(|| anyhow!("missing client cache state for cluster {}", config.name))?;
        let cache_trackers = Arc::new(CacheTrackerSet::new(
            cluster.clone(),
            client_cache.clone(),
            runtime.clone(),
            backend_auth.clone(),
            DEFAULT_TIMEOUT_MS,
        ));
        let tracker_nodes = nodes
            .iter()
            .map(|entry| entry.backend.as_str().to_string())
            .collect();
        cache_trackers.set_nodes(tracker_nodes);

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
            hotkey,
            listen_port,
            backend_nodes,
            client_cache,
            _cache_trackers: cache_trackers,
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
        self.hotkey.record_command(&command_snapshot);
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

            if let Some(response) = self.try_handle_hotkey(&command) {
                let success = !response.is_error();
                metrics::front_command(self.cluster.as_ref(), kind_label, success);
                framed.send(response).await?;
                continue;
            }

            if let Some(response) = self.try_handle_slowlog(&command) {
                let success = !response.is_error();
                metrics::front_command(self.cluster.as_ref(), kind_label, success);
                framed.send(response).await?;
                continue;
            }

            if let Some(hit) = self.client_cache.lookup(&command) {
                self.hotkey.record_command(&command);
                metrics::front_command(self.cluster.as_ref(), kind_label, true);
                framed.send(hit).await?;
                continue;
            }

            let cache_candidate = command.clone();
            let cacheable_read = ClientCache::is_cacheable_read(&cache_candidate);
            let invalidating_write = ClientCache::is_invalidating_write(&cache_candidate);
            let requested_version = command.resp_version_request();
            let response = match self.dispatch(client_id, command).await {
                Ok(resp) => resp,
                Err(err) => {
                    metrics::global_error_incr();
                    metrics::front_error(self.cluster.as_ref(), "dispatch");
                    let message = format!("ERR {err}");
                    RespValue::Error(Bytes::copy_from_slice(message.as_bytes()))
                }
            };

            let success = !response.is_error();
            if success && cacheable_read {
                self.client_cache.store(&cache_candidate, &response);
            }
            if invalidating_write {
                self.client_cache.invalidate_command(&cache_candidate);
            }
            if success {
                if let Some(version) = requested_version {
                    framed.codec_mut().set_version(version);
                }
            }
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

    fn try_handle_hotkey(&self, command: &RedisCommand) -> Option<RespValue> {
        if !command.command_name().eq_ignore_ascii_case(b"HOTKEY") {
            return None;
        }

        Some(crate::hotkey::handle_command(&self.hotkey, command.args()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn node_entry(address: &str, display: &str, weight: usize) -> NodeEntry {
        NodeEntry {
            backend: BackendNode::new(address.to_string()),
            display: Arc::<str>::from(display.to_string()),
            weight,
        }
    }

    #[test]
    fn parse_address_weight_extracts_suffix() {
        let (addr, weight) = parse_address_weight("127.0.0.1:6379:3").unwrap();
        assert_eq!(addr, "127.0.0.1:6379");
        assert_eq!(weight, 3);

        let (addr, weight) = parse_address_weight("10.0.0.1:6380").unwrap();
        assert_eq!(addr, "10.0.0.1:6380");
        assert_eq!(weight, 1);
    }

    #[test]
    fn parse_servers_respects_alias_and_weight() {
        let input = vec!["127.0.0.1:6379:2 main".to_string()];
        let entries = parse_servers(&input).expect("servers");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].backend.as_str(), "127.0.0.1:6379");
        assert_eq!(entries[0].weight, 2);
        assert_eq!(&*entries[0].display, "main");
    }

    #[test]
    fn build_ring_reflects_weight() {
        let entries = vec![node_entry("127.0.0.1:6379", "node", 2)];
        let ring = build_ring(&entries);
        assert_eq!(ring.len(), 2 * VIRTUAL_NODE_FACTOR);
        assert!(ring.windows(2).all(|w| w[0].0 <= w[1].0));
    }

    #[test]
    fn subscription_action_kind_maps_known_values() {
        assert_eq!(
            subscription_action_kind(b"subscribe"),
            Some(SubscriptionKind::Channel)
        );
        assert_eq!(
            subscription_action_kind(b"psubscribe"),
            Some(SubscriptionKind::Pattern)
        );
        assert!(subscription_action_kind(b"unknown").is_none());
    }

    #[test]
    fn subscription_count_parses_arrays() {
        let resp = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"psubscribe")),
            RespValue::BulkString(Bytes::from_static(b"chan")),
            RespValue::Integer(3),
        ]);
        assert_eq!(
            subscription_count(&resp),
            Some((SubscriptionKind::Pattern, 3))
        );
    }

    #[test]
    fn subscription_count_rejects_invalid_payload() {
        let resp = RespValue::Array(vec![RespValue::Integer(1)]);
        assert!(subscription_count(&resp).is_none());
    }

    #[test]
    fn hash_key_differs_for_distinct_inputs() {
        let a = hash_key(b"alpha");
        let b = hash_key(b"beta");
        assert_ne!(a, b);
        assert_eq!(hash_key(b"alpha"), a);
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
    backend_resp_version: RespVersion,
}

impl RedisConnector {
    fn new(
        runtime: Arc<ClusterRuntime>,
        default_timeout_ms: u64,
        backend_auth: Option<BackendAuth>,
        backend_resp_version: RespVersion,
    ) -> Self {
        Self {
            runtime,
            default_timeout_ms,
            reconnect_delay: Duration::from_millis(100),
            max_reconnect_delay: Duration::from_secs(2),
            heartbeat_interval: Duration::from_secs(20),
            backend_auth,
            backend_resp_version,
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

    async fn negotiate_resp_version(
        &self,
        cluster: &str,
        node: &BackendNode,
        framed: &mut Framed<TcpStream, RespCodec>,
    ) -> Result<RespVersion> {
        framed.codec_mut().set_version(RespVersion::Resp2);
        if self.backend_resp_version != RespVersion::Resp3 {
            return Ok(RespVersion::Resp2);
        }

        let timeout_duration = self.current_timeout();
        let mut hello_parts = vec![
            RespValue::BulkString(Bytes::from_static(b"HELLO")),
            RespValue::BulkString(Bytes::from_static(b"3")),
        ];
        if let Some(auth) = &self.backend_auth {
            if let Some((username, password)) = auth.hello_credentials() {
                hello_parts.push(RespValue::BulkString(Bytes::from_static(b"AUTH")));
                hello_parts.push(RespValue::BulkString(username));
                hello_parts.push(RespValue::BulkString(password));
            }
        }
        let hello = RespValue::Array(hello_parts);

        match timeout(timeout_duration, framed.send(hello)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                metrics::backend_error(cluster, node.as_str(), "resp3_handshake");
                return Err(err.context(format!("failed to send RESP3 HELLO to {}", node.as_str())));
            }
            Err(_) => {
                metrics::backend_error(cluster, node.as_str(), "resp3_handshake");
                return Err(anyhow!(
                    "backend {} timed out sending RESP3 HELLO",
                    node.as_str()
                ));
            }
        }

        let reply = match timeout(timeout_duration, framed.next()).await {
            Ok(Some(Ok(value))) => value,
            Ok(Some(Err(err))) => {
                metrics::backend_error(cluster, node.as_str(), "resp3_handshake");
                return Err(err.context(format!(
                    "failed to read RESP3 HELLO reply from {}",
                    node.as_str()
                )));
            }
            Ok(None) => {
                metrics::backend_error(cluster, node.as_str(), "resp3_handshake");
                return Err(anyhow!(
                    "backend {} closed connection during RESP3 HELLO",
                    node.as_str()
                ));
            }
            Err(_) => {
                metrics::backend_error(cluster, node.as_str(), "resp3_handshake");
                return Err(anyhow!(
                    "backend {} timed out waiting for RESP3 HELLO reply",
                    node.as_str()
                ));
            }
        };

        if reply.is_error() {
            info!(
                cluster = %cluster,
                backend = %node.as_str(),
                "backend rejected RESP3 HELLO; falling back to RESP2"
            );
            framed.codec_mut().set_version(RespVersion::Resp2);
            return Ok(RespVersion::Resp2);
        }

        framed.codec_mut().set_version(RespVersion::Resp3);
        Ok(RespVersion::Resp3)
    }

    async fn execute_request(
        &self,
        framed: &mut Framed<TcpStream, RespCodec>,
        request: RedisCommand,
    ) -> Result<RedisResponse> {
        let requested_version = request.resp_version_request();
        let blocking = request.as_blocking();
        let frame = request.to_resp();
        let timeout_duration = self.current_timeout();
        timeout(timeout_duration, framed.send(frame))
            .await
            .context("timed out while sending request")??;

        let response = match blocking {
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
        }?;

        if let Some(version) = requested_version {
            if !response.is_error() {
                framed.codec_mut().set_version(version);
            }
        }
        Ok(response)
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
                            Ok(mut stream) => {
                                metrics::backend_probe_duration(
                                    &cluster,
                                    node.as_str(),
                                    "connect",
                                    attempt_start.elapsed(),
                                );
                                metrics::backend_probe_result(&cluster, node.as_str(), "connect", true);
                                match self
                                    .negotiate_resp_version(&cluster, &node, &mut stream)
                                    .await
                                {
                                    Ok(_) => {
                                        connection = Some(stream);
                                        current_delay = self.reconnect_delay;
                                    }
                                    Err(err) => {
                                        warn!(
                                            cluster = %cluster,
                                            backend = %node.as_str(),
                                            error = %err,
                                            "failed to negotiate RESP version with backend"
                                        );
                                        let _ = respond_to.send(Err(err));
                                        current_delay = self.increase_delay(current_delay);
                                        sleep(current_delay).await;
                                        continue;
                                    }
                                }
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
