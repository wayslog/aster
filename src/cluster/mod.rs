use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use rand::{seq::SliceRandom, thread_rng};
#[cfg(any(unix, windows))]
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};
use tokio_util::codec::{Framed, FramedParts};
use tracing::{debug, info, warn};

use crate::auth::{AuthAction, BackendAuth, FrontendAuthenticator};
use crate::backend::client::{ClientId, FrontConnectionGuard};
use crate::backend::pool::{BackendNode, ConnectionPool, Connector, SessionCommand};
use crate::cache::{tracker::CacheTrackerSet, ClientCache};
use crate::config::{BackupRequestRuntime, ClusterConfig, ClusterRuntime, ConfigManager};
use crate::hotkey::Hotkey;
use crate::info::{InfoContext, ProxyMode};
use crate::metrics;
use crate::protocol::redis::{
    BlockingKind, MultiDispatch, RedisCommand, RespCodec, RespValue, RespVersion, SlotMap,
    SubCommand, SubResponse, SubscriptionKind, SLOT_COUNT,
};
use crate::slowlog::Slowlog;
use crate::utils::{crc16, trim_hash_tag};

const FETCH_INTERVAL: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT_MS: u64 = 1_000;
const MAX_REDIRECTS: u8 = 5;
const PIPELINE_LIMIT: usize = 32;
const FRONT_TCP_KEEPALIVE: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct PendingSubscription {
    command: RedisCommand,
    ack_remaining: usize,
}

pub struct ClusterProxy {
    cluster: Arc<str>,
    hash_tag: Option<Vec<u8>>,
    read_from_slave: bool,
    auth: Option<Arc<FrontendAuthenticator>>,
    backend_auth: Option<BackendAuth>,
    slots: Arc<watch::Sender<SlotMap>>,
    pool: Arc<ConnectionPool<RedisCommand>>,
    fetch_trigger: mpsc::UnboundedSender<()>,
    runtime: Arc<ClusterRuntime>,
    config_manager: Arc<ConfigManager>,
    slowlog: Arc<Slowlog>,
    hotkey: Arc<Hotkey>,
    backup: Arc<BackupRequestController>,
    listen_port: u16,
    seed_nodes: usize,
    client_cache: Arc<ClientCache>,
    _cache_trackers: Arc<CacheTrackerSet>,
}

impl ClusterProxy {
    pub async fn new(
        config: &ClusterConfig,
        runtime: Arc<ClusterRuntime>,
        config_manager: Arc<ConfigManager>,
    ) -> Result<Self> {
        let cluster: Arc<str> = config.name.clone().into();
        let hash_tag = config.hash_tag.as_ref().map(|tag| tag.as_bytes().to_vec());
        let read_from_slave = config.read_from_slave.unwrap_or(false);

        let (slot_tx, _slot_rx) = watch::channel(SlotMap::new());
        let (trigger_tx, trigger_rx) = mpsc::unbounded_channel();

        let backend_auth = config.backend_auth_config().map(BackendAuth::from);
        let connector = Arc::new(ClusterConnector::new(
            runtime.clone(),
            REQUEST_TIMEOUT_MS,
            backend_auth.clone(),
            config.backend_resp_version,
        ));
        let pool = Arc::new(ConnectionPool::new(cluster.clone(), connector.clone()));
        let auth = config
            .frontend_auth_users()
            .map(FrontendAuthenticator::from_users)
            .transpose()?
            .map(Arc::new);

        let listen_port = config.listen_port()?;
        let slowlog = config_manager
            .slowlog_for(&config.name)
            .ok_or_else(|| anyhow!("missing slowlog state for cluster {}", config.name))?;
        let hotkey = config_manager
            .hotkey_for(&config.name)
            .ok_or_else(|| anyhow!("missing hotkey state for cluster {}", config.name))?;
        let backup_runtime = config_manager
            .backup_request_for(&config.name)
            .ok_or_else(|| anyhow!("missing backup request state for cluster {}", config.name))?;
        let backup = Arc::new(BackupRequestController::new(backup_runtime));
        let client_cache = config_manager
            .client_cache_for(&config.name)
            .ok_or_else(|| anyhow!("missing client cache state for cluster {}", config.name))?;
        let cache_trackers = Arc::new(CacheTrackerSet::new(
            cluster.clone(),
            client_cache.clone(),
            runtime.clone(),
            backend_auth.clone(),
            REQUEST_TIMEOUT_MS,
        ));
        let proxy = Self {
            cluster: cluster.clone(),
            hash_tag,
            read_from_slave,
            auth,
            backend_auth,
            slots: Arc::new(slot_tx),
            pool: pool.clone(),
            fetch_trigger: trigger_tx.clone(),
            runtime,
            config_manager,
            slowlog,
            hotkey,
            backup,
            listen_port,
            seed_nodes: config.servers.len(),
            client_cache,
            _cache_trackers: cache_trackers.clone(),
        };

        // trigger an immediate topology fetch
        trigger_tx.send(()).ok();
        tokio::spawn(fetch_topology(
            cluster,
            config.servers.clone(),
            connector,
            proxy.slots.clone(),
            trigger_rx,
            Some(cache_trackers),
        ));

        Ok(proxy)
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

        let framed = Framed::new(socket, RespCodec::default());
        let codec_handle = framed.codec().clone();
        let (mut sink, stream) = framed.split();
        let mut stream = stream.fuse();
        let mut pending: FuturesOrdered<BoxFuture<'static, (RespValue, Option<RespVersion>)>> =
            FuturesOrdered::new();
        let mut _resp3_negotiated = codec_handle.version() == RespVersion::Resp3;
        let mut inflight = 0usize;
        let mut stream_closed = false;
        let mut auth_state = self.auth.as_ref().map(|auth| auth.new_session());

        loop {
            tokio::select! {
                Some((resp, version)) = pending.next(), if inflight > 0 => {
                    inflight -= 1;
                    if let Some(version) = version {
                        codec_handle.set_version(version);
                        _resp3_negotiated = version == RespVersion::Resp3;
                    }
                    sink.send(resp).await?;
                }
                frame_opt = stream.next(), if !stream_closed && inflight < PIPELINE_LIMIT => {
                    match frame_opt {
                        Some(Ok(frame)) => {
                            match RedisCommand::from_resp(frame) {
                                Ok(mut cmd) => {
                                    if let (Some(auth), Some(state)) = (self.auth.as_ref(), auth_state.as_mut()) {
                                        match auth.handle_command(state, &cmd) {
                                            AuthAction::Allow => {}
                                            AuthAction::Rewrite(new_cmd) => {
                                                cmd = new_cmd;
                                            }
                                            AuthAction::Reply(resp) => {
                                                let fut = async move { (resp, None) };
                                                pending.push_back(Box::pin(fut));
                                                inflight += 1;
                                                continue;
                                            }
                                        }
                                    }
                                    if matches!(cmd.as_subscription(), SubscriptionKind::Channel | SubscriptionKind::Pattern) {
                                        let kind_label = cmd.kind_label();
                                        if cmd.args().len() <= 1 {
                                            metrics::global_error_incr();
                                            let command_name = String::from_utf8_lossy(cmd.command_name()).to_ascii_lowercase();
                                            let message = Bytes::from(format!(
                                                "ERR wrong number of arguments for '{}' command",
                                                command_name
                                            ));
                                            sink.send(RespValue::Error(message)).await?;
                                            metrics::front_command(
                                                self.cluster.as_ref(),
                                                kind_label,
                                                false,
                                            );
                                            metrics::front_error(self.cluster.as_ref(), "subscription_arity");
                                            continue;
                                        }
                                        let initial_slot = match subscription_slot_for_command(
                                            &cmd,
                                            self.hash_tag.as_deref(),
                                            None,
                                        ) {
                                            Ok(Some(slot)) => slot,
                                            Ok(None) => {
                                                metrics::global_error_incr();
                                                sink.send(RespValue::Error(Bytes::from_static(
                                                    b"ERR subscription channels must hash to the same slot",
                                                )))
                                                .await?;
                                                metrics::front_command(
                                                    self.cluster.as_ref(),
                                                    kind_label,
                                                    false,
                                                );
                                                metrics::front_error(self.cluster.as_ref(), "subscription_slot");
                                                continue;
                                            }
                                            Err(_) => {
                                                metrics::global_error_incr();
                                                sink.send(RespValue::Error(Bytes::from_static(
                                                    b"ERR subscription channels must hash to the same slot",
                                                )))
                                                .await?;
                                                metrics::front_command(
                                                    self.cluster.as_ref(),
                                                    kind_label,
                                                    false,
                                                );
                                                metrics::front_error(self.cluster.as_ref(), "subscription_slot");
                                                continue;
                                            }
                                        };
                                        while inflight > 0 {
                                            if let Some((resp, version)) = pending.next().await {
                                                inflight -= 1;
                                                if let Some(version) = version {
                                                    codec_handle.set_version(version);
                                                    _resp3_negotiated = version == RespVersion::Resp3;
                                                }
                                                sink.send(resp).await?;
                                            } else {
                                                inflight = 0;
                                            }
                                        }
                                        let raw_stream = stream.into_inner();
                                        let framed_combined = sink
                                            .reunite(raw_stream)
                                            .map_err(|_| anyhow!("failed to reunite frame"))?;
                                        let subscription = self
                                            .run_subscription(
                                                framed_combined.into_parts(),
                                                cmd,
                                                initial_slot,
                                            )
                                            .await;
                                        match subscription {
                                            Ok(Some(parts)) => {
                                                metrics::front_command(
                                                    self.cluster.as_ref(),
                                                    kind_label,
                                                    true,
                                                );
                                                let framed_new = Framed::from_parts(parts);
                                                let (new_sink, new_stream) = framed_new.split();
                                                sink = new_sink;
                                                stream = new_stream.fuse();
                                                pending = FuturesOrdered::new();
                                                inflight = 0;
                                                stream_closed = false;
                                                continue;
                                            }
                                            Ok(None) => {
                                                metrics::front_command(
                                                    self.cluster.as_ref(),
                                                    kind_label,
                                                    true,
                                                );
                                                return Ok(());
                                            }
                                            Err(err) => {
                                                metrics::front_command(
                                                    self.cluster.as_ref(),
                                                    kind_label,
                                                    false,
                                                );
                                                metrics::front_error(self.cluster.as_ref(), "subscription_proxy");
                                                return Err(err);
                                            }
                                        }
                                    }
                                    if let Some(response) = self.try_handle_config(&cmd).await {
                                        let kind_label = cmd.kind_label();
                                        let success = !response.is_error();
                                        metrics::front_command(
                                            self.cluster.as_ref(),
                                            kind_label,
                                            success,
                                        );
                                        let fut = async move { (response, None) };
                                        pending.push_back(Box::pin(fut));
                                        inflight += 1;
                                        continue;
                                    }
                                    if let Some(response) = self.try_handle_info(&cmd) {
                                        metrics::front_command(
                                            self.cluster.as_ref(),
                                            cmd.kind_label(),
                                            true,
                                        );
                                        let fut = async move { (response, None) };
                                        pending.push_back(Box::pin(fut));
                                        inflight += 1;
                                        continue;
                                    }
                                    if let Some(response) = self.try_handle_hotkey(&cmd) {
                                        let success = !response.is_error();
                                        metrics::front_command(
                                            self.cluster.as_ref(),
                                            cmd.kind_label(),
                                            success,
                                        );
                                        let fut = async move { (response, None) };
                                        pending.push_back(Box::pin(fut));
                                        inflight += 1;
                                        continue;
                                    }
                                    if let Some(response) = self.try_handle_slowlog(&cmd) {
                                        let success = !response.is_error();
                                        metrics::front_command(
                                            self.cluster.as_ref(),
                                            cmd.kind_label(),
                                            success,
                                        );
                                        let fut = async move { (response, None) };
                                        pending.push_back(Box::pin(fut));
                                        inflight += 1;
                                        continue;
                                    }
                                    if let Some(hit) = self.client_cache.lookup(&cmd) {
                                        self.hotkey.record_command(&cmd);
                                        metrics::front_command(
                                            self.cluster.as_ref(),
                                            cmd.kind_label(),
                                            true,
                                        );
                                        let fut = async move { (hit, None) };
                                        pending.push_back(Box::pin(fut));
                                        inflight += 1;
                                        continue;
                                    }
                                    let requested_version = cmd.resp_version_request();
                                    let guard = self.prepare_dispatch(client_id, cmd);
                                    let fut = async move {
                                        let resp = guard.await;
                                        let version = if !resp.is_error() {
                                            requested_version
                                        } else {
                                            None
                                        };
                                        (resp, version)
                                    };
                                    pending.push_back(Box::pin(fut));
                                    inflight += 1;
                                }
                                Err(err) => {
                                    metrics::global_error_incr();
                                    metrics::front_error(self.cluster.as_ref(), "parse");
                                    metrics::front_command(self.cluster.as_ref(), "invalid", false);
                                    let message = Bytes::from(format!("ERR {err}"));
                                    let fut = async move { (RespValue::Error(message), None) };
                                    pending.push_back(Box::pin(fut));
                                    inflight += 1;
                                }
                            }
                        }
                        Some(Err(err)) => {
                            metrics::global_error_incr();
                            metrics::front_error(self.cluster.as_ref(), "front_stream");
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

        while let Some((resp, version)) = pending.next().await {
            if let Some(version) = version {
                codec_handle.set_version(version);
                _resp3_negotiated = version == RespVersion::Resp3;
            }
            sink.send(resp).await?;
        }
        sink.close().await?;
        Ok(())
    }

    async fn try_handle_config(&self, command: &RedisCommand) -> Option<RespValue> {
        self.config_manager.handle_command(command).await
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
            mode: ProxyMode::Cluster,
            listen_port: self.listen_port,
            backend_nodes: self.seed_nodes,
        };
        let payload = crate::info::render_info(context, section.as_deref());
        Some(RespValue::BulkString(payload))
    }

    async fn run_subscription(
        &self,
        parts: FramedParts<TcpStream, RespCodec>,
        command: RedisCommand,
        initial_slot: u16,
    ) -> Result<Option<FramedParts<TcpStream, RespCodec>>> {
        let hash_tag = self.hash_tag.as_deref();
        let mut slot = Some(initial_slot);
        let mut current_node = select_node_for_slot(&self.slots, false, initial_slot)?;
        let mut backend = self.open_backend_stream(&current_node).await?;

        let mut pending: VecDeque<PendingSubscription> = VecDeque::new();
        let mut channels: HashSet<Bytes> = HashSet::new();
        let mut patterns: HashSet<Bytes> = HashSet::new();
        metrics::subscription_active(self.cluster.as_ref(), "channel", 0);
        metrics::subscription_active(self.cluster.as_ref(), "pattern", 0);

        let ack_expected = std::cmp::max(
            1,
            expected_ack_count(&command, channels.len(), patterns.len()),
        );
        backend.send(command.to_resp()).await?;
        pending.push_back(PendingSubscription {
            command,
            ack_remaining: ack_expected,
        });

        let front = Framed::from_parts(parts);
        let (mut front_sink, mut front_stream) = front.split();

        let mut continue_running = true;

        while continue_running {
            tokio::select! {
                backend_msg = backend.next() => {
                    match backend_msg {
                        Some(Ok(resp)) => {
                            if let Some(redirect) = parse_redirect(resp.clone())? {
                                let (next_address, next_slot, needs_asking) = match redirect {
                                    Redirect::Moved { slot: new_slot, address } => {
                                        let _ = self.fetch_trigger.send(());
                                        (address, Some(new_slot), false)
                                    }
                                    Redirect::Ask { address } => (address, None, true),
                                };

                                if let Some(new_slot) = next_slot {
                                    slot = Some(new_slot);
                                }
                                current_node = BackendNode::new(next_address);
                                let mut new_backend =
                                    self.open_backend_stream(&current_node).await?;

                                if needs_asking {
                                    let asking =
                                        RedisCommand::new(vec![Bytes::from_static(b"ASKING")])?;
                                    new_backend.send(asking.to_resp()).await?;
                                }

                                let mut new_pending: VecDeque<PendingSubscription> =
                                    VecDeque::new();

                                if !channels.is_empty() {
                                    let mut parts = Vec::with_capacity(channels.len() + 1);
                                    parts.push(Bytes::from_static(b"SUBSCRIBE"));
                                    for name in channels.iter() {
                                        parts.push(name.clone());
                                    }
                                    let resub = RedisCommand::new(parts)?;
                                    new_backend.send(resub.to_resp()).await?;
                                    new_pending.push_back(PendingSubscription {
                                        command: resub,
                                        ack_remaining: channels.len(),
                                    });
                                }

                                if !patterns.is_empty() {
                                    let mut parts = Vec::with_capacity(patterns.len() + 1);
                                    parts.push(Bytes::from_static(b"PSUBSCRIBE"));
                                    for name in patterns.iter() {
                                        parts.push(name.clone());
                                    }
                                    let resub = RedisCommand::new(parts)?;
                                    new_backend.send(resub.to_resp()).await?;
                                    new_pending.push_back(PendingSubscription {
                                        command: resub,
                                        ack_remaining: patterns.len(),
                                    });
                                }

                                for entry in pending.iter() {
                                    new_backend
                                        .send(entry.command.to_resp())
                                        .await?;
                                    new_pending.push_back(entry.clone());
                                }

                                backend = new_backend;
                                pending = new_pending;
                                continue;
                            }

                            let is_ack =
                                apply_subscription_membership(&resp, &mut channels, &mut patterns);
                            metrics::subscription_active(
                                self.cluster.as_ref(),
                                "channel",
                                channels.len(),
                            );
                            metrics::subscription_active(
                                self.cluster.as_ref(),
                                "pattern",
                                patterns.len(),
                            );
                            if is_ack {
                                if let Some(front_cmd) = pending.front_mut() {
                                    if front_cmd.ack_remaining > 0 {
                                        front_cmd.ack_remaining -= 1;
                                    }
                                    if front_cmd.ack_remaining == 0 {
                                        pending.pop_front();
                                    }
                                }
                            }

                            if let Some((_kind, count)) = subscription_count(&resp) {
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
                                SubscriptionKind::Channel | SubscriptionKind::Pattern => {
                                    if cmd.args().len() <= 1 {
                                        metrics::global_error_incr();
                                        let command_name = String::from_utf8_lossy(
                                            cmd.command_name(),
                                        )
                                        .to_ascii_lowercase();
                                        let message = Bytes::from(format!(
                                            "ERR wrong number of arguments for '{}' command",
                                            command_name
                                        ));
                                        front_sink.send(RespValue::Error(message)).await?;
                                        metrics::front_error(self.cluster.as_ref(), "subscription_arity");
                                        continue;
                                    }
                                }
                                SubscriptionKind::Unsubscribe | SubscriptionKind::Punsub => {}
                                SubscriptionKind::None => {
                                    metrics::global_error_incr();
                                    front_sink
                                        .send(RespValue::Error(Bytes::from_static(
                                            b"ERR only subscribe/unsubscribe allowed in subscription mode",
                                        )))
                                        .await?;
                                    metrics::front_error(self.cluster.as_ref(), "subscription_mode");
                                    continue;
                                }
                            }

                            let resolved_slot = match subscription_slot_for_command(
                                &cmd,
                                hash_tag,
                                slot,
                            ) {
                                Ok(value) => value,
                                Err(_) => {
                                    metrics::global_error_incr();
                                    front_sink
                                        .send(RespValue::Error(Bytes::from_static(
                                            b"ERR subscription channels must hash to the same slot",
                                        )))
                                        .await?;
                                    metrics::front_error(self.cluster.as_ref(), "subscription_slot");
                                    continue;
                                }
                            };
                            if let Some(resolved) = resolved_slot {
                                if let Some(current) = slot {
                                    if current != resolved {
                                        metrics::global_error_incr();
                                        front_sink
                                            .send(RespValue::Error(Bytes::from_static(
                                                b"ERR subscription channels must hash to the same slot",
                                            )))
                                            .await?;
                                        metrics::front_error(self.cluster.as_ref(), "subscription_slot");
                                        continue;
                                    }
                                } else {
                                    slot = Some(resolved);
                                }
                            }

                            let ack_expected = std::cmp::max(
                                1,
                                expected_ack_count(&cmd, channels.len(), patterns.len()),
                            );
                            backend.send(cmd.to_resp()).await?;
                            pending.push_back(PendingSubscription {
                                command: cmd,
                                ack_remaining: ack_expected,
                            });
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
        metrics::subscription_active(self.cluster.as_ref(), "channel", channels.len());
        metrics::subscription_active(self.cluster.as_ref(), "pattern", patterns.len());
        Ok(Some(front.into_parts()))
    }

    async fn open_backend_stream(
        &self,
        node: &BackendNode,
    ) -> Result<Framed<TcpStream, RespCodec>> {
        let addr = node.as_str().to_string();
        let timeout_duration = self.runtime.request_timeout(REQUEST_TIMEOUT_MS);
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

    fn prepare_dispatch(
        &self,
        client_id: ClientId,
        command: RedisCommand,
    ) -> BoxFuture<'static, RespValue> {
        let hash_tag = self.hash_tag.clone();
        let read_from_slave = self.read_from_slave;
        let slots = self.slots.clone();
        let pool = self.pool.clone();
        let fetch_trigger = self.fetch_trigger.clone();
        let cluster = self.cluster.clone();
        let slowlog = self.slowlog.clone();
        let hotkey = self.hotkey.clone();
        let backup = self.backup.clone();
        let kind_label = command.kind_label();
        let cache = self.client_cache.clone();
        Box::pin(async move {
            let cache_candidate = command.clone();
            let cacheable_read = ClientCache::is_cacheable_read(&cache_candidate);
            let invalidating_write = ClientCache::is_invalidating_write(&cache_candidate);
            match dispatch_with_context(
                hash_tag,
                read_from_slave,
                slots,
                pool,
                fetch_trigger,
                client_id,
                slowlog,
                hotkey,
                backup,
                cluster.clone(),
                command,
            )
            .await
            {
                Ok(resp) => {
                    let success = !matches!(resp, RespValue::Error(_));
                    if success && cacheable_read {
                        cache.store(&cache_candidate, &resp);
                    }
                    if invalidating_write {
                        cache.invalidate_command(&cache_candidate);
                    }
                    metrics::front_command(cluster.as_ref(), kind_label, success);
                    resp
                }
                Err(err) => {
                    if invalidating_write {
                        cache.invalidate_command(&cache_candidate);
                    }
                    metrics::global_error_incr();
                    metrics::front_command(cluster.as_ref(), kind_label, false);
                    metrics::front_error(cluster.as_ref(), "dispatch");
                    RespValue::Error(Bytes::from(format!("ERR {err}")))
                }
            }
        })
    }
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

fn subscription_slot_for_command(
    command: &RedisCommand,
    hash_tag: Option<&[u8]>,
    current_slot: Option<u16>,
) -> Result<Option<u16>> {
    match command.as_subscription() {
        SubscriptionKind::Channel | SubscriptionKind::Pattern => {
            let slot = derive_slot_from_args(&command.args()[1..], hash_tag)?;
            if let (Some(existing), Some(candidate)) = (current_slot, slot) {
                if existing != candidate {
                    bail!("channel arguments span multiple slots");
                }
            }
            Ok(slot.or(current_slot))
        }
        SubscriptionKind::Unsubscribe | SubscriptionKind::Punsub => {
            if command.args().len() > 1 {
                let slot = derive_slot_from_args(&command.args()[1..], hash_tag)?;
                if let (Some(existing), Some(candidate)) = (current_slot, slot) {
                    if existing != candidate {
                        bail!("unsubscribe arguments span multiple slots");
                    }
                }
                Ok(slot.or(current_slot))
            } else {
                Ok(current_slot)
            }
        }
        SubscriptionKind::None => Ok(current_slot),
    }
}

fn derive_slot_from_args(args: &[Bytes], hash_tag: Option<&[u8]>) -> Result<Option<u16>> {
    let mut slot: Option<u16> = None;
    for arg in args {
        let trimmed = trim_hash_tag(arg.as_ref(), hash_tag);
        let candidate = crc16(trimmed) % SLOT_COUNT;
        if let Some(existing) = slot {
            if existing != candidate {
                bail!("arguments map to different hash slots");
            }
        } else {
            slot = Some(candidate);
        }
    }
    Ok(slot)
}

fn expected_ack_count(
    command: &RedisCommand,
    subscribed_channels: usize,
    subscribed_patterns: usize,
) -> usize {
    let args_len = command.args().len();
    match command.as_subscription() {
        SubscriptionKind::Channel | SubscriptionKind::Pattern => args_len.saturating_sub(1),
        SubscriptionKind::Unsubscribe => {
            if args_len > 1 {
                args_len - 1
            } else {
                subscribed_channels.max(1)
            }
        }
        SubscriptionKind::Punsub => {
            if args_len > 1 {
                args_len - 1
            } else {
                subscribed_patterns.max(1)
            }
        }
        SubscriptionKind::None => 0,
    }
}

fn apply_subscription_membership(
    resp: &RespValue,
    channels: &mut HashSet<Bytes>,
    patterns: &mut HashSet<Bytes>,
) -> bool {
    let items = match resp {
        RespValue::Array(items) if items.len() >= 3 => items,
        _ => return false,
    };

    let action = match &items[0] {
        RespValue::SimpleString(value) | RespValue::BulkString(value) => value.as_ref(),
        _ => return false,
    };

    let name = resp_value_to_bytes(&items[1]);

    match action {
        b"subscribe" => {
            if let Some(channel) = name {
                channels.insert(channel);
            }
            true
        }
        b"unsubscribe" => {
            if let Some(channel) = name {
                channels.remove(&channel);
            }
            true
        }
        b"psubscribe" => {
            if let Some(pattern) = name {
                patterns.insert(pattern);
            }
            true
        }
        b"punsubscribe" => {
            if let Some(pattern) = name {
                patterns.remove(&pattern);
            }
            true
        }
        _ => false,
    }
}

fn resp_value_to_bytes(value: &RespValue) -> Option<Bytes> {
    match value {
        RespValue::BulkString(data) | RespValue::SimpleString(data) => Some(data.clone()),
        RespValue::NullBulk => None,
        _ => None,
    }
}

#[derive(Clone)]
struct ClusterConnector {
    runtime: Arc<ClusterRuntime>,
    default_timeout_ms: u64,
    backend_auth: Option<BackendAuth>,
    heartbeat_interval: Duration,
    reconnect_base_delay: Duration,
    max_reconnect_attempts: usize,
    backend_resp_version: RespVersion,
}

impl ClusterConnector {
    fn new(
        runtime: Arc<ClusterRuntime>,
        default_timeout_ms: u64,
        backend_auth: Option<BackendAuth>,
        backend_resp_version: RespVersion,
    ) -> Self {
        Self {
            runtime,
            default_timeout_ms,
            backend_auth,
            heartbeat_interval: Duration::from_secs(30),
            reconnect_base_delay: Duration::from_millis(50),
            max_reconnect_attempts: 3,
            backend_resp_version,
        }
    }

    async fn open_stream(&self, address: &str) -> Result<Framed<TcpStream, RespCodec>> {
        let timeout_duration = self.current_timeout();
        let stream = timeout(timeout_duration, TcpStream::connect(address))
            .await
            .with_context(|| format!("connection to {} timed out", address))??;
        stream
            .set_nodelay(true)
            .with_context(|| format!("failed to set TCP_NODELAY for {}", address))?;
        #[cfg(any(unix, windows))]
        {
            let keepalive = TcpKeepalive::new()
                .with_time(self.heartbeat_interval)
                .with_interval(self.heartbeat_interval);
            if let Err(err) = SockRef::from(&stream).set_tcp_keepalive(&keepalive) {
                warn!(
                    backend = %address,
                    error = %err,
                    "failed to enable backend TCP keepalive"
                );
            }
        }
        let mut framed = Framed::new(stream, RespCodec::default());
        if let Some(auth) = &self.backend_auth {
            auth.apply_to_stream(&mut framed, timeout_duration, address)
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

    async fn execute(
        &self,
        framed: &mut Framed<TcpStream, RespCodec>,
        command: RedisCommand,
    ) -> Result<RespValue> {
        let requested_version = command.resp_version_request();
        let blocking = command.as_blocking();
        if let Ok(name) = std::str::from_utf8(command.command_name()) {
            if name.eq_ignore_ascii_case("blpop") || name.eq_ignore_ascii_case("brpop") {
                info!(blocking = ?blocking, "cluster connector executing blocking candidate {name}");
            }
        }
        let timeout_duration = self.current_timeout();
        timeout(timeout_duration, framed.send(command.to_resp()))
            .await
            .context("timed out sending command")??;

        let response = match blocking {
            BlockingKind::Queue { .. } | BlockingKind::Stream { .. } => match framed.next().await {
                Some(Ok(value)) => Ok(value),
                Some(Err(err)) => Err(err.into()),
                None => Err(anyhow!("backend closed connection")),
            },
            BlockingKind::None => match timeout(timeout_duration, framed.next()).await {
                Ok(Some(Ok(value))) => Ok(value),
                Ok(Some(Err(err))) => Err(err.into()),
                Ok(None) => Err(anyhow!("backend closed connection")),
                Err(_) => Err(anyhow!("timed out waiting for response")),
            },
        }?;

        if let Some(version) = requested_version {
            if !response.is_error() {
                framed.codec_mut().set_version(version);
            }
        }
        Ok(response)
    }

    async fn connect_with_retry(
        &self,
        node: &BackendNode,
        cluster: &str,
    ) -> Result<Framed<TcpStream, RespCodec>> {
        let mut last_error: Option<anyhow::Error> = None;
        for attempt in 0..self.max_reconnect_attempts {
            let attempt_start = Instant::now();
            match self.open_stream(node.as_str()).await {
                Ok(mut stream) => {
                    metrics::backend_probe_duration(
                        cluster,
                        node.as_str(),
                        "connect",
                        attempt_start.elapsed(),
                    );
                    metrics::backend_probe_result(cluster, node.as_str(), "connect", true);
                    match self
                        .negotiate_resp_version(cluster, node, &mut stream)
                        .await
                    {
                        Ok(_) => return Ok(stream),
                        Err(err) => {
                            warn!(
                                cluster = %cluster,
                                backend = %node.as_str(),
                                attempt = attempt + 1,
                                error = %err,
                                "failed to negotiate RESP version with backend"
                            );
                            last_error = Some(err);
                            if attempt + 1 < self.max_reconnect_attempts {
                                sleep(self.reconnect_base_delay).await;
                            }
                        }
                    }
                }
                Err(err) => {
                    let elapsed = attempt_start.elapsed();
                    metrics::backend_probe_duration(cluster, node.as_str(), "connect", elapsed);
                    metrics::backend_probe_result(cluster, node.as_str(), "connect", false);
                    metrics::backend_error(cluster, node.as_str(), "connect");
                    warn!(
                        cluster = %cluster,
                        backend = %node.as_str(),
                        attempt = attempt + 1,
                        error = %err,
                        "failed to connect backend"
                    );
                    last_error = Some(err);
                    if attempt + 1 < self.max_reconnect_attempts {
                        sleep(self.reconnect_base_delay).await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("unable to connect backend")))
    }

    async fn heartbeat(&self, framed: &mut Framed<TcpStream, RespCodec>) -> Result<()> {
        use RespValue::{Array, BulkString, SimpleString};

        let ping = Array(vec![BulkString(Bytes::from_static(b"PING"))]);
        let timeout_duration = self.current_timeout();
        timeout(timeout_duration, framed.send(ping))
            .await
            .context("timed out sending heartbeat")??;

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

    fn current_timeout(&self) -> Duration {
        self.runtime.request_timeout(self.default_timeout_ms)
    }

    fn slow_response_threshold(&self) -> Duration {
        self.current_timeout()
            .checked_mul(3)
            .unwrap_or_else(|| Duration::from_secs(5))
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
        info!(
            cluster = %cluster,
            backend = %node.as_str(),
            "starting cluster backend session"
        );
        let mut connection: Option<Framed<TcpStream, RespCodec>> = None;
        let mut heartbeat = interval(self.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;

                cmd_opt = rx.recv() => {
                    let cmd = match cmd_opt {
                        Some(cmd) => cmd,
                        None => break,
                    };

                    if connection.is_none() {
                        match self.connect_with_retry(&node, &cluster).await {
                            Ok(stream) => {
                                connection = Some(stream);
                            }
                            Err(err) => {
                                let _ = cmd.respond_to.send(Err(err));
                                continue;
                            }
                        }
                    }

                    if let Some(ref mut framed) = connection {
                        let slow_threshold = self.slow_response_threshold();
                        let started = Instant::now();
                        let result = self.execute(framed, cmd.request).await;
                        let elapsed = started.elapsed();
                        let is_slow = elapsed > slow_threshold;

                        let mut should_drop = false;
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
                                        "cluster backend response slow"
                                    );
                                    result_label = "slow";
                                    should_drop = true;
                                }
                                metrics::backend_request_result(
                                    &cluster,
                                    node.as_str(),
                                    result_label,
                                );
                                if cmd.respond_to.send(Ok(resp)).is_err() {
                                    debug!(
                                        cluster = %cluster,
                                        backend = %node.as_str(),
                                        "cluster backend response dropped (client closed)"
                                    );
                                }
                            }
                            Err(err) => {
                                let kind = classify_backend_error(&err);
                                metrics::backend_request_result(&cluster, node.as_str(), kind);
                                metrics::backend_error(&cluster, node.as_str(), kind);
                                let _ = cmd.respond_to.send(Err(err));
                                should_drop = true;
                            }
                        }

                        if should_drop {
                            connection = None;
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
                            }
                            Err(err) => {
                                metrics::backend_error(&cluster, node.as_str(), "heartbeat");
                                metrics::backend_heartbeat(&cluster, node.as_str(), false);
                                warn!(
                                    cluster = %cluster,
                                    backend = %node.as_str(),
                                    error = %err,
                                    "cluster backend heartbeat failed"
                                );
                                connection = None;
                            }
                        }
                    }
                }
            }
        }

        debug!(
            cluster = %cluster,
            backend = %node.as_str(),
            "cluster backend session terminated"
        );
    }
}

fn classify_backend_error(err: &anyhow::Error) -> &'static str {
    let message = err.to_string();
    if message.contains("timed out") {
        "timeout"
    } else if message.contains("closed connection") {
        "closed"
    } else if message.contains("unexpected heartbeat reply") {
        "protocol"
    } else {
        "execute"
    }
}

async fn fetch_topology(
    cluster: Arc<str>,
    seeds: Vec<String>,
    connector: Arc<ClusterConnector>,
    slots: Arc<watch::Sender<SlotMap>>,
    mut trigger: mpsc::UnboundedReceiver<()>,
    tracker: Option<Arc<CacheTrackerSet>>,
) {
    let mut ticker = tokio::time::interval(FETCH_INTERVAL);
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            _ = trigger.recv() => {},
        }

        if let Err(err) = fetch_once(
            &cluster,
            &seeds,
            connector.clone(),
            slots.clone(),
            tracker.clone(),
        )
        .await
        {
            warn!(cluster = %cluster, error = %err, "failed to refresh cluster topology");
        }
    }
}

async fn fetch_once(
    cluster: &Arc<str>,
    seeds: &[String],
    connector: Arc<ClusterConnector>,
    slots: Arc<watch::Sender<SlotMap>>,
    tracker: Option<Arc<CacheTrackerSet>>,
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
                if let Some(ref watchers) = tracker {
                    watchers.set_nodes(map.all_nodes());
                }
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
    use crate::config::BackupRequestConfig;
    use crate::utils::{crc16, trim_hash_tag};
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;

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

    #[test]
    fn parse_redirect_handles_ask() {
        let value = RespValue::Error(Bytes::from_static(b"ASK 3999 10.0.0.1:6381"));
        let redirect = parse_redirect(value).unwrap().unwrap();
        match redirect {
            Redirect::Ask { address } => assert_eq!(address, "10.0.0.1:6381"),
            other => panic!("unexpected redirect: {:?}", other),
        }
    }

    #[test]
    fn classify_backend_error_detects_categories() {
        assert_eq!(
            classify_backend_error(&anyhow!("timed out waiting")),
            "timeout"
        );
        assert_eq!(
            classify_backend_error(&anyhow!("closed connection")),
            "closed"
        );
        assert_eq!(
            classify_backend_error(&anyhow!("unexpected heartbeat reply")),
            "protocol"
        );
        assert_eq!(classify_backend_error(&anyhow!("other")), "execute");
    }

    #[test]
    fn subscription_slot_tracks_hash_slot() {
        let command = RedisCommand::new(vec![
            Bytes::from_static(b"SUBSCRIBE"),
            Bytes::from_static(b"channel"),
        ])
        .unwrap();
        let slot = subscription_slot_for_command(&command, None, None)
            .unwrap()
            .unwrap();
        let expected = crc16(trim_hash_tag(b"channel", None)) % SLOT_COUNT;
        assert_eq!(slot, expected);

        let unsubscribe = RedisCommand::new(vec![Bytes::from_static(b"UNSUBSCRIBE")]).unwrap();
        let current = subscription_slot_for_command(&unsubscribe, None, Some(slot))
            .unwrap()
            .unwrap();
        assert_eq!(current, slot);
    }

    #[test]
    fn subscription_slot_errors_on_conflicting_channels() {
        let command = RedisCommand::new(vec![
            Bytes::from_static(b"SUBSCRIBE"),
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
        ])
        .unwrap();
        assert!(subscription_slot_for_command(&command, None, None).is_err());
    }

    #[test]
    fn expected_ack_count_matches_subscription_kind() {
        let subscribe = RedisCommand::new(vec![
            Bytes::from_static(b"SUBSCRIBE"),
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
        ])
        .unwrap();
        assert_eq!(expected_ack_count(&subscribe, 0, 0), 2);

        let unsubscribe = RedisCommand::new(vec![Bytes::from_static(b"UNSUBSCRIBE")]).unwrap();
        assert_eq!(expected_ack_count(&unsubscribe, 3, 1), 3);
    }

    #[test]
    fn apply_subscription_membership_updates_sets() {
        let mut channels = HashSet::new();
        let mut patterns = HashSet::new();
        let resp = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"subscribe")),
            RespValue::BulkString(Bytes::from_static(b"chan")),
            RespValue::Integer(1),
        ]);
        assert!(apply_subscription_membership(
            &resp,
            &mut channels,
            &mut patterns
        ));
        assert!(channels.contains(&Bytes::from_static(b"chan")));

        let resp = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"unsubscribe")),
            RespValue::BulkString(Bytes::from_static(b"chan")),
            RespValue::Integer(0),
        ]);
        assert!(apply_subscription_membership(
            &resp,
            &mut channels,
            &mut patterns
        ));
        assert!(channels.is_empty());
    }

    #[test]
    fn derive_slot_from_args_returns_consistent_value() {
        let args = vec![Bytes::from_static(b"foo"), Bytes::from_static(b"foo")];
        let slot = derive_slot_from_args(&args, None).unwrap().unwrap();
        let expected = crc16(trim_hash_tag(b"foo", None)) % SLOT_COUNT;
        assert_eq!(slot, expected);
    }

    #[test]
    fn derive_slot_from_args_fails_for_mixed_slots() {
        let args = vec![Bytes::from_static(b"foo"), Bytes::from_static(b"bar")];
        assert!(derive_slot_from_args(&args, None).is_err());
    }

    #[test]
    fn resp_value_to_bytes_handles_bulk_and_null() {
        let value = RespValue::BulkString(Bytes::from_static(b"key"));
        assert_eq!(
            resp_value_to_bytes(&value),
            Some(Bytes::from_static(b"key"))
        );
        assert!(resp_value_to_bytes(&RespValue::NullBulk).is_none());
    }

    #[test]
    fn select_node_for_slot_prefers_replica_when_allowed() {
        let (slots, _rx) = watch::channel(sample_slot_map());
        let replica = select_node_for_slot(&slots, true, 1).expect("replica");
        assert!(replica.as_str().ends_with(":7001"));
        let master = select_node_for_slot(&slots, false, 1).expect("master");
        assert!(master.as_str().ends_with(":7000"));
    }

    #[test]
    fn select_node_for_slot_errors_when_slot_missing() {
        let (slots, _rx) = watch::channel(SlotMap::new());
        assert!(select_node_for_slot(&slots, false, 42).is_err());
        assert!(replica_node_for_slot(&slots, 42).is_none());
    }

    #[test]
    fn backup_controller_computes_delays_and_averages() {
        let runtime = Arc::new(BackupRequestRuntime::new_for_test(BackupRequestConfig {
            enabled: true,
            trigger_slow_ms: Some(50),
            multiplier: 0.5,
        }));
        let controller = BackupRequestController::new(runtime.clone());
        let master = BackendNode::new("127.0.0.1:7000".to_string());
        let replica = BackendNode::new("127.0.0.1:7001".to_string());
        let plan = controller
            .plan(&master, Some(replica.clone()))
            .expect("plan available");
        assert_eq!(plan.replica.as_str(), replica.as_str());
        assert_eq!(plan.delay, Duration::from_millis(50));

        controller.record_primary(&master, Duration::from_millis(4));
        let avg = controller.average_for(&master).expect("average recorded");
        assert!(avg > 0.0);

        runtime.set_threshold_ms(None);
        runtime.set_multiplier(2.0);
        let delay = controller.delay_for(&master).expect("delay");
        assert!(delay >= Duration::from_micros(1));
    }

    #[test]
    fn subscription_count_parses_string_counts() {
        let resp = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"psubscribe")),
            RespValue::BulkString(Bytes::from_static(b"pattern")),
            RespValue::BulkString(Bytes::from_static(b"5")),
        ]);
        let (kind, count) = subscription_count(&resp).expect("count");
        assert_eq!(kind, SubscriptionKind::Pattern);
        assert_eq!(count, 5);

        let invalid = RespValue::Array(vec![RespValue::BulkString(Bytes::from_static(b"foo"))]);
        assert!(subscription_count(&invalid).is_none());
    }

    #[test]
    fn subscription_action_kind_rejects_unknown() {
        assert!(subscription_action_kind(b"foobar").is_none());
    }

    fn sample_slot_map() -> SlotMap {
        SlotMap::from_slots_response(RespValue::Array(vec![RespValue::Array(vec![
            RespValue::Integer(0),
            RespValue::Integer(10),
            endpoint("127.0.0.1", 7000),
            endpoint("127.0.0.1", 7001),
        ])]))
        .expect("slot map")
    }

    fn endpoint(host: &str, port: i64) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::copy_from_slice(host.as_bytes())),
            RespValue::Integer(port),
        ])
    }
}
#[derive(Debug)]
enum Redirect {
    Moved { slot: u16, address: String },
    Ask { address: String },
}

async fn dispatch_with_context(
    hash_tag: Option<Vec<u8>>,
    read_from_slave: bool,
    slots: Arc<watch::Sender<SlotMap>>,
    pool: Arc<ConnectionPool<RedisCommand>>,
    fetch_trigger: mpsc::UnboundedSender<()>,
    client_id: ClientId,
    slowlog: Arc<Slowlog>,
    hotkey: Arc<Hotkey>,
    backup: Arc<BackupRequestController>,
    cluster: Arc<str>,
    command: RedisCommand,
) -> Result<RespValue> {
    let command_snapshot = command.clone();
    let multi = command.expand_for_multi(hash_tag.as_deref());
    let started = Instant::now();
    let result = if let Some(multi) = multi {
        dispatch_multi(
            hash_tag,
            read_from_slave,
            slots,
            pool,
            fetch_trigger,
            client_id,
            backup,
            cluster,
            multi,
        )
        .await
    } else {
        dispatch_single(
            hash_tag,
            read_from_slave,
            slots,
            pool,
            fetch_trigger,
            client_id,
            backup,
            cluster,
            command,
        )
        .await
    };
    slowlog.maybe_record(&command_snapshot, started.elapsed());
    hotkey.record_command(&command_snapshot);
    result
}

async fn dispatch_multi(
    hash_tag: Option<Vec<u8>>,
    read_from_slave: bool,
    slots: Arc<watch::Sender<SlotMap>>,
    pool: Arc<ConnectionPool<RedisCommand>>,
    fetch_trigger: mpsc::UnboundedSender<()>,
    client_id: ClientId,
    backup: Arc<BackupRequestController>,
    cluster: Arc<str>,
    multi: MultiDispatch,
) -> Result<RespValue> {
    let mut tasks: FuturesOrdered<BoxFuture<'static, Result<SubResponse>>> = FuturesOrdered::new();
    for sub in multi.subcommands.into_iter() {
        let hash_tag = hash_tag.clone();
        let slots = slots.clone();
        let pool = pool.clone();
        let fetch_trigger = fetch_trigger.clone();
        let backup = backup.clone();
        let cluster = cluster.clone();
        let SubCommand { positions, command } = sub;
        tasks.push_back(Box::pin(async move {
            let response = dispatch_single(
                hash_tag,
                read_from_slave,
                slots,
                pool,
                fetch_trigger,
                client_id,
                backup,
                cluster,
                command,
            )
            .await?;
            Ok(SubResponse {
                positions,
                response,
            })
        }));
    }

    let mut responses = Vec::new();
    while let Some(item) = tasks.next().await {
        responses.push(item?);
    }
    multi.aggregator.combine(responses)
}

async fn dispatch_single(
    hash_tag: Option<Vec<u8>>,
    read_from_slave: bool,
    slots: Arc<watch::Sender<SlotMap>>,
    pool: Arc<ConnectionPool<RedisCommand>>,
    fetch_trigger: mpsc::UnboundedSender<()>,
    client_id: ClientId,
    backup: Arc<BackupRequestController>,
    cluster: Arc<str>,
    command: RedisCommand,
) -> Result<RespValue> {
    let blocking = command.as_blocking();
    let is_read_only = command.is_read_only();
    let mut slot = command
        .hash_slot(hash_tag.as_deref())
        .ok_or_else(|| anyhow!("command missing key"))?;
    let mut target_override: Option<BackendNode> = None;

    for _ in 0..MAX_REDIRECTS {
        let target = if let Some(node) = target_override.clone() {
            node
        } else {
            select_node_for_slot(&slots, read_from_slave, slot)?
        };

        if matches!(
            blocking,
            BlockingKind::Queue { .. } | BlockingKind::Stream { .. }
        ) {
            let mut exclusive = pool.acquire_exclusive(&target);
            let response_rx = exclusive.send(command.clone()).await?;
            match response_rx.await {
                Ok(Ok(resp)) => match parse_redirect(resp.clone())? {
                    Some(Redirect::Moved {
                        slot: new_slot,
                        address,
                    }) => {
                        let _ = fetch_trigger.send(());
                        slot = new_slot;
                        target_override = Some(BackendNode::new(address));
                        drop(exclusive);
                        continue;
                    }
                    Some(Redirect::Ask { address }) => {
                        target_override = Some(BackendNode::new(address));
                        drop(exclusive);
                        continue;
                    }
                    None => return Ok(resp),
                },
                Ok(Err(err)) => return Err(err),
                Err(_) => return Err(anyhow!("backend session closed")),
            }
        } else {
            let backup_plan = if target_override.is_none()
                && !read_from_slave
                && is_read_only
                && matches!(blocking, BlockingKind::None)
            {
                replica_node_for_slot(&slots, slot)
                    .and_then(|replica| backup.plan(&target, Some(replica)))
            } else {
                None
            };
            if backup_plan.is_some() {
                metrics::backup_event(cluster.as_ref(), "planned");
            }

            let resp = execute_with_backup(
                pool.clone(),
                client_id,
                &command,
                target.clone(),
                cluster.clone(),
                backup_plan,
                backup.clone(),
            )
            .await?;

            match parse_redirect(resp.clone())? {
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
            }
        }
    }

    Err(anyhow!("too many cluster redirects"))
}

#[derive(Clone)]
struct BackupPlan {
    replica: BackendNode,
    delay: Duration,
}

async fn execute_with_backup(
    pool: Arc<ConnectionPool<RedisCommand>>,
    client_id: ClientId,
    command: &RedisCommand,
    target: BackendNode,
    cluster: Arc<str>,
    plan: Option<BackupPlan>,
    controller: Arc<BackupRequestController>,
) -> Result<RespValue> {
    let primary_rx = pool
        .dispatch(target.clone(), client_id, command.clone())
        .await?;
    if let Some(plan) = plan {
        race_with_backup(
            pool, client_id, command, target, primary_rx, cluster, plan, controller,
        )
        .await
    } else {
        let started = Instant::now();
        let result = primary_rx.await;
        if result.is_ok() {
            controller.record_primary(&target, started.elapsed());
        }
        result?
    }
}

async fn race_with_backup(
    pool: Arc<ConnectionPool<RedisCommand>>,
    client_id: ClientId,
    command: &RedisCommand,
    master: BackendNode,
    primary_rx: oneshot::Receiver<Result<RespValue>>,
    cluster: Arc<str>,
    plan: BackupPlan,
    controller: Arc<BackupRequestController>,
) -> Result<RespValue> {
    let master_start = Instant::now();
    let mut primary_future = Box::pin(primary_rx);
    let mut delay_future = Box::pin(tokio::time::sleep(plan.delay));

    if let Some(result) = tokio::select! {
        res = primary_future.as_mut() => Some(res),
        _ = delay_future.as_mut() => None,
    } {
        if result.is_ok() {
            controller.record_primary(&master, master_start.elapsed());
        }
        metrics::backup_event(cluster.as_ref(), "primary-before");
        return result?;
    }

    let backup_rx = match pool
        .dispatch(plan.replica.clone(), client_id, command.clone())
        .await
    {
        Ok(rx) => {
            metrics::backup_event(cluster.as_ref(), "dispatched");
            rx
        }
        Err(err) => {
            metrics::backup_event(cluster.as_ref(), "dispatch-fail");
            warn!(
                master = %master.as_str(),
                replica = %plan.replica.as_str(),
                error = %err,
                "failed to dispatch backup request; falling back to primary"
            );
            return await_primary_only(primary_future, controller, master, master_start).await;
        }
    };
    let mut backup_future = Box::pin(backup_rx);

    tokio::select! {
        res = primary_future.as_mut() => {
            if res.is_ok() {
                controller.record_primary(&master, master_start.elapsed());
            }
            metrics::backup_event(cluster.as_ref(), "primary-after");
            res?
        }
        res = backup_future.as_mut() => {
            metrics::backup_event(cluster.as_ref(), "replica-win");
            let remaining = primary_future;
            let controller_clone = controller.clone();
            let master_clone = master.clone();
            tokio::spawn(async move {
                let mut future = remaining;
                if let Ok(Ok(_)) = future.as_mut().await {
                    controller_clone.record_primary(&master_clone, master_start.elapsed());
                }
            });
            res?
        }
    }
}

async fn await_primary_only(
    mut primary_future: Pin<Box<oneshot::Receiver<Result<RespValue>>>>,
    controller: Arc<BackupRequestController>,
    master: BackendNode,
    master_start: Instant,
) -> Result<RespValue> {
    match primary_future.as_mut().await {
        Ok(Ok(resp)) => {
            controller.record_primary(&master, master_start.elapsed());
            Ok(resp)
        }
        Ok(Err(err)) => Err(err),
        Err(_) => Err(anyhow!("backend session closed")),
    }
}

fn select_node_for_slot(
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

fn replica_node_for_slot(slots: &watch::Sender<SlotMap>, slot: u16) -> Option<BackendNode> {
    slots
        .borrow()
        .replica_for_slot(slot)
        .map(|addr| BackendNode::new(addr.to_string()))
}

#[derive(Clone)]
struct BackupRequestController {
    runtime: Arc<BackupRequestRuntime>,
    averages: Arc<RwLock<HashMap<String, LatencyAverage>>>,
}

impl BackupRequestController {
    fn new(runtime: Arc<BackupRequestRuntime>) -> Self {
        Self {
            runtime,
            averages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn plan(&self, master: &BackendNode, replica: Option<BackendNode>) -> Option<BackupPlan> {
        let replica = replica?;
        let delay = self.delay_for(master)?;
        Some(BackupPlan { replica, delay })
    }

    fn record_primary(&self, master: &BackendNode, elapsed: Duration) {
        let micros = elapsed.as_secs_f64() * 1_000_000.0;
        let mut guard = self.averages.write();
        let entry = guard
            .entry(master.as_str().to_string())
            .or_insert_with(LatencyAverage::default);
        entry.update(micros);
    }

    fn delay_for(&self, master: &BackendNode) -> Option<Duration> {
        if !self.runtime.enabled() {
            return None;
        }
        let mut candidates = Vec::new();
        if let Some(ms) = self.runtime.threshold_ms() {
            candidates.push(Duration::from_millis(ms));
        }
        if let Some(avg) = self.average_for(master) {
            let multiplier = self.runtime.multiplier();
            if multiplier > 0.0 {
                let scaled = (avg * multiplier).clamp(0.0, u64::MAX as f64);
                let micros = scaled as u64;
                candidates.push(Duration::from_micros(micros));
            }
        }
        candidates.into_iter().min()
    }

    fn average_for(&self, master: &BackendNode) -> Option<f64> {
        let guard = self.averages.read();
        guard.get(master.as_str()).map(|stats| stats.avg_micros)
    }
}

#[derive(Default, Clone)]
struct LatencyAverage {
    avg_micros: f64,
    samples: u64,
}

impl LatencyAverage {
    fn update(&mut self, sample: f64) {
        self.samples = self.samples.saturating_add(1);
        if self.samples == 1 {
            self.avg_micros = sample;
        } else {
            let count = self.samples as f64;
            self.avg_micros += (sample - self.avg_micros) / count;
        }
    }
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
