use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
use rand::{seq::SliceRandom, thread_rng};
#[cfg(any(unix, windows))]
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};
use tokio_util::codec::{Framed, FramedParts};
use tracing::{debug, info, warn};

use crate::auth::{AuthAction, BackendAuth, FrontendAuthenticator};
use crate::backend::client::{ClientId, FrontConnectionGuard};
use crate::backend::pool::{BackendNode, ConnectionPool, Connector, SessionCommand};
use crate::config::{ClusterConfig, ClusterRuntime, ConfigManager};
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
    listen_port: u16,
    seed_nodes: usize,
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
            listen_port,
            seed_nodes: config.servers.len(),
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
        let mut pending: FuturesOrdered<BoxFuture<'static, RespValue>> = FuturesOrdered::new();
        let mut pending_versions: VecDeque<Option<RespVersion>> = VecDeque::new();
        let mut inflight = 0usize;
        let mut stream_closed = false;
        let mut auth_state = self.auth.as_ref().map(|auth| auth.new_session());

        loop {
            tokio::select! {
                Some(resp) = pending.next(), if inflight > 0 => {
                    inflight -= 1;
                    let version_hint = pending_versions
                        .pop_front()
                        .unwrap_or(None);
                    let success = !resp.is_error();
                    if success {
                        if let Some(version) = version_hint {
                            codec_handle.set_version(version);
                        }
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
                                                let fut = async move { resp };
                                                pending_versions.push_back(None);
                                                pending.push_back(Box::pin(fut));
                                                inflight += 1;
                                                continue;
                                            }
                                        }
                                    }

                                    let requested_version = cmd.resp_version_request();

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
                                            if let Some(resp) = pending.next().await {
                                                inflight -= 1;
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
                                                pending_versions = VecDeque::new();
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
                                        let fut = async move { response };
                                        pending_versions.push_back(None);
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
                                        let fut = async move { response };
                                        pending_versions.push_back(None);
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
                                        let fut = async move { response };
                                        pending_versions.push_back(None);
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
                                        let fut = async move { response };
                                        pending_versions.push_back(None);
                                        pending.push_back(Box::pin(fut));
                                        inflight += 1;
                                        continue;
                                    }
                                    let guard = self.prepare_dispatch(client_id, cmd);
                                    pending_versions.push_back(requested_version);
                                    pending.push_back(Box::pin(guard));
                                    inflight += 1;
                                }
                                Err(err) => {
                                    metrics::global_error_incr();
                                    metrics::front_error(self.cluster.as_ref(), "parse");
                                    metrics::front_command(self.cluster.as_ref(), "invalid", false);
                                    let message = Bytes::from(format!("ERR {err}"));
                                    let fut = async move { RespValue::Error(message) };
                                    pending_versions.push_back(None);
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

        while let Some(resp) = pending.next().await {
            let version_hint = pending_versions.pop_front().unwrap_or(None);
            if !resp.is_error() {
                if let Some(version) = version_hint {
                    codec_handle.set_version(version);
                }
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
        let kind_label = command.kind_label();
        Box::pin(async move {
            match dispatch_with_context(
                hash_tag,
                read_from_slave,
                slots,
                pool,
                fetch_trigger,
                client_id,
                slowlog,
                hotkey,
                command,
            )
            .await
            {
                Ok(resp) => {
                    let success = !matches!(resp, RespValue::Error(_));
                    metrics::front_command(cluster.as_ref(), kind_label, success);
                    resp
                }
                Err(err) => {
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
}

impl ClusterConnector {
    fn new(
        runtime: Arc<ClusterRuntime>,
        default_timeout_ms: u64,
        backend_auth: Option<BackendAuth>,
    ) -> Self {
        Self {
            runtime,
            default_timeout_ms,
            backend_auth,
            heartbeat_interval: Duration::from_secs(30),
            reconnect_base_delay: Duration::from_millis(50),
            max_reconnect_attempts: 3,
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
                Ok(stream) => {
                    metrics::backend_probe_duration(
                        cluster,
                        node.as_str(),
                        "connect",
                        attempt_start.elapsed(),
                    );
                    metrics::backend_probe_result(cluster, node.as_str(), "connect", true);
                    return Ok(stream);
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
    multi: MultiDispatch,
) -> Result<RespValue> {
    let mut tasks: FuturesOrdered<BoxFuture<'static, Result<SubResponse>>> = FuturesOrdered::new();
    for sub in multi.subcommands.into_iter() {
        let hash_tag = hash_tag.clone();
        let slots = slots.clone();
        let pool = pool.clone();
        let fetch_trigger = fetch_trigger.clone();
        let SubCommand { positions, command } = sub;
        tasks.push_back(Box::pin(async move {
            let response = dispatch_single(
                hash_tag,
                read_from_slave,
                slots,
                pool,
                fetch_trigger,
                client_id,
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
    command: RedisCommand,
) -> Result<RespValue> {
    let blocking = command.as_blocking();
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
    }

    Err(anyhow!("too many cluster redirects"))
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
