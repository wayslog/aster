use std::collections::{HashSet, VecDeque};
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
use tokio_util::codec::{Framed, FramedParts};
use tracing::{debug, info, warn};

use crate::backend::client::{ClientId, FrontConnectionGuard};
use crate::backend::pool::{BackendNode, ConnectionPool, Connector, SessionCommand};
use crate::config::ClusterConfig;
use crate::metrics;
use crate::protocol::redis::{
    BlockingKind, MultiDispatch, RedisCommand, RespCodec, RespValue, SlotMap, SubscriptionKind,
    SLOT_COUNT,
};
use crate::utils::{crc16, trim_hash_tag};

const FETCH_INTERVAL: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT_MS: u64 = 1_000;
const MAX_REDIRECTS: u8 = 5;
const PIPELINE_LIMIT: usize = 32;

#[derive(Clone)]
struct PendingSubscription {
    command: RedisCommand,
    ack_remaining: usize,
}

pub struct ClusterProxy {
    cluster: Arc<str>,
    hash_tag: Option<Vec<u8>>,
    read_from_slave: bool,
    slots: Arc<watch::Sender<SlotMap>>,
    pool: Arc<ConnectionPool<RedisCommand>>,
    fetch_trigger: mpsc::UnboundedSender<()>,
    backend_timeout: Duration,
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
            backend_timeout: Duration::from_millis(timeout_ms),
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
                                    if matches!(cmd.as_subscription(), SubscriptionKind::Channel | SubscriptionKind::Pattern) {
                                        if cmd.args().len() <= 1 {
                                            metrics::global_error_incr();
                                            let command_name = String::from_utf8_lossy(cmd.command_name()).to_ascii_lowercase();
                                            let message = Bytes::from(format!(
                                                "ERR wrong number of arguments for '{}' command",
                                                command_name
                                            ));
                                            sink.send(RespValue::Error(message)).await?;
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
                                                continue;
                                            }
                                            Err(_) => {
                                                metrics::global_error_incr();
                                                sink.send(RespValue::Error(Bytes::from_static(
                                                    b"ERR subscription channels must hash to the same slot",
                                                )))
                                                .await?;
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
                                        match self
                                            .run_subscription(
                                                framed_combined.into_parts(),
                                                cmd,
                                                initial_slot,
                                            )
                                            .await?
                                        {
                                            Some(parts) => {
                                                let framed_new = Framed::from_parts(parts);
                                                let (new_sink, new_stream) = framed_new.split();
                                                sink = new_sink;
                                                stream = new_stream.fuse();
                                                pending = FuturesOrdered::new();
                                                inflight = 0;
                                                stream_closed = false;
                                                continue;
                                            }
                                            None => return Ok(()),
                                        }
                                    }
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

                            if let Some(count) = subscription_count(&resp) {
                                front_sink.send(resp.clone()).await?;
                                if count == 0 {
                                    continue_running = false;
                                }
                            } else {
                                front_sink.send(resp).await?;
                            }
                        }
                        Some(Err(err)) => return Err(err.into()),
                        None => return Ok(None),
                    }
                }
                front_msg = front_stream.next() => {
                    match front_msg {
                        Some(Ok(frame)) => {
                            let cmd = match RedisCommand::from_resp(frame) {
                                Ok(cmd) => cmd,
                                Err(err) => {
                                    metrics::global_error_incr();
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
                        Some(Err(err)) => return Err(err.into()),
                        None => return Ok(None),
                    }
                }
            }
        }

        let front = front_sink.reunite(front_stream)?;
        Ok(Some(front.into_parts()))
    }

    async fn open_backend_stream(
        &self,
        node: &BackendNode,
    ) -> Result<Framed<TcpStream, RespCodec>> {
        let addr = node.as_str().to_string();
        let stream = timeout(self.backend_timeout, TcpStream::connect(&addr))
            .await
            .with_context(|| format!("connect to {} timed out", addr))??;
        stream
            .set_nodelay(true)
            .with_context(|| format!("failed to set TCP_NODELAY on {}", addr))?;
        Ok(Framed::new(stream, RespCodec::default()))
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
        Box::pin(async move {
            match dispatch_with_context(
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
        let blocking = command.as_blocking();
        if let Ok(name) = std::str::from_utf8(command.command_name()) {
            if name.eq_ignore_ascii_case("blpop") || name.eq_ignore_ascii_case("brpop") {
                info!(blocking = ?blocking, "cluster connector executing blocking candidate {name}");
            }
        }
        timeout(self.timeout, framed.send(command.to_resp()))
            .await
            .context("timed out sending command")??;

        match blocking {
            BlockingKind::Queue { .. } | BlockingKind::Stream { .. } => match framed.next().await {
                Some(Ok(value)) => Ok(value),
                Some(Err(err)) => Err(err.into()),
                None => Err(anyhow!("backend closed connection")),
            },
            BlockingKind::None => match timeout(self.timeout, framed.next()).await {
                Ok(Some(Ok(value))) => Ok(value),
                Ok(Some(Err(err))) => Err(err.into()),
                Ok(None) => Err(anyhow!("backend closed connection")),
                Err(_) => Err(anyhow!("timed out waiting for response")),
            },
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
    command: RedisCommand,
) -> Result<RespValue> {
    if let Some(multi) = command.expand_for_multi() {
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
    }
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
    let mut tasks: FuturesOrdered<BoxFuture<'static, Result<(usize, RespValue)>>> =
        FuturesOrdered::new();
    for sub in multi.subcommands.into_iter() {
        let hash_tag = hash_tag.clone();
        let slots = slots.clone();
        let pool = pool.clone();
        let fetch_trigger = fetch_trigger.clone();
        let command = sub.command.clone();
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
            Ok((sub.position, response))
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

fn subscription_count(resp: &RespValue) -> Option<i64> {
    if let RespValue::Array(items) = resp {
        if items.len() >= 3 {
            let count = match &items[2] {
                RespValue::Integer(value) => Some(*value),
                RespValue::BulkString(bs) => std::str::from_utf8(bs).ok()?.parse::<i64>().ok(),
                _ => None,
            }?;

            if let RespValue::SimpleString(kind) | RespValue::BulkString(kind) = &items[0] {
                if matches_subscribe_kind(kind.as_ref()) {
                    return Some(count);
                }
            }
        }
    }
    None
}

fn matches_subscribe_kind(kind: &[u8]) -> bool {
    matches!(
        kind,
        b"subscribe" | b"unsubscribe" | b"psubscribe" | b"punsubscribe"
    )
}
