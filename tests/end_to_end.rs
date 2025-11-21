use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use libaster::{
    cluster::ClusterProxy,
    config::{Config, ConfigManager},
    protocol::redis::{RespCodec, RespValue, SLOT_COUNT},
    standalone::StandaloneProxy,
    utils::crc16,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, Mutex, RwLock},
    time::{sleep, Duration},
};
use tokio_util::codec::Framed;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_end_to_end_serves_basic_commands() -> Result<()> {
    let backend = match FakeRedisServer::start().await {
        Ok(server) => server,
        Err(err) if permission_denied(&err) => {
            eprintln!("standalone e2e skipped: {err}");
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    let config = render_config(
        "standalone-e2e",
        "redis",
        vec![backend.addr()],
        "127.0.0.1:6500",
    )?;
    let cluster_cfg = config
        .clusters()
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("missing cluster config"))?;
    let manager = Arc::new(ConfigManager::new(
        PathBuf::from("standalone-e2e.toml"),
        &config,
    ));
    let runtime = manager
        .runtime_for(&cluster_cfg.name)
        .context("standalone runtime unavailable")?;
    let proxy = Arc::new(StandaloneProxy::new(&cluster_cfg, runtime, manager)?);

    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("standalone e2e skipped: {err}");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let addr = listener.local_addr().unwrap();
    let proxy_task = {
        let proxy = proxy.clone();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            proxy.handle_connection(socket).await
        })
    };

    let mut client = Framed::new(
        TcpStream::connect(addr).await.context("connect to proxy")?,
        RespCodec::default(),
    );

    assert_eq!(
        send_command(&mut client, vec![&b"PING"[..]]).await?,
        RespValue::SimpleString(Bytes::from_static(b"PONG"))
    );
    assert_eq!(
        send_command(&mut client, vec![&b"SET"[..], &b"foo"[..], &b"bar"[..]]).await?,
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    );
    assert_eq!(
        send_command(&mut client, vec![&b"GET"[..], &b"foo"[..]]).await?,
        RespValue::BulkString(Bytes::from_static(b"bar"))
    );
    let multi = send_command(
        &mut client,
        vec![&b"MGET"[..], &b"foo"[..], &b"missing"[..]],
    )
    .await?;
    assert_eq!(
        multi,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"bar")),
            RespValue::NullBulk
        ])
    );

    drop(client);
    proxy_task.await??;
    backend.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_end_to_end_handles_cross_slot_requests() -> Result<()> {
    let server_a = match FakeRedisServer::start().await {
        Ok(server) => server,
        Err(err) if permission_denied(&err) => {
            eprintln!("cluster e2e skipped: {err}");
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    let server_b = match FakeRedisServer::start().await {
        Ok(server) => server,
        Err(err) if permission_denied(&err) => {
            eprintln!("cluster e2e skipped: {err}");
            server_a.shutdown().await;
            return Ok(());
        }
        Err(err) => {
            server_a.shutdown().await;
            return Err(err);
        }
    };

    let layout = cluster_slots_for(&[server_a.addr(), server_b.addr()]);
    server_a.set_cluster_slots(layout.clone()).await;
    server_b.set_cluster_slots(layout).await;

    let config = render_config(
        "cluster-e2e",
        "redis_cluster",
        vec![server_a.addr(), server_b.addr()],
        "127.0.0.1:6600",
    )?;
    let cluster_cfg = config
        .clusters()
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("missing cluster config"))?;
    let manager = Arc::new(ConfigManager::new(
        PathBuf::from("cluster-e2e.toml"),
        &config,
    ));
    let runtime = manager
        .runtime_for(&cluster_cfg.name)
        .context("cluster runtime unavailable")?;
    let proxy = Arc::new(
        ClusterProxy::new(&cluster_cfg, runtime, manager.clone())
            .await
            .context("build cluster proxy")?,
    );

    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("cluster e2e skipped: {err}");
            server_a.shutdown().await;
            server_b.shutdown().await;
            return Ok(());
        }
        Err(err) => {
            server_a.shutdown().await;
            server_b.shutdown().await;
            return Err(err.into());
        }
    };
    let addr = listener.local_addr().unwrap();
    let proxy_task = {
        let proxy = proxy.clone();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            proxy.handle_connection(socket).await
        })
    };

    let mut client = Framed::new(
        TcpStream::connect(addr)
            .await
            .context("connect to cluster proxy")?,
        RespCodec::default(),
    );
    let midpoint = SLOT_COUNT / 2;
    let key_a = key_for_slot(0..=midpoint - 1);
    let key_b = key_for_slot(midpoint..=SLOT_COUNT - 1);
    wait_for_cluster_ready(&mut client, key_a.as_bytes()).await?;
    wait_for_cluster_ready(&mut client, key_b.as_bytes()).await?;
    server_a
        .redirect_key_once(key_a.as_bytes(), FakeRedirectKind::Ask, server_a.addr())
        .await;
    assert_eq!(
        send_command(
            &mut client,
            vec![&b"SET"[..], key_a.as_bytes(), &b"value-a"[..]]
        )
        .await?,
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    );
    assert_eq!(
        send_command(
            &mut client,
            vec![&b"SET"[..], key_b.as_bytes(), &b"value-b"[..]]
        )
        .await?,
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    );
    let response = send_command(
        &mut client,
        vec![&b"MGET"[..], key_a.as_bytes(), key_b.as_bytes()],
    )
    .await?;
    assert_eq!(
        response,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"value-a")),
            RespValue::BulkString(Bytes::from_static(b"value-b"))
        ])
    );

    drop(client);
    proxy_task.await??;
    server_a.shutdown().await;
    server_b.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_end_to_end_handles_subscription_redirects() -> Result<()> {
    let server_a = match FakeRedisServer::start().await {
        Ok(server) => server,
        Err(err) if permission_denied(&err) => {
            eprintln!("subscription e2e skipped: {err}");
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    let server_b = match FakeRedisServer::start().await {
        Ok(server) => server,
        Err(err) if permission_denied(&err) => {
            eprintln!("subscription e2e skipped: {err}");
            server_a.shutdown().await;
            return Ok(());
        }
        Err(err) => {
            server_a.shutdown().await;
            return Err(err);
        }
    };

    let layout = cluster_slots_for(&[server_a.addr(), server_b.addr()]);
    server_a.set_cluster_slots(layout.clone()).await;
    server_b.set_cluster_slots(layout).await;
    let channel = b"news-channel";

    let config = render_config(
        "cluster-subscribe",
        "redis_cluster",
        vec![server_a.addr(), server_b.addr()],
        "127.0.0.1:6601",
    )?;
    let cluster_cfg = config
        .clusters()
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("missing cluster config"))?;
    let manager = Arc::new(ConfigManager::new(
        PathBuf::from("cluster-subscribe.toml"),
        &config,
    ));
    let runtime = manager
        .runtime_for(&cluster_cfg.name)
        .context("cluster runtime unavailable")?;
    let proxy = Arc::new(
        ClusterProxy::new(&cluster_cfg, runtime, manager.clone())
            .await
            .context("build cluster proxy")?,
    );

    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("subscription e2e skipped: {err}");
            server_a.shutdown().await;
            server_b.shutdown().await;
            return Ok(());
        }
        Err(err) => {
            server_a.shutdown().await;
            server_b.shutdown().await;
            return Err(err.into());
        }
    };
    let addr = listener.local_addr().unwrap();
    let proxy_task = {
        let proxy = proxy.clone();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            proxy.handle_connection(socket).await
        })
    };

    let mut client = Framed::new(
        TcpStream::connect(addr)
            .await
            .context("connect to cluster subscription proxy")?,
        RespCodec::default(),
    );
    wait_for_cluster_ready(&mut client, channel).await?;
    server_a
        .redirect_key_once(channel, FakeRedirectKind::Moved, server_b.addr())
        .await;
    let subscribe = send_command(&mut client, vec![&b"SUBSCRIBE"[..], channel.as_slice()]).await?;
    assert_eq!(
        subscribe,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"subscribe")),
            RespValue::BulkString(Bytes::copy_from_slice(channel)),
            RespValue::Integer(1)
        ])
    );

    server_b.publish(channel, b"payload").await;
    let message = match client.next().await {
        Some(Ok(value)) => value,
        other => anyhow::bail!("missing pubsub message: {:?}", other),
    };
    assert_eq!(
        message,
        RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"message")),
            RespValue::BulkString(Bytes::copy_from_slice(channel)),
            RespValue::BulkString(Bytes::from_static(b"payload"))
        ])
    );

    drop(client);
    proxy_task.await??;
    server_a.shutdown().await;
    server_b.shutdown().await;
    Ok(())
}

async fn send_command<I, T>(
    client: &mut Framed<TcpStream, RespCodec>,
    parts: I,
) -> Result<RespValue>
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    let frame = RespValue::Array(
        parts
            .into_iter()
            .map(|part| RespValue::BulkString(Bytes::copy_from_slice(part.as_ref())))
            .collect(),
    );
    client
        .send(frame)
        .await
        .context("send redis command to proxy")?;
    match client.next().await {
        Some(Ok(value)) => Ok(value),
        Some(Err(err)) => Err(err.into()),
        None => Err(anyhow!("proxy closed connection unexpectedly")),
    }
}

fn render_config(
    name: &str,
    mode: &str,
    servers: Vec<SocketAddr>,
    listen_addr: &str,
) -> Result<Config> {
    let server_list = servers
        .into_iter()
        .map(|addr| format!("{addr}"))
        .collect::<Vec<_>>()
        .join("\", \"");
    let raw = format!(
        r#"
[[clusters]]
name = "{name}"
listen_addr = "{listen}"
thread = 1
servers = ["{servers}"]
cache_type = "{mode}"
client_cache = {{ enabled = false }}
"#,
        listen = listen_addr,
        servers = server_list
    );
    let config: Config = toml::from_str(&raw).context("parse inline config")?;
    config.ensure_valid()?;
    Ok(config)
}

fn cluster_slots_for(nodes: &[SocketAddr]) -> RespValue {
    let half = (SLOT_COUNT / 2) as i64;
    RespValue::Array(vec![
        RespValue::Array(vec![
            RespValue::Integer(0),
            RespValue::Integer(half - 1),
            endpoint(nodes[0]),
            endpoint(nodes[1]),
        ]),
        RespValue::Array(vec![
            RespValue::Integer(half),
            RespValue::Integer((SLOT_COUNT - 1) as i64),
            endpoint(nodes[1]),
            endpoint(nodes[0]),
        ]),
    ])
}

fn endpoint(addr: SocketAddr) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Bytes::copy_from_slice(addr.ip().to_string().as_bytes())),
        RespValue::Integer(addr.port() as i64),
    ])
}

fn key_for_slot(range: std::ops::RangeInclusive<u16>) -> String {
    for attempt in 0..10_000u32 {
        let key = format!("key-{attempt}");
        let slot = crc16(key.as_bytes()) % SLOT_COUNT;
        if range.contains(&slot) {
            return key;
        }
    }
    panic!("unable to find key for slot range {:?}", range);
}

struct FakeRedisServer {
    addr: SocketAddr,
    _state: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    slots: Arc<RwLock<Option<RespValue>>>,
    redirects: Arc<RwLock<HashMap<Vec<u8>, VecDeque<FakeRedirect>>>>,
    subscriptions: Arc<Mutex<HashMap<Vec<u8>, Vec<Subscriber>>>>,
    _next_subscriber_id: Arc<AtomicU64>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl FakeRedisServer {
    async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind fake redis")?;
        let addr = listener.local_addr().context("resolve fake redis addr")?;
        let state = Arc::new(Mutex::new(HashMap::new()));
        let slots = Arc::new(RwLock::new(None));
        let redirects = Arc::new(RwLock::new(HashMap::new()));
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let next_subscriber_id = Arc::new(AtomicU64::new(1));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let task = tokio::spawn({
            let state = state.clone();
            let slots = slots.clone();
            let redirects = redirects.clone();
            let subscriptions = subscriptions.clone();
            let next_subscriber_id = next_subscriber_id.clone();
            async move {
                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => break,
                        accept = listener.accept() => {
                            match accept {
                                Ok((socket, _)) => {
                                    let state = state.clone();
                                    let slots = slots.clone();
                                    let redirects = redirects.clone();
                                    let subscriptions = subscriptions.clone();
                                    let next_subscriber_id = next_subscriber_id.clone();
                                    tokio::spawn(async move {
                                        if let Err(err) = handle_fake_connection(
                                            socket,
                                            state,
                                            slots,
                                            redirects,
                                            subscriptions,
                                            next_subscriber_id,
                                        )
                                        .await
                                        {
                                            eprintln!("fake redis connection error: {err}");
                                        }
                                    });
                                }
                                Err(err) => {
                                    eprintln!("fake redis accept error: {err}");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        Ok(Self {
            addr,
            _state: state,
            slots,
            redirects,
            subscriptions,
            _next_subscriber_id: next_subscriber_id,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn set_cluster_slots(&self, layout: RespValue) {
        let mut guard = self.slots.write().await;
        *guard = Some(layout);
    }

    async fn redirect_key_once(
        &self,
        key: impl AsRef<[u8]>,
        kind: FakeRedirectKind,
        target: SocketAddr,
    ) {
        let mut guard = self.redirects.write().await;
        let entry = guard
            .entry(key.as_ref().to_vec())
            .or_insert_with(VecDeque::new);
        entry.push_back(FakeRedirect { kind, target });
    }

    async fn publish(&self, channel: impl AsRef<[u8]>, payload: impl AsRef<[u8]>) -> usize {
        let mut guard = self.subscriptions.lock().await;
        let key = channel.as_ref().to_vec();
        let payload = payload.as_ref().to_vec();
        if let Some(entries) = guard.get_mut(&key) {
            let mut delivered = 0usize;
            entries.retain(|entry| {
                let message = RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from_static(b"message")),
                    RespValue::BulkString(Bytes::copy_from_slice(&key)),
                    RespValue::BulkString(Bytes::copy_from_slice(&payload)),
                ]);
                if entry.sender.send(message).is_ok() {
                    delivered += 1;
                    true
                } else {
                    false
                }
            });
            delivered
        } else {
            0
        }
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

async fn handle_fake_connection(
    socket: TcpStream,
    state: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    slots: Arc<RwLock<Option<RespValue>>>,
    redirects: Arc<RwLock<HashMap<Vec<u8>, VecDeque<FakeRedirect>>>>,
    subscriptions: Arc<Mutex<HashMap<Vec<u8>, Vec<Subscriber>>>>,
    next_subscriber_id: Arc<AtomicU64>,
) -> Result<()> {
    let framed = Framed::new(socket, RespCodec::default());
    let (sink, mut stream) = framed.split();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut sink = sink;
    let writer = tokio::spawn(async move {
        while let Some(resp) = rx.recv().await {
            if sink.send(resp).await.is_err() {
                break;
            }
        }
    });

    let mut ctx = FakeConnectionContext::new(
        state,
        slots,
        redirects,
        subscriptions,
        next_subscriber_id.fetch_add(1, Ordering::Relaxed),
        tx.clone(),
    );

    while let Some(frame) = stream.next().await {
        let reply = match frame.context("decode RESP frame")? {
            RespValue::Array(parts) => ctx.handle_command(parts).await,
            _ => vec![RespValue::error("ERR invalid request")],
        };
        for resp in reply {
            if tx.send(resp).is_err() {
                break;
            }
        }
    }

    ctx.cleanup().await;
    drop(tx);
    let _ = writer.await;
    Ok(())
}

struct FakeConnectionContext {
    state: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    slots: Arc<RwLock<Option<RespValue>>>,
    redirects: Arc<RwLock<HashMap<Vec<u8>, VecDeque<FakeRedirect>>>>,
    subscriptions: Arc<Mutex<HashMap<Vec<u8>, Vec<Subscriber>>>>,
    subscriber_id: u64,
    channels: HashSet<Vec<u8>>,
    sender: mpsc::UnboundedSender<RespValue>,
}

impl FakeConnectionContext {
    fn new(
        state: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
        slots: Arc<RwLock<Option<RespValue>>>,
        redirects: Arc<RwLock<HashMap<Vec<u8>, VecDeque<FakeRedirect>>>>,
        subscriptions: Arc<Mutex<HashMap<Vec<u8>, Vec<Subscriber>>>>,
        subscriber_id: u64,
        sender: mpsc::UnboundedSender<RespValue>,
    ) -> Self {
        Self {
            state,
            slots,
            redirects,
            subscriptions,
            subscriber_id,
            channels: HashSet::new(),
            sender,
        }
    }

    async fn handle_command(&mut self, parts: Vec<RespValue>) -> Vec<RespValue> {
        if parts.is_empty() {
            return vec![RespValue::error("ERR empty command")];
        }
        if let Some(redirect) = self.maybe_redirect(&parts).await {
            return vec![redirect];
        }
        let name = upper_name(&parts[0]);
        match name.as_slice() {
            b"PING" => vec![RespValue::SimpleString(Bytes::from_static(b"PONG"))],
            b"SET" => self.handle_set(&parts).await,
            b"GET" => self.handle_get(&parts).await,
            b"MGET" => self.handle_mget(&parts).await,
            b"CLUSTER" => self.handle_cluster(&parts).await,
            b"ASKING" => vec![RespValue::SimpleString(Bytes::from_static(b"OK"))],
            b"SUBSCRIBE" => self.handle_subscribe(&parts).await,
            b"UNSUBSCRIBE" => self.handle_unsubscribe(&parts).await,
            _ => vec![RespValue::error("ERR unknown command")],
        }
    }

    async fn handle_set(&self, parts: &[RespValue]) -> Vec<RespValue> {
        if parts.len() < 3 {
            return vec![RespValue::error("ERR wrong number of arguments for 'set'")];
        }
        if let (Some(key), Some(value)) = (bulk_bytes(&parts[1]), bulk_bytes(&parts[2])) {
            let mut guard = self.state.lock().await;
            guard.insert(key, value);
            vec![RespValue::SimpleString(Bytes::from_static(b"OK"))]
        } else {
            vec![RespValue::error("ERR invalid arguments")]
        }
    }

    async fn handle_get(&self, parts: &[RespValue]) -> Vec<RespValue> {
        if parts.len() < 2 {
            return vec![RespValue::error("ERR wrong number of arguments for 'get'")];
        }
        if let Some(key) = bulk_bytes(&parts[1]) {
            let guard = self.state.lock().await;
            match guard.get(&key) {
                Some(value) => vec![RespValue::BulkString(Bytes::copy_from_slice(value))],
                None => vec![RespValue::NullBulk],
            }
        } else {
            vec![RespValue::error("ERR invalid arguments")]
        }
    }

    async fn handle_mget(&self, parts: &[RespValue]) -> Vec<RespValue> {
        let guard = self.state.lock().await;
        let mut values = Vec::new();
        for item in parts.iter().skip(1) {
            if let Some(key) = bulk_bytes(item) {
                if let Some(value) = guard.get(&key) {
                    values.push(RespValue::BulkString(Bytes::copy_from_slice(value)));
                } else {
                    values.push(RespValue::NullBulk);
                }
            } else {
                values.push(RespValue::NullBulk);
            }
        }
        vec![RespValue::Array(values)]
    }

    async fn handle_cluster(&self, parts: &[RespValue]) -> Vec<RespValue> {
        if parts
            .get(1)
            .and_then(bulk_bytes)
            .map(|v| v.eq_ignore_ascii_case(b"SLOTS"))
            .unwrap_or(false)
        {
            match self.slots.read().await.clone() {
                Some(layout) => vec![layout],
                None => vec![RespValue::error("ERR slots unavailable")],
            }
        } else {
            vec![RespValue::error("ERR unknown subcommand")]
        }
    }

    async fn handle_subscribe(&mut self, parts: &[RespValue]) -> Vec<RespValue> {
        if parts.len() < 2 {
            return vec![RespValue::error(
                "ERR wrong number of arguments for 'subscribe'",
            )];
        }
        let mut responses = Vec::new();
        for item in parts.iter().skip(1) {
            if let Some(channel) = bulk_bytes(item) {
                self.channels.insert(channel.clone());
                self.register_channel(channel.clone()).await;
                responses.push(subscription_ack(
                    b"subscribe",
                    channel,
                    self.channels.len() as i64,
                ));
            }
        }
        responses
    }

    async fn handle_unsubscribe(&mut self, parts: &[RespValue]) -> Vec<RespValue> {
        if parts.len() == 1 {
            let mut responses = Vec::new();
            let channels = self.channels.clone();
            for channel in channels.iter() {
                self.channels.remove(channel);
                self.unregister_channel(channel).await;
                responses.push(subscription_ack(
                    b"unsubscribe",
                    channel.clone(),
                    self.channels.len() as i64,
                ));
            }
            responses
        } else {
            let mut responses = Vec::new();
            for item in parts.iter().skip(1) {
                if let Some(channel) = bulk_bytes(item) {
                    self.channels.remove(&channel);
                    self.unregister_channel(&channel).await;
                    responses.push(subscription_ack(
                        b"unsubscribe",
                        channel,
                        self.channels.len() as i64,
                    ));
                }
            }
            responses
        }
    }

    async fn maybe_redirect(&self, parts: &[RespValue]) -> Option<RespValue> {
        let mut guard = self.redirects.write().await;
        for key in extract_keys(parts) {
            if let Some(queue) = guard.get_mut(&key) {
                if let Some(rule) = queue.pop_front() {
                    if queue.is_empty() {
                        guard.remove(&key);
                    }
                    return Some(rule.into_resp(&key));
                }
            }
        }
        None
    }

    async fn register_channel(&self, channel: Vec<u8>) {
        let mut guard = self.subscriptions.lock().await;
        let entry = guard.entry(channel).or_insert_with(Vec::new);
        entry.push(Subscriber {
            id: self.subscriber_id,
            sender: self.sender.clone(),
        });
    }

    async fn unregister_channel(&self, channel: &[u8]) {
        let mut guard = self.subscriptions.lock().await;
        if let Some(entries) = guard.get_mut(channel) {
            entries.retain(|entry| entry.id != self.subscriber_id);
            if entries.is_empty() {
                guard.remove(channel);
            }
        }
    }

    async fn cleanup(&mut self) {
        let channels = std::mem::take(&mut self.channels);
        for channel in channels {
            self.unregister_channel(&channel).await;
        }
    }
}

fn subscription_ack(kind: &[u8], channel: Vec<u8>, count: i64) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Bytes::copy_from_slice(kind)),
        RespValue::BulkString(Bytes::copy_from_slice(&channel)),
        RespValue::Integer(count),
    ])
}

fn extract_keys(parts: &[RespValue]) -> Vec<Vec<u8>> {
    if parts.is_empty() {
        return Vec::new();
    }
    let name = upper_name(&parts[0]);
    match name.as_slice() {
        b"SET" | b"GET" | b"SUBSCRIBE" | b"UNSUBSCRIBE" => {
            parts.iter().skip(1).filter_map(bulk_bytes).collect()
        }
        b"MGET" => parts.iter().skip(1).filter_map(bulk_bytes).collect(),
        _ => Vec::new(),
    }
}

#[derive(Clone)]
struct Subscriber {
    id: u64,
    sender: mpsc::UnboundedSender<RespValue>,
}

#[derive(Clone)]
struct FakeRedirect {
    kind: FakeRedirectKind,
    target: SocketAddr,
}

impl FakeRedirect {
    fn into_resp(self, key: &[u8]) -> RespValue {
        let slot = crc16(key) % SLOT_COUNT;
        match self.kind {
            FakeRedirectKind::Moved => {
                let payload = format!("MOVED {} {}", slot, self.target);
                RespValue::error(payload)
            }
            FakeRedirectKind::Ask => {
                let payload = format!("ASK {} {}", slot, self.target);
                RespValue::error(payload)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum FakeRedirectKind {
    Moved,
    Ask,
}

fn upper_name(value: &RespValue) -> Vec<u8> {
    match value {
        RespValue::BulkString(data) | RespValue::SimpleString(data) => {
            data.iter().map(|b| b.to_ascii_uppercase()).collect()
        }
        other => format!("{other:?}").into_bytes(),
    }
}

fn bulk_bytes(value: &RespValue) -> Option<Vec<u8>> {
    match value {
        RespValue::BulkString(data) | RespValue::SimpleString(data) => Some(data.to_vec()),
        _ => None,
    }
}

fn permission_denied(err: &anyhow::Error) -> bool {
    use std::io::ErrorKind;
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|io_err| io_err.kind() == ErrorKind::PermissionDenied)
            .unwrap_or(false)
    })
}

async fn wait_for_cluster_ready(
    client: &mut Framed<TcpStream, RespCodec>,
    key: &[u8],
) -> Result<()> {
    for _ in 0..20 {
        match send_command(client, vec![&b"GET"[..], key]).await? {
            RespValue::Error(ref err) if cluster_not_ready(err.as_ref()) => {
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            _ => return Ok(()),
        }
    }
    Err(anyhow!(
        "cluster slots not ready for key {} after retries",
        String::from_utf8_lossy(key)
    ))
}

fn cluster_not_ready(message: &[u8]) -> bool {
    message.starts_with(b"ERR slot") || message.starts_with(b"MOVED") || message.starts_with(b"ASK")
}
