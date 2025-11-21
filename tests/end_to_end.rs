use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
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
    sync::{oneshot, Mutex, RwLock},
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
        send_command(
            &mut client,
            vec![&b"SET"[..], &b"foo"[..], &b"bar"[..]]
        )
        .await?,
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    );
    assert_eq!(
        send_command(&mut client, vec![&b"GET"[..], &b"foo"[..]]).await?,
        RespValue::BulkString(Bytes::from_static(b"bar"))
    );
    let multi =
        send_command(&mut client, vec![&b"MGET"[..], &b"foo"[..], &b"missing"[..]]).await?;
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
        TcpStream::connect(addr).await.context("connect to cluster proxy")?,
        RespCodec::default(),
    );
    let midpoint = SLOT_COUNT / 2;
    let key_a = key_for_slot(0..=midpoint - 1);
    let key_b = key_for_slot(midpoint..=SLOT_COUNT - 1);
    wait_for_cluster_ready(&mut client, key_a.as_bytes()).await?;
    wait_for_cluster_ready(&mut client, key_b.as_bytes()).await?;
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
    slots: Arc<RwLock<Option<RespValue>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl FakeRedisServer {
    async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.context("bind fake redis")?;
        let addr = listener.local_addr().context("resolve fake redis addr")?;
        let state = Arc::new(Mutex::new(HashMap::new()));
        let slots = Arc::new(RwLock::new(None));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let task = tokio::spawn({
            let state = state.clone();
            let slots = slots.clone();
            async move {
                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => break,
                        accept = listener.accept() => {
                            match accept {
                                Ok((socket, _)) => {
                                    let state = state.clone();
                                    let slots = slots.clone();
                                    tokio::spawn(async move {
                                        if let Err(err) = handle_fake_connection(socket, state, slots).await {
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
            slots,
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
) -> Result<()> {
    let mut framed = Framed::new(socket, RespCodec::default());
    while let Some(frame) = framed.next().await {
        let reply = match frame.context("decode RESP frame")? {
            RespValue::Array(parts) => handle_fake_command(parts, state.clone(), slots.clone()).await,
            _ => RespValue::error("ERR invalid request"),
        };
        framed.send(reply).await?;
    }
    Ok(())
}

async fn handle_fake_command(
    parts: Vec<RespValue>,
    state: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    slots: Arc<RwLock<Option<RespValue>>>,
) -> RespValue {
    if parts.is_empty() {
        return RespValue::error("ERR empty command");
    }
    let name = upper_name(&parts[0]);
    match name.as_slice() {
        b"PING" => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        b"SET" => {
            if parts.len() < 3 {
                return RespValue::error("ERR wrong number of arguments for 'set'");
            }
            if let (Some(key), Some(value)) = (bulk_bytes(&parts[1]), bulk_bytes(&parts[2])) {
                state.lock().await.insert(key, value);
                RespValue::SimpleString(Bytes::from_static(b"OK"))
            } else {
                RespValue::error("ERR invalid arguments")
            }
        }
        b"GET" => {
            if parts.len() < 2 {
                return RespValue::error("ERR wrong number of arguments for 'get'");
            }
            if let Some(key) = bulk_bytes(&parts[1]) {
                match state.lock().await.get(&key) {
                    Some(value) => RespValue::BulkString(Bytes::copy_from_slice(value)),
                    None => RespValue::NullBulk,
                }
            } else {
                RespValue::error("ERR invalid arguments")
            }
        }
        b"MGET" => {
            let guard = state.lock().await;
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
            RespValue::Array(values)
        }
        b"CLUSTER" if parts
            .get(1)
            .and_then(bulk_bytes)
            .map(|v| v.eq_ignore_ascii_case(b"SLOTS"))
            .unwrap_or(false) =>
        {
            match slots.read().await.clone() {
                Some(layout) => layout,
                None => RespValue::error("ERR slots unavailable"),
            }
        }
        b"ASKING" => RespValue::SimpleString(Bytes::from_static(b"OK")),
        _ => RespValue::error("ERR unknown command"),
    }
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
            RespValue::Error(ref err) if err.as_ref().starts_with(b"ERR slot") => {
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
