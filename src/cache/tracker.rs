use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
#[cfg(any(unix, windows))]
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use crate::auth::BackendAuth;
use crate::cache::{CacheState, ClientCache};
use crate::config::ClusterRuntime;
use crate::protocol::redis::{RespCodec, RespValue, RespVersion};

const TRACKER_BACKOFF_START: Duration = Duration::from_millis(200);
const TRACKER_BACKOFF_MAX: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct CacheTrackerSet {
    cluster: Arc<str>,
    cache: Arc<ClientCache>,
    runtime: Arc<ClusterRuntime>,
    backend_auth: Option<BackendAuth>,
    timeout_ms: u64,
    handles: Mutex<HashMap<Arc<str>, TrackerHandle>>,
}

#[derive(Debug)]
struct TrackerHandle {
    shutdown: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl CacheTrackerSet {
    pub fn new(
        cluster: Arc<str>,
        cache: Arc<ClientCache>,
        runtime: Arc<ClusterRuntime>,
        backend_auth: Option<BackendAuth>,
        timeout_ms: u64,
    ) -> Self {
        Self {
            cluster,
            cache,
            runtime,
            backend_auth,
            timeout_ms,
            handles: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_nodes(&self, nodes: Vec<String>) {
        let mut guard = self.handles.lock();
        let desired: HashSet<Arc<str>> = nodes.iter().map(|n| Arc::<str>::from(n.clone())).collect();

        guard.retain(|addr, handle| {
            if desired.contains(addr) {
                true
            } else {
                let _ = handle.shutdown.send(true);
                handle.join.abort();
                false
            }
        });

        for node in desired {
            if guard.contains_key(&node) {
                continue;
            }
            let handle = self.spawn_tracker(node.clone());
            guard.insert(node, handle);
        }
    }

    fn spawn_tracker(&self, address: Arc<str>) -> TrackerHandle {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let cluster = self.cluster.clone();
        let cache = self.cache.clone();
        let runtime = self.runtime.clone();
        let auth = self.backend_auth.clone();
        let timeout_ms = self.timeout_ms;
        let mut state_rx = cache.subscribe();
        let mut local_shutdown = shutdown_rx;
        let join = tokio::spawn(async move {
            let mut backoff = TRACKER_BACKOFF_START;
            loop {
                if *local_shutdown.borrow() {
                    break;
                }
                if wait_for_enabled(&mut state_rx, &mut local_shutdown).await {
                    match listen_once(
                        cluster.clone(),
                        cache.clone(),
                        runtime.clone(),
                        auth.clone(),
                        timeout_ms,
                        address.clone(),
                        &mut state_rx,
                        &mut local_shutdown,
                    )
                    .await
                    {
                        Ok(()) => {
                            backoff = TRACKER_BACKOFF_START;
                        }
                        Err(err) => {
                            warn!(
                                cluster = %cluster,
                                backend = %address,
                                error = %err,
                                "client cache tracker failed"
                            );
                            if wait_with_shutdown(backoff, &mut local_shutdown).await {
                                break;
                            }
                            backoff = (backoff * 2).min(TRACKER_BACKOFF_MAX);
                        }
                    }
                } else {
                    break;
                }
            }
            debug!(cluster = %cluster, backend = %address, "client cache tracker stopped");
        });
        TrackerHandle {
            shutdown: shutdown_tx,
            join,
        }
    }
}

impl Drop for CacheTrackerSet {
    fn drop(&mut self) {
        let handles = self.handles.lock().drain().collect::<Vec<_>>();
        for (_, handle) in handles {
            let _ = handle.shutdown.send(true);
            handle.join.abort();
        }
    }
}

async fn wait_for_enabled(
    state_rx: &mut watch::Receiver<CacheState>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        if *shutdown.borrow() {
            return false;
        }
        match *state_rx.borrow() {
            CacheState::Enabled => return true,
            CacheState::Disabled | CacheState::Draining => {}
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return false;
                }
            }
            changed = state_rx.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
        }
    }
}

async fn wait_with_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = shutdown.changed() => true,
        _ = sleep(delay) => *shutdown.borrow(),
    }
}

async fn listen_once(
    cluster: Arc<str>,
    cache: Arc<ClientCache>,
    runtime: Arc<ClusterRuntime>,
    backend_auth: Option<BackendAuth>,
    timeout_ms: u64,
    address: Arc<str>,
    state_rx: &mut watch::Receiver<CacheState>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let timeout_duration = runtime.request_timeout(timeout_ms);
    let mut framed = open_stream(&address, timeout_duration, backend_auth.clone()).await?;
    negotiate_resp3(&cluster, &address, timeout_duration, &mut framed, backend_auth.clone()).await?;
    enable_tracking(&cluster, &address, timeout_duration, &mut framed).await?;

    loop {
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            changed = state_rx.changed() => {
                if changed.is_err() || *state_rx.borrow() != CacheState::Enabled {
                    return Ok(());
                }
            }
            frame = framed.next() => match frame {
                Some(Ok(RespValue::Push(items))) => {
                    if let Some(keys) = parse_invalidation(&items) {
                        cache.invalidate_bytes(&keys);
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(err.into()),
                None => return Err(anyhow!("tracking connection closed")),
            }
        }
    }
}

async fn open_stream(
    address: &str,
    timeout_duration: Duration,
    backend_auth: Option<BackendAuth>,
) -> Result<Framed<TcpStream, RespCodec>> {
    let stream = timeout(timeout_duration, TcpStream::connect(address))
        .await
        .with_context(|| format!("connect to {address} timed out"))??;
    stream.set_nodelay(true).context("failed to enable TCP_NODELAY")?;
    #[cfg(any(unix, windows))]
    {
        use socket2::{SockRef, TcpKeepalive};
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(60));
        if let Err(err) = SockRef::from(&stream).set_tcp_keepalive(&keepalive) {
            warn!(backend = %address, error = %err, "failed to set tracker TCP keepalive");
        }
    }
    let mut framed = Framed::new(stream, RespCodec::default());
    if let Some(auth) = &backend_auth {
        auth.apply_to_stream(&mut framed, timeout_duration, address)
            .await?;
    }
    Ok(framed)
}

async fn negotiate_resp3(
    cluster: &str,
    backend: &str,
    timeout_duration: Duration,
    framed: &mut Framed<TcpStream, RespCodec>,
    backend_auth: Option<BackendAuth>,
) -> Result<()> {
    framed.codec_mut().set_version(RespVersion::Resp2);
    let mut hello_parts = vec![
        RespValue::BulkString(Bytes::from_static(b"HELLO")),
        RespValue::BulkString(Bytes::from_static(b"3")),
    ];
    if let Some(auth) = backend_auth {
        if let Some((username, password)) = auth.hello_credentials() {
            hello_parts.push(RespValue::BulkString(Bytes::from_static(b"AUTH")));
            hello_parts.push(RespValue::BulkString(username));
            hello_parts.push(RespValue::BulkString(password));
        }
    }
    let hello = RespValue::Array(hello_parts);
    timeout(timeout_duration, framed.send(hello))
        .await
        .with_context(|| format!("failed to send HELLO to {backend}"))??;
    match timeout(timeout_duration, framed.next()).await {
        Ok(Some(Ok(resp))) => {
            if resp.is_error() {
                bail!("backend {backend} rejected HELLO for cluster {cluster}");
            }
        }
        Ok(Some(Err(err))) => return Err(err.context("HELLO handshake failed")),
        Ok(None) => bail!("backend {backend} closed during HELLO"),
        Err(_) => bail!("backend {backend} timed out waiting for HELLO reply"),
    }
    framed.codec_mut().set_version(RespVersion::Resp3);
    Ok(())
}

async fn enable_tracking(
    cluster: &str,
    backend: &str,
    timeout_duration: Duration,
    framed: &mut Framed<TcpStream, RespCodec>,
) -> Result<()> {
    let command = RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"CLIENT")),
        RespValue::BulkString(Bytes::from_static(b"TRACKING")),
        RespValue::BulkString(Bytes::from_static(b"ON")),
        RespValue::BulkString(Bytes::from_static(b"BCAST")),
        RespValue::BulkString(Bytes::from_static(b"NOLOOP")),
    ]);
    timeout(timeout_duration, framed.send(command))
        .await
        .with_context(|| format!("failed to send CLIENT TRACKING to {backend}"))??;
    match timeout(timeout_duration, framed.next()).await {
        Ok(Some(Ok(resp))) => match resp {
            RespValue::SimpleString(ref s) | RespValue::BulkString(ref s)
                if s.eq_ignore_ascii_case(b"OK") => Ok(()),
            RespValue::Error(err) => Err(anyhow!(
                "backend {backend} rejected CLIENT TRACKING for cluster {cluster}: {}",
                String::from_utf8_lossy(&err)
            )),
            other => Err(anyhow!(
                "unexpected CLIENT TRACKING reply from {backend}: {:?}",
                other
            )),
        },
        Ok(Some(Err(err))) => Err(err.context("CLIENT TRACKING failed")),
        Ok(None) => Err(anyhow!("backend {backend} closed during CLIENT TRACKING")),
        Err(_) => Err(anyhow!("backend {backend} timed out waiting for CLIENT TRACKING")),
    }
}

fn parse_invalidation(items: &[RespValue]) -> Option<Vec<Bytes>> {
    if items.is_empty() {
        return None;
    }
    let label = match &items[0] {
        RespValue::SimpleString(data) | RespValue::BulkString(data) => data,
        _ => return None,
    };
    if !label.eq_ignore_ascii_case(b"invalidate") {
        return None;
    }
    let mut keys = Vec::with_capacity(items.len().saturating_sub(1));
    for item in items.iter().skip(1) {
        match item {
            RespValue::BulkString(data) | RespValue::SimpleString(data) => keys.push(data.clone()),
            _ => {}
        }
    }
    Some(keys)
}
