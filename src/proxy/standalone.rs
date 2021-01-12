pub mod back;
pub mod fnv;
pub mod front;
pub mod ketama;
pub mod ping;
pub mod reload;

use futures::future::ok;
use futures::lazy;
use futures::task::Task;
use futures::unsync::mpsc::{channel, Sender};
use futures::{AsyncSink, Future, Sink, Stream};

use tokio::codec::{Decoder, Encoder};
use tokio::net::TcpStream;
use tokio::prelude::FutureExt;
use tokio::runtime::current_thread;

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::process;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::protocol::{mc, redis};

use crate::metrics::front_conn_incr;

use crate::com::AsError;
use crate::com::{create_reuse_port_listener, gethostbyname};
use crate::com::{CacheType, ClusterConfig, CODE_PORT_IN_USE};
use crate::protocol::IntoReply;

use fnv::fnv1a64;
use ketama::HashRing;

pub trait Request: Clone {
    type Reply: Clone + IntoReply<Self::Reply> + From<AsError>;

    type FrontCodec: Decoder<Item = Self, Error = AsError>
        + Encoder<Item = Self, Error = AsError>
        + Default
        + 'static;
    type BackCodec: Decoder<Item = Self::Reply, Error = AsError>
        + Encoder<Item = Self, Error = AsError>
        + Default
        + 'static;

    fn ping_request() -> Self;
    fn reregister(&mut self, task: Task);

    fn key_hash(&self, hash_tag: &[u8], hasher: fn(&[u8]) -> u64) -> u64;

    fn subs(&self) -> Option<Vec<Self>>;

    fn mark_total(&self, cluster: &str);

    fn mark_remote(&self, cluster: &str);

    fn is_done(&self) -> bool;
    fn is_error(&self) -> bool;

    fn add_cycle(&self);
    fn can_cycle(&self) -> bool;

    fn valid(&self) -> bool;

    fn set_reply<R: IntoReply<Self::Reply>>(&self, t: R);
    fn set_error(&self, t: &AsError);

    fn get_sendtime(&self) -> Option<Instant>;
}

pub struct Cluster<T> {
    pub cc: RefCell<ClusterConfig>,
    hash_tag: Vec<u8>,
    spots: RefCell<HashMap<String, usize>>,
    alias: RefCell<HashMap<String, String>>,

    _marker: PhantomData<T>,
    ring: RefCell<HashRing>,
    conns: RefCell<Conns<T>>,
    pings: RefCell<HashMap<String, Rc<Cell<bool>>>>,
}

impl<T: Request + 'static> Cluster<T> {
    pub(crate) fn run(cc: ClusterConfig) -> Result<(), AsError> {
        let addr = cc
            .listen_addr
            .parse::<SocketAddr>()
            .expect("parse socket never fail");
        let fut = ok::<ClusterConfig, AsError>(cc)
            .and_then(|cc| {
                let hash_tag = cc
                    .hash_tag
                    .as_ref()
                    .map(|x| x.as_bytes().to_vec())
                    .unwrap_or_else(|| vec![]);
                let cluster = Cluster {
                    cc: RefCell::new(cc.clone()),
                    hash_tag,
                    spots: RefCell::new(HashMap::new()),
                    alias: RefCell::new(HashMap::new()),
                    _marker: Default::default(),
                    ring: RefCell::new(HashRing::empty()),
                    conns: RefCell::new(Conns::default()),
                    pings: RefCell::new(HashMap::new()),
                };
                let rc_cluster = Rc::new(cluster);
                rc_cluster.reinit(cc).expect("fail to setup cluster");
                Ok(rc_cluster)
            })
            .and_then(|cluster| {
                let rc_cluster = cluster.clone();
                let ping_fail_limit = cluster.ping_fail_limit();
                if ping_fail_limit > 0 {
                    let ping_interval = cluster.ping_interval();
                    let ping_succ_interval = cluster.ping_succ_interval();
                    // TODO: ping support non-alias mode
                    let alias_map = cluster.alias.borrow().clone();
                    for (alias, node) in alias_map.into_iter() {
                        cluster.setup_ping(
                            &alias,
                            &node,
                            ping_interval,
                            ping_succ_interval,
                            ping_fail_limit,
                        );
                    }
                } else {
                    info!("skip setup ping for {}", cluster.cc.borrow().name);
                }
                Ok(rc_cluster)
            })
            .and_then(|cluster| {
                let rc_cluster = cluster.clone();
                let reloader = reload::Reloader::new(rc_cluster);
                current_thread::spawn(reloader);
                Ok(cluster)
            })
            .and_then(|cluster| {
                let rc_cluster = cluster.clone();
                let listen = match create_reuse_port_listener(&addr) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("listen {} error: {}", addr, e);
                        process::exit(CODE_PORT_IN_USE);
                    }
                };
                let service = listen
                    .incoming()
                    .for_each(move |sock| {
                        let cluster_ref = cluster.clone();
                        if let Err(err) = sock.set_nodelay(true) {
                            warn!(
                                "cluster {} fail to set nodelay but skip, due to {:?}",
                                cluster_ref.cc.borrow().name,
                                err
                            );
                        }
                        let client_str = match sock.peer_addr() {
                            Ok(client) => format!("{}", client),
                            Err(err) => {
                                error!(
                                    "cluster {} fail to get client name due to {:?}",
                                    cluster_ref.cc.borrow().name,
                                    err
                                );
                                "unknown".to_string()
                            }
                        };

                        let codec = T::FrontCodec::default();
                        let (output, input) = codec.framed(sock).split();

                        front_conn_incr(&cluster.cc.borrow().name);
                        let fut = front::Front::new(client_str, cluster_ref, input, output);
                        current_thread::spawn(fut);
                        Ok(())
                    })
                    .map_err(|err| {
                        error!("fail to accept incoming sock due {}", err);
                    });
                current_thread::spawn(service);
                Ok(rc_cluster)
            })
            .map_err(|err| {
                error!("fail to start proxy service... due {:?}", err);
            });
        current_thread::block_on_all(fut).unwrap();
        Ok(())
    }

    fn ping_fail_limit(&self) -> u8 {
        self.cc
            .borrow()
            .ping_fail_limit
            .as_ref()
            .cloned()
            .unwrap_or(0)
    }

    fn ping_interval(&self) -> u64 {
        self.cc
            .borrow()
            .ping_interval
            .as_ref()
            .cloned()
            .unwrap_or(300_000)
    }

    fn ping_succ_interval(&self) -> u64 {
        self.cc
            .borrow()
            .ping_succ_interval
            .as_ref()
            .cloned()
            .unwrap_or(1_000)
    }

    fn setup_ping(
        self: &Rc<Self>,
        alias: &str,
        node: &str,
        ping_interval: u64,
        ping_succ_interval: u64,
        ping_fail_limit: u8,
    ) {
        const CANCEL: bool = false;
        let handle = Rc::new(Cell::new(CANCEL));
        {
            let mut pings = self.pings.borrow_mut();
            pings.insert(node.to_string(), handle.clone());
        }

        let ping = ping::Ping::new(
            Rc::downgrade(&self),
            alias.to_string(),
            node.to_string(),
            handle,
            ping_interval,
            ping_succ_interval,
            ping_fail_limit,
        );
        current_thread::spawn(ping);
    }

    pub(crate) fn reinit(self: &Rc<Self>, cc: ClusterConfig) -> Result<(), AsError> {
        let sls = ServerLine::parse_servers(&cc.servers)?;
        let (nodes, alias, weights) = ServerLine::unwrap_spot(&sls);
        let alias_map: HashMap<_, _> = alias
            .clone()
            .into_iter()
            .zip(nodes.clone().into_iter())
            .collect();
        let alias_rev: HashMap<_, _> = alias_map
            .iter()
            .map(|(x, y)| (y.clone(), x.clone()))
            .collect();
        let spots_map: HashMap<_, _> = if alias.is_empty() {
            nodes
                .clone()
                .into_iter()
                .zip(weights.clone().into_iter())
                .collect()
        } else {
            alias
                .clone()
                .into_iter()
                .zip(weights.clone().into_iter())
                .collect()
        };
        let hash_ring = if alias.is_empty() {
            HashRing::new(nodes, weights)?
        } else {
            HashRing::new(alias, weights)?
        };
        let addrs: HashSet<_> = if !alias_map.is_empty() {
            alias_map.values().map(|x| x.to_string()).collect()
        } else {
            spots_map.keys().map(|x| x.to_string()).collect()
        };
        let old_addrs = self.conns.borrow().addrs();

        let new_addrs = addrs.difference(&old_addrs);
        let unused_addrs = old_addrs.difference(&addrs);
        for addr in new_addrs {
            self.reconnect(&*addr);
            let ping_fail_limit = self.ping_fail_limit();
            if ping_fail_limit > 0 {
                let ping_interval = self.ping_interval();
                let ping_succ_interval = self.ping_succ_interval();
                let alias = alias_rev
                    .get(addr)
                    .expect("alias must be exists")
                    .to_string();
                self.setup_ping(
                    &alias,
                    addr,
                    ping_interval,
                    ping_succ_interval,
                    ping_fail_limit,
                );
            }
        }

        for addr in unused_addrs {
            self.conns.borrow_mut().remove(&addr);
            let mut pings = self.pings.borrow_mut();
            if let Some(handle) = pings.remove(addr) {
                handle.set(true);
            }
        }

        *self.cc.borrow_mut() = cc;
        *self.ring.borrow_mut() = hash_ring;
        *self.alias.borrow_mut() = alias_map;
        *self.spots.borrow_mut() = spots_map;
        Ok(())
    }

    fn has_alias(&self) -> bool {
        !self.alias.borrow().is_empty()
    }

    fn get_node(&self, name: String) -> String {
        if !self.has_alias() {
            return name;
        }

        self.alias
            .borrow()
            .get(&name)
            .expect("alias name must exists")
            .to_string()
    }

    pub(crate) fn add_node(&self, name: String) -> Result<(), AsError> {
        if let Some(weight) = self.spots.borrow().get(&name).cloned() {
            let addr = self.get_node(name.clone());
            let conn = connect(
                &self.cc.borrow().name,
                &addr,
                self.cc.borrow().read_timeout,
                self.cc.borrow().write_timeout,
            )?;
            self.conns.borrow_mut().insert(&addr, conn);
            self.ring.borrow_mut().add_node(name, weight);
        }
        Ok(())
    }

    pub(crate) fn remove_node(&self, name: String) {
        self.ring.borrow_mut().del_node(&name);
        let node = self.get_node(name);
        if self.conns.borrow_mut().remove(&node).is_some() {
            info!("dropping backend connection of {} due active delete", node);
        }
    }

    pub(crate) fn reconnect(&self, addr: &str) {
        let mut conns = self.conns.borrow_mut();
        debug!("trying to reconnect to {}", addr);
        conns.remove(addr);
        match connect(
            &self.cc.borrow().name,
            &addr,
            self.cc.borrow().read_timeout,
            self.cc.borrow().write_timeout,
        ) {
            Ok(sender) => conns.insert(&addr, sender),
            Err(err) => {
                error!("fail to reconnect to {} due {:?}", addr, err);
            }
        }
    }

    pub fn dispatch_to(&self, addr: &str, cmd: T) -> Result<AsyncSink<T>, AsError> {
        if !cmd.can_cycle() {
            // debug!("unable recycle due can't cycle");
            cmd.set_error(&AsError::ProxyFail);
            return Ok(AsyncSink::NotReady(cmd));
        }

        let mut conns = self.conns.borrow_mut();
        loop {
            if let Some(sender) = conns.get_mut(addr).map(|x| x.sender()) {
                match sender.start_send(cmd) {
                    Ok(ret) => {
                        return Ok(ret);
                    }
                    Err(se) => {
                        warn!("dispatch_to meet error addr={}", &addr);
                        let cmd = se.into_inner();
                        cmd.add_cycle();
                        conns.remove(addr);
                        return Ok(AsyncSink::NotReady(cmd));
                    }
                }
            } else {
                debug!("dispatch_to trying to reconnect to {}", addr);
                let sender = connect(
                    &self.cc.borrow().name,
                    &addr,
                    self.cc.borrow().read_timeout,
                    self.cc.borrow().write_timeout,
                )?;
                conns.insert(&addr, sender);
            }
        }
    }

    pub fn dispatch_all(&self, cmds: &mut VecDeque<T>) -> Result<usize, AsError> {
        let mut count = 0usize;
        loop {
            if cmds.is_empty() {
                return Ok(count);
            }
            let cmd = cmds.pop_front().expect("cmds pop front never be empty");
            if !cmd.can_cycle() {
                cmd.set_error(&AsError::ProxyFail);
                count += 1;
                continue;
            }
            let key_hash = cmd.key_hash(&self.hash_tag, fnv1a64);

            let addr = if let Some(name) = self.ring.borrow().get_node(key_hash) {
                self.get_node(name.to_string())
            } else {
                return Ok(count);
            };
            let mut conns = self.conns.borrow_mut();

            if let Some(sender) = conns.get_mut(&addr).map(|x| x.sender()) {
                match sender.start_send(cmd) {
                    Ok(AsyncSink::Ready) => {
                        count += 1;
                    }
                    Ok(AsyncSink::NotReady(cmd)) => {
                        cmds.push_front(cmd);
                        return Ok(count);
                    }
                    Err(se) => {
                        let cmd = se.into_inner();
                        cmd.add_cycle();
                        cmds.push_front(cmd);
                        let sender = connect(
                            &self.cc.borrow().name,
                            &addr,
                            self.cc.borrow().read_timeout,
                            self.cc.borrow().write_timeout,
                        )?;
                        conns.insert(&addr, sender);
                        return Ok(count);
                    }
                }
            } else {
                cmds.push_front(cmd);
                let sender = connect(
                    &self.cc.borrow().name,
                    &addr,
                    self.cc.borrow().read_timeout,
                    self.cc.borrow().write_timeout,
                )?;
                conns.insert(&addr, sender);
                return Ok(count);
            }
        }
    }
}

struct Conns<T> {
    _marker: PhantomData<T>,
    inner: HashMap<String, Conn<Sender<T>>>,
}

impl<T> Conns<T> {
    fn addrs(&self) -> HashSet<String> {
        self.inner.keys().cloned().collect()
    }

    fn get_mut(&mut self, s: &str) -> Option<&mut Conn<Sender<T>>> {
        self.inner.get_mut(s)
    }

    fn remove(&mut self, addr: &str) -> Option<Conn<Sender<T>>> {
        self.inner.remove(addr)
    }

    fn insert(&mut self, s: &str, sender: Sender<T>) {
        let conn = Conn {
            addr: s.to_string(),
            sender,
        };
        self.inner.insert(s.to_string(), conn);
    }
}

impl<T> Default for Conns<T> {
    fn default() -> Conns<T> {
        Conns {
            inner: HashMap::new(),
            _marker: Default::default(),
        }
    }
}

#[allow(unused)]
struct Conn<S> {
    addr: String,
    sender: S,
}

impl<S> Conn<S> {
    fn sender(&mut self) -> &mut S {
        &mut self.sender
    }
}

fn connect<T>(
    cluster: &str,
    node: &str,
    rt: Option<u64>,
    _wt: Option<u64>,
) -> Result<Sender<T>, AsError>
where
    T: Request + 'static,
{
    let node_addr = node.to_string();
    let node_new = node_addr.clone();
    let cluster = cluster.to_string();
    let (tx, rx) = channel(1024 * 8);
    let amt = lazy(|| -> Result<(), ()> { Ok(()) })
        .map_err(|_| AsError::None)
        .map(move |_| gethostbyname(node_addr.as_str()))
        .flatten()
        .and_then(|addr| {
            let report_addr = format!("{:?}", &addr);
            TcpStream::connect(&addr)
                .timeout(Duration::from_millis(100))
                .map_err(move |terr| {
                    error!(
                        "fail to connect ot backend due to {} err {}",
                        report_addr, terr
                    );
                    AsError::SystemError
                })
        })
        .map_err(|err| error!("connect failed by error {}", err))
        .then(move |srslt: Result<TcpStream, ()>| {
            if let Ok(sock) = srslt {
                sock.set_nodelay(true).expect("set nodelay must ok");
                let codec = T::BackCodec::default();
                let (sink, stream) = codec.framed(sock).split();
                let backend =
                    back::Back::new(cluster, node_new, rx, sink, stream, rt.unwrap_or(1000));
                current_thread::spawn(backend);
            } else {
                let blackhole = back::Blackhole::new(node_new, rx);
                current_thread::spawn(blackhole);
            }
            Ok(())
        })
        .and_then(|_| Ok(()));
    current_thread::spawn(amt);
    Ok(tx)
}

struct ServerLine {
    addr: String,
    weight: usize,
    alias: Option<String>,
}

impl ServerLine {
    fn parse_servers(servers: &[String]) -> Result<Vec<ServerLine>, AsError> {
        // e.g.: 192.168.1.2:1074:10 redis-20
        let mut sl = Vec::with_capacity(servers.len());
        for server in servers {
            let mut iter = server.split(' ');
            let first_part = iter.next().expect("first partation must exists");
            if first_part.chars().filter(|x| *x == ':').count() == 1 {
                let alias = iter.next().map(|x| x.to_string());
                sl.push(ServerLine {
                    addr: first_part.to_string(),
                    weight: 1,
                    alias,
                });
            }

            let mut fp_sp = first_part.rsplitn(2, ':').filter(|x| !x.is_empty());
            let weight = {
                let weight_str = fp_sp.next().unwrap_or("1");
                weight_str.parse::<usize>()?
            };
            let addr = fp_sp.next().expect("addr never be absent").to_owned();
            drop(fp_sp);
            let alias = iter.next().map(|x| x.to_string());
            sl.push(ServerLine {
                addr,
                weight,
                alias,
            });
        }
        Ok(sl)
    }

    fn unwrap_spot(sls: &[ServerLine]) -> (Vec<String>, Vec<String>, Vec<usize>) {
        let mut nodes = Vec::new();
        let mut alias = Vec::new();
        let mut weights = Vec::new();
        for sl in sls {
            if sl.alias.is_some() {
                alias.push(
                    sl.alias
                        .as_ref()
                        .cloned()
                        .expect("node addr can't be empty"),
                );
            }
            nodes.push(sl.addr.clone());
            weights.push(sl.weight);
        }
        (nodes, alias, weights)
    }
}

pub(crate) fn spawn(cc: ClusterConfig) {
    match cc.cache_type {
        CacheType::Redis => Cluster::<redis::Cmd>::run(cc).unwrap(),
        CacheType::Memcache | CacheType::MemcacheBinary => Cluster::<mc::Cmd>::run(cc).unwrap(),
        _ => unreachable!(),
    };
}
