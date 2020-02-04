pub mod back;
pub mod fnv;
pub mod front;
pub mod ketama;
pub mod ping;

use futures::future::ok;
use futures::lazy;
use futures::task::Task;
use futures::unsync::mpsc::{channel, Sender};
use futures::{AsyncSink, Future, Sink, Stream};
use tokio::codec::{Decoder, Encoder};
use tokio::net::TcpStream;
use tokio::runtime::current_thread;

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::rc::Rc;
use std::thread::{Builder, JoinHandle};

use crate::protocol::{mc, redis};

#[cfg(feature = "metrics")]
use crate::metrics::{front_conn_incr, thread_incr};

use crate::com::meta::meta_init;
use crate::com::AsError;
use crate::com::{create_reuse_port_listener, set_read_write_timeout};
use crate::com::{CacheType, ClusterConfig};
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

    #[cfg(feature = "metrics")]
    fn mark_total(&self, cluster: &str);
    #[cfg(feature = "metrics")]
    fn mark_remote(&self, cluster: &str);

    fn is_done(&self) -> bool;
    fn is_error(&self) -> bool;

    fn add_cycle(&self);
    fn can_cycle(&self) -> bool;

    fn valid(&self) -> bool;

    fn set_reply<R: IntoReply<Self::Reply>>(&self, t: R);
    fn set_error(&self, t: &AsError);
}

pub struct Cluster<T> {
    pub cc: ClusterConfig,
    hash_tag: Vec<u8>,
    spots: HashMap<String, usize>,
    alias: HashMap<String, String>,

    _maker: PhantomData<T>,
    ring: RefCell<HashRing>,
    conns: RefCell<Conns<T>>,
}

impl<T: Request + 'static> Cluster<T> {
    pub(crate) fn run(cc: ClusterConfig) -> Result<(), AsError> {
        let addr = cc
            .listen_addr
            .clone()
            .parse::<SocketAddr>()
            .expect("parse socket never fail");
        let fut = ok::<ClusterConfig, AsError>(cc)
            .and_then(|cc| {
                let sls = ServerLine::parse_servers(&cc.servers).expect("parse server line failed");
                let (nodes, alias, weights) = ServerLine::unwrap_spot(&sls);
                let alias_map: HashMap<_, _> = alias
                    .clone()
                    .into_iter()
                    .zip(nodes.clone().into_iter())
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
                let hash_tag = cc
                    .hash_tag
                    .as_ref()
                    .map(|x| x.as_bytes().to_vec())
                    .unwrap_or(vec![]);
                let hash_ring = if alias.is_empty() {
                    HashRing::new(nodes, weights)?
                } else {
                    HashRing::new(alias, weights)?
                };
                let ring = RefCell::new(hash_ring);
                let conns: RefCell<Conns<T>> = RefCell::new(Conns::default());
                let cluster = Cluster {
                    cc,
                    _maker: Default::default(),
                    hash_tag,
                    ring,
                    conns,
                    alias: alias_map,
                    spots: spots_map,
                };
                Ok(Rc::new(cluster))
            })
            .and_then(|cluster| {
                let addrs: Vec<_> = if cluster.has_alias() {
                    cluster.alias.values().map(|x| x.to_string()).collect()
                } else {
                    cluster.spots.keys().map(|x| x.to_string()).collect()
                };
                {
                    let mut conns = cluster.conns.borrow_mut();
                    for addr in addrs {
                        if let Ok(conn) = connect(
                            &cluster.cc.name,
                            &addr,
                            cluster.cc.read_timeout.clone(),
                            cluster.cc.write_timeout.clone(),
                        ) {
                            conns.insert(&addr, conn);
                        } else {
                            warn!("fail to connect to {} {}", cluster.cc.name, addr);
                        }
                    }
                }
                Ok(cluster)
            })
            .and_then(|cluster| {
                let rc_cluster = cluster.clone();
                let ping_fail_limit = cluster.cc.ping_fail_limit.as_ref().cloned().unwrap_or(0);
                if ping_fail_limit > 0 {
                    let ping_interval =
                        cluster.cc.ping_interval.as_ref().cloned().unwrap_or(300000);
                    let ping_succ_interval = cluster
                        .cc
                        .ping_succ_interval
                        .as_ref()
                        .cloned()
                        .unwrap_or(1000);
                    for (alias, node) in cluster.alias.iter() {
                        let ping = ping::Ping::new(
                            Rc::downgrade(&cluster),
                            alias.to_string(),
                            node.to_string(),
                            ping_interval,
                            ping_succ_interval,
                            ping_fail_limit,
                        );
                        current_thread::spawn(ping);
                    }
                } else {
                    info!("skip setup ping for {}", cluster.cc.name);
                }
                Ok(rc_cluster)
            })
            .and_then(|cluster| {
                let rc_cluster = cluster.clone();
                let listen = create_reuse_port_listener(&addr).expect("bind never fail");
                let service = listen
                    .incoming()
                    .for_each(move |sock| {
                        sock.set_nodelay(true).expect("set nodelay must ok");
                        let client = sock.peer_addr().expect("peer must have addr");
                        let client_str = format!("{}", client);
                        let codec = T::FrontCodec::default();
                        let (output, input) = codec.framed(sock).split();
                        #[cfg(feature = "metrics")]
                        front_conn_incr(&cluster.cc.name);
                        let fut = front::Front::new(client_str, cluster.clone(), input, output);
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

    fn has_alias(&self) -> bool {
        !self.alias.is_empty()
    }

    fn get_node(&self, name: String) -> String {
        if !self.has_alias() {
            return name;
        }

        self.alias
            .get(&name)
            .expect("alias name must exists")
            .to_string()
    }

    pub(crate) fn add_node(&self, name: String) -> Result<(), AsError> {
        if let Some(weight) = self.spots.get(&name).cloned() {
            let addr = self.get_node(name.clone());
            let conn = connect(
                &self.cc.name,
                &addr,
                self.cc.read_timeout.clone(),
                self.cc.write_timeout.clone(),
            )?;
            self.conns.borrow_mut().insert(&addr, conn);
            self.ring.borrow_mut().add_node(name, weight);
        }
        Ok(())
    }

    pub(crate) fn remove_node(&self, name: String) {
        self.ring.borrow_mut().del_node(&name);
        let node = self.get_node(name);
        if let Some(_) = self.conns.borrow_mut().remove(&node) {
            info!("dropping backend connection of {} due active delete", node);
        }
    }

    pub(crate) fn reconnect(&self, addr: &str) {
        let mut conns = self.conns.borrow_mut();
        debug!("trying to reconnect to {}", addr);
        conns.remove(addr);
        match connect(
            &self.cc.name,
            &addr,
            self.cc.read_timeout.clone(),
            self.cc.write_timeout.clone(),
        ) {
            Ok(sender) => conns.insert(&addr, sender),
            Err(err) => {
                error!("fail to reconnect to {} due {:?}", addr, err);
            }
        }
    }

    pub fn dispatch_to(&self, addr: &str, cmd: T) -> Result<AsyncSink<T>, AsError> {
        if !cmd.can_cycle() {
            // debug!("unable retry due can't cycle");
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
                    &self.cc.name,
                    &addr,
                    self.cc.read_timeout.clone(),
                    self.cc.write_timeout.clone(),
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
                            &self.cc.name,
                            &addr,
                            self.cc.read_timeout.clone(),
                            self.cc.write_timeout.clone(),
                        )?;
                        conns.insert(&addr, sender);
                        return Ok(count);
                    }
                }
            } else {
                cmds.push_front(cmd);
                let sender = connect(
                    &self.cc.name,
                    &addr,
                    self.cc.read_timeout.clone(),
                    self.cc.write_timeout.clone(),
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
    wt: Option<u64>,
) -> Result<Sender<T>, AsError>
where
    T: Request + 'static,
{
    let node_addr = node.to_string();
    let node_new = node_addr.clone();
    let cluster = cluster.to_string();
    let (tx, rx) = channel(1024 * 8);
    let amt = lazy(|| -> Result<(), ()> { Ok(()) })
        .and_then(move |_| {
            let node_clone = node_addr.clone();
            node_addr
                .as_str()
                .parse()
                .map_err(|err| error!("fail to parse addr {} due to {:?}", node_clone, err))
        })
        .and_then(|addr: SocketAddr| {
            TcpStream::connect(&addr)
                .map_err(move |err| error!("fail to connect to {} due to {:?}", &addr, err))
        })
        .then(move |srslt: Result<TcpStream, ()>| {
            if let Ok(sock) = srslt {
                let sock = set_read_write_timeout(sock, rt, wt).expect("set timeout must be ok");
                sock.set_nodelay(true).expect("set nodelay must ok");
                let codec = T::BackCodec::default();
                let (sink, stream) = codec.framed(sock).split();
                let backend = back::Back::new(cluster, node_new, rx, sink, stream);
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

pub fn run(cc: ClusterConfig, ip: Option<String>) -> Vec<JoinHandle<()>> {
    let worker = cc.thread.unwrap_or(4);
    (0..worker)
        .into_iter()
        .map(|_index| {
            let builder = Builder::new();
            let cc = cc.clone();
            let ip = ip.clone();
            builder
                .name(cc.name.clone())
                .spawn(move || {
                    meta_init(cc.clone(), ip);
                    #[cfg(feature = "metrics")]
                    thread_incr();
                    match cc.cache_type {
                        CacheType::Redis => Cluster::<redis::Cmd>::run(cc).unwrap(),
                        CacheType::Memcache | CacheType::MemcacheBinary => {
                            Cluster::<mc::Cmd>::run(cc).unwrap()
                        }
                        _ => unreachable!(),
                    }
                })
                .expect("fail to spawn worker thread")
        })
        .collect()
}
