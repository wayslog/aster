pub mod back;
pub mod fetcher;
pub mod front;
pub mod init;
pub mod redirect;

use crate::com::create_reuse_port_listener;
use crate::com::meta::meta_init;
use crate::com::set_read_write_timeout;
use crate::com::AsError;
use crate::com::ClusterConfig;
use crate::protocol::redis::{new_read_only_cmd, RedisHandleCodec, RedisNodeCodec};
use crate::protocol::redis::{Cmd, ReplicaLayout, SLOTS_COUNT};
use crate::proxy::cluster::fetcher::SingleFlightTrigger;
use crate::utils::crc::crc16;

use crate::metrics::{front_conn_incr, thread_incr};

// use failure::Error;
use futures::future::ok;
use futures::future::*;
use futures::task;
use futures::unsync::mpsc::{channel, Sender};
use futures::AsyncSink;
use futures::{Sink, Stream};

use tokio::net::TcpStream;
use tokio::prelude::FutureExt;
use tokio::runtime::current_thread;
use tokio::timer::Interval;
use tokio_codec::Decoder;

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Redirect {
    Move { slot: usize, to: String },
    Ask { slot: usize, to: String },
}

impl Redirect {
    pub(crate) fn is_ask(&self) -> bool {
        match self {
            Redirect::Ask { .. } => true,
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct Redirection {
    pub target: Redirect,
    pub cmd: Cmd,
}

impl Redirection {
    pub(crate) fn new(is_move: bool, slot: usize, to: String, cmd: Cmd) -> Redirection {
        if is_move {
            Redirection {
                target: Redirect::Move { slot, to },
                cmd,
            }
        } else {
            Redirection {
                target: Redirect::Ask { slot, to },
                cmd,
            }
        }
    }
}

pub use Redirect::{Ask, Move};

pub struct RedirectFuture {}

pub struct Cluster {
    pub cc: RefCell<ClusterConfig>,

    slots: RefCell<Slots>,
    conns: RefCell<Conns>,
    hash_tag: Vec<u8>,
    read_from_slave: bool,

    moved: Sender<Redirection>,
    fetch: RefCell<Option<Rc<SingleFlightTrigger>>>,
    latest: RefCell<Instant>,
}

impl Cluster {
    pub(crate) fn run(cc: ClusterConfig, replica: ReplicaLayout) -> Result<(), AsError> {
        let addr = cc
            .listen_addr
            .clone()
            .parse::<SocketAddr>()
            .expect("parse socket never fail");
        let fut = ok::<ClusterConfig, AsError>(cc)
            .and_then(|mut cc| {
                let read_from_slave = cc.read_from_slave.clone().unwrap_or(false);
                let hash_tag = cc
                    .hash_tag
                    .clone()
                    .map(|x| x.as_bytes().to_vec())
                    .unwrap_or(vec![]);
                let mut slots = Slots::default();
                let (masters, replicas) = replica;
                slots.try_update_all(masters, replicas);
                let (moved, moved_rx) = channel(10240);

                let all_masters = slots.get_all_masters();
                let mut all_lived = HashSet::new();
                cc.servers = all_masters.iter().map(|x| x.clone()).collect();
                let all_servers = cc.servers.clone();
                let mut conns = Conns::default();
                for master in all_servers.into_iter() {
                    let conn = ConnBuilder::new()
                        .moved(moved.clone())
                        .cluster(cc.name.clone())
                        .node(master.clone())
                        .read_timeout(cc.read_timeout.clone())
                        .write_timeout(cc.write_timeout.clone())
                        .connect()?;
                    conns.insert(&master, conn);
                    all_lived.insert(master.clone());
                }

                if read_from_slave {
                    let all_slaves = slots.get_all_replicas();
                    for slave in all_slaves.into_iter() {
                        let conn = ConnBuilder::new()
                            .moved(moved.clone())
                            .cluster(cc.name.clone())
                            .node(slave.clone())
                            .read_timeout(cc.read_timeout.clone())
                            .write_timeout(cc.write_timeout.clone())
                            .replica(true)
                            .connect()?;
                        conns.insert(&slave, conn);
                        all_lived.insert(slave.clone());
                    }
                }
                let cluster = Cluster {
                    cc: RefCell::new(cc),
                    hash_tag,
                    read_from_slave,
                    moved,
                    slots: RefCell::new(slots),
                    conns: RefCell::new(conns),
                    fetch: RefCell::new(None),
                    latest: RefCell::new(Instant::now()),
                };
                Ok((cluster, moved_rx))
            })
            .and_then(|(cluster, moved_rx)| {
                let rc_cluster = Rc::new(cluster);
                let redirect_handler = redirect::RedirectHandler::new(rc_cluster.clone(), moved_rx);
                current_thread::spawn(redirect_handler);
                Ok(rc_cluster)
            })
            .and_then(|rc_cluster| {
                let interval_millis = rc_cluster.cc.borrow().fetch_interval.unwrap_or(1000);
                let interval = Interval::new(
                    Instant::now() + Duration::from_millis(interval_millis),
                    Duration::from_millis(interval_millis),
                );
                let interval_stream = interval
                    .map(|_| fetcher::TriggerBy::Interval)
                    .map_err(|_| AsError::None);

                let (tx, rx) = channel(1024);
                let trigger = Rc::new(SingleFlightTrigger::new(1, tx));
                let _ = rc_cluster.fetch.borrow_mut().replace(trigger);

                let trigger_stream = rx.map_err(|_| AsError::None);
                let fetch =
                    fetcher::Fetch::new(rc_cluster.clone(), trigger_stream.select(interval_stream));
                current_thread::spawn(fetch);
                Ok(rc_cluster)
            })
            .and_then(move |cluster| {
                let listen = create_reuse_port_listener(&addr).expect("bind never fail");
                let service = listen
                    .incoming()
                    .for_each(move |sock| {
                        let cluster = cluster.clone();
                        if let Err(err) = sock.set_nodelay(true) {
                            warn!(
                                "cluster {} fail to set nodelay but skip, due to {:?}",
                                cluster.cc.borrow().name,
                                err
                            );
                        }
                        let client_str = match sock.peer_addr() {
                            Ok(client) => format!("{}", client),
                            Err(err) => {
                                error!(
                                    "cluster {} fail to get client name due to {:?}",
                                    cluster.cc.borrow().name,
                                    err
                                );
                                "unknown".to_string()
                            }
                        };

                        front_conn_incr(&cluster.cc.borrow().name);
                        let codec = RedisHandleCodec {};
                        let (output, input) = codec.framed(sock).split();
                        let fut = front::Front::new(client_str, cluster, input, output);
                        current_thread::spawn(fut);
                        Ok(())
                    })
                    .map_err(|err| {
                        error!("fail to accept incomming sock due {}", err);
                    });
                current_thread::spawn(service);
                Ok(())
            });
        current_thread::spawn(
            fut.map_err(|err| error!("fail to create cluster server due to {}", err)),
        );
        Ok(())
    }
}

impl Cluster {
    fn get_addr(&self, slot: usize, is_read: bool) -> String {
        // trace!("get slot={} and is_read={}", slot, is_read);
        if self.read_from_slave && is_read {
            if let Some(replica) = self.slots.borrow().get_replica(slot) {
                if replica != "" {
                    return replica.to_string();
                }
            }
        }
        self.slots
            .borrow()
            .get_master(slot)
            .map(|x| x.to_string())
            .expect("master addr never be empty")
    }

    pub fn trigger_fetch(&self, trigger_by: fetcher::TriggerBy) {
        if let Some(trigger) = self.fetch.borrow().clone() {
            let if_triggered = match trigger_by {
                fetcher::TriggerBy::Moved => {
                    trigger.try_trigger();
                    true
                }
                _ => trigger.try_trigger(),
            };

            if if_triggered {
                info!("succeed trigger fetch process by {:?}", trigger_by);
                return;
            }
        } else {
            warn!("fail to trigger fetch process due to trigger event uninitialed");
        }
    }

    pub fn dispatch_to(&self, addr: &str, cmd: Cmd) -> Result<AsyncSink<Cmd>, AsError> {
        if !cmd.borrow().can_cycle() {
            cmd.set_error(AsError::ClusterFailDispatch);
            return Ok(AsyncSink::Ready);
        }
        let mut conns = self.conns.borrow_mut();
        loop {
            if let Some(sender) = conns.get_mut(addr).map(|x| x.sender()) {
                match sender.start_send(cmd) {
                    Ok(ret) => {
                        return Ok(ret);
                    }
                    Err(_se) => {
                        warn!("fail to send to backend {} ", addr);
                        self.connect(&addr, &mut conns)?;
                        return Err(AsError::BackendClosedError(addr.to_string()));
                    }
                }
            } else {
                self.connect(&addr, &mut conns)?;
            }
        }
    }

    fn inner_dispatch_all(&self, cmds: &mut VecDeque<Cmd>) -> Result<usize, AsError> {
        let mut count = 0usize;
        loop {
            if cmds.is_empty() {
                return Ok(count);
            }
            let cmd = cmds.pop_front().expect("cmds pop front never be empty");
            if !cmd.borrow().can_cycle() {
                cmd.set_error(AsError::ProxyFail);
                continue;
            }
            let slot = {
                let hash_tag = self.hash_tag.as_ref();
                let signed = cmd.borrow().key_hash(hash_tag, crc16) as usize;
                signed % SLOTS_COUNT
            };

            let addr = self.get_addr(slot, cmd.borrow().is_read());
            let mut conns = self.conns.borrow_mut();

            if let Some(sender) = conns.get_mut(&addr).map(|x| x.sender()) {
                match sender.start_send(cmd) {
                    Ok(AsyncSink::Ready) => {
                        // trace!("success start command into backend");
                        count += 1;
                    }
                    Ok(AsyncSink::NotReady(cmd)) => {
                        cmd.borrow_mut().add_cycle();
                        cmds.push_front(cmd);
                        return Ok(count);
                    }
                    Err(se) => {
                        let cmd = se.into_inner();
                        cmd.borrow_mut().add_cycle();
                        cmds.push_front(cmd);
                        self.connect(&addr, &mut conns)?;
                        return Ok(count);
                    }
                }
            } else {
                cmds.push_front(cmd);
                self.connect(&addr, &mut conns)?;
                return Ok(count);
            }
        }
    }

    pub fn dispatch_all(&self, cmds: &mut VecDeque<Cmd>) -> Result<usize, AsError> {
        let count = self.inner_dispatch_all(cmds)?;
        if count != 0 {
            self.latest.replace(Instant::now());
        }
        Ok(count)
    }

    #[allow(unused)]
    pub(crate) fn since_latest(&self) -> Duration {
        self.latest.borrow().elapsed()
    }

    pub(crate) fn try_update_all_slots(&self, layout: ReplicaLayout) -> bool {
        let (masters, replicas) = layout;
        let updated = self
            .slots
            .borrow_mut()
            .try_update_all(masters.clone(), replicas);
        if updated {
            let handle = &mut self.cc.borrow_mut();
            handle.servers.clear();
            handle.servers.extend_from_slice(&masters);
        }
        updated
    }

    pub(crate) fn update_slot(&self, slot: usize, addr: String) -> bool {
        debug_assert!(slot <= SLOTS_COUNT);
        self.slots.borrow_mut().update_slot(slot, addr)
    }

    pub(crate) fn connect(&self, addr: &str, conns: &mut Conns) -> Result<(), AsError> {
        let is_replica = !self.slots.borrow().is_master(addr);

        let sender = ConnBuilder::new()
            .moved(self.moved.clone())
            .cluster(self.cc.borrow().name.clone())
            .node(addr.to_string())
            .read_timeout(self.cc.borrow().read_timeout.clone())
            .write_timeout(self.cc.borrow().write_timeout.clone())
            .fetch(
                self.fetch
                    .borrow()
                    .as_ref()
                    .map(|x| Rc::downgrade(x))
                    .unwrap_or(Weak::new()),
            )
            .replica(is_replica)
            .connect()?;
        conns.insert(&addr, sender);
        Ok(())
    }
}

pub(crate) struct Conns {
    inner: HashMap<String, Conn<Sender<Cmd>>>,
}

impl Conns {
    fn get_mut(&mut self, s: &str) -> Option<&mut Conn<Sender<Cmd>>> {
        self.inner.get_mut(s)
    }

    fn insert(&mut self, s: &str, sender: Sender<Cmd>) {
        let conn = Conn {
            addr: s.to_string(),
            sender,
        };
        self.inner.insert(s.to_string(), conn);
    }
}

impl Default for Conns {
    fn default() -> Conns {
        Conns {
            inner: HashMap::new(),
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

struct Slots {
    masters: Vec<String>,
    replicas: Vec<Replica>,

    all_masters: HashSet<String>,
    all_replicas: HashSet<String>,
}

impl Slots {
    fn try_update_all(&mut self, masters: Vec<String>, replicas: Vec<Vec<String>>) -> bool {
        let mut changed = false;
        for i in 0..SLOTS_COUNT {
            if self.masters[i] != masters[i] {
                changed = true;
                self.masters[i] = masters[i].clone();
                self.all_masters.insert(masters[i].clone());
            }
        }

        for i in 0..SLOTS_COUNT {
            let len_not_eqal = self.replicas[i].addrs.len() != replicas[i].len();
            if len_not_eqal || self.replicas[i].addrs.as_slice() != replicas[i].as_slice() {
                self.replicas[i] = Replica {
                    addrs: replicas[i].clone(),
                    current: Cell::new(0),
                };
                self.all_replicas.extend(replicas[i].clone().into_iter());
                changed = true;
            }
        }

        changed
    }

    fn update_slot(&mut self, slot: usize, addr: String) -> bool {
        let old = self.masters[slot].clone();
        self.masters[slot] = addr.clone();
        self.all_masters.insert(addr.clone());
        old != addr
    }

    fn get_master(&self, slot: usize) -> Option<&str> {
        self.masters.get(slot).map(|x| x.as_str())
    }

    fn get_replica(&self, slot: usize) -> Option<&str> {
        self.replicas.get(slot).map(|x| x.get_replica())
    }

    fn get_all_masters(&self) -> HashSet<String> {
        self.all_masters.clone()
    }

    fn get_all_replicas(&self) -> HashSet<String> {
        self.all_replicas.clone()
    }

    fn is_master(&self, addr: &str) -> bool {
        self.all_masters.contains(addr)
    }
}

impl Default for Slots {
    fn default() -> Slots {
        let mut masters = Vec::with_capacity(SLOTS_COUNT);
        masters.resize(SLOTS_COUNT, "".to_string());
        let mut replicas = Vec::with_capacity(SLOTS_COUNT);
        replicas.resize(SLOTS_COUNT, Replica::default());
        Slots {
            masters,
            replicas,
            all_masters: HashSet::new(),
            all_replicas: HashSet::new(),
        }
    }
}

#[derive(Clone)]
struct Replica {
    addrs: Vec<String>,
    current: Cell<usize>,
}

impl Replica {
    fn get_replica(&self) -> &str {
        if self.addrs.is_empty() {
            return "";
        }

        let current = self.current.get();
        let len = self.addrs.len();
        let now = self.current.get();
        self.current.set((now + 1) % len);
        &self.addrs[current]
    }
}

impl Default for Replica {
    fn default() -> Replica {
        Replica {
            addrs: Vec::with_capacity(0),
            current: Cell::new(0usize),
        }
    }
}

pub(crate) struct ConnBuilder {
    cluster: Option<String>,
    node: Option<String>,
    moved: Option<Sender<Redirection>>,
    rt: Option<u64>,
    wt: Option<u64>,
    replica: bool,
    fetch: Weak<SingleFlightTrigger>,
}

impl ConnBuilder {
    pub(crate) fn new() -> ConnBuilder {
        ConnBuilder {
            cluster: None,
            node: None,
            moved: None,
            rt: Some(1000),
            wt: Some(1000),
            replica: false,
            fetch: Weak::new(),
        }
    }

    pub(crate) fn fetch(self, fetch: Weak<SingleFlightTrigger>) -> Self {
        let mut cb = self;
        cb.fetch = fetch;
        cb
    }

    pub(crate) fn cluster(self, cluster: String) -> Self {
        let mut cb = self;
        cb.cluster = Some(cluster);
        cb
    }

    pub(crate) fn moved(self, moved: Sender<Redirection>) -> Self {
        let mut cb = self;
        cb.moved = Some(moved);
        cb
    }

    pub(crate) fn read_timeout(self, rt: Option<u64>) -> Self {
        let mut cb = self;
        cb.rt = rt;
        cb
    }

    pub(crate) fn write_timeout(self, wt: Option<u64>) -> Self {
        let mut cb = self;
        cb.wt = wt;
        cb
    }

    pub(crate) fn node(self, node: String) -> Self {
        let mut cb = self;
        cb.node = Some(node);
        cb
    }

    pub(crate) fn replica(self, is_replica: bool) -> Self {
        let mut cb = self;
        cb.replica = is_replica;
        cb
    }

    pub(crate) fn check_valid(&self) -> bool {
        true && self.node.is_some() && self.cluster.is_some() && self.moved.is_some()
    }

    pub(crate) fn connect(self) -> Result<Sender<Cmd>, AsError> {
        if !self.check_valid() {
            error!(
                "fail to open connection to backend {} due param is valid",
                self.node.as_ref().map(|x| x.as_ref()).unwrap_or("unknown")
            );
            return Err(AsError::BadConfig("backend connection config".to_string()));
        }

        let node_addr = self.node.expect("addr must be checked first");
        let node_addr_clone = node_addr.clone();
        let cluster = self
            .cluster
            .expect("cluster name must be checked first")
            .to_string();
        let rt = self.rt.clone();
        let wt = self.wt.clone();
        let moved = self.moved.expect("must be checked first");
        let fetch = self.fetch.clone();

        let (mut tx, rx) = channel(1024 * 8);
        let amt = lazy(|| -> Result<(), ()> { Ok(()) })
            .and_then(move |_| {
                let node_clone = node_addr.clone();
                node_addr
                    .as_str()
                    .parse()
                    .map_err(|err| error!("fail to parse addr {} due to {:?}", node_clone, err))
            })
            .and_then(|addr| {
                let report_addr = format!("{:?}", &addr);
                TcpStream::connect(&addr)
                    .timeout(Duration::from_millis(100))
                    .map_err(move |err| error!("fail to connect to {} {:?}", &report_addr, err))
            })
            .then(move |sock| {
                if let Ok(sock) = sock {
                    let sock =
                        set_read_write_timeout(sock, rt, wt).expect("set timeout must be ok");
                    if let Err(_) = sock.set_nodelay(true) {
                        warn!("fail to set set nodelay when connect to backend but ignore");
                    }

                    let codec = RedisNodeCodec {};
                    let (sink, stream) = codec.framed(sock).split();
                    let backend =
                        back::Back::new(cluster, node_addr_clone, rx, sink, stream, moved);
                    current_thread::spawn(backend);
                } else {
                    error!("fail to conenct to backend {}", node_addr_clone);
                    let blackhole = back::Blackhole::new(node_addr_clone, rx);
                    current_thread::spawn(blackhole);
                    if let Some(trigger) = fetch.upgrade() {
                        trigger.try_trigger();
                    }
                }
                Ok(())
            });
        current_thread::spawn(amt);
        if self.replica {
            let mut cmd = new_read_only_cmd();
            cmd.reregister(task::current());
            Self::silence_send_req(cmd, &mut tx);
        }
        Ok(tx)
    }

    fn silence_send_req(cmd: Cmd, tx: &mut Sender<Cmd>) {
        match tx.start_send(cmd) {
            Ok(AsyncSink::Ready) => {
                debug!("success dispatch to read only replica node");
            }
            Ok(AsyncSink::NotReady(_)) => {
                warn!("fail to initial backend connection of replica due to send fail");
            }
            Err(err) => {
                warn!(
                    "fail to initial backend connection of replica due to {}",
                    err
                );
            }
        }
    }
}

pub fn run(cc: ClusterConfig, ip: Option<String>) -> Vec<JoinHandle<()>> {
    let worker = cc.thread.unwrap_or(4);
    (0..worker)
        .into_iter()
        .map(|_index| {
            let builder = thread::Builder::new();
            let cc = cc.clone();
            let ip = ip.clone();
            builder
                .name(cc.name.clone())
                .spawn(move || {
                    meta_init(cc.clone(), ip);

                    thread_incr();

                    current_thread::block_on_all(
                        init::Initializer::new(cc)
                            .map_err(|err| error!("fail to init cluster due to {}", err)),
                    )
                    .unwrap();
                })
                .expect("fail to spawn worker thread")
        })
        .collect()
}
