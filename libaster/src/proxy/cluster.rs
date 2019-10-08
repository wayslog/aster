pub mod back;
pub mod fetcher;
pub mod front;
pub mod init;
pub mod redirect;

use crate::com::create_reuse_port_listener;
use crate::com::set_read_write_timeout;
use crate::com::AsError;
use crate::com::ClusterConfig;
use crate::protocol::redis::{Cmd, ReplicaLayout, SLOTS_COUNT};
use crate::protocol::redis::{RedisHandleCodec, RedisNodeCodec};
use crate::utils::crc::crc16;

// use failure::Error;
use futures::future::*;

use futures::future::ok;
use futures::unsync::mpsc::{channel, Sender};
use futures::AsyncSink;
use futures::{Sink, Stream};

use tokio::net::TcpStream;
use tokio::runtime::current_thread;
use tokio_codec::Decoder;

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;
use std::thread::{self, JoinHandle};

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

#[derive(Debug, Clone)]
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
    pub cc: ClusterConfig,

    slots: RefCell<Slots>,
    conns: RefCell<Conns>,
    hash_tag: Vec<u8>,
    read_from_slave: bool,

    moved: Sender<Redirection>,
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
                slots.try_update_all(replica);
                let (moved, moved_rx) = channel(10240);

                let all_masters = slots.get_all_masters();
                cc.servers = all_masters.iter().map(|x| x.clone()).collect();
                let all_servers = cc.servers.clone();
                let mut conns = Conns::default();
                for master in all_servers.into_iter() {
                    let conn = connect(
                        moved.clone(),
                        &master,
                        cc.read_timeout.clone(),
                        cc.write_timeout.clone(),
                    )?;
                    conns.insert(&master, conn);
                }

                if read_from_slave {
                    let all_slaves = slots.get_all_slaves();
                    for slave in all_slaves.into_iter() {
                        let conn = connect(
                            moved.clone(),
                            &slave,
                            cc.read_timeout.clone(),
                            cc.write_timeout.clone(),
                        )?;
                        conns.insert(&slave, conn);
                    }
                }
                let cluster = Cluster {
                    cc,
                    hash_tag,
                    read_from_slave,
                    moved,
                    slots: RefCell::new(slots),
                    conns: RefCell::new(conns),
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
                let fetcher = fetcher::Fetcher::new(rc_cluster.clone());
                current_thread::spawn(fetcher);
                Ok(rc_cluster)
            })
            .and_then(move |cluster| {
                let listen = create_reuse_port_listener(&addr).expect("bind never fail");
                let service = listen
                    .incoming()
                    .for_each(move |sock| {
                        let client = sock.peer_addr().expect("peer must have addr");
                        let client_str = format!("{}", client);
                        let codec = RedisHandleCodec {};
                        let (output, input) = codec.framed(sock).split();
                        let fut = front::Front::new(client_str, cluster.clone(), input, output);
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
        trace!("get slot={} and is_read={}", slot, is_read);
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

    pub fn dispatch_to(&self, addr: &str, cmd: Cmd) -> Result<AsyncSink<Cmd>, AsError> {
        if !cmd.borrow().can_cycle() {
            cmd.set_reply(AsError::ClusterFailDispatch);
            return Ok(AsyncSink::Ready);
        }
        let mut conns = self.conns.borrow_mut();
        loop {
            if let Some(sender) = conns.get_mut(addr).map(|x| x.sender()) {
                match sender.start_send(cmd) {
                    Ok(ret) => {
                        return Ok(ret);
                    }
                    Err(se) => {
                        let cmd = se.into_inner();
                        return Ok(AsyncSink::NotReady(cmd));
                    }
                }
            } else {
                let sender = connect(
                    self.moved.clone(),
                    &addr,
                    self.cc.read_timeout.clone(),
                    self.cc.write_timeout.clone(),
                )?;
                conns.insert(&addr, sender);
            }
        }
    }

    pub fn dispatch_all(&self, cmds: &mut VecDeque<Cmd>) -> Result<usize, AsError> {
        let mut count = 0usize;
        loop {
            if cmds.is_empty() {
                return Ok(count);
            }
            let cmd = cmds.pop_front().expect("cmds pop front never be empty");
            if !cmd.borrow().can_cycle() {
                cmd.set_reply(AsError::ClusterFailDispatch);
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
                        let sender = connect(
                            self.moved.clone(),
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
                    self.moved.clone(),
                    &addr,
                    self.cc.read_timeout.clone(),
                    self.cc.write_timeout.clone(),
                )?;
                conns.insert(&addr, sender);
                return Ok(count);
            }
        }
    }

    pub(crate) fn try_update_all_slots(&self, layout: ReplicaLayout) -> bool {
        self.slots.borrow_mut().try_update_all(layout)
    }

    pub(crate) fn update_slot(&self, slot: usize, addr: String) {
        debug_assert!(slot <= SLOTS_COUNT);
        self.slots.borrow_mut().masters[slot] = addr;
    }
}

struct Conns {
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
}

impl Slots {
    fn try_update_all(&mut self, layout: ReplicaLayout) -> bool {
        let (masters, replicas) = layout;
        let mut changed = false;
        for i in 0..SLOTS_COUNT {
            if self.masters[i] != masters[i] {
                changed = true;
                self.masters[i] = masters[i].clone();
            }
        }

        for i in 0..SLOTS_COUNT {
            let len_not_eqal = self.replicas[i].addrs.len() != replicas[i].len();
            if len_not_eqal || self.replicas[i].addrs.as_slice() != replicas[i].as_slice() {
                self.replicas[i] = Replica {
                    addrs: replicas[i].clone(),
                    current: Cell::new(0),
                };
                changed = true;
            }
        }

        changed
    }

    fn get_master(&self, slot: usize) -> Option<&str> {
        self.masters.get(slot).map(|x| x.as_str())
    }

    fn get_replica(&self, slot: usize) -> Option<&str> {
        self.replicas.get(slot).map(|x| x.get_replica())
    }

    fn get_all_masters(&self) -> HashSet<String> {
        self.masters.iter().map(|x| x.clone()).collect()
    }

    fn get_all_slaves(&self) -> HashSet<String> {
        self.replicas
            .iter()
            .map(|x| x.addrs.clone())
            .flatten()
            .collect()
    }
}

impl Default for Slots {
    fn default() -> Slots {
        let mut masters = Vec::with_capacity(SLOTS_COUNT);
        masters.resize(SLOTS_COUNT, "".to_string());
        let mut replicas = Vec::with_capacity(SLOTS_COUNT);
        replicas.resize(SLOTS_COUNT, Replica::default());
        Slots { masters, replicas }
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
        self.current.update(|x| (x + 1) % len);
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

pub fn connect(
    moved: Sender<Redirection>,
    node: &str,
    rt: Option<u64>,
    wt: Option<u64>,
) -> Result<Sender<Cmd>, AsError> {
    let node_addr = node.to_string();
    let node_addr_clone = node_addr.clone();
    let (tx, rx) = channel(1024 * 8);
    let amt = lazy(|| -> Result<(), ()> { Ok(()) })
        .and_then(move |_| {
            let node_clone = node_addr.clone();
            node_addr
                .as_str()
                .parse()
                .map_err(|err| error!("fail to parse addr {:?} to {}", err, node_clone))
        })
        .and_then(|addr| {
            TcpStream::connect(&addr).map_err(|err| error!("fail to connect {:?}", err))
        })
        .and_then(move |sock| {
            // TODO:
            let sock = set_read_write_timeout(sock, rt, wt).expect("set timeout must be ok");

            sock.set_nodelay(true).expect("set nodelay must ok");
            let codec = RedisNodeCodec {};
            let (sink, stream) = codec.framed(sock).split();
            let backend = back::Back::new(node_addr_clone, rx, sink, stream, moved);
            current_thread::spawn(backend);
            Ok(())
        });
    current_thread::spawn(amt);
    Ok(tx)
}

pub fn run(cc: ClusterConfig) -> Vec<JoinHandle<()>> {
    let worker = cc.thread.unwrap_or(4);
    (0..worker)
        .into_iter()
        .map(|index| {
            let num = index + 1;
            let builder = thread::Builder::new();
            let cc = cc.clone();
            builder
                .name(format!("{}-cluster-{}", cc.name, num))
                .spawn(move || {
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
