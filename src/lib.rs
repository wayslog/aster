#![deny(warnings)]

extern crate tokio;
#[macro_use(try_ready)]
extern crate futures;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate bytes;
extern crate num_cpus;
#[macro_use]
extern crate lazy_static;
extern crate btoi;
extern crate crc16;
extern crate itoa;
extern crate net2;
extern crate tokio_codec;
extern crate tokio_io;

pub mod cluster;
pub mod cmd;
pub mod com;
pub mod handler;
pub mod resp;
pub mod slots;

use cmd::{new_cluster_nodes_cmd, Cmd, CmdCodec};
pub use com::*;
use handler::Handler;
use resp::{Resp, RespCodec, RESP_BULK};
use slots::SlotsMap;

use futures::lazy;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{Async, AsyncSink};
// use futures::task::current;
use tokio::executor::current_thread;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::{Future, Sink, Stream};
use tokio_codec::Decoder;

use net2::unix::UnixTcpBuilderExt;
use net2::TcpBuilder;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;
use std::thread;

const MUSK: u16 = 0x3fff;

pub fn run() -> Result<(), ()> {
    env_logger::init();
    info!("start asswecan");
    let config = Config {
        clusters: vec![ClusterConfig {
            bind: "0.0.0.0:9001".to_string(),
            cache_type: CacheType::RedisCluster,
            servers: vec!["127.0.0.1:7010".to_string(), "127.0.0.1:7011".to_string()],
            thread: 4,
        }],
    };

    let ths: Vec<_> = config
        .clusters
        .iter()
        .map(|cc| create_cluster(cc))
        .flatten()
        .collect();

    for th in ths {
        th.join().unwrap();
    }
    Ok(())
}

pub fn create_cluster(cc: &ClusterConfig) -> Vec<thread::JoinHandle<()>> {
    let count = cc.thread;
    (0..count)
        .into_iter()
        .map(|_| {
            let cc = cc.clone();
            thread::spawn(move || {
                let smap = SlotsMap::default();
                let cluster = Cluster {
                    cc: cc,
                    slots: RefCell::new(smap),
                };
                proxy(cluster)
            })
        }).collect()
}

pub fn proxy(cluster: Cluster) {
    let addr = cluster
        .cc
        .bind
        .clone()
        .parse::<SocketAddr>()
        .expect("parse socket never fail");

    let fut = lazy(move || -> Result<(SocketAddr, Cluster), ()> { Ok((addr, cluster)) })
        .and_then(|(addr, mut cluster)| {
            let listen = create_reuse_port_listener(&addr).expect("bind never fail");
            info!("success listen at {}", &cluster.cc.bind);
            cluster.init_node_conn().unwrap();
            let servers = cluster.cc.servers.clone();
            let initilizer = ClusterInitilizer {
                cluster: Rc::new(cluster),
                listen: Some(listen),
                servers: servers,
                cursor: 0,
                info_cmd: new_cluster_nodes_cmd(),
                state: InitState::Pend,
            }.map_err(|err| {
                error!("fail to init cluster with given server due {:?}", err);
            });
            initilizer
        }).and_then(|(cluster, listen)| {
            let rc_cluster = cluster.clone();
            let amt = listen
                .incoming()
                .for_each(move |sock| {
                    let codec = CmdCodec::default();
                    let (cmd_tx, cmd_rx) = codec.framed(sock).split();
                    let cluster = rc_cluster.clone();
                    let handler = Handler::new(cluster, cmd_rx, cmd_tx).map_err(|err| {
                        error!("fail to create new handler due {:?}", err);
                    });
                    current_thread::spawn(handler);
                    Ok(())
                }).map_err(|err| {
                    error!("fail to proxy due {:?}", err);
                });
            current_thread::spawn(amt);
            Ok(())
        });

    current_thread::block_on_all(fut).unwrap();
}

fn create_reuse_port_listener(addr: &SocketAddr) -> Result<TcpListener, std::io::Error> {
    let builder = TcpBuilder::new_v4()?;
    let std_listener = builder
        .reuse_address(true)
        .expect("os not support SO_REUSEADDR")
        .reuse_port(true)
        .expect("os not support SO_REUSEPORT")
        .bind(addr)?
        .listen(std::i32::MAX)?;
    let hd = tokio::reactor::Handle::current();
    TcpListener::from_std(std_listener, &hd)
}

#[allow(unused)]
pub struct Config {
    clusters: Vec<ClusterConfig>,
}

#[derive(Debug, Clone, Copy)]
pub enum CacheType {
    Redis,
    Memcache,
    MemcacheBinary,
    RedisCluster,
}

#[derive(Clone)]
pub struct ClusterConfig {
    pub bind: String,
    pub cache_type: CacheType,
    pub servers: Vec<String>,
    pub thread: usize,
}

pub struct Cluster {
    cc: ClusterConfig,
    slots: RefCell<SlotsMap>,
}

impl Cluster {
    pub fn init_node_conn(&mut self) -> Result<(), Error> {
        // let cmd = new_cluster_nodes_cmd();
        let mut slots_map = self.slots.borrow_mut();
        for addr in &self.cc.servers {
            let tx = self.create_node_conn(&addr)?;
            slots_map.add_node(addr.clone(), tx.clone());
        }

        Ok(())
    }

    pub fn create_node_conn(&self, node: &str) -> AsResult<Sender<Cmd>> {
        let addr_string = node.to_string();
        let nc_string = node.to_string();
        let (tx, rx): (Sender<Cmd>, Receiver<Cmd>) = channel(10240);
        let ret_tx = tx.clone();
        let amt = lazy(|| -> Result<(), ()> { Ok(()) })
            .and_then(move |_| {
                addr_string
                    .as_str()
                    .parse()
                    .map_err(|err| error!("fail to parse addr {:?}", err))
            }).and_then(|addr| {
                TcpStream::connect(&addr).map_err(|err| error!("fail to connect {:?}", err))
            }).and_then(|sock| {
                let codec = RespCodec {};
                let (sink, stream) = codec.framed(sock).split();
                let arx = rx.map_err(|err| {
                    info!("fail to send due to {:?}", err);
                    Error::Critical
                });
                let nc = NodeConn::new(nc_string, sink, stream, arx, tx);
                nc.map_err(|err| error!("fail with error {:?}", err))
            });
        current_thread::spawn(amt);
        Ok(ret_tx)
    }

    fn try_dispatch(sender: &mut Sender<Cmd>, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        match sender.start_send(cmd) {
            Ok(AsyncSink::NotReady(v)) => {
                return Ok(AsyncSink::NotReady(v));
            }
            Ok(AsyncSink::Ready) => {
                return sender
                    .poll_complete()
                    .map_err(|err| {
                        error!("fail to complete send cmd to node conn due {:?}", err);
                        Error::Critical
                    }).map(|_| AsyncSink::Ready)
            }
            Err(err) => {
                error!("send fail with send error: {:?}", err);
                return Err(Error::Critical);
            }
        }
    }

    pub fn execute(&self, node: &String, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        loop {
            let mut slots_map = self.slots.borrow_mut();
            if let Some(sender) = slots_map.get_sender_by_addr(node) {
                return Cluster::try_dispatch(sender, cmd);
            }
            let tx = self.create_node_conn(node)?;
            slots_map.add_node(node.clone(), tx);
        }
    }

    pub fn dispatch(&self, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        let slot = (cmd.borrow().crc() & MUSK) as usize;
        loop {
            let mut slots_map = self.slots.borrow_mut();
            let addr = slots_map.get_addr(slot);
            if let Some(sender) = slots_map.get_sender_by_addr(&addr) {
                return Cluster::try_dispatch(sender, cmd);
            }
            let tx = self.create_node_conn(&addr)?;
            slots_map.add_node(addr, tx);
        }
    }
}

pub enum NodeConnState {
    Collect,
    Send,
    Wait,
    Return,
}

const MAX_NODE_CONN_CONCURRENCY: usize = 512;

pub struct NodeConn<S, O, NI, NO>
where
    S: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd>,
    NI: Stream<Item = Resp, Error = Error>,
    NO: Sink<SinkItem = Resp, SinkError = Error>,
{
    _node: String,
    cursor: usize,
    buffered: VecDeque<Cmd>,

    input: S,
    _resender: O,

    node_rx: NI,
    node_tx: NO,

    state: NodeConnState,
}

impl<S, O, NI, NO> NodeConn<S, O, NI, NO>
where
    S: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd>,
    NI: Stream<Item = Resp, Error = Error>,
    NO: Sink<SinkItem = Resp, SinkError = Error>,
{
    pub fn new(
        node: String,
        node_tx: NO,
        node_rx: NI,
        input: S,
        resender: O,
    ) -> NodeConn<S, O, NI, NO> {
        // let resp_codec = RespCodec {};
        // let (node_tx, node_rx) = resp_codec.framed(socket).split();

        NodeConn {
            _node: node,
            cursor: 0,
            buffered: VecDeque::new(),
            input: input,
            _resender: resender,
            node_tx: node_tx,
            node_rx: node_rx,
            state: NodeConnState::Collect,
        }
    }
}

impl<S, O, NI, NO> Future for NodeConn<S, O, NI, NO>
where
    S: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd>,
    NI: Stream<Item = Resp, Error = Error>,
    NO: Sink<SinkItem = Resp, SinkError = Error>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            match self.state {
                NodeConnState::Collect => {
                    if self.buffered.len() == MAX_NODE_CONN_CONCURRENCY {
                        self.state = NodeConnState::Send;
                        continue;
                    }

                    match self.input.poll().map_err(|err| {
                        error!("fail to recv new command due err {:?}", err);
                    })? {
                        Async::Ready(Some(v)) => {
                            self.buffered.push_back(v);
                        }
                        Async::NotReady => {
                            if self.buffered.len() == 0 {
                                return Ok(Async::NotReady);
                            }
                            self.state = NodeConnState::Send;
                        }

                        Async::Ready(None) => {
                            self.cursor += 1;
                        }
                    }
                }
                NodeConnState::Send => {
                    if self.cursor == self.buffered.len() {
                        self.node_tx.poll_complete().map_err(|err| {
                            error!("fail to poll complete cmd resp {:?}", err);
                        })?;
                        self.state = NodeConnState::Wait;
                        self.cursor = 0;
                        continue;
                    }

                    let cursor = self.cursor;
                    let cmd = self.buffered.get(cursor).cloned().expect("cmd must exists");
                    trace!(
                        "trying to send into backend with cursor={} and buffered={}",
                        cursor,
                        self.buffered.len()
                    );
                    match self
                        .node_tx
                        .start_send(cmd.borrow().req.clone())
                        .map_err(|err| {
                            error!("fail to start send cmd resp {:?}", err);
                        })? {
                        AsyncSink::NotReady(_) => {
                            trace!("fail to send due to chan is full");
                        }
                        AsyncSink::Ready => {
                            self.cursor += 1;
                        }
                    };
                }
                NodeConnState::Wait => {
                    if self.cursor == self.buffered.len() {
                        self.cursor = 0;
                        self.state = NodeConnState::Return;
                        continue;
                    }

                    let cursor = self.cursor;
                    if let Some(resp) = try_ready!(self.node_rx.poll().map_err(|err| {
                        error!("fail to recv reply from node conn {:?}", err);
                    })) {
                        let mut cmd = self
                            .buffered
                            .get_mut(cursor)
                            .expect("resp mut exists")
                            .borrow_mut();
                        cmd.done(resp);
                        self.cursor += 1;
                    } else {
                        // TODO: set done with error
                        info!("quick done with error");
                    }
                }
                NodeConnState::Return => {
                    self.buffered.clear();
                    self.state = NodeConnState::Collect;
                }
            };
        }
        //Ok(Async::Ready(()))
    }
}

pub struct Batch<S>
where
    S: Stream,
{
    input: S,
    max: usize,
}

impl<S> Stream for Batch<S>
where
    S: Stream,
{
    type Item = VecDeque<S::Item>;
    type Error = S::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let mut buf = VecDeque::new();
        loop {
            match self.input.poll() {
                Ok(Async::NotReady) => {
                    if buf.is_empty() {
                        return Ok(Async::NotReady);
                    }
                    return Ok(Async::Ready(Some(buf)));
                }
                Ok(Async::Ready(None)) => {
                    return Ok(Async::Ready(None));
                }
                Ok(Async::Ready(Some(item))) => {
                    buf.push_back(item);
                    if buf.len() == self.max {
                        return Ok(Async::Ready(Some(buf)));
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum InitState {
    Pend,
    Wait,
}

#[allow(unused)]
pub struct ClusterInitilizer {
    cluster: Rc<Cluster>,
    listen: Option<TcpListener>,
    servers: Vec<String>,
    cursor: usize,
    info_cmd: Cmd,
    state: InitState,
}

impl Future for ClusterInitilizer {
    type Item = (Rc<Cluster>, TcpListener);
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            match self.state {
                InitState::Pend => {
                    let cursor = self.cursor;
                    if cursor == self.servers.len() {
                        return Err(Error::Critical);
                    }
                    let addr = self.servers.get(cursor).cloned().unwrap();
                    match self.cluster.execute(&addr, self.info_cmd.clone())? {
                        AsyncSink::NotReady(_) => return Ok(Async::NotReady),
                        AsyncSink::Ready => {
                            self.state = InitState::Wait;
                        }
                    }
                    self.cursor += 1;
                }

                InitState::Wait => {
                    let cmd = self.info_cmd.clone();
                    if !cmd.borrow().is_done() {
                        return Ok(Async::NotReady);
                    }
                    // debug!("cmd has been done {:?}", cmd);

                    let cmd_borrow = cmd.borrow_mut();
                    let resp = cmd_borrow.reply.as_ref().unwrap();

                    if resp.rtype != RESP_BULK {
                        self.state = InitState::Pend;
                        continue;
                    }

                    let mut slots_map = self.cluster.slots.borrow_mut();
                    slots_map.try_update_all(resp.data.as_ref().expect("never be empty"));
                    let mut listener = None;
                    std::mem::swap(&mut listener, &mut self.listen);
                    return Ok(Async::Ready((self.cluster.clone(), listener.unwrap())));
                }
            }
        }
    }
}
