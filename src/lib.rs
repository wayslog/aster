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
extern crate tokio_codec;
extern crate tokio_io;

pub mod cluster;
pub mod cmd;
pub mod com;
pub mod handler;
pub mod resp;
pub mod slots;
use cmd::Cmd;
pub use com::*;
use resp::{Resp, RespCodec};
use slots::SlotsMap;

use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{Async, AsyncSink};
use tokio::executor::current_thread;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::{Future, Sink, Stream};
use tokio_codec::Decoder;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::cell::RefCell;

const MUSK: u16 = 0x3fff;

pub fn run() -> Result<(), ()> {
    Ok(())
}

pub fn proxy() {}

#[allow(unused)]
pub struct Config {
    clusters: Vec<ClusterConfig>,
}

#[derive(Debug)]
pub enum CacheType {
    Redis,
    Memcache,
    MemcacheBinary,
    RedisCluster,
}

pub struct ClusterConfig {
    pub bind: String,
    pub cache_type: CacheType,
}

#[allow(unused)]
pub struct Cluster {
    cc: ClusterConfig,
    slots: RefCell<SlotsMap>,
}

impl Cluster {
    #[allow(unused_variables)]
    pub fn proxy(&self) -> AsResult<()> {
        let addr = self
            .cc
            .bind
            .clone()
            .parse::<SocketAddr>()
            .expect("parse socket never fail");

        let (handler_tx, handler_rx) = channel::<Resp>(1024);

        let listen = TcpListener::bind(&addr)?;
        let amt = listen.incoming().for_each(|sock| {
            debug!("accept new incoming socket");
            let codec = RespCodec {};

            // let client_tx = client_rx.clone();
            let (sink, stream) = codec.framed(sock).split();
            let input = stream
                .forward(handler_tx.clone())
                .map(|_| debug!("connection closed by client"))
                .map_err(|err| error!("fail to handle proxy due to {:?}", err));
            current_thread::spawn(input);

            let (client_tx, client_rx) = channel::<Resp>(1024);
            let output = sink
                .send_all(client_rx.map_err(|_| Error::None))
                .map(|_| debug!("connection closed by cluster"))
                .map_err(|err| error!("fail to handle proxy due to {:?}", err));
            current_thread::spawn(output);

            Ok(())
        });

        Ok(())
    }

    #[allow(unused)]
    fn create_remote(&self) {
        let (tx, rx) = channel::<String>(16);
        let (esend, erecv) = channel::<Endpoint>(1024);

        let amt = rx
            .map_err(|err| {
                error!("fail to receive new node {:?}", err);
            })
            .and_then(|node| {
                info!("add new connection to addr {}", &node);
                node.parse()
                    .map_err(|err| error!("fail to parse addr {:?}", err))
            })
            .and_then(|addr| {
                TcpStream::connect(&addr).map_err(|err| error!("fail to connect {:?}", err))
            })
            .zip(erecv)
            .and_then(|(sock, endpoint)| {
                info!("create new socket with endpoint");
                let rc = RespCodec {};
                let (sink, stream) = rc.framed(sock).split();
                let Endpoint { send, recv } = endpoint;
                // TODO: add Endpoint types
                let down = sink
                    .send_all(recv.map_err(|_emptyerr| Error::None))
                    .map(|_| trace!("down stream completed"))
                    .map_err(|err| error!("fail to send to backend node {:?}", err));
                current_thread::spawn(down);
                let up = stream
                    .forward(send)
                    .map(|_| trace!("up stream completed"))
                    .map_err(|err| error!("fail to recv and forward from backend node {:?}", err));
                current_thread::spawn(up);
                Ok(())
            })
            .for_each(|_| Ok(()));
        current_thread::spawn(amt);
    }

    pub fn create_node_conn(&self, node: &str) -> AsResult<Sender<Cmd>> {
        let _addr = node.parse::<SocketAddr>()?;

        Err(Error::None)
    }

    pub fn dispatch(&self, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        let slot = (cmd.borrow().crc() & MUSK) as usize;
        loop {
            let mut slots_map = self.slots.borrow_mut();
            let addr = slots_map.get_addr(slot);
            {
                let sender_opt = slots_map.get_sender_by_addr(&addr);
                if let Some(sender) = sender_opt {
                    match sender.start_send(cmd) {
                        Ok(v) => return Ok(v),
                        Err(err) => {
                            trace!("send fail with send error: {:?}", err);
                            return Err(Error::Critical);
                        }
                    }
                }
            }

            let tx = self.create_node_conn(&addr)?;
            slots_map.add_node(addr, tx);
        }
    }
}

pub struct Endpoint {
    send: Sender<Resp>,
    recv: Receiver<Resp>,
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

    node_tx: NO,
    node_rx: NI,

    state: NodeConnState,
}

impl<S, O, NI, NO> NodeConn<S, O, NI, NO>
where
    S: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd>,
    NI: Stream<Item = Resp, Error = Error>,
    NO: Sink<SinkItem = Resp, SinkError = Error>,
{}

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
                        Async::Ready(None) => {
                            self.state = NodeConnState::Send;
                        }
                        Async::NotReady => {
                            self.state = NodeConnState::Send;
                        }
                    }
                }
                NodeConnState::Send => {
                    if self.cursor == self.buffered.len() {
                        self.state = NodeConnState::Wait;
                        self.cursor = 0;
                        continue;
                    }

                    let cursor = self.cursor;
                    let cmd = self.buffered.get(cursor).cloned().expect("cmd must exists");

                    match self
                        .node_tx
                        .start_send(cmd.borrow().req.clone())
                        .map_err(|err| {
                            error!("fail to start send cmd resp {:?}", err);
                        })? {
                        AsyncSink::NotReady(_) => {
                            self.node_tx.poll_complete().map_err(|err| {
                                error!("fail to poll complete cmd resp {:?}", err);
                            })?;
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
            break;
        }
        Ok(Async::Ready(()))
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
