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
use cmd::{Cmd, CmdCodec};
pub use com::*;
use handler::Handler;
use resp::{Resp, RespCodec};
use slots::SlotsMap;

use futures::lazy;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{Async, AsyncSink};
use tokio::executor::current_thread;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::{Future, Sink, Stream};
use tokio_codec::Decoder;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;

const MUSK: u16 = 0x3fff;

pub fn run() -> Result<(), ()> {
    env_logger::init();
    info!("start asswecan");
    proxy();
    Ok(())
}

pub fn proxy() {
    let slots_data = r#"f43cbe589d47d409bdf14624d950014a32e03a78 127.0.0.1:7013@17013 slave 9a44630c1dbbf7c116e90f21d1746198d3a1305a 0 1534150532241 35 connected
2f84714c64297a241d7701b72ec2d9b53b173d86 127.0.0.1:7014@17014 slave 768595f1a4b657315916893cae9e9ab355cd55f7 0 1534150530000 5 connected
9a44630c1dbbf7c116e90f21d1746198d3a1305a 127.0.0.1:7010@17010 myself,master - 0 1534150529000 35 connected 0-5460
480ca425ee0e990108115e7da97450bf72246552 127.0.0.1:7012@17012 master - 0 1534150530227 36 connected 10923-16383
c1ceb9b25a4aa7102acdc546182bf2d855b357f1 127.0.0.1:7015@17015 slave 480ca425ee0e990108115e7da97450bf72246552 0 1534150531235 36 connected
768595f1a4b657315916893cae9e9ab355cd55f7 127.0.0.1:7011@17011 master - 0 1534150529219 2 connected 5461-10922"#;
    let mut smap = SlotsMap::default();
    smap.try_update_all(slots_data.as_bytes());

    let cluster = Cluster {
        cc: ClusterConfig {
            bind: "0.0.0.0:9001".to_string(),
            cache_type: CacheType::RedisCluster,
        },
        slots: RefCell::new(smap),
    };

    let addr = cluster
        .cc
        .bind
        .clone()
        .parse::<SocketAddr>()
        .expect("parse socket never fail");

    let listen = TcpListener::bind(&addr).expect("bind never fail");
    info!("success listen at {}", &cluster.cc.bind);
    let rc_cluster = Rc::new(cluster);
    let amt = listen
        .incoming()
        .for_each(|sock| {
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
    current_thread::block_on_all(amt).unwrap();
}

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

    pub fn dispatch(&self, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        let slot = (cmd.borrow().crc() & MUSK) as usize;
        loop {
            let mut slots_map = self.slots.borrow_mut();
            let addr = slots_map.get_addr(slot);
            {
                let sender_opt = slots_map.get_sender_by_addr(&addr);
                trace!("dispatch one command for addr {:?}", sender_opt);
                if let Some(sender) = sender_opt {
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
