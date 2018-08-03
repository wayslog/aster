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

// use std::cell::RefCell;
// use std::mem;
// use std::rc::{Rc, Weak};

// use futures::future::join_all;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{Async, AsyncSink};
// use futures::{Async, AsyncSink, Future, IntoFuture, Poll, Sink};

// use aho_corasick::{AcAutomaton, Automaton, Match};
use tokio::executor::current_thread;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::{Future, Sink, Stream};
use tokio_codec::Decoder;
// use tokio::prelude::*;

use std::collections::VecDeque;
use std::net::SocketAddr;
// use std::sync::Mutex;
// use std::convert::From;
// use std::hash::{Hash, Hasher};
// use std::io::Error as IoError;
// use std::sync::atomic::bool;
// use std::sync::Arc;

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
    slots: SlotsMap,
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
            }).and_then(|node| {
                info!("add new connection to addr {}", &node);
                node.parse()
                    .map_err(|err| error!("fail to parse addr {:?}", err))
            }).and_then(|addr| {
                TcpStream::connect(&addr).map_err(|err| error!("fail to connect {:?}", err))
            }).zip(erecv)
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
            }).for_each(|_| Ok(()));
        current_thread::spawn(amt);
    }

    pub fn dispatch(&self, _cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        Ok(AsyncSink::Ready)
    }
}

pub struct Endpoint {
    send: Sender<Resp>,
    recv: Receiver<Resp>,
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
