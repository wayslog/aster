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
extern crate aho_corasick;
extern crate btoi;
extern crate tokio_codec;
extern crate tokio_io;

pub mod com;
pub use com::*;

// use std::cell::RefCell;
// use std::mem;
// use std::rc::{Rc, Weak};

// use futures::future::join_all;
// use futures::task::{self, Task};
use futures::sync::mpsc::{channel, Receiver, SendError, Sender};
// use futures::{Async, AsyncSink, Future, IntoFuture, Poll, Sink};

// use aho_corasick::{AcAutomaton, Automaton, Match};
use bytes::BufMut;
use bytes::BytesMut;
use std::collections::HashMap;
use tokio::executor::current_thread;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::{Future, Sink, Stream};
use tokio_codec::{Decoder, Encoder};
// use tokio::prelude::*;

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

pub struct Config {
    Clusters: Vec<ClusterConfig>,
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

pub struct HashRing<T> {
    nodes: HashMap<String, T>,
    slots: HashMap<isize, String>,
}

pub struct Cluster<T> {
    cc: ClusterConfig,
    ring: HashRing<T>,
}

impl<T> Cluster<T> {
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

    fn createRemote(&self) {
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
                    .map(|_| trace!("down stream complated"))
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
}

pub struct Endpoint {
    send: Sender<Resp>,
    recv: Receiver<Resp>,
}

pub struct RespCodec {}

impl Decoder for RespCodec {
    type Item = Resp;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let item = Resp::parse(&src)
            .map(|x| Some(x))
            .or_else(|err| match err {
                Error::MoreData => Ok(None),
                ev => Err(ev),
            })?;
        if let Some(resp) = item {
            src.advance(resp.binary_size());
            return Ok(Some(resp));
        }
        Ok(None)
    }
}

impl Encoder for RespCodec {
    type Item = Resp;
    type Error = Error;

    fn encode(&mut self, mut item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let size = item.write(dst)?;
        trace!("encode write bytes size {}", size);
        Ok(())
    }
}

pub type RespType = u8;
pub const RESP_STRING: RespType = '+' as u8;
pub const RESP_INT: RespType = ':' as u8;
pub const RESP_ERROR: RespType = '-' as u8;
pub const RESP_BULK: RespType = '$' as u8;
pub const RESP_ARRAY: RespType = '*' as u8;

pub const BYTE_CR: u8 = '\r' as u8;
pub const BYTE_LF: u8 = '\n' as u8;

#[derive(Clone, Debug)]
pub enum Resp {
    Plain {
        rtype: RespType,
        data: Vec<u8>,
    },
    Bulk {
        rtype: RespType,
        size: Vec<u8>,
        data: Vec<u8>,
    },
    Array {
        rtype: RespType,
        count: Vec<u8>,
        items: Vec<Resp>,
    },
}

impl Resp {
    fn parse(src: &[u8]) -> AsResult<Self> {
        let mut iter = src.splitn(2, |x| *x == BYTE_LF);
        let line = iter.next().ok_or(Error::MoreData)?;

        let line_size = line.len();
        let rtype = line[0];

        match rtype {
            RESP_STRING | RESP_INT | RESP_ERROR => Ok(Resp::Plain {
                rtype: rtype,
                data: line[1..line_size - 2].to_vec(),
            }),
            RESP_BULK => {
                let data = iter.next().ok_or(Error::MoreData)?;
                Ok(Resp::Bulk {
                    rtype: rtype,
                    size: line[1..line_size - 2].to_vec(),
                    data: data[1..data.len() - 2].to_vec(),
                })
            }

            RESP_ARRAY => {
                let count_bs = &line[1..line_size - 2];
                let count = btoi::btoi::<usize>(count_bs)?;
                let mut items = Vec::with_capacity(count);
                let mut parsed = line_size;
                for _ in 0..count {
                    let item = Self::parse(&src[parsed..])?;
                    parsed += item.binary_size();
                    items.push(item);
                }
                Ok(Resp::Array {
                    rtype: rtype,
                    count: count_bs.to_vec(),
                    items: items,
                })
            }
            _ => unreachable!(),
        }
    }

    fn write(&mut self, dst: &mut BytesMut) -> AsResult<usize> {
        match self {
            Resp::Plain { data, rtype } => {
                dst.put_u8(*rtype);
                dst.put(data.clone());
                dst.put_u8(BYTE_CR);
                dst.put_u8(BYTE_LF);
                Ok(3 + data.len())
            }
            Resp::Bulk { data, size, rtype } => {
                dst.put_u8(*rtype);
                dst.put(size.clone());
                dst.put_u8(BYTE_CR);
                dst.put_u8(BYTE_LF);
                dst.put(data.clone());
                dst.put_u8(BYTE_CR);
                dst.put_u8(BYTE_LF);
                Ok(5 + size.len() + data.len())
            }
            Resp::Array {
                rtype,
                count,
                items,
            } => {
                let mut size = 1 + count.len() + 2;
                dst.put_u8(*rtype);
                dst.put(count.clone());
                dst.put_u8(BYTE_CR);
                dst.put_u8(BYTE_LF);
                for item in items {
                    size += item.write(dst)?;
                }
                Ok(size)
            }
        }
    }

    fn binary_size(&self) -> usize {
        match self {
            Resp::Plain { data, .. } => 1 + data.len() + 2,
            Resp::Bulk { data, size, .. } => 1 + size.len() + 2 + data.len() + 2,
            Resp::Array { count, items, .. } => {
                let mut size = 1 + count.len() + 2;
                let arr_size: usize = items.iter().map(|x| x.binary_size()).sum();
                size += arr_size;
                size
            }
        }
    }
}
