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
// use futures::stream::Fuse;
// use futures::task::{self, Task};
use futures::unsync::mpsc::{channel, Receiver, SendError, Sender};
// use futures::{Async, AsyncSink, Future, IntoFuture, Poll, Sink};

// use aho_corasick::{AcAutomaton, Automaton, Match};
use bytes::BytesMut;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream};
use tokio::prelude::{Stream, Future};
use tokio_codec::{Decoder, Encoder};
// use tokio::prelude::*;

// use std::sync::Mutex;
// use std::convert::From;
// use std::hash::{Hash, Hasher};
// use std::io::Error as IoError;
// use std::sync::atomic::bool;
// use std::sync::Arc;

pub fn run() -> Result<(), ()> {
    Ok(())
}

pub fn proxy() {
}


pub struct Config {
    Clusters : Vec<ClusterConfig>,
}

#[derive(Debug)]
pub enum CacheType{
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
    nodes: HashMap<String,T>,
    slots: HashMap<isize, String>,
}

pub struct Cluster<T> {
    cc: ClusterConfig,
    ring: HashRing<T>,
}


impl<T> Cluster<T> {
    pub fn proxy() {}

    pub fn initRemote() {
        let (tx, rx) = channel::<String>(16);

        let amt = rx.map_err(|err|{
            error!("fail to receive new node {:?}", err);
        }).and_then(|node| {
            info!("add new connection to addr {}", &node);
            node.parse().map_err(|err| error!("fail to parse addr {:?}", err))
        }).and_then(|addr|{
            TcpStream::connect(&addr).map_err(|err| error!("fail to connect {:?}", err))
        }).and_then(|sock|{
            info!("new socket");
            let rc = RespCodec{};
            let (sink, stream) = rc.framed(sock).split();
            // TODO: add Endpoint types
            Ok(())
        });
    }
}

pub struct RespCodec {}

impl Decoder for RespCodec {

    type Item = Resp;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        unimplemented!();
    }
}

impl Encoder for RespCodec {
    type Item = Resp;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        unimplemented!();
    }
}

pub struct Resp {
}
