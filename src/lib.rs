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
extern crate tokio_codec;
extern crate tokio_io;

pub mod com;
pub use com::*;

// use std::cell::RefCell;
// use std::mem;
// use std::rc::{Rc, Weak};

// use futures::future::join_all;
use futures::sync::mpsc::{channel, Receiver, SendError, Sender};
use futures::task::{self, Task};
use futures::Async;
// use futures::{Async, AsyncSink, Future, IntoFuture, Poll, Sink};

// use aho_corasick::{AcAutomaton, Automaton, Match};
use bytes::BufMut;
use bytes::BytesMut;
use tokio::executor::current_thread;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::{Future, Sink, Stream};
use tokio_codec::{Decoder, Encoder};
// use tokio::prelude::*;

use std::collections::{HashMap, HashSet};
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

/// Command is a type for Redis Command.
pub struct Command {
    pub is_done: bool,
    pub is_ask: bool,
    pub is_inline: bool,

    pub is_complex: bool,
    pub cmd_type: CmdType,

    pub task: Task,

    pub req: Resp,
    pub reply: Option<Resp>,
}

impl Command {
    fn from_resp(resp: Resp) -> Command {
        let local_task = task::current();
        // TODO: how to get command type
        // let cmd_type = CMD_TYPE.get()

        Command{
            is_done: false,
            is_ask: false,
            is_inline: false,

            is_complex: false,
            cmd_type: CmdType::NotSupport,

            task: local_task,
            req: resp,
            reply: None,
        }
    }
}

pub struct CommandStream<S: Stream<Item = Resp, Error = Error>> {
    input: S,
}

impl<S> Stream for CommandStream<S>
where
    S: Stream<Item = Resp, Error = Error>,
{
    type Item = Command;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if let Some(resp) = try_ready!(self.input.poll()) {
            return Ok(Async::Ready(Some(Command::from_resp(resp))));
        }
        Ok(Async::Ready(None))
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
    type Item = Vec<S::Item>;
    type Error = S::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let mut buf = Vec::new();
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
                    buf.push(item);
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
pub enum CmdType {
    Read,
    Write,
    Ctrl,
    NotSupport,
}

lazy_static! {
    pub static ref CMD_COMPLEX: HashSet<&'static [u8]> = {
        let cmds = vec!["MSET", "MGET", "DEL", "EXISTS", "EVAL", "EVALSHAR"];

        let mut hset = HashSet::new();
        for cmd in &cmds[..] {
            hset.insert(cmd.as_bytes());
        }
        hset
    };
    pub static ref CMD_TYPE: HashMap<&'static [u8], CmdType> = {
        let mut hmap = HashMap::new();

        // special commands
        hmap.insert("DEL".as_bytes(), CmdType::Write);
        hmap.insert("DUMP".as_bytes(), CmdType::Read);
        hmap.insert("EXISTS".as_bytes(), CmdType::Read);
        hmap.insert("EXPIRE".as_bytes(), CmdType::Write);
        hmap.insert("EXPIREAT".as_bytes(), CmdType::Write);
        hmap.insert("KEYS".as_bytes(), CmdType::NotSupport);
        hmap.insert("MIGRATE".as_bytes(), CmdType::NotSupport);
        hmap.insert("MOVE".as_bytes(), CmdType::NotSupport);
        hmap.insert("OBJECT".as_bytes(), CmdType::NotSupport);
        hmap.insert("PERSIST".as_bytes(), CmdType::Write);
        hmap.insert("PEXPIRE".as_bytes(), CmdType::Write);
        hmap.insert("PEXPIREAT".as_bytes(), CmdType::Write);
        hmap.insert("PTTL".as_bytes(), CmdType::Read);
        hmap.insert("RANDOMKEY".as_bytes(), CmdType::NotSupport);
        hmap.insert("RENAME".as_bytes(), CmdType::NotSupport);
        hmap.insert("RENAMENX".as_bytes(), CmdType::NotSupport);
        hmap.insert("RESTORE".as_bytes(), CmdType::Write);
        hmap.insert("SCAN".as_bytes(), CmdType::NotSupport);
        hmap.insert("SORT".as_bytes(), CmdType::Write);
        hmap.insert("TTL".as_bytes(), CmdType::Read);
        hmap.insert("TYPE".as_bytes(), CmdType::Read);
        hmap.insert("WAIT".as_bytes(), CmdType::NotSupport);
        // string key
        hmap.insert("APPEND".as_bytes(), CmdType::Write);
        hmap.insert("BITCOUNT".as_bytes(), CmdType::Read);
        hmap.insert("BITOP".as_bytes(), CmdType::NotSupport);
        hmap.insert("BITPOS".as_bytes(), CmdType::Read);
        hmap.insert("DECR".as_bytes(), CmdType::Write);
        hmap.insert("DECRBY".as_bytes(), CmdType::Write);
        hmap.insert("GET".as_bytes(), CmdType::Read);
        hmap.insert("GETBIT".as_bytes(), CmdType::Read);
        hmap.insert("GETRANGE".as_bytes(), CmdType::Read);
        hmap.insert("GETSET".as_bytes(), CmdType::Write);
        hmap.insert("INCR".as_bytes(), CmdType::Write);
        hmap.insert("INCRBY".as_bytes(), CmdType::Write);
        hmap.insert("INCRBYFLOAT".as_bytes(), CmdType::Write);
        hmap.insert("MGET".as_bytes(), CmdType::Read);
        hmap.insert("MSET".as_bytes(), CmdType::Write);
        hmap.insert("MSETNX".as_bytes(), CmdType::NotSupport);
        hmap.insert("PSETEX".as_bytes(), CmdType::Write);
        hmap.insert("SET".as_bytes(), CmdType::Write);
        hmap.insert("SETBIT".as_bytes(), CmdType::Write);
        hmap.insert("SETEX".as_bytes(), CmdType::Write);
        hmap.insert("SETNX".as_bytes(), CmdType::Write);
        hmap.insert("SETRANGE".as_bytes(), CmdType::Write);
        hmap.insert("STRLEN".as_bytes(), CmdType::Read);
        // hash type
        hmap.insert("HDEL".as_bytes(), CmdType::Write);
        hmap.insert("HEXISTS".as_bytes(), CmdType::Read);
        hmap.insert("HGET".as_bytes(), CmdType::Read);
        hmap.insert("HGETALL".as_bytes(), CmdType::Read);
        hmap.insert("HINCRBY".as_bytes(), CmdType::Write);
        hmap.insert("HINCRBYFLOAT".as_bytes(), CmdType::Write);
        hmap.insert("HKEYS".as_bytes(), CmdType::Read);
        hmap.insert("HLEN".as_bytes(), CmdType::Read);
        hmap.insert("HMGET".as_bytes(), CmdType::Read);
        hmap.insert("HMSET".as_bytes(), CmdType::Write);
        hmap.insert("HSET".as_bytes(), CmdType::Write);
        hmap.insert("HSETNX".as_bytes(), CmdType::Write);
        hmap.insert("HSTRLEN".as_bytes(), CmdType::Read);
        hmap.insert("HVALS".as_bytes(), CmdType::Read);
        hmap.insert("HSCAN".as_bytes(), CmdType::Read);
        // list type
        hmap.insert("BLPOP".as_bytes(), CmdType::NotSupport);
        hmap.insert("BRPOP".as_bytes(), CmdType::NotSupport);
        hmap.insert("BRPOPLPUSH".as_bytes(), CmdType::NotSupport);
        hmap.insert("LINDEX".as_bytes(), CmdType::Read);
        hmap.insert("LINSERT".as_bytes(), CmdType::Write);
        hmap.insert("LLEN".as_bytes(), CmdType::Read);
        hmap.insert("LPOP".as_bytes(), CmdType::Write);
        hmap.insert("LPUSH".as_bytes(), CmdType::Write);
        hmap.insert("LPUSHX".as_bytes(), CmdType::Write);
        hmap.insert("LRANGE".as_bytes(), CmdType::Read);
        hmap.insert("LREM".as_bytes(), CmdType::Write);
        hmap.insert("LSET".as_bytes(), CmdType::Write);
        hmap.insert("LTRIM".as_bytes(), CmdType::Write);
        hmap.insert("RPOP".as_bytes(), CmdType::Write);
        hmap.insert("RPOPLPUSH".as_bytes(), CmdType::Write);
        hmap.insert("RPUSH".as_bytes(), CmdType::Write);
        hmap.insert("RPUSHX".as_bytes(), CmdType::Write);
        // set type
        hmap.insert("SADD".as_bytes(), CmdType::Write);
        hmap.insert("SCARD".as_bytes(), CmdType::Read);
        hmap.insert("SDIFF".as_bytes(), CmdType::Read);
        hmap.insert("SDIFFSTORE".as_bytes(), CmdType::Write);
        hmap.insert("SINTER".as_bytes(), CmdType::Read);
        hmap.insert("SINTERSTORE".as_bytes(), CmdType::Write);
        hmap.insert("SISMEMBER".as_bytes(), CmdType::Read);
        hmap.insert("SMEMBERS".as_bytes(), CmdType::Read);
        hmap.insert("SMOVE".as_bytes(), CmdType::Write);
        hmap.insert("SPOP".as_bytes(), CmdType::Write);
        hmap.insert("SRANDMEMBER".as_bytes(), CmdType::Read);
        hmap.insert("SREM".as_bytes(), CmdType::Write);
        hmap.insert("SUNION".as_bytes(), CmdType::Read);
        hmap.insert("SUNIONSTORE".as_bytes(), CmdType::Write);
        hmap.insert("SSCAN".as_bytes(), CmdType::Read);
        // zset type
        hmap.insert("ZADD".as_bytes(), CmdType::Write);
        hmap.insert("ZCARD".as_bytes(), CmdType::Read);
        hmap.insert("ZCOUNT".as_bytes(), CmdType::Read);
        hmap.insert("ZINCRBY".as_bytes(), CmdType::Write);
        hmap.insert("ZINTERSTORE".as_bytes(), CmdType::Write);
        hmap.insert("ZLEXCOUNT".as_bytes(), CmdType::Read);
        hmap.insert("ZRANGE".as_bytes(), CmdType::Read);
        hmap.insert("ZRANGEBYLEX".as_bytes(), CmdType::Read);
        hmap.insert("ZRANGEBYSCORE".as_bytes(), CmdType::Read);
        hmap.insert("ZRANK".as_bytes(), CmdType::Read);
        hmap.insert("ZREM".as_bytes(), CmdType::Write);
        hmap.insert("ZREMRANGEBYLEX".as_bytes(), CmdType::Write);
        hmap.insert("ZREMRANGEBYRANK".as_bytes(), CmdType::Write);
        hmap.insert("ZREMRANGEBYSCORE".as_bytes(), CmdType::Write);
        hmap.insert("ZREVRANGE".as_bytes(), CmdType::Read);
        hmap.insert("ZREVRANGEBYLEX".as_bytes(), CmdType::Read);
        hmap.insert("ZREVRANGEBYSCORE".as_bytes(), CmdType::Read);
        hmap.insert("ZREVRANK".as_bytes(), CmdType::Read);
        hmap.insert("ZSCORE".as_bytes(), CmdType::Read);
        hmap.insert("ZUNIONSTORE".as_bytes(), CmdType::Write);
        hmap.insert("ZSCAN".as_bytes(), CmdType::Read);
        // hyper log type
        hmap.insert("PFADD".as_bytes(), CmdType::Write);
        hmap.insert("PFCOUNT".as_bytes(), CmdType::Read);
        hmap.insert("PFMERGE".as_bytes(), CmdType::Write);
        // eval type
        hmap.insert("EVAL".as_bytes(), CmdType::Write);
        hmap.insert("EVALSHA".as_bytes(), CmdType::NotSupport);
        // ctrl type
        hmap.insert("AUTH".as_bytes(), CmdType::NotSupport);
        hmap.insert("ECHO".as_bytes(), CmdType::Ctrl);
        hmap.insert("PING".as_bytes(), CmdType::Ctrl);
        hmap.insert("INFO".as_bytes(), CmdType::Ctrl);
        hmap.insert("PROXY".as_bytes(), CmdType::NotSupport);
        hmap.insert("SLOWLOG".as_bytes(), CmdType::NotSupport);
        hmap.insert("QUIT".as_bytes(), CmdType::NotSupport);
        hmap.insert("SELECT".as_bytes(), CmdType::NotSupport);
        hmap.insert("TIME".as_bytes(), CmdType::NotSupport);
        hmap.insert("CONFIG".as_bytes(), CmdType::NotSupport);

        hmap
    };
}
