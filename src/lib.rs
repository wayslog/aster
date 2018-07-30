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
extern crate itoa;
extern crate tokio_codec;
extern crate crc16;
extern crate tokio_io;

pub mod com;
pub use com::*;

// use std::cell::RefCell;
// use std::mem;
// use std::rc::{Rc, Weak};

// use futures::future::join_all;
use futures::sync::mpsc::{channel, Receiver, SendError, Sender};
use futures::task::{self, Task};
use futures::{Async, AsyncSink};
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

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;
use std::mem;
// use std::sync::Mutex;
// use std::convert::From;
// use std::hash::{Hash, Hasher};
// use std::io::Error as IoError;
// use std::sync::atomic::bool;
// use std::sync::Arc;

pub const SLOTS_COUNT: usize = 16384;
pub static LF_STR: &'static str = "\n";

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

pub struct Slots(Vec<String>);

impl Slots {
    fn parse(data :&[u8]) -> AsResult<Slots> {
        let content = String::from_utf8_lossy(data);
        let mut slots = Vec::with_capacity(SLOTS_COUNT);
        slots.resize(SLOTS_COUNT, "".to_owned());
        let mapper = content.split(LF_STR).filter_map(|line|{
            if line.len() == 0 {
                return None;
            }

            let items:Vec<_> = line.split(" ").collect();
            if !items[2].contains("master") {
                return None;
            }
            let sub_slots:Vec<_> = items[8..].iter().map(|x| x).map(|item|{
                Self::parse_item(item)
            }).flatten().collect();
            let addr = items[1].split("@").next().expect("must contains addr");

            Some((addr.to_owned(), sub_slots))
        });
        let mut count = 0;
        for (addr, ss) in mapper {
            for i in ss.into_iter() {
                slots[i] = addr.clone();
                count += 1;
            }
        }
        if count != SLOTS_COUNT {
            return Err(Error::BadSlotsMap);
        } else {
            Ok(Slots(slots))
        }
    }

    fn parse_item(item: &str) -> Vec<usize> {
        let mut slots = Vec::new();
        if item.len() == 0 {
            return slots;
        }
        let mut iter = item.split("-");
        let begin_str = iter.next().expect("must have integer");
        let begin = begin_str.parse::<usize>().expect("must parse integer done");
        if let Some(end_str) = iter.next(){
            let end = end_str.parse::<usize>().expect("must parse end integer done");
            for i in begin..=end {
                slots.push(i);
            }
        } else {
            slots.push(begin);
        }
        slots
    }
}

impl Slots {
    fn crc16(&self) -> u16 {
        let mut state = crc16::State::<crc16::XMODEM>::new();
        for addr in self.0.iter(){
            state.update(addr.as_bytes());
        }
        state.get()
    }
}

pub struct SlotsMap {
    nodes: HashMap<String, Sender<Cmd>>,
    slots: Vec<String>,
    crc16: u16,
}

impl SlotsMap {
    pub fn try_update_all(&mut self, data: &[u8]) -> bool {
        match Slots::parse(data) {
            Ok(slots) => {
                let mut slots = slots;
                if self.crc16() == slots.crc16() {
                    return false;
                }
                mem::swap(&mut self.slots, &mut slots.0);
                true
            }
            Err(err) => {
                warn!("fail to update slots map by given data due {:?}", err);
                false
            }
        }
    }

    pub fn get_sender_by_addr(&mut self, node: String) -> &mut Sender<Cmd> {
         self.nodes.entry(node).or_insert_with(||{
            let (tx, _rx) = channel(1024);
            tx
        })
    }

    pub fn get_addr(&mut self, slot: usize) -> String {
        self.slots.get(slot).cloned().expect("slot must be full matched")
    }

    fn crc16(&self) -> u16 {
        self.crc16
    }

}

pub struct Cluster {
    cc: ClusterConfig,
    slots: SlotsMap,
}

impl Cluster {
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

    pub fn dispatch(&self, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        Ok(AsyncSink::Ready)
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

pub const BYTES_CRLF: &'static [u8] = b"\r\n";
pub const BYTES_NULL_RESP: &'static [u8] = b"-1\r\n";

#[derive(Clone, Debug)]
pub struct Resp {
    pub rtype: RespType,
    pub data: Option<Vec<u8>>,
    pub array: Option<Vec<Resp>>,
}

impl Resp {
    fn parse(src: &[u8]) -> AsResult<Self> {
        let mut iter = src.splitn(2, |x| *x == BYTE_LF);
        let line = iter.next().ok_or(Error::MoreData)?;

        let line_size = line.len();
        let rtype = line[0];

        match rtype {
            RESP_STRING | RESP_INT | RESP_ERROR => Ok(Resp {
                rtype: rtype,
                data: Some(line[1..line_size - 2].to_vec()),
                array: None,
            }),
            RESP_BULK => {
                let count = btoi::btoi::<isize>(&line[1..line_size - 2])?;
                if count == -1 {
                    return Ok(Resp {
                        rtype: rtype,
                        data: None,
                        array: None,
                    });
                }
                let size = count as usize + 2;
                let data = iter.next().ok_or(Error::MoreData)?;
                if data.len() < size {
                    return Err(Error::MoreData);
                }

                Ok(Resp {
                    rtype: rtype,
                    data: Some(data[..size].to_vec()),
                    array: None,
                })
            }

            RESP_ARRAY => {
                let count_bs = &line[1..line_size - 2];
                let count = btoi::btoi::<isize>(count_bs)?;
                if count == -1 {
                    return Ok(Resp {
                        rtype: rtype,
                        data: None,
                        array: None,
                    });
                }

                let mut items = Vec::with_capacity(count as usize);
                let mut parsed = line_size;
                for _ in 0..count {
                    let item = Self::parse(&src[parsed..])?;
                    parsed += item.binary_size();
                    items.push(item);
                }

                Ok(Resp {
                    rtype: rtype,
                    data: Some(count_bs.to_vec()),
                    array: Some(items),
                })
            }
            _ => unreachable!(),
        }
    }

    fn write(&mut self, dst: &mut BytesMut) -> AsResult<usize> {
        match self.rtype {
            RESP_STRING | RESP_ERROR | RESP_INT => {
                dst.put_u8(self.rtype);
                let data = self.data.as_ref().expect("never empty");
                dst.put(data);
                dst.put(BYTES_CRLF);
                Ok(1 + 2 + data.len())
            }
            RESP_BULK => {
                dst.put_u8(self.rtype);
                if self.is_null() {
                    dst.put(BYTES_NULL_RESP);
                    return Ok(5);
                }

                let data = self.data.as_ref().expect("never nulll");
                let data_len = data.len();
                let len_len = itoa::write(&mut dst[1..], data_len)?;
                dst.put(BYTES_CRLF);
                dst.put(data);
                dst.put(BYTES_CRLF);
                Ok(1 + len_len + 2 + data_len + 2)
            }
            RESP_ARRAY => {
                dst.put_u8(self.rtype);
                if self.is_null() {
                    dst.put(BYTES_NULL_RESP);
                    return Ok(5);
                }

                let data = self.data.as_ref().expect("never null");
                dst.put(data);
                dst.put(BYTES_CRLF);
                let mut size = 1 + data.len() + 2;

                for item in self.array.as_mut().expect("never empty") {
                    size += item.write(dst)?;
                }
                Ok(size)
            }
            _ => unreachable!(),
        }
    }

    fn is_null(&self) -> bool {
        match self.rtype {
            RESP_BULK => self.data.is_none(),
            RESP_ARRAY => self.array.is_none(),
            _ => false,
        }
    }

    fn ascii_len(mut n: usize) -> usize {
        let mut len = 0;
        loop {
            if n == 0 {
                return len;
            } else if n < 10 {
                return len + 1;
            } else if n < 100 {
                return len + 2;
            } else if n < 1000 {
                return len + 3;
            } else {
                n /= 1000;
                len += 4;
            }
        }
    }

    fn binary_size(&self) -> usize {
        match self.rtype {
            RESP_STRING | RESP_ERROR | RESP_INT => {
                3 + self.data.as_ref().expect("never be empty").len()
            }
            RESP_BULK => {
                if self.is_null() {
                    return 5;
                }
                let dlen = self.data.as_ref().expect("never null").len();

                1 + Self::ascii_len(dlen) + 2 + dlen + 2
            }
            RESP_ARRAY => {
                if self.is_null() {
                    return 5;
                }
                let mut size = 1 + self.data.as_ref().expect("never null").len() + 2;
                for item in self.array.as_ref().expect("never empty") {
                    size += item.binary_size();
                }
                size
            }
            _ => unreachable!(),
        }
    }

    fn get(&self, i: usize) -> Option<&Self> {
        self.array
            .as_ref()
            .map(|x| x.get(i))
            .expect("must be array")
    }

    fn get_mut(&mut self, i: usize) -> Option<&mut Self> {
        self.array
            .as_mut()
            .map(|x| x.get_mut(i))
            .expect("must be array")
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
    fn from_resp(mut resp: Resp) -> Command {
        let local_task = task::current();
        Self::cmd_to_upper(&mut resp);
        let cmd_type = Self::get_cmd_type(&resp);
        let is_complex = Self::is_complex(&resp);

        Command {
            is_done: false,
            is_ask: false,
            is_inline: false,

            is_complex: is_complex,
            cmd_type: cmd_type,

            task: local_task,
            req: resp,
            reply: None,
        }
    }

    fn is_done(&self) -> bool {
        self.is_done
    }

    fn cmd_to_upper(resp: &mut Resp) {
        let cmd = resp.get_mut(0).expect("never be empty");
        update_to_upper(cmd.data.as_mut().expect("never null"));
    }

    fn is_complex(resp: &Resp) -> bool {
        let cmd = resp.get(0).expect("never be empty");
        CMD_COMPLEX.contains(&cmd.data.as_ref().expect("never null")[..])
    }

    fn get_cmd_type(resp: &Resp) -> CmdType {
        let cmd = resp.get(0).expect("never be empty");
        if let Some(&ctype) = CMD_TYPE.get(&cmd.data.as_ref().expect("never null")[..]) {
            return ctype;
        }
        CmdType::NotSupport
    }
}

pub struct CommandStream<S: Stream<Item = Resp, Error = Error>> {
    input: S,
}

impl<S> CommandStream<S>
where
    S: Stream<Item = Resp, Error = Error>,
{
    pub fn new(input: S) -> Self {
        Self { input: input }
    }
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

pub struct RcCmd<S: Stream> {
    input: S,
}

impl<S: Stream> Stream for RcCmd<S> {
    type Item = Rc<S::Item>;
    type Error = S::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if let Some(item) = try_ready!(self.input.poll()) {
            return Ok(Async::Ready(Some(Rc::new(item))));
        }
        Ok(Async::NotReady)
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

pub type Cmd = Rc<RefCell<Command>>;

pub enum State {
    Void,
    Batching,
    Writing,
}

pub struct Handler<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    cluster: Rc<Cluster>,

    input: I,
    output: O,

    cmd: Option<Cmd>,
    state: State,
}

impl<I, O> Handler<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    fn try_write_back(&mut self, cmd: Cmd) -> Result<Async<()>, Error> {
        if let AsyncSink::NotReady(_) = self.output.start_send(cmd)? {
            return Ok(Async::NotReady);
        }
        self.output.poll_complete().map_err(|err| {
            error!{"send error due to {:?}", err};
            Error::Critical
        })
    }

    fn fork_cmd(&mut self) -> Cmd {
        self.cmd.as_ref().cloned().expect("never be empty")
    }
}

impl<I, O> Future for Handler<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<()>, Self::Error> {
        loop {
            match self.state {
                State::Void => {
                    if let Some(rc_cmd) = try_ready!(self.input.poll()) {
                        self.cmd = Some(rc_cmd);
                        self.state = State::Batching;
                        continue;
                    }
                    return Ok(Async::NotReady);
                }
                State::Batching => {
                    let rc_cmd = self.fork_cmd();
                    let rslt = self.cluster.dispatch(rc_cmd.clone())?;
                    match rslt {
                        AsyncSink::NotReady(_) => return Ok(Async::NotReady),
                        AsyncSink::Ready => self.state = State::Writing,
                    };
                }
                State::Writing => {
                    let rc_cmd = self.fork_cmd();
                    if !rc_cmd.borrow().is_done() {
                        return Ok(Async::NotReady);
                    }
                    try_ready!(self.try_write_back(rc_cmd.clone()));
                    self.state = State::Void
                }
            };
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
    pub static ref CMD_COMPLEX: BTreeSet<&'static [u8]> = {
        let cmds = vec!["MSET", "MGET", "DEL", "EXISTS", "EVAL", "EVALSHAR"];

        let mut hset = BTreeSet::new();
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
