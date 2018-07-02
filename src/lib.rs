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

pub mod com;

pub use com::*;

use std::mem;
use std::cell::RefCell;
use std::rc::{Rc, Weak};

use futures::future::join_all;
use futures::unsync::mpsc::{channel, Receiver, Sender};
use futures::{Async, Future, IntoFuture, Poll, Stream};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use aho_corasick::{AcAutomaton, Automaton, Match};
use std::io::Error as IoError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const delim: u8 = '\n' as u8;
const SPACE: u8 = ' ' as u8;

pub fn proxy() -> Result<()> {
    env_logger::init();
    info!("ass we can!");
    Ok(())
}

fn handle(addr: &str) -> std::result::Result<(), ()> {
    let (tx, rx) = channel(1024);
    let cluster = Rc::new(RefCell::new(Cluster {
        cc: ClusterConfig {
            servers: Vec::new(),
        },
    }));

    let pcluster = cluster.clone();
    let dcluster = cluster.clone();

    let dispatch = Dispatch {
        cluster: dcluster,
        rx: rx,
        txs: vec![tx.clone()],
    };

    // TODO: add cluster to executor
    let nodes: Vec<_> = (&*pcluster.borrow())
        .cc
        .servers
        .clone()
        .into_iter()
        .map(|server| forword(server.to_owned()))
        .collect();

    let taddr = addr.parse().expect("bad listen address");
    let listener = TcpListener::bind(&taddr).unwrap();
    let server = listener
        .incoming()
        .map_err(|e| error!("accept fail: {}", e))
        .for_each(move |sock| {
            let handler = Handler {
                tx: tx.clone(),
                cluster: cluster.clone(),
                state: State::Empty,
                sock: Socket {
                    sock: sock,
                    buf: BytesMut::new(),
                    eof: false,
                },
            };
            tokio::executor::current_thread::spawn(handler);
            Ok(())
        });

    let amt = join_all(nodes).join3(dispatch, server).map(|_| ());
    tokio::executor::current_thread::block_on_all(amt)
}

fn forword(
    addr: String,
) -> impl Future<Item = tokio::executor::Spawn, Error = ()> + 'static + Send {
    let taddr = addr.parse().expect("bad server address");
    TcpStream::connect(&taddr)
        .map(move |sock| {
            let node = Executor {
                addr: addr.to_owned(),
                sock: Socket {
                    buf: BytesMut::new(),
                    sock: sock,
                    eof: false,
                },
            };
            tokio::executor::spawn(node)
        })
        .map_err(|err| error!("erro {:?}", err))
}

pub struct Socket {
    buf: BytesMut,
    sock: TcpStream,
    eof: bool,
}

impl Socket {
    fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    fn buffer(&self) -> &BytesMut {
        &self.buf
    }

    fn poll_read(&mut self) -> Poll<usize, IoError> {
        loop {
            if !self.buf.has_remaining_mut() {
                let len = self.buf.len();
                self.buf.reserve(len);
            }

            match self.sock.read_buf(&mut self.buf) {
                Ok(Async::Ready(size)) => {
                    if size == 0 {
                        self.eof = false;
                    } else {
                        return Ok(Async::Ready(size));
                    }
                }
                other => {
                    return other;
                }
            };
        }
    }

    fn poll_write(&mut self) -> Poll<usize, IoError> {
        Ok(Async::NotReady)
    }
}

pub struct Handler {
    cluster: Rc<RefCell<Cluster>>,
    sock: Socket,
    tx: Sender<MsgBatch>,
    batch: MsgBatch,
}

impl Future for Handler {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        loop {
            match self.batch.decode(self.sock.buffer_mut()) {
                Ok(size) => {
                    trace!("decoded msg: {}", size);
                    self.sock.buf.advance(size);
                },
                Err(Error::MoreData)  => {
                    let mut batch = MsgBatch::default();
                    mem::swap(&mut self.batch, &mut batch);
                    self.tx.send(batch);
                }
                Err(err) => {
                    error!("fail to parse {:?}", err);
                },
            };
            let _size = try_ready!(self.sock.poll_read().map_err(|err| {
                error!("error when proxy read: {:?}", err);
            }));
        }

    }
}

pub struct ClusterConfig {
    servers: Vec<String>,
}

pub struct Cluster {
    cc: ClusterConfig,
}

unsafe impl Send for Cluster {}

pub struct Dispatch {
    cluster: Rc<RefCell<Cluster>>,
    rx: Receiver<MsgBatch>,
    txs: Vec<Sender<MsgBatch>>,
}

impl Future for Dispatch {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::NotReady)
    }
}

pub struct Executor {
    addr: String,
    sock: Socket,
}

impl Future for Executor {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::NotReady)
    }
}

pub struct MsgBatch {
    buf: BytesMut,
    reqs: Vec<MCReq>,
    cursor: usize,
}

impl MsgBatch {
    pub fn len(&self) -> usize {
        self.cursor
    }

    fn push(&mut self, req: MCReq) {
        self.reqs.push(req)
    }

    fn decode(&mut self, data:&mut [u8]) -> Result<usize> {
        loop {
            if self.cursor == data.len() {
                return Ok(self.len());
            }
            let req = MCReq::decode(&mut data[self.cursor..])?;
            self.cursor+= req.len();
            self.reqs.push(req);
        }
    }
}

impl Default for MsgBatch {
    fn default() -> Self {
        MsgBatch {
            buf: BytesMut::new(),
            reqs: Vec::new(),
            cursor: 0,
        }
    }
}

pub trait Msg {
    fn cmd(&self) -> &[u8];
    fn key(&self) -> &[u8];
    // data is the full data of the Msg.
    // In request, that's full request;
    // In response, that's full response;
    fn data(&self) -> &[u8];
}

fn position(data: &[u8], byte: u8) -> Option<usize> {
    let mut i = 0;
    for &d in data {
        if d == byte {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn rposition(data: &[u8], byte: u8) -> Option<usize> {
    let mut i = data.len() - 1;
    loop {
        if i == data.len() {
            return None;
        }
        if data[i] == byte {
            return Some(i);
        }
        i -= 1;
    }
}

fn count(data: &[u8], byte: u8) -> usize {
    let mut cum = 0;
    for &b in data {
        if byte == b {
            cum += 1;
        }
    }
    cum
}

lazy_static!{
    static ref CMDS: Vec<&'static str> = {
        vec![
            // storage cmd
            // 0 ~ 5
            "set", "add", "replace", "append", "prepend", "cas",
            // retrieval cmd
            // 6 ~ 7
            "get", "gets",
            // delete cmd,
            // 8
            "delete",
            // incr,decr cmd
            // 9 ~ 10
            "incr", "decr",
            // touch
            // 11
            "touch",
            // get and touch
            // 12 ~ 13
            "gat", "gats",
        ]
    };
    static ref CMD_MAP: AcAutomaton<&'static str> = {
        AcAutomaton::new(CMDS.clone())
    };
}

pub struct GatCmd {
    pub cmd: Cmd,
    pub expire: Range,
}

pub struct Cmd {
    pub cmd: Range,
    pub key: Range,
    pub data: Vec<u8>,
    pub done: AtomicBool,
}

pub enum MCReq {
    Msg(Cmd),
    Gat(Vec<GatCmd>),
    Sub(Vec<MCReq>),
}

impl MCReq {
    pub fn len(&self) -> usize {
        match &self {
            MCReq::Msg(cmd) => cmd.data.len(),
            MCReq::Gat(cmds) => cmds.get(0).map(|gat| gat.cmd.data.len()).unwrap(),
            MCReq::Sub(cmds) => cmds.get(0).map(|sub| sub.len()).unwrap(),
        }
    }


    pub fn decode(src: &mut [u8]) -> Result<Self> {
        let pos = position(src, delim).ok_or(Error::BadMsg)?;

        let Match {
            pati: p,
            start: s,
            end: e,
        } = CMD_MAP.find(src).next().ok_or(Error::BadCmd)?;
        let cmd = Range(s, e);
        match p {
            0...5 => Self::decode_storage(src, cmd, pos + 1, p == 5),
            6 | 7 => Self::decode_retrieval(src, cmd, pos + 1),
            8...11 => Self::decode_one_line(src, cmd, pos + 1),
            12 | 13 => Self::decode_get_and_touch(src, cmd, pos + 1),
            _ => unreachable!(),
        }
    }

    fn decode_get_and_touch(src: &mut [u8], cmd: Range, end: usize) -> Result<MCReq> {
        let expire = Self::get_field(&src[cmd.1 + 1..end], cmd.1 + 1)?;

        let mut i = expire.1 + 1;
        let cum = {
            let key_bytes = &src[i..end];
            count(key_bytes, SPACE)
        };
        let mut subs = Vec::with_capacity(cum);
        let key_end = end - 2;

        loop {
            if i >= key_end {
                break;
            }

            let key = MCReq::get_field(&src[i..key_end], i)?;
            i = key.1 + 1;
            let ptr = src.as_mut_ptr();
            let data = unsafe { Vec::from_raw_parts(ptr, end, end) };

            let sub = GatCmd {
                cmd: Cmd {
                    cmd: cmd.clone(),
                    key: key,
                    data: data,
                    done: AtomicBool::new(false),
                },
                expire: expire.clone(),
            };
            subs.push(sub);
        }
        Ok(MCReq::Gat(subs))
    }

    fn decode_storage(src: &mut [u8], cmd: Range, end: usize, cas: bool) -> Result<MCReq> {
        let len = {
            let line = &src[..end];
            let len = Self::decode_len(&line, cas)?;
            line.len() + len + 2
        };
        let key = Self::get_field(&src[cmd.1 + 1..], cmd.1 + 1)?;
        let ptr = src.as_mut_ptr();
        let data = unsafe { Vec::from_raw_parts(ptr, len, len) };

        Ok(MCReq::Msg(Cmd {
            cmd: cmd,
            key: key,
            data: data,
            done: AtomicBool::new(false),
        }))
    }

    fn decode_one_line(src: &mut [u8], cmd: Range, end: usize) -> Result<MCReq> {
        let key = MCReq::get_field(&src[cmd.1 + 1..end], cmd.1 + 1)?;

        let ptr = src.as_mut_ptr();
        let data = unsafe { Vec::from_raw_parts(ptr, end, end) };

        Ok(MCReq::Msg(Cmd {
            cmd: cmd,
            key: key,
            data: data,
            done: AtomicBool::new(false),
        }))
    }

    fn decode_retrieval(src: &mut [u8], cmd: Range, end: usize) -> Result<MCReq> {
        let mut i = cmd.1 + 1;

        let cum = {
            let key_bytes = &src[i..end];
            count(key_bytes, SPACE)
        };

        let mut subs = Vec::with_capacity(cum);
        let key_end = end - 2;

        loop {
            if i >= key_end {
                break;
            }

            let key = MCReq::get_field(&src[i..key_end], i)?;
            i = key.1 + 1;
            // let j = position(&src[i..key_end], SPACE).ok_or(Error::BadKey)?;
            // let key = Range(i, j);

            let ptr = src.as_mut_ptr();
            let data = unsafe { Vec::from_raw_parts(ptr, end, end) };

            let sub = MCReq::Msg(Cmd {
                cmd: cmd.clone(),
                key: key,
                data: data,
                done: AtomicBool::new(false),
            });
            subs.push(sub);
        }
        Ok(MCReq::Sub(subs))
    }

    fn decode_len(src: &[u8], cas: bool) -> Result<usize> {
        let k = rposition(&src[..], SPACE).ok_or(Error::BadMsg)?;
        let j = rposition(&src[..k], SPACE).ok_or(Error::BadMsg)?;
        if cas {
            let i = rposition(&src[..j], SPACE).ok_or(Error::BadMsg)?;
            btoi::btoi(&src[i + 1..j]).map_err(|_err| Error::BadMsg)
        } else {
            btoi::btoi(&src[j + 1..k]).map_err(|_err| Error::BadMsg)
        }
    }

    fn get_field(data: &[u8], offset: usize) -> Result<Range> {
        let i = position(data, SPACE).ok_or(Error::BadKey)?;
        let j = position(&data[i..], SPACE).ok_or(Error::BadKey)?;
        Ok(Range(i + offset, j + offset))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Range(usize, usize);

impl Default for Range {
    fn default() -> Self {
        Range(0, 0)
    }
}
