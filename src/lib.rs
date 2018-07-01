extern crate tokio;
#[macro_use(try_ready)]
extern crate futures;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate bytes;
extern crate crossbeam;
extern crate num_cpus;
#[macro_use]
extern crate lazy_static;
extern crate aho_corasick;
extern crate btoi;

pub mod com;

pub use com::*;

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
const space: u8 = ' ' as u8;

pub fn proxy() -> Result<()> {
    env_logger::init();
    info!("ass we can!");
    Ok(())
}

fn handle(addr: &str) -> Result<()> {
    let cluster = Arc::new(Cluster {
        cc: ClusterConfig {},
    });

    let taddr = addr.parse().expect("bad listen address");
    let listener = TcpListener::bind(&taddr).unwrap();
    let server = listener
        .incoming()
        .map_err(|e| error!("accept fail: {}", e))
        .for_each(|sock| {
            let exec = Executor {
                cluster: cluster.clone(),
                sock: Socket {
                    sock: sock,
                    buf: BytesMut::new(),
                    eof: false,
                },
            };
            tokio::executor::spawn(exec)
        });
    // TODO: add cluster to executor
    tokio::run(server);
    Ok(())
}

pub struct Executor {
    cluster: Arc<Cluster>,
    sock: Socket,
}

pub struct Socket {
    buf: BytesMut,
    sock: TcpStream,

    eof: bool,
}

impl Socket {
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

impl Future for Executor {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
        Ok(Async::NotReady)
    }
}

pub struct ClusterConfig {}

pub struct Cluster {
    cc: ClusterConfig,
}

pub struct Node {
    addr: String,
}

pub struct MsgBatch {
    buf: BytesMut,
    count: usize,
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
            "set", // 0
            "add",
            "replace",
            "append",
            "prepend",
            "cas", // 5
            // retrieval cmd
            "get", // 6
            "gets", // 7
            // delete cmd,
            "delete", // 8
            // incr,decr cmd
            "incr", // 9
            "decr", // 10
            // touch
            "touch", // 11
            // get and touch
            "gat", // 12
            "gats", // 13
        ]
    };
    static ref CMD_MAP: AcAutomaton<&'static str> = {
        AcAutomaton::new(CMDS.clone())
    };
}

pub struct GetAndTouch {
    pub cmd: Range,
    pub expire: Range,
    pub key: Range,
    pub data: Vec<u8>,
    pub done: AtomicBool,
}

pub enum MCReq {
    Msg {
        cmd: Range,
        key: Range,
        data: Vec<u8>,
        done: AtomicBool,
    },
    Gat(Vec<GetAndTouch>),
    Sub(Vec<MCReq>),
}

impl MCReq {
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
            count(key_bytes, space)
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

            let sub = GetAndTouch {
                cmd: cmd.clone(),
                key: key,
                expire: expire.clone(),
                data: data,
                done: AtomicBool::new(false),
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

        Ok(MCReq::Msg {
            cmd: cmd,
            key: key,
            data: data,
            done: AtomicBool::new(false),
        })
    }

    fn decode_one_line(src: &mut [u8], cmd: Range, end: usize) -> Result<MCReq> {
        let key = MCReq::get_field(&src[cmd.1 + 1..end], cmd.1 + 1)?;

        let ptr = src.as_mut_ptr();
        let data = unsafe { Vec::from_raw_parts(ptr, end, end) };

        Ok(MCReq::Msg {
            cmd: cmd,
            key: key,
            data: data,
            done: AtomicBool::new(false),
        })
    }

    fn decode_retrieval(src: &mut [u8], cmd: Range, end: usize) -> Result<MCReq> {
        let mut i = cmd.1 + 1;

        let cum = {
            let key_bytes = &src[i..end];
            count(key_bytes, space)
        };

        let mut subs = Vec::with_capacity(cum);
        let key_end = end - 2;

        loop {
            if i >= key_end {
                break;
            }

            let key = MCReq::get_field(&src[i..key_end], i)?;
            i = key.1 + 1;
            // let j = position(&src[i..key_end], space).ok_or(Error::BadKey)?;
            // let key = Range(i, j);

            let ptr = src.as_mut_ptr();
            let data = unsafe { Vec::from_raw_parts(ptr, end, end) };

            let sub = MCReq::Msg {
                cmd: cmd.clone(),
                key: key,
                data: data,
                done: AtomicBool::new(false),
            };
            subs.push(sub);
        }
        Ok(MCReq::Sub(subs))
    }

    fn decode_len(src: &[u8], cas: bool) -> Result<usize> {
        let k = rposition(&src[..], space).ok_or(Error::BadMsg)?;
        let j = rposition(&src[..k], space).ok_or(Error::BadMsg)?;
        if cas {
            let i = rposition(&src[..j], space).ok_or(Error::BadMsg)?;
            btoi::btoi(&src[i + 1..j]).map_err(|_err| Error::BadMsg)
        } else {
            btoi::btoi(&src[j + 1..k]).map_err(|_err| Error::BadMsg)
        }
    }

    fn get_field(data: &[u8], offset: usize) -> Result<Range> {
        let i = position(data, space).ok_or(Error::BadKey)?;
        let j = position(&data[i..], space).ok_or(Error::BadKey)?;
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
