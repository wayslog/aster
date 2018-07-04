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

use std::cell::RefCell;
use std::mem;
use std::rc::{Rc, Weak};

// use futures::future::join_all;
use futures::stream::Fuse;
use futures::task::{self, Task};
use futures::unsync::mpsc::{channel, Receiver, SendError, Sender};
use futures::{Async, AsyncSink, Future, IntoFuture, Poll, Sink};

use aho_corasick::{AcAutomaton, Automaton, Match};
use bytes::BytesMut;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::Stream;
use tokio_codec::{BytesCodec, Decoder, Encoder};
// use tokio::prelude::*;

use std::convert::From;
use std::hash::{Hash, Hasher};
// use std::io::Error as IoError;
// use std::sync::atomic::bool;
// use std::sync::Arc;

const delim: u8 = '\n' as u8;
const SPACE: u8 = ' ' as u8;

pub fn run() -> AsResult<()> {
    env_logger::init();
    info!("ass we can!");
    Ok(())
}

pub fn proxy() {
    // let (tx, rx) = channel(1024);
    let addr = "127.0.0.1:7788";
    let taddr = addr.parse().expect("bad listen address");
    let listener = TcpListener::bind(&taddr).unwrap();
    let server = listener
        .incoming()
        .map_err(|e| error!("accept fail: {}", e))
        .for_each(move |sock| {
            // let mbc = MsgBatchCodec {};
            // let (sink, stream) = mbc.framed(sock).split();
            // let forward = stream
            //     .map_err(|err| {
            //         error!("read msg from batch fail: {:?}", err);
            //     })
            //     .forward(tx.clone().sink_map_err(|err| {
            //         error!("forword msg fail:{:?}", err);
            //     }))
            //     .map(|_| ());

            // tokio::executor::current_thread::spawn(forward);
            Ok(())
        });
}

pub struct Dispatcher<T, U, H>
where
    T: Stream<Item = Rc<RefCell<MsgBatch>>, Error = Error>,
    U: Sink<SinkItem = Rc<RefCell<MsgBatch>>, SinkError = Error>,
    H: Hasher,
{
    hasher: H,
    rx: T,
    chans: HashMap<String, U>,
}

pub struct Handler<T, U>
where
    T: Stream<Item = MsgBatch, Error = Error>,
    U: Sink<SinkItem = Rc<RefCell<MsgBatch>>, SinkError = Error>,
{
    stream: Option<Fuse<T>>,
    sink: Option<U>,

    state: HandlerState<T::Item>,
    tx: Sender<AsTask>,
}

pub enum HandlerState<T> {
    Done,
    Buffered(Rc<RefCell<T>>),
    Batch(Rc<RefCell<T>>),
    Write(Rc<RefCell<T>>),
    Wait(Rc<RefCell<T>>),
}

impl<T> Clone for HandlerState<T> {
    fn clone(&self) -> HandlerState<T> {
        match self {
            HandlerState::Done => HandlerState::Done,
            HandlerState::Buffered(rc) => HandlerState::Buffered(rc.clone()),
            HandlerState::Batch(rc) => HandlerState::Batch(rc.clone()),
            HandlerState::Write(rc) => HandlerState::Write(rc.clone()),
            HandlerState::Wait(rc) => HandlerState::Wait(rc.clone()),
        }
    }
}

impl<T, U> Handler<T, U>
where
    T: Stream<Item = MsgBatch, Error = Error>,
    U: Sink<SinkItem = Rc<RefCell<MsgBatch>>, SinkError = Error>,
{
    pub fn stream_mut(&mut self) -> Option<&mut T> {
        self.stream.as_mut().map(|x| x.get_mut())
    }

    pub fn stream_ref(&self) -> Option<&T> {
        self.stream.as_ref().map(|x| x.get_ref())
    }

    pub fn sink_mut(&mut self) -> Option<&mut U> {
        self.sink.as_mut()
    }

    pub fn sink_ref(&self) -> Option<&U> {
        self.sink.as_ref()
    }

    fn try_send(&mut self, item: Weak<RefCell<MsgBatch>>) -> Poll<(), Error> {
        // add debug_assert
        let task = AsTask {
            task: task::current(),
            mb: item.clone(),
        };
        match self.tx.start_send(task).map_err(|err| {
            error!("fail to send :{:?}", err);
            Error::Critical
        })? {
            AsyncSink::NotReady(_) => {
                // self.state = HandlerState::Buffered(item.upgrade().expect("item never empty"));
                Ok(Async::NotReady)
            }
            AsyncSink::Ready => Ok(Async::Ready(())),
        }
    }

    fn try_to_wait(&mut self, item: Rc<RefCell<MsgBatch>>) -> Poll<(), Error> {
        if item.borrow().is_done() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn try_to_sink(&mut self, item: Rc<RefCell<MsgBatch>>) -> Poll<(), Error> {
        match self
            .sink_mut()
            .take()
            .expect("sink never empty")
            .start_send(item)
            .map_err(|err| {
                error!("fail to send to sink:{:?}", err);
                Error::Critical
            })? {
            AsyncSink::NotReady(_) => Ok(Async::NotReady),
            AsyncSink::Ready => Ok(Async::Ready(())),
        }
    }

    fn try_to_complete(&mut self) -> Poll<(), Error> {
        self.sink_mut()
            .take()
            .expect("sink never empty")
            .poll_complete()
            .map_err(|err| {
                error!("fail to send to sink:{:?}", err);
                Error::Critical
            })
    }

    fn try_to_execute(&mut self, item: Async<Option<MsgBatch>>) -> Poll<(), Error> {
        match item {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(Some(batch)) => {
                let item = Rc::new(RefCell::new(batch));
                self.state = HandlerState::Buffered(item);
                Ok(Async::Ready(()))
            }
            Async::Ready(None) => {
                try_ready!(self.tx.close().map_err(|err| {
                    error!("fail to send :{:?}", err);
                    Error::Critical
                }));
                try_ready!(
                    self.sink_mut()
                        .take()
                        .expect("sink never empty")
                        .close()
                        .map_err(|err| {
                            error!("fail to send :{:?}", err);
                            Error::Critical
                        })
                );
                Ok(Async::Ready(()))
            }
        }
    }
}

impl<S, U> Future for Handler<S, U>
where
    S: Stream<Item = MsgBatch, Error = Error>,
    U: Sink<SinkItem = Rc<RefCell<MsgBatch>>, SinkError = Error>,
{
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let state = self.state.clone();
            match state {
                HandlerState::Done => {
                    let batch = self
                        .stream_mut()
                        .take()
                        .expect("stream never empty")
                        .poll()?;
                    try_ready!(self.try_to_execute(batch));
                }
                HandlerState::Buffered(rc) => {
                    try_ready!(self.try_send(Rc::downgrade(&rc)));
                    self.state = HandlerState::Batch(rc);
                }
                HandlerState::Batch(rc) => {
                    try_ready!(self.try_to_wait(rc.clone()));
                    self.state = HandlerState::Write(rc);
                }
                HandlerState::Write(rc) => {
                    try_ready!(self.try_to_sink(rc.clone()));
                    self.state = HandlerState::Wait(rc);
                }
                HandlerState::Wait(_rc) => {
                    try_ready!(self.try_to_complete());
                    self.state = HandlerState::Done;
                }
            }
        }
    }
}

pub struct MsgBatchCodec {}

impl Decoder for MsgBatchCodec {
    type Item = MsgBatch;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut batch = MsgBatch::default();
        let size = batch.decode(src)?;
        if size == 0 {
            return Ok(None);
        }
        src.advance(size);
        Ok(Some(batch))
    }
}

impl Encoder for MsgBatchCodec {
    type Item = Rc<RefCell<MsgBatch>>;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if dst.len() == 0 {
            dst.clone_from_slice(&item.borrow().buf);
        } else {
            dst.extend_from_slice(&item.borrow().buf);
        }
        Ok(())
    }
}

pub struct AsTask {
    task: Task,
    mb: Weak<RefCell<MsgBatch>>,
}

impl AsTask {
    pub fn notify(&self) {
        self.task.notify();
    }

    pub fn upgrade(&self) -> Option<Rc<RefCell<MsgBatch>>> {
        self.mb.upgrade()
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
    pub fn is_done(&self) -> bool {
        self.reqs.iter().all(|x| x.is_done())
    }

    pub fn push(&mut self, req: MCReq) {
        self.reqs.push(req)
    }

    pub fn msgs_mut(&mut self) -> &mut [MCReq] {
        &mut self.reqs
    }

    fn decode(&mut self, data: &mut [u8]) -> AsResult<usize> {
        loop {
            if self.cursor == data.len() {
                return Ok(self.len());
            }
            let req = MCReq::decode(&mut data[self.cursor..])?;
            self.cursor += req.len();
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
    pub done: bool,
}

impl Cmd {
    fn done(&mut self) {
        self.done = true;
    }
}

pub enum MCReq {
    Msg(Cmd),
    Gat(Vec<GatCmd>),
    Sub(Vec<MCReq>),
}

impl MCReq {
    pub fn is_done(&self) -> bool {
        // TODO: impl it
        true
    }

    pub fn len(&self) -> usize {
        match &self {
            MCReq::Msg(cmd) => cmd.data.len(),
            MCReq::Gat(cmds) => cmds.get(0).map(|gat| gat.cmd.data.len()).unwrap(),
            MCReq::Sub(cmds) => cmds.get(0).map(|sub| sub.len()).unwrap(),
        }
    }

    pub fn decode(src: &mut [u8]) -> AsResult<Self> {
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

    fn decode_get_and_touch(src: &mut [u8], cmd: Range, end: usize) -> AsResult<MCReq> {
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
                    done: false,
                },
                expire: expire.clone(),
            };
            subs.push(sub);
        }
        Ok(MCReq::Gat(subs))
    }

    fn decode_storage(src: &mut [u8], cmd: Range, end: usize, cas: bool) -> AsResult<MCReq> {
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
            done: false,
        }))
    }

    fn decode_one_line(src: &mut [u8], cmd: Range, end: usize) -> AsResult<MCReq> {
        let key = MCReq::get_field(&src[cmd.1 + 1..end], cmd.1 + 1)?;

        let ptr = src.as_mut_ptr();
        let data = unsafe { Vec::from_raw_parts(ptr, end, end) };

        Ok(MCReq::Msg(Cmd {
            cmd: cmd,
            key: key,
            data: data,
            done: false,
        }))
    }

    fn decode_retrieval(src: &mut [u8], cmd: Range, end: usize) -> AsResult<MCReq> {
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
                done: false,
            });
            subs.push(sub);
        }
        Ok(MCReq::Sub(subs))
    }

    fn decode_len(src: &[u8], cas: bool) -> AsResult<usize> {
        let k = rposition(&src[..], SPACE).ok_or(Error::BadMsg)?;
        let j = rposition(&src[..k], SPACE).ok_or(Error::BadMsg)?;
        if cas {
            let i = rposition(&src[..j], SPACE).ok_or(Error::BadMsg)?;
            btoi::btoi(&src[i + 1..j]).map_err(|_err| Error::BadMsg)
        } else {
            btoi::btoi(&src[j + 1..k]).map_err(|_err| Error::BadMsg)
        }
    }

    fn get_field(data: &[u8], offset: usize) -> AsResult<Range> {
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
