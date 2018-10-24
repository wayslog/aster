//! proxy is the mod which contains genneral proxy
use com::*;
use fnv::Fnv1a64;
use ketama::HashRing;

use futures::sync::mpsc::Sender;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use tokio_codec::{Decoder, Encoder};

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::rc::Rc;

const MAX_CONCURRENCY: usize = 1024;

pub struct Proxy<T: Request> {
    ring: RefCell<HashRing<Fnv1a64>>,
    conns: RefCell<HashMap<String, Sender<T>>>,
}

impl<T: Request> Proxy<T> {
    fn create_conn(_node: String) -> AsResult<Sender<T>> {
        unimplemented!();
    }

    fn dispatch_all(&self, cmds: &mut VecDeque<T>) -> Result<AsyncSink<()>, Error> {
        let mut nodes = HashSet::new();
        loop {
            if let Some(cmd) = cmds.front().cloned() {
                let node = self.ring.borrow().get_node(cmd.key());
                if let Some(conn) = self.conns.borrow_mut().get_mut(&node) {
                    match conn.start_send(cmd).map_err(|err| {
                        error!("fail to dispatch to backend due to {:?}", err);
                        Error::Critical
                    })? {
                        AsyncSink::Ready => {
                            let _ = cmds.pop_front().expect("pop_front never be empty");
                            nodes.insert(node.clone());
                        }
                        AsyncSink::NotReady(_) => {
                            break;
                        }
                    }
                } else {
                    let conn = Self::create_conn(node.clone())?;
                    self.conns.borrow_mut().insert(node, conn);
                }
            } else {
                return Ok(AsyncSink::Ready);
            }
        }

        if !nodes.is_empty() {
            for node in nodes.into_iter() {
                self.conns
                    .borrow_mut()
                    .get_mut(&node)
                    .expect("node connection is never be absent")
                    .poll_complete()
                    .map_err(|err| {
                        error!("fail to complete to proxy to backend due to {:?}", err);
                        Error::Critical
                    })?;
            }
        }

        Ok(AsyncSink::Ready)
    }
}

pub trait Request: Sized + Clone + Debug {
    type Reply: Clone + Debug + Sized;
    type HandleCodec: Decoder<Item = Self, Error = Error> + Encoder<Item = Self, Error = Error>;
    type NodeCodec: Decoder<Item = Self::Reply, Error = Error> + Encoder<Item = Self, Error = Error>;

    fn handle_codec() -> Self::HandleCodec;
    fn node_codec() -> Self::NodeCodec;

    fn key(&self) -> Vec<u8>;
    fn subs(&self) -> Option<Vec<Self>>;
    fn is_done(&self) -> bool;

    fn valid(&self) -> bool;
    fn done(&self, data: Self::Reply);
    fn done_with_error(&self, err: Error);
}

pub struct Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
    D: Into<T>,
{
    proxy: Rc<Proxy<T>>,
    input: I,
    output: O,
    cmds: VecDeque<T>,
    count: usize,
    waitq: VecDeque<T>,
}

impl<T, I, O, D> Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
    D: Into<T>,
{
    fn try_send(&mut self) -> Result<Async<()>, Error> {
        loop {
            if self.waitq.is_empty() {
                return Ok(Async::NotReady);
            }

            if let AsyncSink::NotReady(()) = self.proxy.dispatch_all(&mut self.waitq)? {
                return Ok(Async::NotReady);
            }
        }
    }

    fn try_write(&mut self) -> Result<Async<()>, Error> {
        let ret: Result<Async<()>, Error> = Ok(Async::NotReady);
        loop {
            if self.cmds.is_empty() {
                break;
            }

            let rc_cmd = self.cmds.front().cloned().expect("cmds is never be None");
            if !rc_cmd.is_done() {
                break;
            }

            match self.output.start_send(rc_cmd)? {
                AsyncSink::NotReady(_) => {
                    break;
                }
                AsyncSink::Ready => {
                    let _ = self.cmds.pop_front().unwrap();
                    self.count += 1;
                }
            }
        }

        if self.count > 0 {
            try_ready!(self.output.poll_complete());
            self.count = 0;
        }

        ret
    }
}

impl<T, I, O, D> Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
    D: Into<T>,
{
    fn try_read(&mut self) -> Result<Async<Option<()>>, Error> {
        loop {
            if self.cmds.len() > MAX_CONCURRENCY {
                return Ok(Async::NotReady);
            }

            match try_ready!(self.input.poll()) {
                Some(val) => {
                    let cmd: T = Into::into(val);
                    self.cmds.push_back(cmd.clone());
                    if !cmd.valid() {
                        continue;
                    }

                    if let Some(subs) = cmd.subs() {
                        for sub in subs.into_iter() {
                            self.waitq.push_back(sub);
                        }
                    } else {
                        self.waitq.push_back(cmd);
                    }
                }
                None => {
                    return Ok(Async::Ready(None));
                }
            }
        }
    }
}

impl<T, I, O, D> Future for Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
    D: Into<T>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_read = true;
        let mut can_send = true;
        let mut can_write = true;

        loop {
            if !(can_read && can_send && can_write) {
                return Ok(Async::NotReady);
            }

            // step 1: poll read from input stream.
            if can_read {
                // read until the input stream is NotReady.
                match self.try_read()? {
                    Async::NotReady => {
                        can_read = false;
                    }
                    Async::Ready(None) => {
                        return Ok(Async::Ready(()));
                    }
                    Async::Ready(Some(())) => {}
                }
            }

            // step 2: send to cluster.
            if can_send {
                // send until the output stream is unsendable.
                match self.try_send()? {
                    Async::NotReady => {
                        can_send = false;
                    }
                    Async::Ready(_) => {}
                }
            }

            // step 3: wait all the cluster is done.
            if can_write {
                // step 4: poll send back to client.
                match self.try_write()? {
                    Async::NotReady => {
                        can_write = false;
                    }
                    Async::Ready(_) => {}
                }
            }
        }
    }
}

pub struct NodeDown<T, I, O>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
{
    closed: bool,
    input: I,
    output: O,
    store: VecDeque<T>,
    buf: Rc<RefCell<VecDeque<T>>>,
    count: usize,
}

impl<T, I, O> NodeDown<T, I, O>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
{
    fn try_forword(&mut self) -> AsResult<()> {
        loop {
            if !self.store.is_empty() {
                let cmd = self
                    .store
                    .front()
                    .cloned()
                    .expect("node down store is never be empty");
                match self.output.start_send(cmd)? {
                    AsyncSink::NotReady(_) => {
                        return Ok(());
                    }
                    AsyncSink::Ready => {
                        let cmd = self
                            .store
                            .pop_front()
                            .expect("try_forward store never be empty");
                        self.buf.borrow_mut().push_back(cmd);
                        self.count += 1;
                        continue;
                    }
                }
            }

            match self.input.poll()? {
                Async::Ready(Some(v)) => {
                    self.store.push_back(v);
                }

                Async::Ready(None) => {
                    self.closed = true;
                    return Ok(());
                }

                Async::NotReady => {
                    return Ok(());
                }
            }
        }
    }
}

impl<T, I, O> Future for NodeDown<T, I, O>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
{
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.closed {
            return Ok(Async::Ready(()));
        }

        self.try_forword()
            .map_err(|err| error!("fail to forward due to {:?}", err))?;

        if self.count > 0 {
            try_ready!(self.output.poll_complete().map_err(|err| {
                error!("fail to flush into backend due to {:?}", err);
                self.closed = true;
            }));
            self.count = 0;
        }
        Ok(Async::NotReady)
    }
}

pub struct NodeRecv<T, S>
where
    S: Stream<Item = T::Reply, Error = Error>,
    T: Request,
{
    closed: bool,
    recv: S,
    buf: Rc<RefCell<VecDeque<T>>>,
}

impl<T, S> Future for NodeRecv<T, S>
where
    S: Stream<Item = T::Reply, Error = Error>,
    T: Request,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.closed {
            return Ok(Async::Ready(()));
        }

        loop {
            if let Some(resp) = try_ready!(self.recv.poll().map_err(|err| {
                error!("fail to recv from back end, may closed due to {:?}", err);
                self.closed = true;
            })) {
                let cmd = self.buf.borrow_mut().pop_front().unwrap();
                cmd.done(resp);
            } else {
                error!("TODO: should quick error for");
                self.closed = true;
                return Ok(Async::Ready(()));
            }
        }
    }
}
