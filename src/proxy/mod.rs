//! proxy is the mod which contains genneral proxy
use com::*;

use futures::{Async, AsyncSink, Future, Sink, Stream};

use std::collections::VecDeque;
use std::fmt::Debug;
use std::rc::Rc;
// use std::cell::RefCell;
use std::marker::PhantomData;

const MAX_CONCURRENCY: usize = 1024;

pub struct Proxy<T: Request> {
    _placeholder: PhantomData<T>,
}

impl<T: Request> Proxy<T> {
    fn dispatch_all(&self, _cmds: &mut VecDeque<T>) -> Result<AsyncSink<usize>, Error> {
        unimplemented!()
    }
}

pub trait Request: Sized + Clone + Debug {
    type Reply: Clone + Debug + Sized;
    type ReqError: Clone;

    fn key(&self) -> &[u8];
    fn cmd(&self) -> &[u8];
    fn subs(&self) -> Option<Vec<Self>>;
    fn is_done(&self) -> bool;
    fn is_complex(&self) -> bool;

    fn is_notsupport(&self) -> bool;

    fn done(&self, data: Self::Reply);
    fn done_with_error(&self, err: &Self::ReqError);
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

            if let AsyncSink::NotReady(_count) = self.proxy.dispatch_all(&mut self.waitq)? {
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
    T: Request<ReqError = Vec<u8>>,
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
                    let is_complex = cmd.is_complex();
                    self.cmds.push_back(cmd.clone());

                    if is_complex {
                        for sub in cmd.subs().expect("sub_reqs in try_read never be empty") {
                            self.waitq.push_back(sub);
                        }
                    } else {
                        if cmd.is_notsupport() {
                            // TODO: impl as lazy_static
                            let notsupport = b"cmd not support".to_vec();
                            cmd.done_with_error(&notsupport);
                            continue;
                        }
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
    T: Request<ReqError = Vec<u8>>,
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
