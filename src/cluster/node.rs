use crate::com::*;
use crate::redis::cmd::{new_asking_cmd, Cmd};
use crate::redis::resp::{Resp, RESP_ERROR};

use tokio::prelude::{Async, AsyncSink, Future, Sink, Stream};

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

const REDIRECT_MOVED_DATA: &[u8] = b"MOVED ";
const REDIRECT_ASK_DATA: &[u8] = b"ASK";

pub struct NodeDown<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Rc<Resp>, SinkError = Error>,
{
    closed: bool,
    input: I,
    output: O,
    store: VecDeque<Cmd>,
    buf: Rc<RefCell<VecDeque<Cmd>>>,
    count: usize,
}

impl<I, O> NodeDown<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Rc<Resp>, SinkError = Error>,
{
    pub fn new(input: I, output: O, buf: Rc<RefCell<VecDeque<Cmd>>>) -> NodeDown<I, O> {
        NodeDown {
            input,
            output,
            buf,
            closed: false,
            store: VecDeque::new(),
            count: 0,
        }
    }

    fn try_forword(&mut self) -> AsResult<()> {
        loop {
            if !self.store.is_empty() {
                let req = self
                    .store
                    .front()
                    .map(|cmd| cmd.rc_req())
                    .expect("node down store is never be empty");
                match self.output.start_send(req)? {
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
                    if v.is_ask() {
                        self.store.push_back(new_asking_cmd());
                    }
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

impl<I, O> Future for NodeDown<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Rc<Resp>, SinkError = Error>,
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

pub struct NodeRecv<S, R>
where
    S: Stream<Item = Resp, Error = Error>,
    R: Sink<SinkItem = (String, Cmd)>,
{
    closed: bool,
    recv: S,
    rstore: Option<(String, Cmd)>,
    redirect: R,
    buf: Rc<RefCell<VecDeque<Cmd>>>,
}

impl<S, R> NodeRecv<S, R>
where
    S: Stream<Item = Resp, Error = Error>,
    R: Sink<SinkItem = (String, Cmd)>,
{
    pub fn new(recv: S, buf: Rc<RefCell<VecDeque<Cmd>>>, redirect: R) -> NodeRecv<S, R> {
        NodeRecv {
            closed: false,
            recv,
            buf,
            rstore: None,
            redirect,
        }
    }

    fn parse_redirect(resp: &Resp) -> Option<(bool, String)> {
        if resp.rtype != RESP_ERROR {
            return None;
        }

        if let Some(ref data) = resp.data.as_ref() {
            if data.starts_with(REDIRECT_ASK_DATA) {
                let addr = read_redirect_addr(data);
                return Some((true, addr));
            } else if data.starts_with(REDIRECT_MOVED_DATA) {
                let addr = read_redirect_addr(data);
                return Some((false, addr));
            }
        }

        None
    }

    fn try_redirect(&mut self) -> Result<Async<()>, Error> {
        let mut redirectiion = None;
        std::mem::swap(&mut redirectiion, &mut self.rstore);
        let (addr, cmd) = redirectiion.expect("try_redirect get redirectiion never be empty");
        match self.redirect.start_send((addr, cmd)).map_err(|_err| {
            error!("fail to redirect to cluster maybe cluster is down");
            Error::Critical
        })? {
            AsyncSink::NotReady((addr, cmd)) => {
                self.rstore = Some((addr, cmd));
                Ok(Async::NotReady)
            }
            AsyncSink::Ready => Ok(Async::Ready(())),
        }
    }
}

impl<S, R> Future for NodeRecv<S, R>
where
    S: Stream<Item = Resp, Error = Error>,
    R: Sink<SinkItem = (String, Cmd)>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.closed {
            return Ok(Async::Ready(()));
        }

        loop {
            if self.rstore.is_some() {
                try_ready!(self
                    .try_redirect()
                    .map_err(|err| error!("fail to try_redirect due to {:?}", err)));
            }

            if let Some(resp) = try_ready!(self.recv.poll().map_err(|err| {
                error!("fail to recv from back end, may closed due to {:?}", err);
                self.closed = true;
            })) {
                let cmd = self.buf.borrow_mut().pop_front().unwrap();
                if cmd.is_ignore_reply() {
                    continue;
                }

                if let Some((is_ask, addr)) = Self::parse_redirect(&resp) {
                    cmd.set_is_ask(is_ask);
                    self.rstore = Some((addr, cmd));
                    continue;
                } else {
                    cmd.done(resp);
                }
            } else {
                error!("TODO: should quick error for");
                self.closed = true;
                return Ok(Async::Ready(()));
            }
        }
    }
}

const SPC_BYTE: u8 = b' ';

fn read_redirect_addr(data: &[u8]) -> String {
    let mut iter = data.rsplit(|x| *x == SPC_BYTE);
    let addr = String::from_utf8_lossy(iter.next().unwrap());
    addr.into_owned()
}
