use cmd::Cmd;
use com::*;
use resp::Resp;

use tokio::prelude::{Async, AsyncSink, Future, Sink, Stream};

use std::cell::RefCell;
use std::collections::VecDeque;
use std::mem;
use std::rc::Rc;

pub struct NodeDown<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Resp, SinkError = Error>,
{
    closed: bool,
    input: I,
    output: O,
    store: Option<Cmd>,
    buf: Rc<RefCell<VecDeque<Cmd>>>,
    count: usize,
}

impl<I, O> NodeDown<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Resp, SinkError = Error>,
{
    pub fn new(input: I, output: O, buf: Rc<RefCell<VecDeque<Cmd>>>) -> NodeDown<I, O> {
        NodeDown {
            input: input,
            output: output,
            closed: false,
            store: None,
            buf: buf,
            count: 0,
        }
    }

    fn try_forword(&mut self) -> AsResult<()> {
        loop {
            if self.store.is_some() {
                let mut cmd = None;
                mem::swap(&mut cmd, &mut self.store);
                let cmd = cmd.unwrap();
                let rx_cmd = cmd.clone();
                let req = rx_cmd.borrow().req.clone();
                match self.output.start_send(req)? {
                    AsyncSink::NotReady(_) => {
                        self.store = Some(cmd);
                        return Ok(());
                    }
                    AsyncSink::Ready => {
                        self.buf.borrow_mut().push_back(cmd);
                        self.count += 1;
                    }
                }
            }

            match self.input.poll()? {
                Async::Ready(Some(v)) => {
                    self.store = Some(v);
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
    O: Sink<SinkItem = Resp, SinkError = Error>,
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

pub struct NodeRecv<S>
where
    S: Stream<Item = Resp, Error = Error>,
{
    closed: bool,
    recv: S,
    buf: Rc<RefCell<VecDeque<Cmd>>>,
}

impl<S> NodeRecv<S>
where
    S: Stream<Item = Resp, Error = Error>,
{
    pub fn new(recv: S, buf: Rc<RefCell<VecDeque<Cmd>>>) -> NodeRecv<S> {
        NodeRecv {
            closed: false,
            recv: recv,
            buf: buf,
        }
    }
}

impl<S> Future for NodeRecv<S>
where
    S: Stream<Item = Resp, Error = Error>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            if self.closed {
                return Ok(Async::Ready(()));
            }

            if let Some(val) = try_ready!(self.recv.poll().map_err(|err| {
                error!("fail to recv from back end, may closed due to {:?}", err);
                self.closed = true;
            })) {
                let cmd = self.buf.borrow_mut().pop_front().unwrap();
                cmd.borrow_mut().done(val);
            } else {
                error!("TODO: should quick error for");
            }
        }
    }
}
