use self::super::Request;
use com::*;

use futures::unsync::oneshot::{channel, Receiver, Sender};
use futures::{Async, AsyncSink, Future, Sink, Stream};

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

pub fn spawn_node<T, I, O, S>(input: I, output: O, recv: S) -> (NodeDown<T, I, O>, NodeRecv<T, S>)
where
    T: Request,
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    S: Stream<Item = T::Reply, Error = Error>,
{
    let (tx, rx) = channel();
    let buf = Rc::new(RefCell::new(VecDeque::new()));
    let node_down = NodeDown::new(input, output, buf.clone(), tx);
    let node_recv = NodeRecv::new(recv, buf.clone(), rx);
    (node_down, node_recv)
}

pub struct NodeDown<T, I, O>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
{
    closed: bool,
    close_chan: Option<Sender<bool>>,
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
    pub fn new(
        input: I,
        output: O,
        buf: Rc<RefCell<VecDeque<T>>>,
        close: Sender<bool>,
    ) -> NodeDown<T, I, O> {
        Self {
            closed: false,
            close_chan: Some(close),
            input: input,
            output: output,
            store: VecDeque::new(),
            buf: buf,
            count: 0,
        }
    }

    fn try_forword(&mut self) -> AsResult<()> {
        loop {
            if !self.store.is_empty() {
                let req = self
                    .store
                    .front()
                    .cloned()
                    .expect("node down store is never be empty");

                if req.is_done() {
                    let _ = self.store.pop_front().unwrap();
                    continue;
                }

                match self.output.start_send(req)? {
                    AsyncSink::NotReady(_) => {
                        return Ok(());
                    }
                    AsyncSink::Ready => {
                        let req = self
                            .store
                            .pop_front()
                            .expect("try_forward store never be empty");
                        self.buf.borrow_mut().push_back(req);
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

    fn done_first(&self) {
        for req in self.store.iter() {
            req.done_with_error(Error::ClusterDown);
        }
        for item in self.buf.borrow_mut().iter() {
            item.done_with_error(Error::ClusterDown);
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
            self.done_first();
            let close = self
                .close_chan
                .take()
                .expect("never be empty for close channel");
            close
                .send(self.closed)
                .expect("close chan never be close first");
            return Ok(Async::Ready(()));
        }

        self.try_forword().map_err(|err| {
            self.done_first();
            error!("fail to forward due to {:?}", err);
        })?;

        if self.count > 0 {
            try_ready!(self.output.poll_complete().map_err(|err| {
                error!("fail to flush into backend due to {:?}", err);
                self.done_first();
                self.closed = true;
            }));
            // debug!("trying to poll_complete with count {}", self.count);
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
    close_chan: Receiver<bool>,
    recv: S,
    buf: Rc<RefCell<VecDeque<T>>>,
}

impl<T, S> NodeRecv<T, S>
where
    S: Stream<Item = T::Reply, Error = Error>,
    T: Request,
{
    pub fn new(stream: S, buf: Rc<RefCell<VecDeque<T>>>, close: Receiver<bool>) -> NodeRecv<T, S> {
        Self {
            closed: false,
            recv: stream,
            buf: buf,
            close_chan: close,
        }
    }
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

        if let Ok(Async::Ready(_)) = self.close_chan.poll() {
            debug!("close by down");
            self.closed = true;
            for item in self.buf.borrow_mut().iter() {
                item.done_with_error(Error::ClusterDown);
            }
            return Ok(Async::Ready(()));
        }

        loop {
            if let Some(reply) = try_ready!(self.recv.poll().map_err(|err| {
                error!("fail to recv from back end, may closed due to {:?}", err);
                for item in self.buf.borrow_mut().iter() {
                    item.done_with_error(Error::Critical);
                }
                self.closed = true;
            })) {
                let req = self.buf.borrow_mut().pop_front().unwrap();
                req.done(reply);
            } else {
                for item in self.buf.borrow_mut().iter() {
                    error!("TODO: should quick error for");
                    item.done_with_error(Error::ClusterDown);
                }
                self.closed = true;
                return Ok(Async::Ready(()));
            }
        }
    }
}
