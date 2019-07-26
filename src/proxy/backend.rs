use crate::com::*;
use crate::proxy::Request;
use futures::{Async, AsyncSink, Future, Sink, Stream};

use std::collections::VecDeque;

const BATCH_SIZE: usize = 1024;

enum State {
    Running,
    Closing,
    Closed,
}

impl State {
    fn is_closed(&self) -> bool {
        match self {
            State::Closed => return true,
            _ => return false,
        }
    }

    fn is_closing(&self) -> bool {
        match self {
            State::Closing => return true,
            _ => return false,
        }
    }
}

pub struct Backend<T, I, O, S>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    S: Stream<Item = T::Reply, Error = Error>,
    T: Request,
{
    addr: String,
    state: State,
    store: Option<T>,
    buf: VecDeque<T>,

    up: I,
    down: O,
    recv: S,
}

impl<T, I, O, S> Backend<T, I, O, S>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    S: Stream<Item = T::Reply, Error = Error>,
    T: Request,
{
    pub fn new(addr: &str, input: I, output: O, recv: S) -> Backend<T, I, O, S> {
        Backend {
            addr: addr.to_string(),
            state: State::Running,
            store: None,
            buf: VecDeque::with_capacity(1024),
            up: input,
            down: output,
            recv,
        }
    }

    // recv from upstream
    fn try_recv(&mut self) -> Result<usize, Error> {
        let mut count = 0;
        loop {
            if count == BATCH_SIZE {
                return Ok(count);
            }

            if self.buf.is_empty() {
                return Ok(count);
            }

            let resp = match self.recv.poll() {
                Ok(Async::Ready(Some(r))) => r,
                Ok(Async::Ready(None)) => {
                    self.state = State::Closing;
                    return Ok(count);
                }
                Ok(Async::NotReady) => {
                    return Ok(count);
                }
                Err(err) => {
                    return Err(err);
                }
            };

            let front = self
                .buf
                .pop_front()
                .expect("buffer is never empty for endpoint");
            front.done(resp);
            count += 1;
        }
    }

    // forward message from upstream into downstream and copy them into local buffer.
    fn try_forward(&mut self) -> Result<usize, Error> {
        let usize = self.try_forward_inner()?;
        if usize > 0 {
            self.down.poll_complete().map_err(|err| {
                error!("failt to flush back to {} for {:?}", &self.addr, err);
                Error::Critical
            })?;
        }
        Ok(usize)
    }

    fn try_forward_inner(&mut self) -> Result<usize, Error> {
        let mut count = 0;
        loop {
            if count == BATCH_SIZE || self.buf.len() == BATCH_SIZE {
                return Ok(count);
            }

            if let Some(req) = self.store.take() {
                // forward into backend
                match self.down.start_send(req.clone()) {
                    Ok(AsyncSink::NotReady(r)) => {
                        self.store.replace(r);
                        return Ok(count);
                    }
                    Ok(AsyncSink::Ready) => {
                        self.buf.push_back(req);
                        count += 1;
                    }
                    Err(err) => {
                        error!("fail to forward to {} due {:?}", &self.addr, err);
                        req.done_with_error(Error::Critical);
                        return Err(err);
                    }
                }
            }

            let pret = match self.up.poll() {
                Ok(Async::Ready(Some(p))) => p,
                Ok(Async::Ready(None)) => {
                    self.state = State::Closing;
                    return Ok(count);
                }
                Ok(Async::NotReady) => return Ok(count),
                Err(err) => {
                    warn!("unable poll from cluster upstream due {:?}", err);
                    return Err(err);
                }
            };
            // maybe judge is_noreply first
            self.store.replace(pret);
        }
    }

    // clean all the
    fn on_close(&mut self) -> Result<(), Error> {
        if let Some(req) = self.store.take() {
            req.done_with_error(Error::Critical);
        }

        self.buf
            .iter()
            .for_each(|item| item.done_with_error(Error::Critical));
        self.down.close().map_err(|e| {
            error!(
                "fail to close the internal connection to {} succeed due to {:?}",
                &self.addr, e
            );
            Error::Critical
        })?;

        self.state = State::Closed;
        Ok(())
    }
}

impl<T, I, O, S> Future for Backend<T, I, O, S>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    S: Stream<Item = T::Reply, Error = Error>,
    T: Request,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_forward = true;
        let mut can_recv = true;

        loop {
            if self.state.is_closed() {
                return Ok(Async::Ready(()));
            }

            if self.state.is_closing() {
                self.on_close()?;
                return Ok(Async::Ready(()));
            }

            if !can_forward && !can_recv {
                return Ok(Async::NotReady);
            }

            match self.try_recv() {
                Ok(count) if count == 0 => {
                    can_recv = false;
                }
                Ok(_) => {}
                Err(err) => {
                    error!(
                        "fail to read reply from backend {} due {:?}",
                        &self.addr, err
                    );
                    can_recv = false;
                }
            }

            match self.try_forward() {
                Ok(count) if count == 0 => {
                    can_forward = false;
                }
                Ok(_) => {}
                Err(err) => {
                    error!("fail to forward request to {} due to {:?}", &self.addr, err);
                    can_forward = false;
                    self.state = State::Closing;
                }
            }
        }
    }
}
