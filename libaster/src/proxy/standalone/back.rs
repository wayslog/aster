use crate::com::AsError;

use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;

use crate::proxy::standalone::Request;

const MAX_PIPELINE: usize = 128;

#[derive(Eq, PartialEq)]
enum State {
    Running,
    Closing,
    Closed,
}

impl State {
    fn is_closing(&self) -> bool {
        self == &State::Closing
    }

    fn is_closed(&self) -> bool {
        self == &State::Closed
    }
}

pub struct Back<T, I, O, R>
where
    T: Request,
    I: Stream<Item = T, Error = ()>,
    O: Sink<SinkItem = T, SinkError = AsError>,
    R: Stream<Item = T::Reply, Error = AsError>,
{
    addr: String,
    state: State,

    store: Option<T>,
    cmdq: VecDeque<T>,

    input: I,
    output: O,
    recv: R,
}

impl<T, I, O, R> Back<T, I, O, R>
where
    T: Request,
    I: Stream<Item = T, Error = ()>,
    O: Sink<SinkItem = T, SinkError = AsError>,
    R: Stream<Item = T::Reply, Error = AsError>,
{
    pub fn new(addr: String, input: I, output: O, recv: R) -> Back<T, I, O, R> {
        Back {
            addr,
            input,
            output,
            recv,
            state: State::Running,
            store: None,
            cmdq: VecDeque::with_capacity(MAX_PIPELINE),
        }
    }

    fn try_forward(&mut self) -> Result<Async<State>, AsError> {
        let mut count = 0;
        let mut ret_state = State::Running;

        for _ in 0..MAX_PIPELINE {
            if let Some(cmd) = self.store.take() {
                let rcmd = cmd.clone();
                match self.output.start_send(cmd) {
                    Ok(AsyncSink::NotReady(cmd)) => {
                        self.store = Some(cmd);
                        break;
                    }
                    Ok(AsyncSink::Ready) => {
                        count += 1;
                        self.cmdq.push_back(rcmd);
                    }
                    Err(err) => {
                        error!(
                            "fail to send cmd to backend to {} due to {}",
                            self.addr, err
                        );
                        rcmd.set_error(&err);
                        return Err(err);
                    }
                }
            }

            match self.input.poll() {
                Ok(Async::Ready(Some(cmd))) => {
                    self.store = Some(cmd);
                }
                Ok(Async::Ready(None)) => {
                    ret_state = State::Closing;
                    break;
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Err(_) => unreachable!(),
            }
        }

        if count > 0 {
            self.output.poll_complete()?;
            return Ok(Async::Ready(ret_state));
        } else {
            return Ok(Async::NotReady);
        }
    }

    fn try_recv(&mut self) -> Result<Async<()>, AsError> {
        let mut count = 0usize;
        for _ in 0..MAX_PIPELINE {
            if self.cmdq.is_empty() {
                break;
            }

            let msg = match self.recv.poll() {
                Ok(Async::Ready(Some(msg))) => {
                    count += 1;
                    msg
                }
                Ok(Async::Ready(None)) => {
                    return Err(AsError::BackendClosedError(self.addr.clone()));
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Err(err) => {
                    error!("fail to recv from backend connection due {:?}", err);
                    return Err(err.into());
                }
            };

            let cmd = self.cmdq.pop_front().expect("cmdq never be empty");
            cmd.set_reply(msg);
        }
        if count > 0 {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn on_closed(&mut self) {
        for cmd in self.cmdq.drain(0..) {
            cmd.set_error(&AsError::BackendClosedError(self.addr.clone()));
        }
        if let Some(cmd) = self.store.take() {
            cmd.set_error(&AsError::BackendClosedError(self.addr.clone()));
        }
        loop {
            match self.input.poll() {
                Ok(Async::Ready(Some(cmd))) => {
                    cmd.set_error(&AsError::BackendClosedError(self.addr.clone()));
                }
                Ok(Async::Ready(None)) | Ok(Async::NotReady) => {
                    break;
                }
                Err(_) => unreachable!(),
            }
        }
    }
}

impl<T, I, O, R> Future for Back<T, I, O, R>
where
    T: Request,
    I: Stream<Item = T, Error = ()>,
    O: Sink<SinkItem = T, SinkError = AsError>,
    R: Stream<Item = T::Reply, Error = AsError>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_recv = true;
        let mut can_forward = true;
        loop {
            // trace!("tracing backend calls to {}", self.addr);
            if self.state.is_closing() {
                debug!("backend {} is closing", self.addr);
                self.on_closed();
                self.state = State::Closed;
            }
            if self.state.is_closed() {
                debug!("backend {} is closed", self.addr);
                return Ok(Async::Ready(()));
            }

            if !can_recv && !can_forward {
                return Ok(Async::NotReady);
            }

            if can_recv {
                match self.try_recv() {
                    Ok(Async::NotReady) => {
                        // trace!("backend recv is not ready");
                        can_recv = false;
                    }
                    Ok(Async::Ready(_)) => {
                        can_forward = true;
                        // trace!("backend recv is ready");
                    }
                    Err(err) => {
                        warn!("backend {} recv is error {}", self.addr, err);
                        self.state = State::Closing;
                        continue;
                    }
                }
            }

            if can_forward {
                match self.try_forward() {
                    Ok(Async::NotReady) => {
                        // trace!("backend forward is not ready");
                        can_forward = false;
                    }
                    Ok(Async::Ready(State::Closing)) => {
                        // trace!("backend forward is active closing");
                        self.state = State::Closing;
                    }
                    Ok(Async::Ready(_)) => {
                        can_recv = true;
                        // trace!("backend forward is ready");
                    }
                    Err(err) => {
                        warn!("backend {} forward is error {}", self.addr, err);
                        self.state = State::Closing;
                        continue;
                    }
                }
            }
        }
    }
}

pub struct Blackhole<T, S>
where
    T: Request,
    S: Stream<Item = T>,
{
    addr: String,
    input: S,
}

impl<T, S> Blackhole<T, S>
where
    T: Request,
    S: Stream<Item = T>,
{
    pub fn new(addr: String, input: S) -> Blackhole<T, S> {
        Blackhole { addr, input }
    }
}

impl<T, S> Future for Blackhole<T, S>
where
    T: Request,
    S: Stream<Item = T>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            match self.input.poll() {
                Ok(Async::Ready(Some(cmd))) => {
                    cmd.set_error(&AsError::BackendClosedError(self.addr.clone()));
                }
                Ok(Async::Ready(None)) => {
                    return Ok(Async::Ready(()));
                }
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }
                Err(_) => {
                    unreachable!("rx chan is never be fail");
                }
            }
        }
    }
}
