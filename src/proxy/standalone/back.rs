use crate::com::AsError;

use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;

use crate::proxy::standalone::Request;
use std::time::{Duration, Instant};
use tokio::timer::Delay;

const MAX_PIPELINE: usize = 512;

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
    #[allow(unused)]
    cluster: String,
    addr: String,
    state: State,

    store: Option<T>,
    cmdq: VecDeque<T>,

    input: I,
    output: O,
    recv: R,

    rt: Duration,
    timeout: Delay,
    delayed: u64,
}

impl<T, I, O, R> Back<T, I, O, R>
where
    T: Request,
    I: Stream<Item = T, Error = ()>,
    O: Sink<SinkItem = T, SinkError = AsError>,
    R: Stream<Item = T::Reply, Error = AsError>,
{
    pub fn new(
        cluster: String,
        addr: String,
        input: I,
        output: O,
        recv: R,
        rt: u64,
    ) -> Back<T, I, O, R> {
        Back {
            cluster,
            addr,
            input,
            output,
            recv,
            state: State::Running,
            store: None,
            cmdq: VecDeque::with_capacity(MAX_PIPELINE),
            rt: Duration::from_millis(rt),
            timeout: Delay::new(Instant::now()),
            delayed: 0,
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
                        rcmd.mark_remote(&self.cluster);
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
            Ok(Async::Ready(ret_state))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn try_recv(&mut self) -> Result<Async<()>, AsError> {
        let mut count = 0usize;
        for _ in 0..MAX_PIPELINE {
            if self.cmdq.is_empty() {
                break;
            }

            if let Some(send_at) = self.cmdq.front().unwrap().get_sendtime() {
                if send_at.elapsed() > self.rt {
                    let cmd = self.cmdq.pop_front().unwrap();
                    cmd.set_error(&AsError::CmdTimeout);
                    self.delayed += 1;
                    continue;
                }
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
                    error!("fail to recv from {} due {:?}", self.addr, err);
                    return Err(err);
                }
            };

            if self.delayed > 0 {
                self.delayed -= 1;
                continue;
            }

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
                break;
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
                        warn!("fail to recv from {} error {}", self.addr, err);
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
                        warn!("fail to forward to {} error {}", self.addr, err);
                        self.state = State::Closing;
                        continue;
                    }
                }
            }
        }

        if self.cmdq.len() > 0 {
            // we are waiting for the reply of the 1st cmd in the waiting queue.
            // set a waker to wake us up in case of a timeout of that reply occurs.
            self.timeout.reset(Instant::now() + self.rt);
            let _ = self.timeout.poll();
        }
        Ok(Async::NotReady)
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
                    info!("backend bloackhole clear the connection for {}", self.addr);
                    cmd.set_error(&AsError::BackendClosedError(self.addr.clone()));
                }
                _ => {
                    info!("backend blackhole exists of {}", self.addr);
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}
