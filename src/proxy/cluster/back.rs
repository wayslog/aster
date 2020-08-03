use crate::com::AsError;
use crate::protocol::redis::{Cmd, Message};
use crate::proxy::cluster::Redirection;

use crate::proxy::standalone::Request;
use futures::unsync::mpsc::SendError;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::timer::Delay;

const MAX_PIPELINE: usize = 8 * 1024;

#[derive(Eq, PartialEq)]
enum State {
    Running,
    ActiveClosing,
    Closing,
    Closed,
}

impl State {
    fn is_closing(&self) -> bool {
        self == &State::Closing
    }

    fn is_active_closing(&self) -> bool {
        self == &State::ActiveClosing
    }

    fn is_closed(&self) -> bool {
        self == &State::Closed
    }
}

#[allow(unused)]
pub struct Back<I, O, R, M>
where
    I: Stream<Item = Cmd, Error = ()>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
    R: Stream<Item = Message, Error = AsError>,
    M: Sink<SinkItem = Redirection, SinkError = SendError<Redirection>>,
{
    cluster: String,
    addr: String,
    state: State,

    ask_readed: bool,
    redirect_store: Option<Redirection>,
    store: Option<Cmd>,
    cmdq: VecDeque<Cmd>,

    inner_err: AsError,

    input: I,
    output: O,
    recv: R,
    moved: M,

    rt: Duration,
    delayed: u64,
    timeout: Delay,
}

impl<I, O, R, M> Back<I, O, R, M>
where
    I: Stream<Item = Cmd, Error = ()>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
    R: Stream<Item = Message, Error = AsError>,
    M: Sink<SinkItem = Redirection, SinkError = SendError<Redirection>>,
{
    pub fn new(
        cluster: String,
        addr: String,
        input: I,
        output: O,
        recv: R,
        moved: M,
        rt: u64,
    ) -> Back<I, O, R, M> {
        let inner_err = AsError::ConnClosed(addr.clone());
        Back {
            cluster,
            addr,
            input,
            output,
            recv,
            moved,
            inner_err,

            state: State::Running,
            ask_readed: false,
            redirect_store: None,
            store: None,
            cmdq: VecDeque::with_capacity(MAX_PIPELINE),
            rt: Duration::from_millis(rt),
            delayed: 0,
            timeout: Delay::new(Instant::now()),
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
                        rcmd.cluster_mark_remote(&self.cluster);
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

            if self.state.is_active_closing() {
                break;
            }

            match self.input.poll() {
                Ok(Async::Ready(Some(cmd))) => {
                    self.store = Some(cmd);
                }
                Ok(Async::Ready(None)) => {
                    ret_state = State::ActiveClosing;
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
            if let Some(redirection) = self.redirect_store.take() {
                match self.moved.start_send(redirection) {
                    Ok(AsyncSink::NotReady(red)) => {
                        self.redirect_store = Some(red);
                        break;
                    }
                    Ok(AsyncSink::Ready) => {
                        count += 1;
                    }
                    Err(se) => {
                        let red: Redirection = se.into_inner();
                        error!("fail to redirect cmd {:?}", red.target);
                        red.cmd.set_error(AsError::RedirectFailError);
                        return Err(AsError::RedirectFailError);
                    }
                }
            }

            if self.cmdq.is_empty() {
                break;
            }

            if let Some(send_at) = self.cmdq.front().unwrap().get_sendtime() {
                if send_at.elapsed() > self.rt {
                    let cmd = self.cmdq.pop_front().unwrap();
                    cmd.set_error(AsError::CmdTimeout);
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
                    error!("fail to recv from back {} due {:?}", self.addr, err);
                    return Err(err);
                }
            };

            let is_ask = self
                .cmdq
                .front()
                .map(|x| x.borrow().is_ask())
                .expect("front always have cmd");

            if is_ask {
                self.ask_readed = !self.ask_readed;
                if self.ask_readed {
                    continue;
                }
            }

            if self.delayed > 0 {
                self.delayed -= 1;
                continue;
            }

            let cmd = self.cmdq.pop_front().expect("cmdq never be empty");
            if let Some(redirect) = msg.check_redirect() {
                {
                    let mut inner_cmd = cmd.borrow_mut();
                    inner_cmd.add_cycle();
                    if redirect.is_ask() {
                        inner_cmd.set_ask();
                        inner_cmd.unset_moved();
                    } else {
                        inner_cmd.set_moved();
                        inner_cmd.unset_ask();
                    }
                }
                self.redirect_store = Some(Redirection {
                    target: redirect,
                    cmd,
                });
            } else {
                cmd.set_reply(msg);
            }
        }
        if count > 0 {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn on_closed(&mut self) {
        if let Some(cmd) = self.store.take() {
            cmd.set_error(&self.inner_err);
        }
        for cmd in self.cmdq.drain(0..) {
            cmd.set_error(&self.inner_err);
        }
    }

    fn has_cmd(&mut self) -> bool {
        self.store.is_some() || !self.cmdq.is_empty()
    }
}

impl<I, O, R, M> Future for Back<I, O, R, M>
where
    I: Stream<Item = Cmd, Error = ()>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
    R: Stream<Item = Message, Error = AsError>,
    M: Sink<SinkItem = Redirection, SinkError = SendError<Redirection>>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_recv = true;
        let mut can_forward = true;
        loop {
            // trace!("tracing backend calls to {}", self.addr);
            if self.state.is_closing() {
                // debug!("backend {} is closing", self.addr);
                self.on_closed();
                self.state = State::Closed;
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
                    Ok(Async::Ready(State::ActiveClosing)) => {
                        // trace!("backend forward is active closing");
                        self.state = State::ActiveClosing;
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

            if self.state.is_closed() {
                // debug!("backend {} is closed", self.addr);
                if !self.has_cmd() {
                    return Ok(Async::Ready(()));
                } else {
                    self.state = State::Closing;
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

pub struct Blackhole<S>
where
    S: Stream<Item = Cmd>,
{
    addr: String,
    inner_err: AsError,
    input: S,
}

impl<S> Blackhole<S>
where
    S: Stream<Item = Cmd>,
{
    pub fn new(addr: String, input: S) -> Blackhole<S> {
        let inner_err = AsError::ConnClosed(addr.clone());
        Blackhole {
            addr,
            input,
            inner_err,
        }
    }
}

impl<S> Future for Blackhole<S>
where
    S: Stream<Item = Cmd>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            match self.input.poll() {
                Ok(Async::Ready(Some(cmd))) => {
                    debug!(
                        "backend close cmd in blackhole addr:{} and cmd:{:?}",
                        self.addr, cmd
                    );
                    cmd.set_error(&self.inner_err);
                }
                _ => {
                    info!("backend blackhole exists of {}", self.addr);
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}
