use crate::com::AsError;
use crate::protocol::redis::Cmd;
use crate::proxy::cluster::Cluster;

use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;
use std::rc::Rc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Running,
    Closing,
    Closed,
}

pub struct Front<I, O>
where
    I: Stream<Item = Cmd, Error = AsError>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
{
    cluster: Rc<Cluster>,

    client: String,

    input: I,
    output: O,

    sendq: VecDeque<Cmd>,
    waitq: VecDeque<Cmd>,
    state: State,

    // batch_max must greater than waitq_max
    batch_max: usize,
}

impl<I, O> Front<I, O>
where
    I: Stream<Item = Cmd, Error = AsError>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
{
    fn try_reply(&mut self) -> Result<Async<usize>, AsError> {
        let mut count = 0usize;
        loop {
            if self.waitq.is_empty() {
                break;
            }
            let cmd = self.waitq.pop_front().expect("command never be error");
            if !cmd.borrow().is_done() {
                break;
            }
            match self.output.start_send(cmd) {
                Ok(AsyncSink::Ready) => {
                    count += 1;
                }
                Ok(AsyncSink::NotReady(cmd)) => {
                    self.waitq.push_front(cmd);
                    break;
                }
                Err(err) => {
                    error!("fail to reply to client {} {}", self.client, err);
                    self.output.close()?;
                    return Err(err);
                }
            }
        }

        if count > 0 {
            self.output.poll_complete()?;
        }
        Ok(Async::Ready(count))
    }

    fn try_send(&mut self) -> Result<usize, AsError> {
        self.cluster.dispatch_all(&mut self.sendq)
    }

    fn try_recv(&mut self) -> Result<Async<usize>, AsError> {
        let mut count = 0usize;
        loop {
            if self.waitq.len() == self.batch_max {
                return Ok(Async::Ready(count));
            }

            let cmd = try_ready!(self.input.poll());
            if let Some(cmd) = cmd {
                count += 1;
                let is_done = cmd.borrow().is_done();
                if !is_done {
                    // for done command, never send to backend
                    if let Some(subs) = cmd.borrow().subs() {
                        self.sendq.extend(subs.into_iter());
                    } else {
                        self.sendq.push_back(cmd.clone());
                    }
                }
                self.waitq.push_back(cmd);
            } else {
                self.state = State::Closing;
            }
        }
    }
}

impl<I, O> Future for Front<I, O>
where
    I: Stream<Item = Cmd, Error = AsError>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_reply = true;
        let mut can_send = true;
        let mut can_recv = self.state == State::Running;
        loop {
            if self.state == State::Closing {
                can_recv = false;
            }

            if self.state == State::Closed {
                return Ok(Async::Ready(()));
            }

            if !(can_reply || can_send || can_recv) {
                return Ok(Async::NotReady);
            }

            if can_reply {
                match self.try_reply() {
                    Ok(Async::NotReady) => {
                        can_reply = false;
                    }
                    Ok(Async::Ready(size)) => {
                        if size == 0 {
                            can_reply = false;
                        } else {
                            can_recv = self.state == State::Running;
                            can_send = true;
                        }

                        if self.waitq.is_empty() && self.state == State::Closing {
                            self.state = State::Closed;
                            return Ok(Async::Ready(()));
                        }
                    }
                    Err(err) => {
                        error!(
                            "fail to send response to client {} due to {}",
                            self.client, err
                        );
                        self.state = State::Closed;
                        return Err(());
                    }
                }
            }

            if can_send {
                match self.try_send() {
                    Ok(size) => {
                        if size == 0 {
                            can_send = false;
                        } else {
                            can_reply = true;
                            can_recv = self.state == State::Running;
                        }
                    }
                    Err(_) => {
                        // FIXME: fixed it.
                        unreachable!("only occur when cluster reconnect backend failed");
                    }
                }
            }

            if can_recv {
                match self.try_recv() {
                    Ok(Async::Ready(size)) => {
                        if size == 0 && self.waitq.len() == self.batch_max {
                            can_send = true;
                        }
                        can_recv = self.state == State::Running;
                    }
                    Ok(Async::NotReady) => {
                        can_recv = false;
                    }
                    Err(err) => {
                        error!("fail to read from client {} due to {}", self.client, err);
                        self.state = State::Closing;
                        can_recv = self.state == State::Running;
                    }
                }
            }
        }
    }
}
