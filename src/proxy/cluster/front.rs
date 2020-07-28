use crate::com::AsError;
use crate::protocol::redis::Cmd;
use crate::proxy::cluster::fetcher::TriggerBy;
use crate::proxy::cluster::Cluster;

use futures::task;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;
use std::rc::Rc;

const MAX_BATCH_SIZE: usize = 8 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Running,
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
}

impl<I, O> Front<I, O>
where
    I: Stream<Item = Cmd, Error = AsError>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
{
    pub fn new(client: String, cluster: Rc<Cluster>, input: I, output: O) -> Front<I, O> {
        Front {
            cluster,
            client,
            input,
            output,
            sendq: VecDeque::with_capacity(MAX_BATCH_SIZE),
            waitq: VecDeque::with_capacity(MAX_BATCH_SIZE),
            state: State::Running,
        }
    }

    fn try_reply(&mut self) -> Result<Async<usize>, AsError> {
        let mut count = 0usize;
        loop {
            if self.waitq.is_empty() {
                break;
            }

            let cmd = self.waitq.pop_front().expect("command never be error");

            if !cmd.borrow().is_done() {
                self.waitq.push_front(cmd);
                break;
            }

            if cmd.borrow().is_error() {
                self.cluster.trigger_fetch(TriggerBy::Error);
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
        Ok(self.cluster.dispatch_all(&mut self.sendq)?)
    }

    fn try_recv(&mut self) -> Result<usize, AsError> {
        let mut count = 0usize;
        loop {
            if self.waitq.len() == MAX_BATCH_SIZE {
                return Ok(count);
            }

            let cmd = match self.input.poll() {
                Ok(Async::Ready(cmd)) => cmd,
                Ok(Async::NotReady) => return Ok(count),
                Err(err) => return Err(err),
            };

            if let Some(mut cmd) = cmd {
                count += 1;
                cmd.reregister(task::current());
                cmd.cluster_mark_total(&self.cluster.cc.borrow().name);

                if cmd.check_valid() && !cmd.borrow().is_done() {
                    // for done command, never send to backend
                    if let Some(subs) = cmd.borrow().subs() {
                        self.sendq.extend(subs.into_iter());
                    } else {
                        self.sendq.push_back(cmd.clone());
                    }
                }
                self.waitq.push_back(cmd);
            } else {
                self.state = State::Closed;
                return Ok(0);
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
            if self.state == State::Closed {
                // debug!("front drop of {}", self.client);
                return Ok(Async::Ready(()));
            }

            if !(can_reply || can_send || can_recv) {
                return Ok(Async::NotReady);
            }

            if can_reply {
                match self.try_reply() {
                    Ok(Async::NotReady) => {
                        // trace!("front reply is not ready");
                        can_reply = false;
                    }
                    Ok(Async::Ready(size)) => {
                        if size == 0 {
                            can_reply = false;
                        } else {
                            can_recv = self.state == State::Running;
                            can_send = true;
                        }
                        // trace!("front reply is ready and reply {}", size);
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
                        // trace!("front send is ready and send {}", size);
                    }
                    Err(_) => {
                        // FIXME: fixed it.
                        unreachable!("only occur when cluster reconnect backend failed");
                    }
                }
            }

            if can_recv {
                match self.try_recv() {
                    Ok(size) => {
                        can_send = true;
                        can_reply = true;
                        can_recv = 0 != size && self.state == State::Running;
                        // trace!("front recv is ready and recv {}", size);
                    }
                    Err(err) => {
                        error!("fail to read from client {} due to {}", self.client, err);
                        self.state = State::Closed;
                        can_recv = self.state == State::Running;
                    }
                }
            }
        }
    }
}

impl<I, O> Drop for Front<I, O>
where
    I: Stream<Item = Cmd, Error = AsError>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
{
    fn drop(&mut self) {
        crate::metrics::front_conn_decr(&self.cluster.cc.borrow().name);
    }
}
