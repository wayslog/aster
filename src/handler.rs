use cmd::Cmd;
use com::*;
use std::rc::Rc;
use Cluster;

// use aho_corasick::{AcAutomaton, Automaton, Match};
use tokio::prelude::{Async, AsyncSink, Future, Sink, Stream};

pub enum State {
    Void,
    Batching,
    Writing,
}

pub struct Handler<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    cluster: Rc<Cluster>,

    input: I,
    output: O,

    cmd: Option<Cmd>,
    state: State,
}

impl<I, O> Handler<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    fn try_write_back(&mut self, cmd: Cmd) -> Result<Async<()>, Error> {
        if let AsyncSink::NotReady(_) = self.output.start_send(cmd)? {
            return Ok(Async::NotReady);
        }
        self.output.poll_complete().map_err(|err| {
            error!{"send error due to {:?}", err};
            Error::Critical
        })
    }

    fn fork_cmd(&mut self) -> Cmd {
        self.cmd.as_ref().cloned().expect("never be empty")
    }
}

impl<I, O> Future for Handler<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<()>, Self::Error> {
        loop {
            match self.state {
                State::Void => {
                    if let Some(rc_cmd) = try_ready!(self.input.poll()) {
                        self.cmd = Some(rc_cmd);
                        self.state = State::Batching;
                        continue;
                    }
                    return Ok(Async::NotReady);
                }
                State::Batching => {
                    let rc_cmd = self.fork_cmd();
                    let rslt = self.cluster.dispatch(rc_cmd.clone())?;
                    match rslt {
                        AsyncSink::NotReady(_) => return Ok(Async::NotReady),
                        AsyncSink::Ready => self.state = State::Writing,
                    };
                }
                State::Writing => {
                    let rc_cmd = self.fork_cmd();
                    if !rc_cmd.borrow().is_done() {
                        return Ok(Async::NotReady);
                    }
                    try_ready!(self.try_write_back(rc_cmd.clone()));
                    self.state = State::Void
                }
            };
        }
    }
}
