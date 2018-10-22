use cmd::{Cmd, CmdType, RESP_OBJ_ERROR_NOT_SUPPORT, RESP_OBJ_STRING_PONG};
use com::*;
use resp::Resp;
use tokio::prelude::{Async, AsyncSink, Future, Sink, Stream};
use Cluster;

const MAX_CONCURRENCY: usize = 1024 * 8;
// use aho_corasick::{AcAutomaton, Automaton, Match};
// use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

pub struct Handle<I, O>
where
    I: Stream<Item = Resp, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    cluster: Rc<Cluster>,

    input: I,
    output: O,
    cmds: VecDeque<Cmd>,
    count: usize,
    waitq: VecDeque<Cmd>,
}

impl<I, O> Handle<I, O>
where
    I: Stream<Item = Resp, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    pub fn new(cluster: Rc<Cluster>, input: I, output: O) -> Handle<I, O> {
        Handle {
            cluster: cluster,
            input: input,
            output: output,
            cmds: VecDeque::new(),
            count: 0,
            waitq: VecDeque::new(),
        }
    }

    fn deal_ctrl(&mut self, cmd: Cmd) {
        let req = cmd.rc_req();
        let cmd_bytes = req.cmd_bytes();
        if cmd_bytes == b"PING" {
            cmd.done(RESP_OBJ_STRING_PONG.clone());
        } else {
            cmd.done_with_error(&RESP_OBJ_ERROR_NOT_SUPPORT);
        }
    }

    fn try_read(&mut self) -> Result<Async<Option<()>>, Error> {
        loop {
            if self.cmds.len() > MAX_CONCURRENCY {
                return Ok(Async::NotReady);
            }

            match try_ready!(self.input.poll()) {
                Some(val) => {
                    let rc_cmd = Cmd::from(val);
                    let is_complex = rc_cmd.is_complex();
                    self.cmds.push_back(rc_cmd.clone());
                    if is_complex {
                        for sub in rc_cmd
                            .sub_reqs()
                            .expect("sub_reqs in try_read never be empty")
                        {
                            self.waitq.push_back(sub);
                        }
                    } else {
                        let cmd_type = rc_cmd.cmd_type();
                        match cmd_type {
                            CmdType::NotSupport => {
                                rc_cmd.done_with_error(&RESP_OBJ_ERROR_NOT_SUPPORT);
                                continue;
                            }
                            CmdType::Ctrl => {
                                self.deal_ctrl(rc_cmd);
                                continue;
                            }
                            _ => {}
                        }
                        self.waitq.push_back(rc_cmd);
                    }
                }
                None => {
                    return Ok(Async::Ready(None));
                }
            }
        }
    }

    fn try_send(&mut self) -> Result<Async<()>, Error> {
        loop {
            if self.waitq.is_empty() {
                return Ok(Async::NotReady);
            }

            if let AsyncSink::NotReady(_count) = self.cluster.dispatch_all(&mut self.waitq)? {
                return Ok(Async::NotReady);
            }
        }
    }

    fn try_write(&mut self) -> Result<Async<()>, Error> {
        let ret: Result<Async<()>, Error> = Ok(Async::NotReady);
        loop {
            if self.cmds.is_empty() {
                break;
            }

            let rc_cmd = self.cmds.front().cloned().expect("cmds is never be None");
            if !rc_cmd.is_done() {
                break;
            }

            match self.output.start_send(rc_cmd)? {
                AsyncSink::NotReady(_) => {
                    break;
                }
                AsyncSink::Ready => {
                    let _ = self.cmds.pop_front().unwrap();
                    self.count += 1;
                }
            }
        }

        if self.count > 0 {
            try_ready!(self.output.poll_complete());
            self.count = 0;
        }

        ret
    }
}

impl<I, O> Future for Handle<I, O>
where
    I: Stream<Item = Resp, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_read = true;
        let mut can_send = true;
        let mut can_write = true;

        loop {
            if !(can_read && can_send && can_write) {
                return Ok(Async::NotReady);
            }

            // step 1: poll read from input stream.
            if can_read {
                // read until the input stream is NotReady.
                match self.try_read()? {
                    Async::NotReady => {
                        can_read = false;
                    }
                    Async::Ready(None) => {
                        return Ok(Async::Ready(()));
                    }
                    Async::Ready(Some(())) => {}
                }
            }

            // step 2: send to cluster.
            if can_send {
                // send until the output stream is unsendable.
                match self.try_send()? {
                    Async::NotReady => {
                        can_send = false;
                    }
                    Async::Ready(_) => {}
                }
            }

            // step 3: wait all the cluster is done.
            if can_write {
                // step 4: poll send back to client.
                match self.try_write()? {
                    Async::NotReady => {
                        can_write = false;
                    }
                    Async::Ready(_) => {}
                }
            }
        }
        // Ok(Async::NotReady)
    }
}
