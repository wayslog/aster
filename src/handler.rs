use cmd::{Cmd, CmdType, Command, RESP_OBJ_ERROR_NOT_SUPPORT};
use com::*;
use std::rc::Rc;
use Cluster;

const MAX_CURRENCY: usize = 1024 * 8;
// use aho_corasick::{AcAutomaton, Automaton, Match};
use std::collections::VecDeque;
use tokio::prelude::{Async, AsyncSink, Future, Sink, Stream};

pub struct Handler<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    cluster: Rc<Cluster>,

    input: I,
    output: O,

    bcmds: VecDeque<Cmd>,
    wcmds: VecDeque<Cmd>,
    // cmd: Option<Cmd>,
    subs: VecDeque<Cmd>,
    can_continue: bool,
}

impl<I, O> Handler<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    pub fn new(cluster: Rc<Cluster>, input: I, output: O) -> Self {
        Handler {
            cluster: cluster,
            input: input,
            output: output,
            bcmds: VecDeque::with_capacity(MAX_CURRENCY),
            subs: VecDeque::new(),
            wcmds: VecDeque::with_capacity(MAX_CURRENCY),
            can_continue: true,
        }
    }

    fn try_read_cmds(&mut self) -> Result<usize, Error> {
        let mut count = 0;
        loop {
            if self.bcmds.len() == MAX_CURRENCY {
                break;
            }

            if let Async::Ready(Some(rc_cmd)) = self.input.poll()? {
                self.bcmds.push_back(rc_cmd);
                count += 1;
                continue;
            }
            self.can_continue = false;
            break;
        }
        Ok(count)
    }

    fn try_send_cmds(&mut self) -> Result<(), Error> {
        loop {
            if self.bcmds.is_empty() {
                return Ok(());
            }

            if self.wcmds.len() == MAX_CURRENCY {
                return Ok(());
            }

            let rc_cmd = self.bcmds.front().cloned().expect("front never none");
            if rc_cmd.borrow().is_complex() && self.subs.is_empty() {
                self.subs = rc_cmd
                    .borrow()
                    .sub_reqs
                    .as_ref()
                    .map(|x| x.iter().map(Clone::clone).collect())
                    .expect("complex commands must have subs");
            }
            let cmd_type = rc_cmd.borrow().get_cmd_type();
            match cmd_type {
                CmdType::NotSupport | CmdType::Ctrl => {
                    rc_cmd
                        .borrow_mut()
                        .done_with_error(&RESP_OBJ_ERROR_NOT_SUPPORT);
                    self.wcmds.push_back(rc_cmd);
                    let _ = self.bcmds.pop_front().unwrap();
                    continue;
                }
                _ => {}
            };

            if rc_cmd.borrow().is_complex() {
                // 从 subs 里面读并pop 直到pop结束
                loop {
                    if self.subs.is_empty() {
                        break;
                    }
                    let rc_sub = self.subs.front().cloned().expect("sub front never none");

                    match self.cluster.dispatch(rc_sub)? {
                        AsyncSink::NotReady(_) => {
                            self.can_continue = false;
                            return Ok(());
                        }
                        AsyncSink::Ready => {
                            let _ = self.subs.pop_front().unwrap();
                        }
                    };
                }
            } else {
                let rslt = self.cluster.dispatch(rc_cmd)?;
                match rslt {
                    AsyncSink::NotReady(_) => {
                        self.can_continue = false;
                        return Ok(());
                    }
                    AsyncSink::Ready => {}
                };
            }

            let tmp_cmd = self.bcmds.pop_front().expect("long live front cmd");
            self.wcmds.push_back(tmp_cmd);
        }
    }

    fn try_write_without_flush(&mut self) -> Result<(), Error> {
        loop {
            if self.wcmds.is_empty() {
                return Ok(());
            }

            let rc_cmd = self
                .wcmds
                .front()
                .cloned()
                .expect("front write-back cmd is never none");
            if !rc_cmd.borrow().is_done() {
                self.can_continue = false;
                return Ok(());
            }

            if let AsyncSink::NotReady(_) = self.output.start_send(rc_cmd)? {
                self.can_continue = false;
                return Ok(());
            }
            let _ = self.wcmds.pop_front().unwrap();
        }
    }

    fn try_write_back(&mut self) -> Result<(), Error> {
        self.try_write_without_flush()?;
        if let Async::NotReady = self.output.poll_complete().map_err(|err| {
            error!{"send error due to {:?}", err};
            Error::Critical
        })? {
            self.can_continue = false;
        }

        Ok(())
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
            if !self.can_continue {
                self.can_continue = !self.can_continue;
                return Ok(Async::NotReady);
            }

            // 1. read until reach MAX_CNCURRENCY
            let _count = self.try_read_cmds()?;

            // 2. send until bcmds is zero or NotReady
            self.try_send_cmds()?;

            // 3. write back bcmds until zero or NotReady
            self.try_write_back()?;
        }
    }
}

pub struct Handle<I, O>
where
    I: Stream<Item = Resp, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{
    cluster: Rc<Cluster>,

    input: I,
    output: O,
    cmds: VecDeque<Cmd>,
    // cmd: Option<Cmd>,
    waitq: VecDeque<Cmd>,
    closed: bool,
}


impl<I, O> Handle<I, O>
where
    I: Stream<Item = Resp, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{

    fn try_read(&mut self) -> Result<Async<Option<()>>, Error> {
        loop {
            match try_ready!(self.input.poll()) {
                Some(val) => {
                    let cmd = Command::from_resp(val);
                    let is_complex = cmd.is_complex();
                    let rc_cmd = Rc::new(cmd);
                    if is_complex {
                        for sub in rc_cmd.sub_reqs.as_ref().expect("never be empty") {
                            self.waitq.push_back(sub);
                        }
                    } else {
                        self.waitq.push_back(rc_cmd);
                    }
                }
                None => {
                    return Ok(Async::Ready(None));
                },
            }
        }
    }
}

impl<I, O> Future for Handle<I, O>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd, SinkError = Error>,
{

    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_read = true;
        let mut can_send = true;
        let mut can_write = true;


        loop {
            if !can_read || !can_send || !can_write {
                return Ok(Async::NotReady);
            }
            // step 1: poll read from input stream.
            if can_read {
                // read until the input stream is NotReady.
                match self.try_read()? {
                    Async::NotReady => {
                        can_read = false;
                        continue;
                    }
                    Async::Ready(None) => {return Ok(Async::Ready(()));}
                    Async::Ready(Some(())) => {},
                }

            }

            // step 2: send to cluster.
            if can_send {
                // send until the output stream is unsendable.
            }
            // step 3: wait all the cluster is done.
            if can_write {
                // step 4: poll send back to client.
            }
        }
        // Ok(Async::NotReady)
    }
}
