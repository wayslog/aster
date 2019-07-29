use crate::com::*;
use crate::redis::cmd::{new_asking_cmd, Cmd, RESP_OBJ_BACKEND_CLOSED, RESP_OBJ_BACKEND_ERROR};
use crate::redis::resp::{Resp, RESP_ERROR};

use tokio::prelude::{Async, AsyncSink, Future, Sink, Stream};

use std::collections::VecDeque;
use std::rc::Rc;

const BATCH_SIZE: usize = 1024;

const REDIRECT_MOVED_DATA: &[u8] = b"MOVED ";
const REDIRECT_ASK_DATA: &[u8] = b"ASK";

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
enum State {
    Running,
    Closing,
    Closed,
}

impl State {
    fn is_closed(self) -> bool {
        self == State::Closed
    }

    fn is_closing(self) -> bool {
        self == State::Closing
    }
}

pub struct Backend<I, O, S, R>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Rc<Resp>, SinkError = Error>,
    S: Stream<Item = Resp, Error = Error>,
    R: Sink<SinkItem = (String, Cmd)>,
{
    addr: String,
    state: State,
    store: VecDeque<Cmd>,
    rstore: Option<(String, Cmd)>,
    buf: VecDeque<Cmd>,

    input: I,
    output: O,
    recv: S,
    redirect: R,
}

impl<I, O, S, R> Backend<I, O, S, R>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Rc<Resp>, SinkError = Error>,
    S: Stream<Item = Resp, Error = Error>,
    R: Sink<SinkItem = (String, Cmd)>,
{
    pub fn mew(addr: String, input: I, output: O, recv: S, redirect: R) -> Backend<I, O, S, R> {
        Backend {
            state: State::Running,
            store: VecDeque::new(),
            rstore: None,
            buf: VecDeque::with_capacity(BATCH_SIZE),

            addr,
            input,
            output,
            recv,
            redirect,
        }
    }

    fn on_close(&mut self) -> Result<(), ()> {
        for cmd in self.buf.iter() {
            cmd.done_with_error(&&RESP_OBJ_BACKEND_CLOSED);
        }
        for cmd in self.store.iter() {
            cmd.done_with_error(&RESP_OBJ_BACKEND_CLOSED);
        }
        self.output.close().map_err(|err| {
            error!(
                "fail to close the internal connection to {} due to {:?}",
                self.addr, err
            );
        })?;

        self.state = State::Closed;
        Ok(())
    }

    #[inline]
    fn parse_redirect(resp: &Resp) -> Option<(bool, String)> {
        if resp.rtype != RESP_ERROR {
            return None;
        }
        if let Some(ref data) = resp.data.as_ref() {
            if data.starts_with(REDIRECT_ASK_DATA) {
                let addr = read_redirect_addr(data);
                return Some((true, addr));
            } else if data.starts_with(REDIRECT_MOVED_DATA) {
                let addr = read_redirect_addr(data);
                return Some((false, addr));
            }
        }
        None
    }

    fn try_redirect(&mut self) -> Result<Async<()>, Error> {
        let mut redirectiion = None;
        std::mem::swap(&mut redirectiion, &mut self.rstore);
        let (addr, cmd) = redirectiion.expect("try_redirect get redirectiion never be empty");
        match self.redirect.start_send((addr, cmd)).map_err(|_err| {
            error!("fail to redirect to cluster maybe cluster is down");
            Error::UnableRedirect
        })? {
            AsyncSink::NotReady((addr, cmd)) => {
                self.rstore = Some((addr, cmd));
                Ok(Async::NotReady)
            }
            AsyncSink::Ready => Ok(Async::Ready(())),
        }
    }

    fn try_recv(&mut self) -> Result<usize, Error> {
        let mut count = 0;
        loop {
            if count == BATCH_SIZE {
                return Ok(count);
            }

            if self.rstore.is_some() {
                match self.try_redirect()? {
                    Async::NotReady => {
                        return Ok(count);
                    }
                    Async::Ready(()) => {}
                }
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

            count += 1;
            let cmd = self
                .buf
                .pop_front()
                .expect("buffer is never empty for endpoint");
            if cmd.is_ignore_reply() {
                continue;
            }

            if let Some((is_ask, addr)) = Self::parse_redirect(&resp) {
                cmd.set_is_ask(is_ask);
                self.rstore = Some((addr, cmd));
            } else {
                cmd.done(resp);
            }
        }
    }

    // forward message from upstream into downstream and copy them into local buffer.
    fn try_forward(&mut self) -> Result<usize, Error> {
        let usize = self.try_forward_inner()?;
        if usize > 0 {
            self.output.poll_complete().map_err(|err| {
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

            if let Some(req) = self.store.front() {
                // forward into backend
                match self.output.start_send(req.rc_req()) {
                    Ok(AsyncSink::NotReady(_)) => {
                        return Ok(count);
                    }
                    Ok(AsyncSink::Ready) => {
                        let req = self.store.pop_front().expect("cluster store never empty");
                        self.buf.push_back(req);
                        count += 1;
                    }
                    Err(err) => {
                        error!("fail to forward to {} due {:?}", &self.addr, err);
                        req.done_with_error(&RESP_OBJ_BACKEND_ERROR);
                        return Err(err);
                    }
                }
            }

            let pret = match self.input.poll() {
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
            if pret.is_ask() {
                self.store.push_back(new_asking_cmd());
            }
            // maybe judge is_noreply first
            self.store.push_back(pret);
        }
    }
}

impl<I, O, S, R> Future for Backend<I, O, S, R>
where
    I: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Rc<Resp>, SinkError = Error>,
    S: Stream<Item = Resp, Error = Error>,
    R: Sink<SinkItem = (String, Cmd)>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_forward = true;
        let mut can_recv = true;
        loop {
            if self.state.is_closing() {
                self.on_close()?;
            }

            if self.state.is_closed() {
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

const SPC_BYTE: u8 = b' ';

fn read_redirect_addr(data: &[u8]) -> String {
    let mut iter = data.rsplit(|x| *x == SPC_BYTE);
    let addr = String::from_utf8_lossy(iter.next().unwrap());
    addr.into_owned()
}
