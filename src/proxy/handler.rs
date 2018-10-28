use com::*;
use proxy::{Proxy, Request};

use futures::task;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};

use std::collections::VecDeque;
use std::fmt::Debug;
use std::rc::Rc;

const MAX_CONCURRENCY: usize = 1024;

pub struct HandleInput<T, U, D>
where
    T: Stream<Error = D>,
    U: Sink<SinkItem = T::Item>,
    D: Debug,
{
    sink: U,
    stream: T,
    buffered: Option<T::Item>,
    count: usize,
}

impl<T, U, D> HandleInput<T, U, D>
where
    T: Stream<Error = D>,
    U: Sink<SinkItem = T::Item>,
    D: Debug,
{
    pub fn new(stream: T, sink: U) -> HandleInput<T, U, D> {
        Self {
            sink: sink,
            stream: stream,
            buffered: None,
            count: 0,
        }
    }
    fn try_start_send(&mut self, item: T::Item) -> Poll<(), U::SinkError> {
        debug_assert!(self.buffered.is_none());
        if let AsyncSink::NotReady(item) = self.sink.start_send(item)? {
            self.buffered = Some(item);
            return Ok(Async::NotReady);
        }
        Ok(Async::Ready(()))
    }
}

impl<T, U, D> Future for HandleInput<T, U, D>
where
    T: Stream<Error = D>,
    U: Sink<SinkItem = T::Item>,
    D: Debug,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            if let Some(item) = self.buffered.take() {
                match self
                    .try_start_send(item)
                    .map_err(|_err| error!("fail to send to back end"))?
                {
                    Async::NotReady => break,
                    Async::Ready(_) => {
                        self.count += 1;
                        continue;
                    }
                }
            }

            match self
                .stream
                .poll()
                .map_err(|err| error!("fail to poll from upstream {:?}", err))?
            {
                Async::Ready(Some(item)) => {
                    self.buffered = Some(item);
                }
                Async::Ready(None) => {
                    try_ready!(
                        self.sink
                            .close()
                            .map_err(|_err| error!("fail to close handle tx"))
                    );
                    return Ok(Async::Ready(()));
                }
                Async::NotReady => {
                    break;
                }
            }
        }

        if self.count > 0 {
            try_ready!(self.sink.poll_complete().map_err(|_err| {
                error!("fail to poll_complete to back end");
            }));
            self.count = 0;
        }
        Ok(Async::NotReady)
    }
}

pub struct Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
    D: Into<T>,
{
    closed: bool,
    proxy: Rc<Proxy<T>>,
    input: I,
    output: O,
    cmds: VecDeque<T>,
    count: usize,
    waitq: VecDeque<T>,
}

impl<T, I, O, D> Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request + 'static,
    D: Into<T>,
{
    pub fn new(proxy: Rc<Proxy<T>>, input: I, output: O) -> Handle<T, I, O, D> {
        Handle {
            closed: false,
            proxy: proxy,
            input: input,
            output: output,
            cmds: VecDeque::new(),
            count: 0,
            waitq: VecDeque::new(),
        }
    }

    fn try_send(&mut self) -> Result<Async<()>, Error> {
        loop {
            if self.waitq.is_empty() {
                return Ok(Async::NotReady);
            }

            if let AsyncSink::NotReady(()) = self.proxy.dispatch_all(&mut self.waitq)? {
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

            let rc_req = self.cmds.front().cloned().expect("cmds is never be None");
            if !rc_req.is_done() {
                break;
            }

            match self.output.start_send(rc_req)? {
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

impl<T, I, O, D> Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
    D: Into<T>,
{
    fn try_read(&mut self) -> Result<Async<Option<()>>, Error> {
        loop {
            if self.cmds.len() > MAX_CONCURRENCY {
                return Ok(Async::NotReady);
            }

            match try_ready!(self.input.poll()) {
                Some(val) => {
                    let req: T = Into::into(val);
                    req.reregister(task::current());
                    self.cmds.push_back(req.clone());
                    if !req.valid() {
                        continue;
                    }

                    if let Some(subs) = req.subs() {
                        for sub in subs.into_iter() {
                            self.waitq.push_back(sub);
                        }
                    } else {
                        self.waitq.push_back(req);
                    }
                }
                None => {
                    return Ok(Async::Ready(None));
                }
            }
        }
    }
}

impl<T, I, O, D> Future for Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request + 'static,
    D: Into<T>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_read = !self.closed;
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

            // step 2: send to proxy.
            if can_send {
                // send until the output stream is unsendable.
                match self.try_send() {
                    Ok(Async::NotReady) => {
                        can_send = false;
                    }
                    Ok(Async::Ready(_)) => {}
                    Err(err) => {
                        error!(
                            "proxy handle trying to close the connection due to {:?}",
                            err
                        );
                        for cmd in self.waitq.iter() {
                            cmd.done_with_error(Error::ClusterDown);
                        }
                        self.waitq.clear();
                        self.closed = true;
                    }
                }
            }

            // step 3: wait all the proxy is done.
            if can_write {
                if self.closed && self.cmds.is_empty() {
                    match self.output.close() {
                        Ok(_) => {}
                        Err(err) => {
                            error!("close fail due to {:?}", err);
                        }
                    }

                    return Ok(Async::Ready(()));
                }

                // step 4: poll send back to client.
                match self.try_write()? {
                    Async::NotReady => {
                        can_write = false;
                    }
                    Async::Ready(_) => {}
                }
            }
        }
    }
}
