use crate::proxy::*;

use futures::{task, Async, AsyncSink, Future};
use tokio::timer::Interval;

use std::rc::Weak;
use std::time::Duration;

pub struct Ping<T>
where
    T: Request + 'static,
{
    proxy: Weak<Proxy<T>>,
    addr: String,
    req: Option<T>,
    max_retry: usize,
    retry: usize,
    interval: Interval,
    is_alive: bool,
}

impl<T: Request + 'static> Ping<T> {
    pub fn new(
        proxy: Weak<Proxy<T>>,
        addr: String,
        max_retry: usize,
        interval: u64,
        is_alive: bool,
    ) -> Ping<T> {
        Ping {
            proxy,
            addr,
            max_retry,
            is_alive,

            retry: 0,
            req: None,
            interval: Interval::new_interval(Duration::from_millis(interval)),
        }
    }
}

impl<T: Request> Future for Ping<T> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            debug!("trying to into ping poll to {}", self.addr);
            if self.retry >= self.max_retry {
                let proxy = match self.proxy.upgrade() {
                    None => return Ok(Async::Ready(())),
                    Some(pc) => pc,
                };
                if self.is_alive {
                    info!("disable node {} from hash ring", self.addr);
                    disable_node(proxy, &self.addr);
                } else {
                    info!("recovery node {} to hash ring", self.addr);
                    recovery_node(proxy, &self.addr);
                }
                return Ok(Async::Ready(()));
            }

            if self.req.is_none() {
                let rslt = try_ready!(self.interval.poll().map_err(|err| {
                    error!("fetch by internal fail due {:?}", err);
                }));
                if rslt.is_none() {
                    return Ok(Async::Ready(()));
                }
                debug!("trying to execute ping command to {}", self.addr);

                let req = T::ping_request();
                let local_task = task::current();
                req.reregister(local_task);
                let proxy = match self.proxy.upgrade() {
                    None => return Ok(Async::Ready(())),
                    Some(pc) => pc,
                };

                match proxy.execute(&self.addr, req.clone()) {
                    Ok(AsyncSink::Ready) => {
                        self.req = Some(req);
                    }
                    Ok(AsyncSink::NotReady(_)) => {
                        return Ok(Async::NotReady);
                    }
                    Err(err) => {
                        warn!("connection ping to {} is error {:?}", self.addr, err);
                        if self.is_alive {
                            self.retry += 1;
                        } else {
                            self.retry = 0
                        };
                        continue;
                    }
                }
            }

            if self.req.is_some() {
                debug!("wait for command to done for {}", self.addr);
                if !self
                    .req
                    .as_ref()
                    .expect("ping request is never be empty")
                    .is_done()
                {
                    debug!("ping to backend {} not done", self.addr);
                    return Ok(Async::NotReady);
                }
                debug!("send ping success to {}", self.addr);

                if self.is_alive {
                    self.retry = 0;
                } else {
                    self.retry += 1;
                }
                self.req = None;
            }
        }
    }
}
