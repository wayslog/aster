// use com::*;
use proxy::*;

use futures::{task, Async, AsyncSink, Future};
use tokio::timer::Interval;
use std::time::Duration;

//use std::marker::PhantomData;

pub struct Ping<T>
where
    T: Request + 'static,
{
    proxy: Rc<Proxy<T>>,
    addr: String,
    req: Option<T>,
    max_retry: usize,
    retry: usize,
    interval: Interval,
}

impl<T: Request> Ping<T> {
    pub fn new(proxy: Rc<Proxy<T>>, addr: String, max_retry: usize, interval: u64) -> Ping<T> {
        Ping{
            proxy: proxy,
            addr: addr,
            max_retry: max_retry,
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
            if self.retry == self.max_retry {
                info!("remove node {} from hash ring", self.addr);
                self.proxy.del_node(&self.addr);
            }

            if self.req.is_none() {
                if let None = try_ready!(self.interval.poll().map_err(|err| {
                    error!("fetch by internal fail due {:?}", err);
                })) {
                    return Ok(Async::Ready(()));
                }

                let req = T::ping_request();
                let local_task = task::current();
                req.reregister(local_task);

                match self.proxy.execute(&self.addr, req.clone()) {
                    Ok(AsyncSink::Ready) => {
                        info!("baka");
                        self.retry += 1;
                        self.req = Some(req);
                    }
                    Ok(AsyncSink::NotReady(_)) => {
                        info!("kaba");
                        return Ok(Async::NotReady);
                    }
                    Err(err) => {
                        warn!("connection ping to {} is error {:?}", self.addr, err);
                        self.retry += 1;
                        continue;
                    }
                }
            }

            if self.req.is_some() {
                if !self
                    .req
                    .as_ref()
                    .expect("ping request is never be empty")
                    .is_done()
                {
                    return Ok(Async::NotReady);
                }

                if self.retry >= self.max_retry {
                    info!("re-add node {} to hash ring", self.addr);
                    self.proxy.add_node(&self.addr);
                }

                self.req = None;
                self.retry = 0;
            }
        }
    }
}
