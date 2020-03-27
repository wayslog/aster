use futures::task;
use futures::{Async, AsyncSink, Future, Stream};
use tokio::timer::Interval;

use std::cell::Cell;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

use crate::proxy::standalone::{Cluster, Request};

#[derive(Debug)]
enum State<T> {
    Justice(bool),
    OnFail,
    OnSuccess,
    Sending(T),
    Waitting(T),
}

pub struct Ping<T: Request> {
    cluster: Weak<Cluster<T>>,

    name: String,
    addr: String,

    fail_interval: Interval,
    succ_interval: Interval,

    count: u8,
    limit: u8,

    state: State<T>,
    cancel: Rc<Cell<bool>>,
}

impl<T: Request> Ping<T> {
    pub fn new(
        cluster: Weak<Cluster<T>>,
        name: String,
        addr: String,
        cancel: Rc<Cell<bool>>,
        interval_millis: u64,
        succ_interval_millis: u64,
        limit: u8,
    ) -> Ping<T> {
        let fail_interval = Interval::new(
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(interval_millis),
        );
        let succ_interval = Interval::new(
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(succ_interval_millis),
        );

        Ping {
            cluster,
            name,
            addr,
            fail_interval,
            succ_interval,
            limit,
            count: 0,
            state: State::OnSuccess,
            cancel,
        }
    }
}

impl<T: Request + 'static> Future for Ping<T> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            if self.cancel.get() {
                info!("ping to {}({}) was canceld by handle", self.name, self.addr);
                return Ok(Async::Ready(()));
            }

            match self.state {
                State::Justice(is_last_succ) => {
                    if is_last_succ {
                        if self.count > self.limit {
                            // removed but success next time
                            if let Some(cluster) = self.cluster.upgrade() {
                                cluster.add_node(self.name.clone()).map_err(|err| {
                                    error!(
                                        "fail to add node for {} due to {:?}",
                                        self.name.clone(),
                                        err
                                    );
                                })?;
                            } else {
                                return Ok(Async::Ready(()));
                            }
                        }
                        self.count = 0;
                        self.state = State::OnSuccess;
                    } else {
                        self.count = self.count.wrapping_add(1);
                        debug!("ping state fail count={} limit={}", self.count, self.limit);

                        #[allow(clippy::comparison_chain)]
                        if self.count == self.limit {
                            if let Some(cluster) = self.cluster.upgrade() {
                                info!("remove node={} addr={} by ping error", self.name, self.addr);
                                cluster.remove_node(self.name.clone());
                            } else {
                                return Ok(Async::Ready(()));
                            }
                            self.state = State::OnFail;
                        } else if self.count > self.limit {
                            self.state = State::OnFail;
                        } else {
                            self.state = State::OnSuccess;
                        }

                        if let Some(cluster) = self.cluster.upgrade() {
                            cluster.reconnect(&self.addr);
                        } else {
                            return Ok(Async::Ready(()));
                        }
                    }
                }
                State::OnSuccess => match self.succ_interval.poll() {
                    Ok(Async::Ready(Some(_))) => {
                        let mut cmd = T::ping_request();
                        cmd.reregister(task::current());
                        self.state = State::Sending(cmd);
                    }
                    Ok(Async::Ready(None)) => {
                        return Ok(Async::Ready(()));
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                    Err(err) => {
                        warn!("fail to poll interval due when succ {:?}", err);
                    }
                },
                State::OnFail => match self.fail_interval.poll() {
                    Ok(Async::Ready(Some(_))) => {
                        let mut cmd = T::ping_request();
                        cmd.reregister(task::current());
                        self.state = State::Sending(cmd);
                    }
                    Ok(Async::Ready(None)) => {
                        return Ok(Async::Ready(()));
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                    Err(err) => {
                        warn!("fail to poll interval due when fail {:?}", err);
                    }
                },
                State::Sending(ref cmd) => {
                    let rc_cmd = cmd.clone();
                    if !rc_cmd.can_cycle() {
                        self.state = State::Justice(false);
                        continue;
                    }
                    if let Some(cluster) = self.cluster.upgrade() {
                        match cluster.dispatch_to(&self.addr, rc_cmd) {
                            Ok(AsyncSink::NotReady(_)) => {
                                return Ok(Async::NotReady);
                            }
                            Ok(AsyncSink::Ready) => {
                                self.state = State::Waitting(cmd.clone());
                            }
                            Err(err) => {
                                info!("fail to dispatch_to {} due to {:?}", self.addr, err);
                                self.state = State::Justice(false);
                            }
                        }
                    } else {
                        info!("cluster has drooped and exit ping");
                        return Ok(Async::Ready(()));
                    }
                }

                State::Waitting(ref cmd) => {
                    if !cmd.is_done() {
                        return Ok(Async::NotReady);
                    }
                    self.state = State::Justice(!cmd.is_error());
                }
            }
        }
    }
}
