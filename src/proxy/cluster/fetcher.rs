use futures::{Async, AsyncSink, Future, Stream};
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};
use tokio::timer::Interval;

use futures::task;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::protocol::redis::{new_cluster_slots_cmd, slots_reply_to_replicas, Cmd};
use crate::proxy::cluster::Cluster;

enum State {
    Interval,
    Random,
    Sending(String, Cmd),
    Waiting(String, Cmd),
    Done(String, Cmd),
}

pub struct Fetcher {
    cluster: Rc<Cluster>,
    rng: ThreadRng,
    state: State,
    interval: Interval,
}

impl Fetcher {
    pub fn new(cluster: Rc<Cluster>) -> Fetcher {
        let interval_millis = cluster.cc.fetch_interval.unwrap_or(60 * 30 * 1000);
        let interval = Interval::new(
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(interval_millis),
        );
        Fetcher {
            cluster,
            interval,
            rng: thread_rng(),
            state: State::Interval,
        }
    }
}

impl Future for Fetcher {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.cluster.cc.servers.is_empty() {
            return Ok(Async::Ready(()));
        }
        loop {
            match &self.state {
                State::Interval => match self.interval.poll() {
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Ok(Async::Ready(Some(_))) => {
                        self.state = State::Random;
                    }
                    Ok(Async::Ready(None)) => {
                        return Ok(Async::Ready(()));
                    }
                    Err(err) => {
                        error!("fail to poll from interval {}", err);
                        return Err(());
                    }
                },
                State::Random => {
                    let position = self.rng.gen_range(0, self.cluster.cc.servers.len());
                    let addr = self.cluster.cc.servers[position].clone();
                    let cmd = new_cluster_slots_cmd();
                    cmd.borrow_mut().reregister(task::current());
                    self.state = State::Sending(addr, cmd);
                }
                State::Sending(addr, cmd) => match self.cluster.dispatch_to(addr, cmd.clone()) {
                    Ok(AsyncSink::NotReady(_)) => {
                        return Ok(Async::NotReady);
                    }
                    Ok(AsyncSink::Ready) => {
                        self.state = State::Waiting(addr.clone(), cmd.clone());
                    }
                    Err(err) => {
                        error!("fail to fetch CLUSTER SLOTS due to {}", err);
                        self.state = State::Interval;
                    }
                },
                State::Waiting(addr, cmd) => {
                    if !cmd.borrow().is_done() {
                        return Ok(Async::NotReady);
                    }
                    self.state = State::Done(addr.clone(), cmd.clone());
                }
                State::Done(addr, cmd) => {
                    let layout = match slots_reply_to_replicas(cmd.clone()) {
                        Ok(Some(layout)) => layout,
                        Ok(None) => {
                            warn!("slots not full covered, this may be not allow in aster");
                            self.state = State::Interval;
                            continue;
                        }
                        Err(err) => {
                            warn!("fail to parse cmd reply due {}", err);
                            self.state = State::Interval;
                            continue;
                        }
                    };
                    if self.cluster.try_update_all_slots(layout) {
                        info!("succeed to update cluster slots table by {}", addr);
                    } else {
                        debug!("unable to change cluster slots table, this may not be an error");
                    }
                    self.state = State::Interval;
                }
            }
        }
    }
}
