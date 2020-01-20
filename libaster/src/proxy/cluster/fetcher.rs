use futures::{Async, AsyncSink, Future, Stream};
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};

use futures::task;
use std::rc::Rc;
use std::time::Instant;

use crate::com::AsError;
use crate::protocol::redis::{new_cluster_slots_cmd, slots_reply_to_replicas, Cmd};
use crate::proxy::cluster::Cluster;

enum State {
    Interval,
    Random,
    Sending(String, Cmd),
    Waiting(String, Cmd),
    Done(String, Cmd),
}

pub struct Fetch<R>
where
    R: Stream<Item = Instant, Error = AsError>,
{
    cluster: Rc<Cluster>,
    rng: ThreadRng,
    state: State,
    trigger: R,
}

impl<R> Fetch<R>
where
    R: Stream<Item = Instant, Error = AsError>,
{
    pub fn new(cluster: Rc<Cluster>, trigger: R) -> Fetch<R> {
        Fetch {
            cluster,
            trigger,
            rng: thread_rng(),
            state: State::Interval,
        }
    }
}

impl<R> Future for Fetch<R>
where
    R: Stream<Item = Instant, Error = AsError>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.cluster.cc.servers.is_empty() {
            return Ok(Async::Ready(()));
        }
        loop {
            match &self.state {
                State::Interval => match self.trigger.poll() {
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
                    let addr = self
                        .cluster
                        .cc
                        .servers
                        .iter()
                        .nth(position)
                        .cloned()
                        .unwrap();
                    let mut cmd = new_cluster_slots_cmd();
                    cmd.reregister(task::current());
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
                        error!("fail to fetch CLUSTER SLOTS from {} due to {}", addr, err);
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
                            warn!("fail to parse cmd reply from {} due {}", addr, err);
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
