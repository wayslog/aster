use futures::unsync::mpsc::Sender;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};

use futures::task;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::com::AsError;
use crate::protocol::redis::{new_cluster_slots_cmd, slots_reply_to_replicas, Cmd};
use crate::proxy::cluster::Cluster;

#[derive(Debug, Clone)]
pub enum TriggerBy {
    Interval,
    Moved,
    Error,
}

pub(crate) type TriggerSender = Sender<TriggerBy>;

pub struct SingleFlightTrigger {
    ticker: Duration,
    latest: RefCell<Instant>,

    counter: Cell<u32>,
    fetch: RefCell<TriggerSender>,
}

lazy_static! {
    static ref GAPS: HashSet<u32> = {
        let mut set = HashSet::new();
        for i in 4..=16 {
            set.insert(2u32.pow(i));
        }
        set
    };
}

impl SingleFlightTrigger {
    pub fn new(interval: u64, fetch: TriggerSender) -> Self {
        SingleFlightTrigger {
            ticker: Duration::from_secs(interval),
            latest: RefCell::new(Instant::now()),
            counter: Cell::new(0),
            fetch: RefCell::new(fetch),
        }
    }

    pub fn try_trigger(&self) -> bool {
        if self.incr_counter() || self.latest.borrow().elapsed() > self.ticker {
            self.trigger();
            true
        } else {
            false
        }
    }

    pub fn ensure_trgger(&self) {
        self.trigger();
    }

    fn trigger(&self) {
        let mut fetch = self.fetch.borrow_mut();
        if fetch.start_send(TriggerBy::Error).is_ok() && fetch.poll_complete().is_ok() {
            self.latest.replace(Instant::now());
            self.counter.replace(0);
            info!("succeed trigger fetch process");
            return;
        }
        warn!("fail to trigger fetch process due fetch channel is full or closed.");
    }

    fn incr_counter(&self) -> bool {
        let now = self.counter.get().wrapping_add(1);
        self.counter.set(now);
        if Self::check_gap(now & 0x00_00f_fff) {
            return true;
        }
        false
    }

    fn check_gap(left: u32) -> bool {
        GAPS.contains(&left)
    }
}

enum State {
    Interval,
    Random,
    Sending(String, Cmd),
    Waiting(String, Cmd),
    Done(String, Cmd),
}

pub struct Fetch<R>
where
    R: Stream<Item = TriggerBy, Error = AsError>,
{
    cluster: Rc<Cluster>,
    rng: ThreadRng,
    state: State,
    trigger: R,
    #[allow(unused)]
    gap: Duration,
}

impl<R> Fetch<R>
where
    R: Stream<Item = TriggerBy, Error = AsError>,
{
    pub fn new(cluster: Rc<Cluster>, trigger: R) -> Fetch<R> {
        Fetch {
            cluster,
            trigger,
            rng: thread_rng(),
            state: State::Interval,
            gap: Duration::from_secs(1), // 30 mins
        }
    }
}

impl<R> Future for Fetch<R>
where
    R: Stream<Item = TriggerBy, Error = AsError>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.cluster.cc.borrow().servers.is_empty() {
            return Ok(Async::Ready(()));
        }
        loop {
            match &self.state {
                State::Interval => match self.trigger.poll() {
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Ok(Async::Ready(Some(trigger_by))) => {
                        match trigger_by {
                            TriggerBy::Interval => {
                                // if self.cluster.since_latest() < self.gap {
                                //     continue;
                                // }
                                // trace!("interval succ");
                            }
                            TriggerBy::Error => {
                                debug!("fetcher success trigger by proxy error");
                            }
                            TriggerBy::Moved => {
                                debug!("fetcher success trigger by moved");
                            }
                        }
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
                    let position = self
                        .rng
                        .gen_range(0, self.cluster.cc.borrow().servers.len());
                    let addr = self
                        .cluster
                        .cc
                        .borrow()
                        .servers
                        .get(position)
                        .cloned()
                        .unwrap();
                    info!("start fetch from remote address {}", addr);
                    let mut cmd = new_cluster_slots_cmd();
                    cmd.reregister(task::current());
                    self.state = State::Sending(addr, cmd);
                }
                State::Sending(addr, cmd) => match self.cluster.dispatch_to(addr, cmd.clone()) {
                    Ok(AsyncSink::NotReady(_)) => {
                        cmd.borrow_mut().add_cycle();
                        // trace!("sending not ready");
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
                        // trace!("wait never done");
                        return Ok(Async::NotReady);
                    }
                    // trace!("wait done");
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
