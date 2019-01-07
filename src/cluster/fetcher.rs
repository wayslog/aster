use crate::cluster::Cluster;
use crate::com::*;
use crate::redis::cmd::{new_cluster_nodes_cmd, Cmd};
use crate::redis::resp::RESP_BULK;

use futures::task;
use tokio::prelude::{Async, AsyncSink, Stream};
use tokio::timer::Interval;

use std::rc::Rc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Copy)]
enum FetchState {
    Pending,
    Ready,
    Wait,
    Done,
}

pub struct Fetcher {
    cluster: Rc<Cluster>,
    cursor: usize,
    servers: Vec<String>,
    state: FetchState,
    info_cmd: Cmd,
    internal: Interval,
}

impl Fetcher {
    pub fn new(cluster: Rc<Cluster>) -> Fetcher {
        let servers = cluster.cc.servers.clone();
        let duration = Duration::from_secs(cluster.cc.fetch.as_ref().cloned().unwrap_or(30 * 60));
        Fetcher {
            cluster,
            servers,
            cursor: 0,
            state: FetchState::Pending,
            info_cmd: new_cluster_nodes_cmd(),
            internal: Interval::new(Instant::now(), duration),
        }
    }
}

impl Stream for Fetcher {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        loop {
            // debug!("fetch status cursor={} cmd={:?}", self.cursor, self.info_cmd);
            match self.state {
                FetchState::Pending => {
                    // initialize
                    self.info_cmd = new_cluster_nodes_cmd();
                    let local_task = task::current();
                    self.info_cmd.cmd_reregister(local_task);
                    self.state = FetchState::Ready;
                }
                FetchState::Ready => {
                    let rslt = try_ready!(self.internal.poll().map_err(|err| {
                        error!("fetch by internal fail due {:?}", err);
                        Error::Critical
                    }));

                    if rslt.is_none() {
                        return Ok(Async::Ready(None));
                    }

                    let cursor = self.cursor;
                    if cursor == self.servers.len() {
                        debug!("fail to update slots map but pass the turn");
                        self.state = FetchState::Done;
                        continue;
                    }
                    let addr = self.servers.get(cursor).cloned().unwrap();
                    debug!("trying to execute cmd to {}", addr);
                    match self.cluster.execute(&addr, self.info_cmd.clone())? {
                        AsyncSink::NotReady(_) => {
                            debug!("not done for fetcher wait");
                            return Ok(Async::NotReady);
                        }
                        AsyncSink::Ready => {
                            debug!("execute done ready {}", addr);
                            self.state = FetchState::Wait;
                        }
                    }
                    self.cursor += 1;
                }
                FetchState::Wait => {
                    let cmd = self.info_cmd.clone();
                    if !cmd.is_done() {
                        debug!("not done for fetcher wait");
                        return Ok(Async::NotReady);
                    }

                    let resp = cmd
                        .swap_reply()
                        .expect("fetch result never be empty for an done cmd");

                    if resp.rtype != RESP_BULK {
                        warn!("fetch fail due to bad resp {:?}", resp);
                        self.state = FetchState::Ready;
                        continue;
                    }

                    let mut slots_map = self.cluster.slots.borrow_mut();
                    let updated =
                        slots_map.try_update_all(resp.data.as_ref().expect("never be empty"));
                    if updated {
                        info!("success update slotsmap due slots map is changed");
                    } else {
                        debug!("skip to update due cluster slots map is never changed");
                    }
                    self.state = FetchState::Done;
                }
                FetchState::Done => {
                    self.cursor = 0;
                    self.state = FetchState::Ready;
                    self.info_cmd = new_cluster_nodes_cmd();
                    let task = task::current();
                    self.info_cmd.cmd_reregister(task);
                    return Ok(Async::Ready(Some(())));
                }
            }
        }
    }
}
