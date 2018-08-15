use tokio::prelude::{Async, AsyncSink, Stream};
use tokio::timer::Interval;

use self::super::Cluster;
use cmd::{new_cluster_nodes_cmd, Cmd};
use com::*;
use resp::RESP_BULK;
use std::rc::Rc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Copy)]
enum FetchState {
    Ready,
    Wait,
}

pub struct Fetcher {
    cluster: Rc<Cluster>,
    cursor: usize,
    servers: Vec<String>,
    state: FetchState,
    info_cmd: Cmd,
    is_oneshot: bool,
    internal: Interval,
}

impl Fetcher {
    pub fn new(cluster: Rc<Cluster>) -> Fetcher {
        Self::new_fetcher(cluster, true)
    }

    fn new_fetcher(cluster: Rc<Cluster>, is_oneshot: bool) -> Fetcher {
        let servers = cluster.cc.servers.clone();
        let duration = Duration::from_secs(cluster.cc.fetch);
        Fetcher {
            cluster: cluster,
            cursor: 0,
            servers: servers,
            state: FetchState::Ready,
            info_cmd: new_cluster_nodes_cmd(),
            is_oneshot: is_oneshot,
            internal: Interval::new(Instant::now() + duration, duration),
        }
    }
}

impl Stream for Fetcher {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        info!("trying to fetch");
        if let None = try_ready!(self.internal.poll().map_err(|err| {
            error!("fetch by internal fail due {:?}", err);
            Error::Critical
        })) {
            return Ok(Async::Ready(None));
        }

        loop {
            match self.state {
                FetchState::Ready => {
                    let cursor = self.cursor;
                    if cursor == self.servers.len() {
                        warn!("fail to update slots map but pass the turn");
                        if self.is_oneshot {
                            return Ok(Async::Ready(None));
                        } else {
                            self.cursor = 0;
                            self.info_cmd = new_cluster_nodes_cmd();
                            return Ok(Async::Ready(Some(())));
                        }
                    }
                    let addr = self.servers.get(cursor).cloned().unwrap();
                    match self.cluster.execute(&addr, self.info_cmd.clone())? {
                        AsyncSink::NotReady(_) => return Ok(Async::NotReady),
                        AsyncSink::Ready => {
                            self.state = FetchState::Wait;
                        }
                    }
                    self.cursor += 1;
                }
                FetchState::Wait => {
                    let cmd = self.info_cmd.clone();
                    if !cmd.borrow().is_done() {
                        return Ok(Async::NotReady);
                    }

                    let cmd_borrow = cmd.borrow_mut();
                    let resp = cmd_borrow.reply.as_ref().unwrap();

                    if resp.rtype != RESP_BULK {
                        self.state = FetchState::Ready;
                        continue;
                    }

                    let mut slots_map = self.cluster.slots.borrow_mut();
                    slots_map.try_update_all(resp.data.as_ref().expect("never be empty"));
                    self.cursor = 0;
                    self.info_cmd = new_cluster_nodes_cmd();
                    return Ok(Async::Ready(Some(())));
                }
            }
        }
    }
}
