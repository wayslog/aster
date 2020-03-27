use futures::task;
use futures::unsync::mpsc::{channel, Sender};
use futures::{Async, AsyncSink, Future, Sink};

use crate::com::AsError;
use crate::com::ClusterConfig;
use crate::protocol::redis::{new_cluster_slots_cmd, slots_reply_to_replicas, Cmd};
use crate::proxy::cluster::{Cluster, ConnBuilder};

enum State {
    Pending,
    Connecting,
    Fetching(Sender<Cmd>, Cmd),
    Waitting(Sender<Cmd>, Cmd),
    Done(Cmd),
}

pub struct Initializer {
    cc: ClusterConfig,
    current: usize,
    state: State,
}

impl Initializer {
    pub fn new(cc: ClusterConfig) -> Initializer {
        Initializer {
            cc,
            current: 0,
            state: State::Pending,
        }
    }
}

impl Future for Initializer {
    type Item = ();
    type Error = AsError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            match &mut self.state {
                State::Pending => {
                    // debug!("start initialing cluster {}", self.cc.name);
                    self.current = 0;
                    self.state = State::Connecting;
                }
                State::Connecting => {
                    if self.current >= self.cc.servers.len() {
                        return Err(AsError::ClusterAllSeedsDie(self.cc.name.clone()));
                    }
                    let addr = self.cc.servers[self.current].clone();
                    // debug!("start to create connection to backend {}", &addr);
                    let (tx, _rx) = channel(0); // mock moved channel for backend is never be moved
                    let conn = ConnBuilder::new()
                        .moved(tx)
                        .cluster(self.cc.name.clone())
                        .node(addr.to_string())
                        .read_timeout(self.cc.read_timeout.clone())
                        .write_timeout(self.cc.write_timeout.clone())
                        .connect();
                    self.current += 1;

                    match conn {
                        Ok(sender) => {
                            let mut cmd = new_cluster_slots_cmd();
                            cmd.reregister(task::current());
                            self.state = State::Fetching(sender, cmd);
                        }
                        Err(err) => {
                            warn!("fail to connect to backend {} due to {}", &addr, &err);
                            self.state = State::Connecting;
                        }
                    }
                }
                State::Fetching(ref mut sender, ref cmd) => match sender.start_send(cmd.clone()) {
                    Ok(AsyncSink::NotReady(_cmd)) => {
                        // trace!("init: backend is not ready");
                        return Ok(Async::NotReady);
                    }
                    Ok(AsyncSink::Ready) => {
                        // trace!("init: backend is not ready");
                        self.state = State::Waitting(sender.clone(), cmd.clone());
                    }
                    Err(err) => {
                        let addr = self.cc.servers[self.current - 1].clone();
                        error!("fail to send CLUSTER SLOTS cmd to {} due {}", addr, err);
                        if let Err(_err) = sender.close() {
                            warn!("init fetching  connection can't be closed properly, skip");
                        }
                        self.state = State::Connecting;
                    }
                },
                State::Waitting(ref mut sender, ref mut cmd) => {
                    if !cmd.borrow().is_done() {
                        return Ok(Async::NotReady);
                    }
                    if let Err(_err) = sender.close() {
                        warn!("init waitting connection can't be closed properly, skip");
                    }
                    debug!("CLUSTER SLOTS get response as {:?}", cmd);
                    self.state = State::Done(cmd.clone());
                }
                State::Done(cmd) => match slots_reply_to_replicas(cmd.clone()) {
                    Ok(Some(replica)) => {
                        let cluster = Cluster::run(self.cc.clone(), replica);
                        match cluster {
                            Ok(_) => {
                                info!("succeed to create cluster {}", self.cc.name);
                                return Ok(Async::Ready(()));
                            }
                            Err(err) => {
                                warn!("fail to init cluster {} due to {}", self.cc.name, err);
                                self.state = State::Connecting;
                            }
                        }
                    }
                    Ok(None) => {
                        warn!("slots not full covered, this may be not allow in aster");
                        self.state = State::Connecting;
                    }
                    Err(err) => {
                        warn!("fail to parse cmd reply due {}", err);
                        self.state = State::Connecting;
                    }
                },
            }
        }
    }
}
