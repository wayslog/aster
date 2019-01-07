use crate::cluster::Cluster;
use crate::com::*;
use crate::redis::cmd::{new_cluster_nodes_cmd, Cmd};
use crate::redis::resp::RESP_BULK;

use tokio::net::TcpListener;
use tokio::prelude::{Async, AsyncSink, Future};

use std::mem;
use std::rc::Rc;

#[derive(Clone, Copy, Debug)]
enum InitState {
    Ready,
    InitConn,
    Pend,
    Wait,
}

pub struct ClusterInitilizer {
    cluster: Rc<Cluster>,
    listen: Option<TcpListener>,
    servers: Vec<String>,
    cursor: usize,
    info_cmd: Cmd,
    state: InitState,
}

impl ClusterInitilizer {
    pub fn new(cluster: Cluster, listen: TcpListener) -> ClusterInitilizer {
        let servers = cluster.cc.servers.clone();
        ClusterInitilizer {
            cluster: Rc::new(cluster),
            listen: Some(listen),
            servers,
            cursor: 0,
            info_cmd: new_cluster_nodes_cmd(),
            state: InitState::Ready,
        }
    }
}

impl Future for ClusterInitilizer {
    type Item = (Rc<Cluster>, TcpListener);
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            match self.state {
                InitState::Ready => {
                    let tx = Cluster::create_redirect(self.cluster.clone());
                    self.cluster.set_redirect(tx);
                    self.state = InitState::InitConn;
                }
                InitState::InitConn => {
                    self.cluster.init_node_conn()?;
                    self.state = InitState::Pend;
                }
                InitState::Pend => {
                    let cursor = self.cursor;
                    if cursor == self.servers.len() {
                        return Err(Error::Critical);
                    }
                    let addr = self.servers.get(cursor).cloned().unwrap();
                    match self.cluster.execute(&addr, self.info_cmd.clone())? {
                        AsyncSink::NotReady(_) => return Ok(Async::NotReady),
                        AsyncSink::Ready => {
                            self.state = InitState::Wait;
                        }
                    }
                    self.cursor += 1;
                }

                InitState::Wait => {
                    let cmd = self.info_cmd.clone();
                    if !cmd.is_done() {
                        return Ok(Async::NotReady);
                    }
                    // debug!("cmd has been done {:?}", cmd);

                    let resp = cmd
                        .swap_reply()
                        .expect("reply never be empty for an done cmd");

                    if resp.rtype != RESP_BULK {
                        self.state = InitState::Pend;
                        continue;
                    }

                    let mut slots_map = self.cluster.slots.borrow_mut();
                    slots_map.try_update_all(resp.data.as_ref().expect("never be empty"));
                    let mut listener = None;
                    mem::swap(&mut listener, &mut self.listen);
                    return Ok(Async::Ready((self.cluster.clone(), listener.unwrap())));
                }
            }
        }
    }
}
