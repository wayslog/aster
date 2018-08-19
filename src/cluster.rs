use self::super::ClusterConfig;
use cmd::{Cmd, MUSK};
use com::*;
use node::{NodeDown, NodeRecv};
use resp::RespCodec;
use slots::SlotsMap;

use futures::lazy;
use futures::unsync::mpsc::{channel, Receiver, Sender};
use futures::AsyncSink;
use tokio::executor::current_thread;
use tokio::net::TcpStream;
use tokio::prelude::{Future, Sink, Stream};
use tokio_codec::Decoder;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

pub struct Cluster {
    pub cc: ClusterConfig,
    pub slots: RefCell<SlotsMap>,
}

impl Cluster {
    pub fn init_node_conn(&mut self) -> Result<(), Error> {
        // let cmd = new_cluster_nodes_cmd();
        let mut slots_map = self.slots.borrow_mut();
        for addr in &self.cc.servers {
            let tx = self.create_node_conn(&addr)?;
            slots_map.add_node(addr.clone(), tx.clone());
        }

        Ok(())
    }

    pub fn create_node_conn(&self, node: &str) -> AsResult<Sender<Cmd>> {
        let addr_string = node.to_string();
        let (tx, rx): (Sender<Cmd>, Receiver<Cmd>) = channel(1024);
        let ret_tx = tx.clone();
        let amt = lazy(|| -> Result<(), ()> { Ok(()) })
            .and_then(move |_| {
                addr_string
                    .as_str()
                    .parse()
                    .map_err(|err| error!("fail to parse addr {:?}", err))
            }).and_then(|addr| {
                TcpStream::connect(&addr).map_err(|err| error!("fail to connect {:?}", err))
            }).and_then(|sock| {
                let codec = RespCodec {};
                let (sink, stream) = codec.framed(sock).split();
                let arx = rx.map_err(|err| {
                    info!("fail to send due to {:?}", err);
                    Error::Critical
                });
                let buf = Rc::new(RefCell::new(VecDeque::new()));
                let nd = NodeDown::new(arx, sink, buf.clone());
                current_thread::spawn(nd);
                let nr = NodeRecv::new(stream, buf.clone());
                current_thread::spawn(nr);
                Ok(())
            });
        current_thread::spawn(amt);
        Ok(ret_tx)
    }

    fn try_dispatch(sender: &mut Sender<Cmd>, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        match sender.start_send(cmd) {
            Ok(AsyncSink::NotReady(v)) => {
                return Ok(AsyncSink::NotReady(v));
            }
            Ok(AsyncSink::Ready) => {
                return sender
                    .poll_complete()
                    .map_err(|err| {
                        error!("fail to complete send cmd to node conn due {:?}", err);
                        Error::Critical
                    }).map(|_| AsyncSink::Ready)
            }
            Err(err) => {
                error!("send fail with send error: {:?}", err);
                return Err(Error::Critical);
            }
        }
    }

    pub fn execute(&self, node: &String, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        loop {
            let mut slots_map = self.slots.borrow_mut();
            if let Some(sender) = slots_map.get_sender_by_addr(node) {
                return Cluster::try_dispatch(sender, cmd);
            }
            let tx = self.create_node_conn(node)?;
            slots_map.add_node(node.clone(), tx);
        }
    }

    pub fn dispatch(&self, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        let slot = (cmd.borrow().crc() & MUSK) as usize;
        loop {
            let mut slots_map = self.slots.borrow_mut();
            let addr = slots_map.get_addr(slot);
            if let Some(sender) = slots_map.get_sender_by_addr(&addr) {
                return Cluster::try_dispatch(sender, cmd);
            }
            let tx = self.create_node_conn(&addr)?;
            slots_map.add_node(addr, tx);
        }
    }
}
