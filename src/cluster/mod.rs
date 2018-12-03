mod fetcher;
mod handler;
mod init;
mod node;
mod slots;

use self::super::ClusterConfig;
use self::node::{NodeDown, NodeRecv};
use self::slots::SlotsMap;
use com::*;
use redis::cmd::{Cmd, CmdCodec};
use redis::resp::RespCodec;

use self::fetcher::Fetcher;
use self::handler::Handle;
use self::init::ClusterInitilizer;

use futures::lazy;
use futures::unsync::mpsc::{channel, Receiver, Sender};
use futures::{Async, AsyncSink};
use tokio::net::TcpStream;
use tokio::prelude::{Future, Sink, Stream};
use tokio::runtime::current_thread;
use tokio_codec::Decoder;

use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::rc::Rc;

pub struct Cluster {
    pub cc: ClusterConfig,
    pub slots: RefCell<SlotsMap>,
    tx: RefCell<Option<Sender<(String, Cmd)>>>,
}

impl Cluster {
    pub fn new(cc: ClusterConfig) -> Cluster {
        Cluster {
            cc,
            slots: RefCell::new(SlotsMap::default()),
            tx: RefCell::new(None),
        }
    }

    pub fn set_redirect(&self, sender: Sender<(String, Cmd)>) {
        *self.tx.borrow_mut() = Some(sender);
    }

    pub fn init_node_conn(&self) -> Result<(), Error> {
        let mut slots_map = self.slots.borrow_mut();
        for addr in &self.cc.servers {
            let tx = self.create_node_conn(&addr)?;
            slots_map.add_node(addr.clone(), tx.clone());
        }

        Ok(())
    }

    pub fn create_redirect(cluster: Rc<Cluster>) -> Sender<(String, Cmd)> {
        let (tx, rx) = channel(2048);
        struct Redirection {
            recv: Receiver<(String, Cmd)>,
            cluster: Rc<Cluster>,
            store: Option<(String, Cmd)>,
        }

        impl Future for Redirection {
            type Item = ();
            type Error = ();

            fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
                loop {
                    if self.store.is_some() {
                        let mut redirect = None;
                        std::mem::swap(&mut redirect, &mut self.store);
                        if let Some((addr, cmd)) = redirect {
                            match self.cluster.execute(&addr, cmd).map_err(|err| {
                                error!("fail to redirect command due to {:?}", err);
                            })? {
                                AsyncSink::NotReady(val) => {
                                    self.store = Some((addr, val));
                                    return Ok(Async::NotReady);
                                }
                                AsyncSink::Ready => {
                                    debug!("success redirect one command");
                                }
                            }
                        }
                    }

                    if let Some((addr, cmd)) = try_ready!(self
                        .recv
                        .poll()
                        .map_err(|err| error!("fail to redirect the command due to {:?}", err)))
                    {
                        self.store = Some((addr, cmd));
                    } else {
                        warn!("redirect future is dropped");
                        return Ok(Async::Ready(()));
                    }
                }
            }
        }

        current_thread::spawn(Redirection {
            recv: rx,
            cluster,
            store: None,
        });

        tx
    }

    pub fn create_node_conn(&self, node: &str) -> AsResult<Sender<Cmd>> {
        let addr_string = node.to_string();
        let (tx, rx): (Sender<Cmd>, Receiver<Cmd>) = channel(1024);
        let ret_tx = tx.clone();
        let redirection = self
            .tx
            .borrow()
            .as_ref()
            .cloned()
            .expect("redirect channel is never be empty");
        let rt = self.cc.read_timeout.clone();
        let wt = self.cc.write_timeout.clone();
        let amt = lazy(|| -> Result<(), ()> { Ok(()) })
            .and_then(move |_| {
                addr_string
                    .as_str()
                    .parse()
                    .map_err(|err| error!("fail to parse addr {:?}", err))
            })
            .and_then(|addr| {
                TcpStream::connect(&addr).map_err(|err| error!("fail to connect {:?}", err))
            })
            .and_then(move |sock| {
                let sock = set_read_write_timeout(sock, rt, wt)
                    .expect("set read/write timeout in cluster must be ok");
                sock.set_nodelay(true).expect("set nodelay must ok");
                let codec = RespCodec {};
                let (sink, stream) = codec.framed(sock).split();
                let arx = rx.map_err(|err| {
                    info!("fail to send due to {:?}", err);
                    Error::Critical
                });
                let buf = Rc::new(RefCell::new(VecDeque::new()));
                let nd = NodeDown::new(arx, sink, buf.clone());
                current_thread::spawn(nd);
                let nr = NodeRecv::new(stream, buf.clone(), redirection);
                current_thread::spawn(nr);
                Ok(())
            });
        current_thread::spawn(amt);
        Ok(ret_tx)
    }

    fn try_dispatch(sender: &mut Sender<Cmd>, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        match sender.start_send(cmd) {
            Ok(AsyncSink::NotReady(v)) => Ok(AsyncSink::NotReady(v)),
            Ok(AsyncSink::Ready) => sender
                .poll_complete()
                .map_err(|err| {
                    error!("fail to complete send cmd to node conn due {:?}", err);
                    Error::Critical
                })
                .map(|_| AsyncSink::Ready),
            Err(err) => {
                error!("send fail with send error: {:?}", err);
                Err(Error::Critical)
            }
        }
    }

    pub fn execute(&self, node: &str, cmd: Cmd) -> Result<AsyncSink<Cmd>, Error> {
        loop {
            let mut slots_map = self.slots.borrow_mut();
            if let Some(sender) = slots_map.get_sender_by_addr(node) {
                return Cluster::try_dispatch(sender, cmd);
            }
            let tx = self.create_node_conn(node)?;
            slots_map.add_node(node.to_string(), tx);
        }
    }

    pub fn dispatch_all(&self, cmds: &mut VecDeque<Cmd>) -> Result<AsyncSink<usize>, Error> {
        let mut node_set = HashSet::new();
        let mut is_ready = true;
        let mut count = 0;
        loop {
            if !is_ready {
                break;
            }

            let cmd = match cmds.front().cloned() {
                Some(val) => val,
                None => break,
            };

            let slot = cmd.crc() as usize;
            let mut slots_map = self.slots.borrow_mut();
            loop {
                let addr = slots_map.get_addr(slot);
                if let Some(sender) = slots_map.get_sender_by_addr(&addr) {
                    // try to send, when success, insert into node_set
                    match sender.start_send(cmd) {
                        Ok(AsyncSink::NotReady(_v)) => {
                            is_ready = false;
                            break;
                        }
                        Ok(AsyncSink::Ready) => {
                            node_set.insert(addr.clone());
                            count += 1;
                            let _cmd = cmds.pop_front().unwrap();
                            break;
                        }
                        Err(err) => {
                            error!("send fail with send error: {:?}", err);
                            return Err(Error::Critical);
                        }
                    }
                }
                let tx = self.create_node_conn(&addr)?;
                slots_map.add_node(addr, tx);
            }
        }

        for node in node_set.into_iter() {
            let mut slots_map = self.slots.borrow_mut();
            let sender = slots_map
                .get_sender_by_addr(&node)
                .expect("never be null after send");
            sender.poll_complete().map_err(|err| {
                error!("fail to complete send cmd to node conn due {:?}", err);
                Error::Critical
            })?;
        }

        if is_ready {
            Ok(AsyncSink::Ready)
        } else {
            Ok(AsyncSink::NotReady(count))
        }
    }
}

pub fn start_cluster(cluster: Cluster) {
    let addr = cluster
        .cc
        .listen_addr
        .clone()
        .parse::<SocketAddr>()
        .expect("parse socket never fail");

    let fut = lazy(move || -> Result<(SocketAddr, Cluster), ()> { Ok((addr, cluster)) })
        .and_then(|(addr, cluster)| {
            let listen = create_reuse_port_listener(&addr).expect("bind never fail");
            // cluster.init_node_conn().unwrap();
            ClusterInitilizer::new(cluster, listen).map_err(|err| {
                error!("fail to init cluster with given server due {:?}", err);
            })
        })
        .and_then(|(cluster, listen)| {
            // TODO: how to spawn timer func with current_thread
            let fetcher = Fetcher::new(cluster.clone())
                .for_each(|_| {
                    debug!("success fetch new slots_map");
                    Ok(())
                })
                .map_err(|err| {
                    error!("fail to fetch new slots_mapd due {:?}", err);
                });
            current_thread::spawn(fetcher);
            Ok((cluster, listen))
        })
        .and_then(|(cluster, listen)| {
            let rc_cluster = cluster.clone();
            let amt = listen
                .incoming()
                .for_each(move |sock| {
                    let sock = set_read_write_timeout(
                        sock,
                        cluster.cc.read_timeout,
                        cluster.cc.write_timeout,
                    )
                    .expect("set read/write timeout in cluster frontend must be ok");
                    sock.set_nodelay(true).expect("set nodelay must ok");
                    let codec = CmdCodec::default();
                    let (cmd_tx, resp_rx) = codec.framed(sock).split();
                    let cluster = rc_cluster.clone();
                    // TODO: remove magic number.
                    let (handle_resp_tx, handle_resp_rx) = channel(2048);
                    let input = resp_rx
                        .forward(handle_resp_tx)
                        .map_err(|err| match err {
                            Error::IoError(ref e) if e.kind() == ErrorKind::ConnectionReset => {}
                            e => error!("fail to send into handle due to {:?}", e),
                        })
                        .then(|_| Ok(()));
                    current_thread::spawn(input);
                    let handle = Handle::new(
                        cluster,
                        handle_resp_rx.map_err(|_| {
                            error!("fail to pass handle");
                            Error::Critical
                        }),
                        cmd_tx,
                    )
                    .map_err(|err| {
                        error!("get handle error due {:?}", err);
                    });
                    current_thread::spawn(handle);
                    Ok(())
                })
                .map_err(|err| {
                    error!("fail to start_cluster due {:?}", err);
                });
            current_thread::spawn(amt);
            Ok(())
        });

    current_thread::block_on_all(fut).unwrap();
}
