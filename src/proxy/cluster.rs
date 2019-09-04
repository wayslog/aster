pub mod back;
pub mod front;

use crate::com::AsError;
use crate::com::ClusterConfig;
use crate::protocol::redis::{Cmd, ReplicaLayer, SLOTS_COUNT};
use crate::utils::crc::crc16;

// use failure::Error;
use futures::task::Task;
use futures::unsync::mpsc::{channel, Receiver, SendError, Sender};
use futures::AsyncSink;
use futures::{Sink, Stream};
use log::Level::{Debug, Trace};

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Redirect {
    Move { slot: usize, to: String },
    Ask { slot: usize, to: String },
}

impl Redirect {
    pub(crate) fn is_ask(&self) -> bool {
        match self {
            Redirect::Ask { .. } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Redirection {
    pub target: Redirect,
    pub cmd: Cmd,
}

pub use Redirect::{Ask, Move};

pub struct RedirectFuture {
}

pub struct Cluster {
    pub cc: ClusterConfig,

    slots: Slots,
    conns: RefCell<Conns>,
    hash_tag: Vec<u8>,
    read_from_slave: bool,

    moved: Sender<Redirection>,
}

impl Cluster {
    fn get_addr(&self, slot: usize, is_read: bool) -> String {
        if self.read_from_slave && is_read {
            if let Some(replica) = self.slots.get_replica(slot) {
                if replica != "" {
                    return replica.to_string();
                }
            }
        }
        self.slots
            .get_master(slot)
            .map(|x| x.to_string())
            .expect("master addr never be empty")
    }

    fn connect(&self, addr: &str, conns: &mut Conns) -> Result<(), AsError> {

        Ok(())
    }

    pub fn dispath_to(&self, addr: &str, cmd: Cmd) -> Result<AsyncSink<Cmd>, AsError> {
        if !cmd.borrow().can_cycle() {
            cmd.set_reply(AsError::ClusterFailDispatch);
            return Ok(AsyncSink::Ready);
        }
        let mut conns = self.conns.borrow_mut();
        if let Some(sender) = conns.get_mut(addr).map(|x| x.sender()) {
            match sender.start_send(cmd) {
                Ok(ret) => {
                    return Ok(ret);
                }
                Err(se) => {
                    let cmd = se.into_inner();
                    return Ok(AsyncSink::NotReady(cmd));
                }
            }
        } else {
            unreachable!("connection must be initial first");
        }
    }

    pub fn dispatch_all(&self, cmds: &mut VecDeque<Cmd>) -> Result<usize, AsError> {
        let mut count = 0usize;
        loop {
            if cmds.is_empty() {
                return Ok(count);
            }
            let cmd = cmds.pop_front().expect("cmds pop front never be empty");
            if !cmd.borrow().can_cycle() {
                cmd.set_reply(AsError::ClusterFailDispatch);
                continue;
            }
            let slot = cmd.borrow().key_hash(self.hash_tag.as_ref(), crc16);
            let addr = self.get_addr(slot, cmd.borrow().is_read());
            let mut conns = self.conns.borrow_mut();

            if let Some(sender) = conns.get_mut(&addr).map(|x| x.sender()) {
                match sender.start_send(cmd) {
                    Ok(AsyncSink::Ready) => {
                        if log_enabled!(Trace) {
                            trace!("success start command into backend");
                        }
                        count += 1;
                    }
                    Ok(AsyncSink::NotReady(cmd)) => {
                        cmd.borrow_mut().add_cycle();
                        cmds.push_front(cmd);
                        return Ok(count);
                    }
                    Err(se) => {
                        let cmd = se.into_inner();
                        cmd.borrow_mut().add_cycle();
                        cmds.push_front(cmd);
                        self.connect(&addr, &mut conns)?;
                        return Ok(count);
                    }
                }
            } else {
                unreachable!("connection must be initial first");
            }
        }
    }
}

struct Conns {
    inner: HashMap<String, Conn<Sender<Cmd>>>,
}

impl Conns {
    fn get_mut(&mut self, s: &str) -> Option<&mut Conn<Sender<Cmd>>> {
        self.inner.get_mut(s)
    }
}

struct Conn<S> {
    addr: String,
    sender: S,
}

impl<S> Conn<S> {
    fn sender(&mut self) -> &mut S {
        &mut self.sender
    }
}

struct Slots {
    masters: Vec<String>,
    replicas: Vec<Replica>,

    moved_counter: Vec<u32>,
    moved_threshold: u32,
}

impl Slots {
    fn try_move(&mut self, slot: usize, new: &str) -> Option<String> {
        let count = self.moved_counter[slot].clone();
        if count >= self.moved_threshold {
            self.moved_counter[slot] = 0;
            let old = self.masters.get(slot).cloned();
            self.masters.insert(slot, new.to_string());
            return old;
        }
        self.moved_counter[slot] = count + 1;
        None
    }

    fn try_update_all(&mut self, layer: ReplicaLayer) -> bool {
        let (masters, replicas) = layer;
        let mut changed = false;
        for i in 0..SLOTS_COUNT {
            if self.masters[i] != masters[i] {
                changed = true;
                self.masters[i] = masters[i].clone();
                self.moved_counter[i] = 0;
            }
        }

        for i in 0..SLOTS_COUNT {
            if self.replicas[i].addrs.as_slice() != replicas[i].as_slice() {
                self.replicas[i] = Replica {
                    addrs: replicas[i].clone(),
                    current: Cell::new(0),
                };
                changed = true;
            }
        }

        changed
    }

    fn get_master(&self, slot: usize) -> Option<&str> {
        self.masters.get(slot).map(|x| x.as_str())
    }

    fn get_replica(&self, slot: usize) -> Option<&str> {
        self.replicas.get(slot).map(|x| x.get_replica())
    }
}

struct Replica {
    addrs: Vec<String>,
    current: Cell<usize>,
}

impl Replica {
    fn get_replica(&self) -> &str {
        if self.addrs.is_empty() {
            return "";
        }
        let current = self.current.get();
        let len = self.addrs.len();
        self.current.update(|x| (x + 1) % len);
        &self.addrs[current]
    }
}
