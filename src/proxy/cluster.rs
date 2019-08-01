use crate::com::AsError;
use crate::com::ClusterConfig;
use crate::protocol::redis::{Command, Message, MessageIter, ReplicaLayer, SLOTS_COUNT};
use crate::utils::crc::crc16;

use failure::Error;
use futures::task::Task;
use futures::unsync::mpsc::{channel, Receiver, SendError, Sender};
use futures::AsyncSink;
use futures::{Sink, Stream};
use log::Level::{Trace, Debug};

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};

// macro_rules! primary {
//     ($conns:expr, s:expr) => {
//         $conns.get_mut($s).map(|&mut x| x.get_primary())
//     };
// }


pub struct Cluster {
    pub cc: ClusterConfig,

    slots: Slots,
    conns: RefCell<Conns>,
    hash_tag: Vec<u8>,
    read_from_slave: bool,
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

    pub fn dispatch_all(&self, cmds: &mut VecDeque<Command>) -> Result<(), AsError> {
        loop {
            if cmds.is_empty() {
                return Ok(());
            }
            let cmd = cmds
                .pop_front()
                .expect("due to check len first, pop front never be empty");
            let slot = cmd.key_hash(self.hash_tag.as_ref(), crc16);
            let addr = self.get_addr(slot, cmd.is_read());
            let mut conns = self.conns.borrow_mut();

            if let Some(sender) = conns.get_mut(&addr).map(|x| x.primary()) {
                match sender.start_send(cmd) {
                    Ok(AsyncSink::Ready) => {
                        if log_enabled!(Trace) {
                            debug!("success start command into backend");
                        }
                    }
                    Ok(AsyncSink::NotReady(mut cmd)) => {
                        cmd.add_cycle();
                        cmds.push_front(cmd);
                        return Ok(());
                    }
                    Err(se) => {
                        let mut cmd = se.into_inner();
                        cmd.add_cycle();
                        cmds.push_front(cmd);
                        self.connect(&addr, &mut conns)?;
                        return Ok(());
                    }
                }
            } else {
                unreachable!("connection must be initial first");
            }
        }
    }
}

struct Conns {
    inner: HashMap<String, Conn<Sender<Command>>>,
}

impl Conns {
    fn get_mut(&mut self, s: &str) -> Option<&mut Conn<Sender<Command>>> {
        self.inner.get_mut(s)
    }
}

struct Conn<S> {
    addr: String,
    primary: S,
    secondary: Option<S>,
}

impl<S> Conn<S> {
    fn primary(&mut self) -> &mut S {
        &mut self.primary
    }
    fn secondary(&mut self) -> Option<&mut S> {
        self.secondary.as_mut()
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
