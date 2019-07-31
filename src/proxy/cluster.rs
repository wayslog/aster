use crate::com::AsError;
use crate::com::ClusterConfig;
use crate::protocol::redis::{Command, Message, MessageIter};

use failure::Error;
use futures::task::Task;
use futures::unsync::mpsc::{channel, Receiver, SendError, Sender};
use futures::AsyncSink;
use futures::{Sink, Stream};

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

pub struct Cluster {
    pub cc: ClusterConfig,

    slots: Slots,
    conns: Conns,
}

type Conns = HashMap<String, Conn<Sender<Command>>>;

struct Conn<S: Sink<SinkItem = Command>> {
    addr: String,
    primary: S,
    secondary: Option<S>,
}

enum State {
    Unchecked,
    Checked(u16),
}

struct Slots {
    state: State,
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

    fn update_all(
        &mut self,
        masters: Vec<String>,
        replicas: Vec<Vec<String>>,
    ) -> Result<bool, Error> {

        Ok(false)
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
        let current = self.current.get();
        let len = self.addrs.len();
        self.current.update(|x| (x + 1) % len);
        &self.addrs[current]
    }
}
