pub mod back;
pub mod fnv;
pub mod front;
pub mod ketama;

use futures::task::Task;
use futures::unsync::mpsc::Sender;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use tokio::codec::{Decoder, Encoder};

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::com::AsError;
use crate::com::ClusterConfig;
use crate::protocol::IntoReply;

use fnv::{fnv1a64, Fnv1a64};
use ketama::HashRing;

const MAX_RETRY: u8 = 8;

pub trait Request: Clone + Debug {
    type Reply: Clone + Debug + Into<Self> + IntoReply<Self::Reply> + From<AsError>;

    type FrontCodec: Decoder<Item = Self, Error = AsError>
        + Encoder<Item = Self, Error = AsError>
        + Default;
    type BackCodec: Decoder<Item = Self::Reply, Error = AsError>
        + Encoder<Item = Self, Error = AsError>
        + Default;

    fn ping_request() -> Self;
    fn reregister(&mut self, task: Task);

    fn key_hash(&self, hash_tag: &[u8], hasher: fn(&[u8]) -> u64) -> u64;

    fn subs(&self) -> Option<Vec<Self>>;

    fn is_done(&self) -> bool;
    fn is_error(&self) -> bool;

    fn valid(&self) -> bool;

    fn set_reply<R: IntoReply<Self::Reply>>(&self, t: R);
    fn set_error(&self, t: &AsError);
}

pub struct Cluster<T> {
    pub cc: ClusterConfig,
    hash_tag: Vec<u8>,
    _maker: PhantomData<T>,
    ring: RefCell<HashRing>,
    conns: RefCell<Conns<T>>,
}

impl<T: Request> Cluster<T> {
    pub fn dispatch_to(&self, addr: &str, cmd: T) -> Result<AsyncSink<T>, AsError> {
        let mut conns = self.conns.borrow_mut();
        loop {
            info!("dispatch to addr={}", &addr);
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
                let sender = connect(&addr)?;
                conns.insert(&addr, sender);
            }
        }
    }

    pub fn dispatch_all(&self, cmds: &mut VecDeque<T>) -> Result<usize, AsError> {
        let mut count = 0usize;
        loop {
            if cmds.is_empty() {
                return Ok(count);
            }
            let cmd = cmds.pop_front().expect("cmds pop front never be empty");
            let key_hash = cmd.key_hash(&self.hash_tag, fnv1a64);
            let addr = self.ring.borrow().get_node(key_hash).to_string();
            let mut conns = self.conns.borrow_mut();

            if let Some(sender) = conns.get_mut(&addr).map(|x| x.sender()) {
                match sender.start_send(cmd) {
                    Ok(AsyncSink::Ready) => {
                        count += 1;
                    }
                    Ok(AsyncSink::NotReady(cmd)) => {
                        cmds.push_front(cmd);
                        return Ok(count);
                    }
                    Err(se) => {
                        let cmd = se.into_inner();
                        cmds.push_front(cmd);
                        let sender = connect(&addr)?;
                        conns.insert(&addr, sender);
                        return Ok(count);
                    }
                }
            } else {
                cmds.push_front(cmd);
                let sender = connect(&addr)?;
                conns.insert(&addr, sender);
                return Ok(count);
            }
        }
    }
}

struct Conns<T> {
    _marker: PhantomData<T>,
    inner: HashMap<String, Conn<Sender<T>>>,
}

impl<T> Conns<T> {
    fn get_mut(&mut self, s: &str) -> Option<&mut Conn<Sender<T>>> {
        self.inner.get_mut(s)
    }

    fn insert(&mut self, s: &str, sender: Sender<T>) {
        let conn = Conn {
            addr: s.to_string(),
            sender,
        };
        self.inner.insert(s.to_string(), conn);
    }
}

impl<T> Default for Conns<T> {
    fn default() -> Conns<T> {
        Conns {
            inner: HashMap::new(),
            _marker: Default::default(),
        }
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

impl<S> Drop for Conn<S> {
    fn drop(&mut self) {
        info!("connection to backend {} is disconnected", self.addr);
    }
}

fn connect<T>(node: &str) -> Result<Sender<T>, AsError>
where
    T: Request,
{
    unimplemented!()
}
