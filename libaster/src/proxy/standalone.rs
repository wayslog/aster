use futures::task::Task;
use futures::AsyncSink;
use tokio::codec::{Decoder, Encoder};

use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::com::AsError;
use crate::protocol::IntoReply;

pub mod back;
pub mod fnv;
pub mod front;
pub mod ketama;

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
    _maker: PhantomData<T>,
}

impl<T: Request> Cluster<T> {
    pub fn dispatch_to(&self, addr: &str, cmd: T) -> Result<AsyncSink<T>, AsError> {
        unimplemented!()
    }

    pub fn dispatch_all(&self, cmds: &mut VecDeque<T>) -> Result<usize, AsError> {
        unimplemented!()
    }
}
