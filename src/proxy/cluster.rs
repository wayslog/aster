use crate::com::ClusterConfig;

use futures::task::Task;
use futures::AsyncSink;
use futures::{Sink, Stream};

use failure::Error;

use std::collections::VecDeque;
// use std::borrow::Borrow;
// use std::net::ToSocketAddrs;

pub struct Cluster<T, R, E>
where
    T: Request + 'static,
    R: Router<End = E>,
    E: Endpoint<Req = T>,
{
    pub cfg: ClusterConfig,
    pub router: R,
}

pub trait Request: Sized + Clone {
    type Reply: Clone;

    fn key_hash<H>(&self, hash: H) -> u64
    where
        H: Fn(&[u8]) -> u64;

    fn reregister(&mut self, task: Task);
    fn subs(&self) -> Vec<Self>;
    fn is_done(&self) -> bool;
    fn is_valid(&self) -> bool;
    fn done(&self, reply: Self::Reply);
    fn done_error(&self, err: Error);
}

pub trait ToRoutine {
    fn to_routine(&self) -> Result<Routine, Error>;
}

pub enum Routine {
    Slots {
        addr: String,
        slots: Vec<u64>,
    },
    Weight {
        addr: String,
        alias: String,
        weight: usize,
    },
}

/// [Router] is the trait which must controls connections.
pub trait Router {
    type End: Endpoint;

    fn get(&self, hash: u64) -> Option<Self::End>;
    fn get_by_addr(&self, hash: u64) -> Option<Self::End>;

    fn add<R: ToRoutine>(&self, r: R, e: Self::End) -> Result<(), Error>;
    fn del<R: ToRoutine>(&self, r: R) -> Result<(), Error>;
    fn swap<R: ToRoutine>(&self, r: R, e: Self::End) -> Result<(), Error>;
}

/// [Endpoint] is the trait which receive request and manage connection.
pub trait Endpoint {
    type Req: Request;

    fn addr(&self) -> &str;
    fn execute(&self, req: Self::Req) -> Result<AsyncSink<Self::Req>, Error>;
}


pub struct RcEndpoint<D, T, R>
where
    D: Sink<SinkItem = T>,
    T: Request,
    R: Stream<Item = T::Reply, Error = Error>,
{
    addr: String,
    closed: bool,
    buffer: VecDeque<R>,
    down: D,
    recv: R,
}
