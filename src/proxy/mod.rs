//! proxy is the mod which contains genneral proxy
use com::*;
use create_reuse_port_listener;
use fnv::Fnv1a64;
use ketama::HashRing;
use ClusterConfig;

use futures::lazy;
use futures::task::{self, Task};
use futures::unsync::mpsc::{channel, Sender};
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use tokio::net::TcpStream;
use tokio::runtime::current_thread;
use tokio_codec::{Decoder, Encoder};

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::rc::Rc;

const MAX_CONCURRENCY: usize = 1024;

pub fn start_proxy<T: Request + 'static>(proxy: Proxy<T>) {
    let addr = proxy
        .cc
        .bind
        .clone()
        .parse::<SocketAddr>()
        .expect("parse socket never fail");

    let fut = lazy(move || -> Result<(SocketAddr, Proxy<T>), ()> { Ok((addr, proxy)) })
        .and_then(|(addr, proxy)| {
            let listen = create_reuse_port_listener(&addr).expect("bind never fail");
            Ok((Rc::new(proxy), listen))
        })
        .and_then(|(proxy, listen)| {
            let rc_proxy = proxy.clone();
            let amt = listen
                .incoming()
                .for_each(move |sock| {
                    let codec = T::handle_codec();
                    let (req_tx, req_rx) = codec.framed(sock).split();
                    let (handle_tx, handle_rx) = channel(2048);
                    let input = HandleInput {
                        sink: handle_tx,
                        stream: req_rx,
                        buffered: None,
                        count: 0,
                    };
                    current_thread::spawn(input);

                    let proxy = rc_proxy.clone();
                    let handle = Handle::new(
                        proxy,
                        handle_rx.map_err(|_err| {
                            error!("fail to recv from upstream handle rx");
                            Error::Critical
                        }),
                        req_tx,
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

#[allow(unused)]
struct ServerLine {
    addr: String,
    weight: usize,
    alias: Option<String>,
}

impl ServerLine {
    fn parse_servers(servers: &[String]) -> AsResult<Vec<ServerLine>> {
        // e.g.: 192.168.1.2:1074:10 redis-20
        let mut sl = Vec::with_capacity(servers.len());
        for server in servers {
            let mut iter = server.split(" ");
            let first_part = iter.next().expect("first partation must exists");
            if first_part.chars().filter(|x| *x == ':').count() == 1 {
                let alias = iter.next().map(|x| x.to_string());
                sl.push(ServerLine {
                    addr: first_part.to_string(),
                    weight: 1,
                    alias: alias,
                });
            }

            let mut fp_sp = first_part.rsplitn(2, ':').filter(|x| !x.is_empty());
            let weight = {
                let weight_str = fp_sp.next().unwrap_or("1");
                weight_str.parse::<usize>()?
            };
            let addr = fp_sp.next().expect("addr never be absent").to_owned();
            drop(fp_sp);
            let alias = iter.next().map(|x| x.to_string());
            sl.push(ServerLine {
                addr: addr,
                weight: weight,
                alias: alias,
            });
        }
        Ok(sl)
    }

    fn unwrap_spot(sls: &Vec<ServerLine>) -> (Vec<String>, Vec<String>, Vec<usize>) {
        let mut nodes = Vec::new();
        let mut alias = Vec::new();
        let mut weights = Vec::new();
        for sl in sls {
            if sl.alias.is_some() {
                alias.push(
                    sl.alias
                        .as_ref()
                        .cloned()
                        .expect("node addr can't be empty"),
                );
            }
            nodes.push(sl.addr.clone());
            weights.push(sl.weight);
        }
        (nodes, alias, weights)
    }
}

pub struct Proxy<T: Request> {
    pub cc: ClusterConfig,
    ring: RefCell<HashRing<Fnv1a64>>,
    conns: RefCell<HashMap<String, Sender<T>>>,
    is_alias: bool,
    alias: RefCell<HashMap<String, String>>,
}

impl<T: Request + 'static> Proxy<T> {
    pub fn new(cc: ClusterConfig) -> AsResult<Proxy<T>> {
        let sls = ServerLine::parse_servers(&cc.servers)?;
        let (addrs, alias, weights) = ServerLine::unwrap_spot(&sls);
        let ring = if alias.is_empty() {
            HashRing::<Fnv1a64>::new(addrs.clone(), weights)?
        } else {
            HashRing::<Fnv1a64>::new(alias.clone(), weights)?
        };
        let alias_map: HashMap<_, _> = alias
            .iter()
            .enumerate()
            .map(|(index, alias_name)| (alias_name.clone(), addrs[index].clone()))
            .collect();

        Ok(Proxy {
            cc: cc,
            ring: RefCell::new(ring),
            conns: RefCell::new(HashMap::new()),
            is_alias: !alias_map.is_empty(),
            alias: RefCell::new(alias_map),
        })
    }

    fn create_conn(node: String) -> AsResult<Sender<T>> {
        let node_addr = node.clone();
        let (tx, rx) = channel(10234 * 8);
        let ret_tx = tx.clone();
        let amt = lazy(|| -> Result<(), ()> { Ok(()) })
            .and_then(move |_| {
                node_addr
                    .as_str()
                    .parse()
                    .map_err(|err| error!("fail to parse addr {:?}", err))
            })
            .and_then(|addr| {
                TcpStream::connect(&addr).map_err(|err| error!("fail to connect {:?}", err))
            })
            .and_then(|sock| {
                let codec = <T as Request>::node_codec();
                let (sink, stream) = codec.framed(sock).split();
                let arx = rx.map_err(|err| {
                    info!("fail to send due to {:?}", err);
                    Error::Critical
                });
                let buf = Rc::new(RefCell::new(VecDeque::new()));
                let nd = NodeDown {
                    closed: false,
                    input: arx,
                    output: sink,
                    store: VecDeque::with_capacity(4),
                    buf: buf.clone(),
                    count: 0,
                };
                current_thread::spawn(nd);
                let nr = NodeRecv {
                    closed: false,
                    recv: stream,
                    buf: buf.clone(),
                };
                current_thread::spawn(nr);
                Ok(())
            });
        current_thread::spawn(amt);
        Ok(ret_tx)
    }

    fn dispatch_all(&self, cmds: &mut VecDeque<T>) -> Result<AsyncSink<()>, Error> {
        let mut nodes = HashSet::new();
        loop {
            if let Some(req) = cmds.front().cloned() {
                let name = self.ring.borrow().get_node(req.key());
                let node = if self.is_alias {
                    self.alias
                        .borrow()
                        .get(&name)
                        .expect("alias get never be empty")
                        .to_string()
                } else {
                    name
                };

                let mut conns = self.conns.borrow_mut();
                {
                    if let Some(conn) = conns.get_mut(&node) {
                        match conn.start_send(req).map_err(|err| {
                            error!("fail to dispatch to backend due to {:?}", err);
                            Error::Critical
                        })? {
                            AsyncSink::Ready => {
                                let _ = cmds.pop_front().expect("pop_front never be empty");
                                nodes.insert(node.clone());
                            }
                            AsyncSink::NotReady(_) => {
                                break;
                            }
                        };
                        continue;
                    }
                }

                let conn = Self::create_conn(node.clone())?;
                conns.insert(node, conn);
            } else {
                return Ok(AsyncSink::Ready);
            }
        }

        if !nodes.is_empty() {
            for node in nodes.into_iter() {
                self.conns
                    .borrow_mut()
                    .get_mut(&node)
                    .expect("node connection is never be absent")
                    .poll_complete()
                    .map_err(|err| {
                        error!("fail to complete to proxy to backend due to {:?}", err);
                        Error::Critical
                    })?;
            }
        }

        Ok(AsyncSink::Ready)
    }
}

pub trait Request: Sized + Clone + Debug {
    type Reply: Clone + Debug;
    type HandleCodec: Decoder<Item = Self, Error = Error> + Encoder<Item = Self, Error = Error>;
    type NodeCodec: Decoder<Item = Self::Reply, Error = Error> + Encoder<Item = Self, Error = Error>;

    fn reregister(&self, task: Task);
    fn handle_codec() -> Self::HandleCodec;
    fn node_codec() -> Self::NodeCodec;

    fn key(&self) -> Vec<u8>;
    fn subs(&self) -> Option<Vec<Self>>;
    fn is_done(&self) -> bool;

    fn valid(&self) -> bool;
    fn done(&self, data: Self::Reply);
    fn done_with_error(&self, err: Error);
}

pub struct HandleInput<T, U, D>
where
    T: Stream<Error = D>,
    U: Sink<SinkItem = T::Item>,
    D: Debug,
{
    sink: U,
    stream: T,
    buffered: Option<T::Item>,
    count: usize,
}

impl<T, U, D> HandleInput<T, U, D>
where
    T: Stream<Error = D>,
    U: Sink<SinkItem = T::Item>,
    D: Debug,
{
    fn try_start_send(&mut self, item: T::Item) -> Poll<(), U::SinkError> {
        debug_assert!(self.buffered.is_none());
        if let AsyncSink::NotReady(item) = self.sink.start_send(item)? {
            self.buffered = Some(item);
            return Ok(Async::NotReady);
        }
        Ok(Async::Ready(()))
    }
}

impl<T, U, D> Future for HandleInput<T, U, D>
where
    T: Stream<Error = D>,
    U: Sink<SinkItem = T::Item>,
    D: Debug,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            if let Some(item) = self.buffered.take() {
                match self
                    .try_start_send(item)
                    .map_err(|_err| error!("fail to send to back end"))?
                {
                    Async::NotReady => break,
                    Async::Ready(_) => {
                        self.count += 1;
                        continue;
                    }
                }
            }

            match self
                .stream
                .poll()
                .map_err(|err| error!("fail to poll from upstream {:?}", err))?
            {
                Async::Ready(Some(item)) => {
                    self.buffered = Some(item);
                }
                Async::Ready(None) => {
                    try_ready!(
                        self.sink
                            .close()
                            .map_err(|_err| error!("fail to close handle tx"))
                    );
                    return Ok(Async::Ready(()));
                }
                Async::NotReady => {
                    break;
                }
            }
        }

        if self.count > 0 {
            try_ready!(self.sink.poll_complete().map_err(|_err| {
                error!("fail to poll_complete to back end");
            }));
            self.count = 0;
        }
        Ok(Async::NotReady)
    }
}

pub struct Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
    D: Into<T>,
{
    proxy: Rc<Proxy<T>>,
    input: I,
    output: O,
    cmds: VecDeque<T>,
    count: usize,
    waitq: VecDeque<T>,
}

impl<T, I, O, D> Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request + 'static,
    D: Into<T>,
{
    fn new(proxy: Rc<Proxy<T>>, input: I, output: O) -> Handle<T, I, O, D> {
        Handle {
            proxy: proxy,
            input: input,
            output: output,
            cmds: VecDeque::new(),
            count: 0,
            waitq: VecDeque::new(),
        }
    }

    fn try_send(&mut self) -> Result<Async<()>, Error> {
        loop {
            if self.waitq.is_empty() {
                return Ok(Async::NotReady);
            }

            if let AsyncSink::NotReady(()) = self.proxy.dispatch_all(&mut self.waitq)? {
                return Ok(Async::NotReady);
            }
        }
    }

    fn try_write(&mut self) -> Result<Async<()>, Error> {
        let ret: Result<Async<()>, Error> = Ok(Async::NotReady);
        loop {
            if self.cmds.is_empty() {
                break;
            }

            let rc_req = self.cmds.front().cloned().expect("cmds is never be None");
            if !rc_req.is_done() {
                break;
            }

            match self.output.start_send(rc_req)? {
                AsyncSink::NotReady(_) => {
                    break;
                }
                AsyncSink::Ready => {
                    let _ = self.cmds.pop_front().unwrap();
                    self.count += 1;
                }
            }
        }

        if self.count > 0 {
            try_ready!(self.output.poll_complete());
            self.count = 0;
        }

        ret
    }
}

impl<T, I, O, D> Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
    D: Into<T>,
{
    fn try_read(&mut self) -> Result<Async<Option<()>>, Error> {
        loop {
            if self.cmds.len() > MAX_CONCURRENCY {
                return Ok(Async::NotReady);
            }

            match try_ready!(self.input.poll()) {
                Some(val) => {
                    let req: T = Into::into(val);
                    req.reregister(task::current());
                    self.cmds.push_back(req.clone());
                    if !req.valid() {
                        continue;
                    }

                    if let Some(subs) = req.subs() {
                        for sub in subs.into_iter() {
                            self.waitq.push_back(sub);
                        }
                    } else {
                        self.waitq.push_back(req);
                    }
                }
                None => {
                    return Ok(Async::Ready(None));
                }
            }
        }
    }
}

impl<T, I, O, D> Future for Handle<T, I, O, D>
where
    I: Stream<Item = D, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request + 'static,
    D: Into<T>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let mut can_read = true;
        let mut can_send = true;
        let mut can_write = true;

        loop {
            if !(can_read && can_send && can_write) {
                return Ok(Async::NotReady);
            }

            // step 1: poll read from input stream.
            if can_read {
                // read until the input stream is NotReady.
                match self.try_read()? {
                    Async::NotReady => {
                        can_read = false;
                    }
                    Async::Ready(None) => {
                        return Ok(Async::Ready(()));
                    }
                    Async::Ready(Some(())) => {}
                }
            }

            // step 2: send to proxy.
            if can_send {
                // send until the output stream is unsendable.
                match self.try_send()? {
                    Async::NotReady => {
                        can_send = false;
                    }
                    Async::Ready(_) => {}
                }
            }

            // step 3: wait all the proxy is done.
            if can_write {
                // step 4: poll send back to client.
                match self.try_write()? {
                    Async::NotReady => {
                        can_write = false;
                    }
                    Async::Ready(_) => {}
                }
            }
        }
    }
}

pub struct NodeDown<T, I, O>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
{
    closed: bool,
    input: I,
    output: O,
    store: VecDeque<T>,
    buf: Rc<RefCell<VecDeque<T>>>,
    count: usize,
}

impl<T, I, O> NodeDown<T, I, O>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
{
    fn try_forword(&mut self) -> AsResult<()> {
        loop {
            if !self.store.is_empty() {
                let req = self
                    .store
                    .front()
                    .cloned()
                    .expect("node down store is never be empty");
                match self.output.start_send(req)? {
                    AsyncSink::NotReady(_) => {
                        return Ok(());
                    }
                    AsyncSink::Ready => {
                        let req = self
                            .store
                            .pop_front()
                            .expect("try_forward store never be empty");
                        self.buf.borrow_mut().push_back(req);
                        self.count += 1;
                        continue;
                    }
                }
            }

            match self.input.poll()? {
                Async::Ready(Some(v)) => {
                    self.store.push_back(v);
                }

                Async::Ready(None) => {
                    self.closed = true;
                    return Ok(());
                }

                Async::NotReady => {
                    return Ok(());
                }
            }
        }
    }
}

impl<T, I, O> Future for NodeDown<T, I, O>
where
    I: Stream<Item = T, Error = Error>,
    O: Sink<SinkItem = T, SinkError = Error>,
    T: Request,
{
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.closed {
            return Ok(Async::Ready(()));
        }

        self.try_forword()
            .map_err(|err| error!("fail to forward due to {:?}", err))?;

        if self.count > 0 {
            try_ready!(self.output.poll_complete().map_err(|err| {
                error!("fail to flush into backend due to {:?}", err);
                self.closed = true;
            }));
            self.count = 0;
        }
        Ok(Async::NotReady)
    }
}

pub struct NodeRecv<T, S>
where
    S: Stream<Item = T::Reply, Error = Error>,
    T: Request,
{
    closed: bool,
    recv: S,
    buf: Rc<RefCell<VecDeque<T>>>,
}

impl<T, S> Future for NodeRecv<T, S>
where
    S: Stream<Item = T::Reply, Error = Error>,
    T: Request,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.closed {
            return Ok(Async::Ready(()));
        }

        loop {
            if let Some(reply) = try_ready!(self.recv.poll().map_err(|err| {
                error!("fail to recv from back end, may closed due to {:?}", err);
                self.closed = true;
            })) {
                let req = self.buf.borrow_mut().pop_front().unwrap();
                req.done(reply);
            } else {
                error!("TODO: should quick error for");
                self.closed = true;
                return Ok(Async::Ready(()));
            }
        }
    }
}
