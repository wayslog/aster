#![deny(warnings)]

extern crate tokio;
#[macro_use(try_ready)]
extern crate futures;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate bytes;
extern crate num_cpus;
#[macro_use]
extern crate lazy_static;
extern crate btoi;
extern crate crc16;
extern crate itoa;
extern crate net2;
extern crate tokio_codec;
extern crate tokio_io;
#[macro_use]
extern crate serde_derive;
extern crate toml;

mod cluster;
mod cmd;
mod com;
pub mod fetcher;
mod handler;
mod init;
pub mod node;
mod resp;
mod slots;

pub use cluster::Cluster;
use cmd::CmdCodec;
pub use com::*;
// use fetcher::Fetcher;
use handler::Handler;
use init::ClusterInitilizer;
use resp::Resp;
use slots::SlotsMap;

use futures::lazy;
use futures::Async;
// use futures::task::current;
use tokio::executor::current_thread;
use tokio::net::TcpListener;
use tokio::prelude::{Future, Stream};
use tokio_codec::Decoder;

use net2::unix::UnixTcpBuilderExt;
use net2::TcpBuilder;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::thread;

pub fn run() -> Result<(), std::io::Error> {
    env_logger::init();
    let config = load_config();
    info!("asswecan has been lunched with config={:?}", config);
    let ths: Vec<_> = config
        .clusters
        .iter()
        .map(|cc| create_cluster(cc))
        .flatten()
        .collect();

    for th in ths {
        th.join().unwrap();
    }
    Ok(())
}

fn load_config() -> Config {
    use std::env;
    let path = env::var("AS_CFG").unwrap_or("as.toml".to_string());
    use std::fs;
    use std::io::{BufReader, Read};

    let fd = fs::File::open(&path).expect("fail to open config file(default: as.toml)");
    let mut rd = BufReader::new(fd);
    let mut data = String::new();
    rd.read_to_string(&mut data)
        .expect("fail to read config file");

    toml::from_str(&data).expect("fail to parse toml")
}

pub fn create_cluster(cc: &ClusterConfig) -> Vec<thread::JoinHandle<()>> {
    let count = cc.thread;
    (0..count)
        .into_iter()
        .map(|_| {
            let cc = cc.clone();
            thread::spawn(move || {
                let smap = SlotsMap::default();
                let cluster = Cluster {
                    cc: cc,
                    slots: RefCell::new(smap),
                };
                start_cluster(cluster)
            })
        }).collect()
}

pub fn start_cluster(cluster: Cluster) {
    let addr = cluster
        .cc
        .bind
        .clone()
        .parse::<SocketAddr>()
        .expect("parse socket never fail");

    let fut = lazy(move || -> Result<(SocketAddr, Cluster), ()> { Ok((addr, cluster)) })
        .and_then(|(addr, mut cluster)| {
            let listen = create_reuse_port_listener(&addr).expect("bind never fail");
            info!("success listen at {}", &cluster.cc.bind);
            cluster.init_node_conn().unwrap();
            let initilizer = ClusterInitilizer::new(cluster, listen).map_err(|err| {
                error!("fail to init cluster with given server due {:?}", err);
            });
            initilizer
        }).and_then(|(cluster, listen)| {
            // TODO: how to spawn timer func with current_thread
            // let fetcher = Fetcher::new(cluster.clone())
            //     .for_each(|_| {
            //         debug!("success fetch new slots_map");
            //         Ok(())
            //     }).map_err(|err| {
            //         error!("fail to fetch new slots_mapd due {:?}", err);
            //     });
            // current_thread::spawn(fetcher);
            Ok((cluster, listen))
        }).and_then(|(cluster, listen)| {
            let rc_cluster = cluster.clone();
            let amt = listen
                .incoming()
                .for_each(move |sock| {
                    let codec = CmdCodec::default();
                    let (cmd_tx, cmd_rx) = codec.framed(sock).split();
                    let cluster = rc_cluster.clone();
                    let handler = Handler::new(cluster, cmd_rx, cmd_tx).map_err(|err| {
                        error!("fail to create new handler due {:?}", err);
                    });
                    current_thread::spawn(handler);
                    Ok(())
                }).map_err(|err| {
                    error!("fail to start_cluster due {:?}", err);
                });
            current_thread::spawn(amt);
            Ok(())
        });

    current_thread::block_on_all(fut).unwrap();
}

fn create_reuse_port_listener(addr: &SocketAddr) -> Result<TcpListener, std::io::Error> {
    let builder = TcpBuilder::new_v4()?;
    let std_listener = builder
        .reuse_address(true)
        .expect("os not support SO_REUSEADDR")
        .reuse_port(true)
        .expect("os not support SO_REUSEPORT")
        .bind(addr)?
        .listen(std::i32::MAX)?;
    let hd = tokio::reactor::Handle::current();
    TcpListener::from_std(std_listener, &hd)
}

#[derive(Deserialize, Debug)]
pub struct Config {
    clusters: Vec<ClusterConfig>,
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub enum CacheType {
    Redis,
    Memcache,
    MemcacheBinary,
    RedisCluster,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub bind: String,
    pub cache_type: CacheType,
    pub servers: Vec<String>,
    pub thread: usize,
    pub fetch: u64,
}

pub struct Batch<S>
where
    S: Stream,
{
    input: S,
    max: usize,
}

impl<S> Stream for Batch<S>
where
    S: Stream,
{
    type Item = VecDeque<S::Item>;
    type Error = S::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let mut buf = VecDeque::new();
        loop {
            match self.input.poll() {
                Ok(Async::NotReady) => {
                    if buf.is_empty() {
                        return Ok(Async::NotReady);
                    }
                    return Ok(Async::Ready(Some(buf)));
                }
                Ok(Async::Ready(None)) => {
                    return Ok(Async::Ready(None));
                }
                Ok(Async::Ready(Some(item))) => {
                    buf.push_back(item);
                    if buf.len() == self.max {
                        return Ok(Async::Ready(Some(buf)));
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }
}
