#![deny(warnings)]
#![feature(test)]

extern crate test;
extern crate tokio;
#[macro_use(try_ready)]
extern crate futures;
extern crate env_logger;
#[macro_use]
extern crate log;
extern crate bytes;
#[macro_use]
extern crate lazy_static;
extern crate btoi;
extern crate itoa;
extern crate net2;
extern crate num_cpus;
extern crate tokio_codec;
#[macro_use]
extern crate serde_derive;
extern crate hashbrown;
extern crate md5;
extern crate toml;

mod cluster;
mod com;
mod crc;
mod mc;
mod notify;
mod proxy;
mod redis;

use cluster::{start_cluster, Cluster};
pub use com::*;
// use futures::task::current;
use std::thread;

pub fn run() -> Result<(), std::io::Error> {
    env_logger::init();
    let config = load_config();
    info!("aster has shined with config={:?}", config);
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
    let path = env::var("AS_CFG").unwrap_or_else(|_| "as.toml".to_string());
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
    let count = if let Some(&thread) = cc.thread.as_ref() {
        usize::max(thread, 1)
    } else {
        num_cpus::get()
    };
    info!(
        "aster start {} listen at {} with {} thread",
        &cc.name, &cc.listen_addr, count
    );
    (0..count)
        .map(|i| {
            let cc = cc.clone();
            let name = cc.name.clone();
            let thb = thread::Builder::new().name(format!("cluster-{}-{}", name, i + 1));
            thb.spawn(move || match cc.cache_type {
                CacheType::Memcache => {
                    let p = proxy::Proxy::new(cc).unwrap();
                    proxy::start_proxy::<mc::Req>(p);
                }
                CacheType::RedisCluster => {
                    let cluster = Cluster::new(cc);
                    start_cluster(cluster)
                }
                CacheType::Redis => {
                    let p = proxy::Proxy::new(cc).unwrap();
                    proxy::start_proxy::<redis::cmd::Cmd>(p);
                }
                _ => {
                    warn!("cache type is not supported");
                }
            })
            .unwrap()
        })
        .collect()
}
#[derive(Deserialize, Debug)]
pub struct Config {
    clusters: Vec<ClusterConfig>,
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub enum CacheType {
    #[serde(rename = "redis")]
    Redis,
    #[serde(rename = "memcache")]
    Memcache,
    #[serde(rename = "memcache_binary")]
    MemcacheBinary,
    #[serde(rename = "redis_cluster")]
    RedisCluster,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub listen_addr: String,
    pub hash_tag: Option<String>,

    pub thread: Option<usize>,
    pub cache_type: CacheType,

    pub read_timeout: Option<u64>,
    pub write_timeout: Option<u64>,

    pub servers: Vec<String>,

    // cluster special
    pub fetch: Option<u64>,
    pub read_from_slave: Option<bool>,

    // proxy special
    pub ping_fail_limit: Option<usize>,
    pub ping_interval: Option<usize>,

    // dead codes

    // command not support now
    pub dial_timeout: Option<u64>,
    // dead option: not support other proto
    pub listen_proto: Option<String>,

    // dead option: always 1
    pub node_connections: Option<usize>,
}
