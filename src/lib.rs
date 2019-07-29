// #![deny(warnings)]
#[macro_use(try_ready)]
extern crate futures;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use crate::cluster::{start_cluster, Cluster};
pub use crate::com::*;
use std::thread;
use std::thread::JoinHandle;

mod cluster;
mod com;
mod crc;
mod mc;
mod mcbin;
mod notify;
pub mod stringview;

pub mod proxy;
pub mod redis;

const EXIT_DUE_CONFIG: i32 = 1;

const ASTER_VERSION_MAJOR: i32 = 0;
const ASTER_VERSION_MINOR: i32 = 1;
const ASTER_VERSION_PATCH: i32 = 5;

pub fn run() -> Result<(), std::io::Error> {
    env_logger::init();
    let config = load_config_or_print_help();
    info!("aster has shined with config={:?}", config);
    let ths: Vec<JoinHandle<()>> = config
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

fn load_config_or_print_help() -> Config {
    match load_config() {
        Ok(cfg) => return cfg,
        Err(err) => {
            eprintln!("fail to load config due {:?}", err);
        }
    }

    print_help();
    std::process::exit(EXIT_DUE_CONFIG);
}

fn print_help() {
    eprintln!(
        r#"

aster-proxy({}.{}.{}): A fast and lightweight cache (a.k.a: memcache,redis,redis-cluster) proxy.

  basic usage: AS_CFG=as.toml path/to/binary/aster-proxy

"#,
        ASTER_VERSION_MAJOR, ASTER_VERSION_MINOR, ASTER_VERSION_PATCH
    );
}

fn load_config() -> Result<Config, Error> {
    use std::env;
    let path = env::var("AS_CFG").unwrap_or_else(|_| "as.toml".to_string());
    use std::fs;
    use std::io::{BufReader, Read};

    let fd = fs::File::open(&path)?;
    let mut rd = BufReader::new(fd);
    let mut data = String::new();
    rd.read_to_string(&mut data)
        .expect("fail to read config file");

    Ok(toml::from_str(&data).unwrap())
}

pub fn create_cluster(cc: &ClusterConfig) -> Vec<thread::JoinHandle<()>> {
    let count = cc.thread.unwrap_or_else(num_cpus::get).max(1);

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
                CacheType::MemcacheBinary => {
                    let p = proxy::Proxy::new(cc).unwrap();
                    proxy::start_proxy::<mcbin::Req>(p);
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
    pub ping_fail_limit: Option<u64>,
    pub ping_interval: Option<u64>,
    pub server_retry_timeout: Option<u64>,

    // dead codes

    // command not support now
    pub dial_timeout: Option<u64>,
    // dead option: not support other proto
    pub listen_proto: Option<String>,

    // dead option: always 1
    pub node_connections: Option<usize>,
}
