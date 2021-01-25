use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::Path;

use crate::com::{AsError, DEFAULT_FETCH_INTERVAL_MS, ENV_ASTER_DEFAULT_THREADS};

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default)]
    pub clusters: Vec<ClusterConfig>,
}

impl Config {
    pub fn cluster(&self, name: &str) -> Option<ClusterConfig> {
        for cluster in &self.clusters {
            if cluster.name == name {
                return Some(cluster.clone());
            }
        }
        None
    }

    fn servers_map(&self) -> BTreeMap<String, BTreeSet<String>> {
        self.clusters
            .iter()
            .map(|x| {
                (
                    x.name.clone(),
                    x.servers.iter().map(|x| x.to_string()).collect(),
                )
            })
            .collect()
    }

    pub fn reload_equals(&self, other: &Config) -> bool {
        let equals_map = self.servers_map();
        let others_map = other.servers_map();
        equals_map == others_map
    }

    pub fn valid(&self) -> Result<(), AsError> {
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(p: P) -> Result<Config, AsError> {
        let path = p.as_ref();
        let data = fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&data)?;
        let thread = Config::load_thread_from_env();
        for cluster in &mut cfg.clusters[..] {
            if cluster.thread.is_none() {
                cluster.thread = Some(thread);
            }
        }
        Ok(cfg)
    }

    fn load_thread_from_env() -> usize {
        let thread_str = env::var(ENV_ASTER_DEFAULT_THREADS).unwrap_or_else(|_| "4".to_string());
        thread_str.parse::<usize>().unwrap_or_else(|_| 4)
    }
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

impl Default for CacheType {
    fn default() -> CacheType {
        CacheType::RedisCluster
    }
}

#[derive(Clone, Debug, Deserialize, Default)]
pub struct ClusterConfig {
    pub name: String,
    pub listen_addr: String,
    pub hash_tag: Option<String>,

    pub thread: Option<usize>,
    pub cache_type: CacheType,

    pub read_timeout: Option<u64>,
    pub write_timeout: Option<u64>,

    #[serde(default)]
    pub servers: Vec<String>,

    // cluster special
    pub fetch_interval: Option<u64>,
    pub read_from_slave: Option<bool>,

    // proxy special
    pub ping_fail_limit: Option<u8>,
    pub ping_interval: Option<u64>,
    pub ping_succ_interval: Option<u64>,

    // dead codes

    // command not support now
    pub dial_timeout: Option<u64>,
    // dead option: not support other proto
    pub listen_proto: Option<String>,

    // dead option: always 1
    pub node_connections: Option<usize>,
}

impl ClusterConfig {
    pub(crate) fn hash_tag_bytes(&self) -> Vec<u8> {
        self.hash_tag
            .as_ref()
            .map(|x| x.as_bytes().to_vec())
            .unwrap_or_else(|| vec![])
    }

    pub(crate) fn fetch_interval_ms(&self) -> u64 {
        self.fetch_interval.unwrap_or(DEFAULT_FETCH_INTERVAL_MS)
    }
}
