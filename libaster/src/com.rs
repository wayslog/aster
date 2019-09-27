use net2::TcpBuilder;
use std::net::SocketAddr;
use tokio::net::TcpListener;

pub use failure::Error;

use std::fs::File;
use std::io::Read;
use std::num;
use std::path::Path;

#[derive(Debug, Fail)]
pub enum AsError {
    #[fail(display = "config is bad for fields {}", _0)]
    BadConfig(String),
    #[fail(display = "fail to parse int in config")]
    StrParseIntError(num::ParseIntError),

    #[fail(display = "invalid message")]
    BadMessage,

    #[fail(display = "message is ok but request bad or not allowed")]
    BadReqeust,

    #[fail(display = "request not spport")]
    RequestNotSupport,

    #[fail(display = "message reply is bad")]
    BadReply,

    #[fail(display = "proxy fail")]
    ProxyFail,

    #[fail(display = "fail due retry send too much")]
    RequestReachMaxCycle,

    #[fail(display = "fail to parse integer {}", _0)]
    ParseIntError(btoi::ParseIntegerError),

    #[fail(display = "CLUSTER SLOTS must be replied with array")]
    WrongClusterSlotsReplyType,

    #[fail(display = "CLUSTER SLOTS must contains slot info")]
    WrongClusterSlotsReplySlot,

    #[fail(display = "cluster fail to dispatch command")]
    ClusterFailDispatch,

    #[fail(display = "unexcept io error {}", _0)]
    IoError(tokio::io::Error),

    #[fail(display = "remote connection has active close connection {}", _0)]
    BackendClosedError(String),

    #[fail(display = "fail to redirect command")]
    RedirectFailError,

    #[fail(display = "fail to init cluster {} due to all seed nodes is die", _0)]
    ClusterAllSeedsDie(String),

    #[fail(display = "fail to load config toml error {}", _0)]
    ConfigError(toml::de::Error),

    #[fail(display = "there is nothing happend")]
    None,
}

impl From<tokio::io::Error> for AsError {
    fn from(oe: tokio::io::Error) -> AsError {
        AsError::IoError(oe)
    }
}

impl From<btoi::ParseIntegerError> for AsError {
    fn from(oe: btoi::ParseIntegerError) -> AsError {
        AsError::ParseIntError(oe)
    }
}

impl From<toml::de::Error> for AsError {
    fn from(oe: toml::de::Error) -> AsError {
        AsError::ConfigError(oe)
    }
}

impl From<num::ParseIntError> for AsError {
    fn from(oe: num::ParseIntError) -> AsError {
        AsError::StrParseIntError(oe)
    }
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub clusters: Vec<ClusterConfig>,
}

impl Config {
    #[cfg(not(test))]
    pub fn load<P: AsRef<Path>>(p: P) -> Result<Config, AsError> {
        let path = p.as_ref();
        let mut data = String::new();
        let mut fd = File::open(path)?;
        fd.read_to_string(&mut data)?;
        let cfg = toml::from_str(&data)?;
        Ok(cfg)
    }

    // for test default load
    #[cfg(test)]
    pub fn load<P: AsRef<Path>>(p: P) -> Result<Config, AsError> {
        let path = p.as_ref();
        let mut data = String::new();
        let mut fd = File::open(path)?;
        fd.read_to_string(&mut data)?;
        let cfg = toml::from_str(&data)?;
        Ok(cfg)
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
    pub fetch_interval: Option<u64>,
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

#[cfg(windows)]
pub(crate) fn create_reuse_port_listener(addr: &SocketAddr) -> Result<TcpListener, std::io::Error> {
    let builder = TcpBuilder::new_v4()?;
    let std_listener = builder
        .reuse_address(true)
        .expect("os not support SO_REUSEADDR")
        .bind(addr)?
        .listen(std::i32::MAX)?;
    let hd = tokio::reactor::Handle::current();
    TcpListener::from_std(std_listener, &hd)
}

#[cfg(not(windows))]
pub(crate) fn create_reuse_port_listener(addr: &SocketAddr) -> Result<TcpListener, std::io::Error> {
    use net2::unix::UnixTcpBuilderExt;

    let builder = TcpBuilder::new_v4()?;
    let std_listener = builder
        .reuse_address(true)
        .expect("os not support SO_REUSEADDR")
        .reuse_port(true)
        .expect("os not support SO_REUSEPORT")
        .bind(addr)?
        .listen(std::i32::MAX)?;
    let hd = tokio::reactor::Handle::default();
    TcpListener::from_std(std_listener, &hd)
}
