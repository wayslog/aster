// #![deny(warnings)]
#![feature(cell_update)]
#![feature(option_flattening)]

#[macro_use(try_ready)]
extern crate futures;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate failure;

pub mod com;
pub mod protocol;
pub mod proxy;
pub(crate) mod utils;

use failure::Error;

pub fn run() -> Result<(), Error> {
    env_logger::init();

    let example = com::ClusterConfig {
        name: "baka".to_string(),
        listen_addr: "127.0.0.1:7788".to_string(),
        hash_tag: Some("[]".to_string()),

        thread: Some(1),
        cache_type: com::CacheType::RedisCluster,

        read_timeout: None,
        write_timeout: None,

        servers: vec!["127.0.0.1:7000".to_string()],

        // cluster special
        fetch_interval: Some(1000 * 60 * 60),
        read_from_slave: None,

        // proxy special
        ping_fail_limit: None,
        ping_interval: None,

        // dead codes

        // command not support now
        dial_timeout: None,
        // dead option: not support other proto
        listen_proto: None,

        // dead option: always 1
        node_connections: None,
    };
    Ok(proxy::cluster::run(example))
}
