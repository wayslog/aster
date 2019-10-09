// #![deny(warnings)]
#![feature(cell_update)]
#![feature(option_flattening)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate clap;

#[cfg(feature = "metrics")]
#[macro_use]
extern crate prometheus;
#[cfg(feature = "metrics")]
pub mod metrics;

use clap::App;

pub const ASTER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod com;
pub mod protocol;
pub mod proxy;
pub(crate) mod utils;

use failure::Error;

pub fn run() -> Result<(), Error> {
    env_logger::init();
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();
    let config = matches.value_of("config").unwrap_or("default.toml");
    info!("[aster] loading config from {}", config);
    let cfg = com::Config::load(&config)?;
    debug!("use config : {:?}", cfg);

    let mut ths = Vec::new();
    for cluster in cfg.clusters.clone().into_iter() {
        info!(
            "starting aster cluster {} in addr {}",
            cluster.name, cluster.listen_addr
        );

        match cluster.cache_type {
            com::CacheType::RedisCluster => {
                let jhs = proxy::cluster::run(cluster);
                ths.extend(jhs);
            }
            _ => {
                let jhs = proxy::standalone::run(cluster);
                ths.extend(jhs);
            }
        }
    }

    #[cfg(feature = "metrics")]
    {
        let port_str = matches.value_of("metrics").unwrap_or("2110");
        let port = port_str.parse::<usize>().unwrap_or(2110);
        spwan_metrics(port);
    }

    for th in ths {
        th.join().unwrap();
    }
    Ok(())
}

#[cfg(feature = "metrics")]
use std::thread;
#[cfg(feature = "metrics")]
fn spwan_metrics(port: usize) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("aster-metrics-thread".to_string())
        .spawn(move || metrics::init(port).unwrap())
        .unwrap()
}
