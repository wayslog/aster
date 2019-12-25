// #![deny(warnings)]
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
    let ip = matches.value_of("ip").map(|x| x.to_string());
    info!("[aster] loading config from {}", config);
    let cfg = com::Config::load(&config)?;
    debug!("use config : {:?}", cfg);
    assert!(
        !cfg.clusters.is_empty(),
        "clusters is absent of config file"
    );

    let mut ths = Vec::new();
    for cluster in cfg.clusters.clone().into_iter() {
        if cluster.servers.is_empty() {
            warn!(
                "fail to running cluster {} in addr {} due filed `servers` is empty",
                cluster.name, cluster.listen_addr
            );
            continue;
        }

        if cluster.name.is_empty() {
            warn!(
                "fail to running cluster {} in addr {} due filed `name` is empty",
                cluster.name, cluster.listen_addr
            );
            continue;
        }

        info!(
            "starting aster cluster {} in addr {}",
            cluster.name, cluster.listen_addr
        );

        match cluster.cache_type {
            com::CacheType::RedisCluster => {
                let jhs = proxy::cluster::run(cluster, ip.clone());
                ths.extend(jhs);
            }
            _ => {
                let jhs = proxy::standalone::run(cluster, ip.clone());
                ths.extend(jhs);
            }
        }
    }

    #[cfg(feature = "metrics")]
    {
        let port_str = matches.value_of("metrics").unwrap_or("2110");
        let port = port_str.parse::<usize>().unwrap_or(2110);
        spawn_metrics(port);
    }

    for th in ths {
        th.join().unwrap();
    }
    Ok(())
}

#[cfg(feature = "metrics")]
use std::thread;
#[cfg(feature = "metrics")]
fn spawn_metrics(port: usize) -> Vec<thread::JoinHandle<()>> {
    vec![
        thread::Builder::new()
            .name("http-service".to_string())
            .spawn(move || metrics::init(port).unwrap())
            .unwrap(),
        thread::Builder::new()
            .name("measure-service".to_string())
            .spawn(move || metrics::measure_system().unwrap())
            .unwrap(),
    ]
}
