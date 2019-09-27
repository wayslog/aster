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

use clap::App;

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
    for th in ths {
        th.join().unwrap();
    }
    Ok(())
}
