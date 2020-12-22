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

#[macro_use]
extern crate prometheus;

pub mod metrics;

use clap::App;

pub const ASTER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod com;
pub mod protocol;
pub mod proxy;
pub(crate) mod utils;

use std::thread::{self, Builder, JoinHandle};
use std::time::Duration;
use std::io::Write;

use failure::Error;
use chrono::Local;

use com::meta::{load_meta, meta_init};
use com::ClusterConfig;
use metrics::thread_incr;

pub fn run() -> Result<(), Error> {
    env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info");
    env_logger::builder()
        .format(|buf, r| {
            writeln!(
                buf,
                "{} {} [{}:{}] {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                r.level(),
                r.module_path().unwrap_or("<unnamed>"),
                r.line().unwrap_or(0),
                r.args(),
            )
        })
        .init();

    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).version(ASTER_VERSION).get_matches();
    let config = matches.value_of("config").unwrap_or("default.toml");
    let watch_file = config.to_string();
    let ip = matches.value_of("ip").map(|x| x.to_string());
    let enable_reload = matches.is_present("reload");
    info!("[aster-{}] loading config from {}", ASTER_VERSION, config);
    let cfg = com::Config::load(&config)?;
    debug!("use config : {:?}", cfg);
    assert!(
        !cfg.clusters.is_empty(),
        "clusters is absent of config file"
    );
    crate::proxy::standalone::reload::init(&watch_file, cfg.clone(), enable_reload)?;

    let mut ths = Vec::new();
    for cluster in cfg.clusters.into_iter() {
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
                let jhs = spawn_worker(&cluster, ip.clone(), proxy::cluster::spawn);
                ths.extend(jhs);
            }
            _ => {
                let jhs = spawn_worker(&cluster, ip.clone(), proxy::standalone::spawn);
                ths.extend(jhs);
            }
        }
    }

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

fn spawn_worker<T>(cc: &ClusterConfig, ip: Option<String>, spawn_fn: T) -> Vec<JoinHandle<()>>
where
    T: Fn(ClusterConfig) + Copy + Send + 'static,
{
    let worker = cc.thread.unwrap_or(4);
    let meta = load_meta(cc.clone(), ip);
    info!("setup meta info with {:?}", meta);
    (0..worker)
        .map(|_index| {
            let cc = cc.clone();
            let meta = meta.clone();
            Builder::new()
                .name(cc.name.clone())
                .spawn(move || {
                    thread_incr();
                    meta_init(meta);
                    spawn_fn(cc);
                })
                .expect("fail to spawn worker thread")
        })
        .collect()
}

fn spawn_metrics(port: usize) -> Vec<thread::JoinHandle<()>> {
    // wait for worker thread to be ready
    thread::sleep(Duration::from_secs(3));
    vec![
        thread::Builder::new()
            .name("aster-http-srv".to_string())
            .spawn(move || metrics::init(port).unwrap())
            .unwrap(),
        thread::Builder::new()
            .name("measure-service".to_string())
            .spawn(move || metrics::measure_system().unwrap())
            .unwrap(),
    ]
}
