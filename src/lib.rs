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

use failure::Error;

use com::meta::{load_meta, meta_init};
use com::ClusterConfig;
use metrics::thread_incr;
use metrics::slowlog::{self, Entry};

use std::time::Duration;

use futures::sync::mpsc::{channel, Sender};

pub fn run() -> Result<(), Error> {
    env_logger::init();
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).version(ASTER_VERSION).get_matches();
    if matches.is_present("version") {
        println!("{}", ASTER_VERSION);
        return Ok(())
    }
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

    let (tx, rx) = channel(1024);

    let slowlog_slower_than = matches.value_of("slowlog-slower-than").unwrap_or("10").parse::<u128>().unwrap();

    let mut ths = Vec::new();
    for mut cluster in cfg.clusters.into_iter() {
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

        cluster.set_slowlog_slow_than(slowlog_slower_than);

        info!(
            "starting aster cluster {} in addr {}",
            cluster.name, cluster.listen_addr
        );

        
        match cluster.cache_type {
            com::CacheType::RedisCluster => {
                let jhs = spwan_worker(&cluster, ip.clone(), tx.clone(), proxy::cluster::spawn);
                ths.extend(jhs);
            }
            _ => {
                let jhs = spwan_worker(&cluster, ip.clone(), tx.clone(), proxy::standalone::spawn);
                ths.extend(jhs);
            }
        }
    }

    let slowlog_file_path = matches.value_of("slowlog-file-path").unwrap_or("aster-slowlog.log").to_string();
    let slowlog_file_size = matches.value_of("slowlog-file-size").unwrap_or("200").parse::<u32>().unwrap();
    let slowlog_file_backup = matches.value_of("slowlog-file-backup").unwrap_or("3").parse::<u8>().unwrap();
    thread::spawn(move || slowlog::run(slowlog_file_path, slowlog_file_size, slowlog_file_backup, rx));

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

fn spwan_worker<T>(cc: &ClusterConfig, ip: Option<String>, slowlog_tx: Sender<Entry>, spawn_fn: T) -> Vec<JoinHandle<()>>
where
    T: Fn(ClusterConfig, Sender<Entry>) + Copy + Send + 'static,
{
    let worker = cc.thread.unwrap_or(4);
    let meta = load_meta(cc.clone(), ip);
    info!("setup meta info with {:?}", meta);
    (0..worker)
        .map(|_index| {
            let cc = cc.clone();
            let meta = meta.clone();
            let tx = slowlog_tx.clone();
            Builder::new()
                .name(cc.name.clone())
                .spawn(move || {
                    thread_incr();
                    meta_init(meta);
                    spawn_fn(cc, tx);
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
