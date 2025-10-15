//! Core library entrypoint for the rewritten aster proxy.
//!
//! The implementation is currently limited to runtime bootstrap, CLI parsing,
//! and configuration handling. Functional proxy components will be added in
//! subsequent steps.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Builder;
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};

pub mod auth;
pub mod backend;
pub mod cluster;
pub mod config;
pub mod meta;
pub mod metrics;
pub mod protocol;
pub mod standalone;
pub mod utils;

use crate::cluster::ClusterProxy;
use crate::config::{CacheType, Config};
use crate::meta::{derive_meta, scope_with_meta};
use crate::standalone::StandaloneProxy;

/// CLI definition for the proxy.
#[derive(Debug, Parser)]
#[command(
    name = "aster-proxy",
    version,
    about = "Aster is a lightweight proxy for Redis and Redis Cluster."
)]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, value_name = "FILE", default_value = "default.toml")]
    config: PathBuf,
    /// Override the advertised IP address.
    #[arg(short = 'i', long = "ip", value_name = "ADDR")]
    ip: Option<String>,
    /// Prometheus metrics port.
    #[arg(
        short = 'm',
        long = "metrics",
        value_name = "PORT",
        default_value_t = 2110
    )]
    metrics: u16,
    /// Enable configuration reload support.
    #[arg(short = 'r', long = "reload")]
    reload: bool,
}

#[derive(Debug)]
struct BootstrapOptions {
    config: PathBuf,
    override_ip: Option<String>,
    metrics_port: u16,
    reload_enabled: bool,
}

impl From<Cli> for BootstrapOptions {
    fn from(value: Cli) -> Self {
        Self {
            config: value.config,
            override_ip: value.ip,
            metrics_port: value.metrics,
            reload_enabled: value.reload,
        }
    }
}

/// Launch the proxy service.
pub fn run() -> Result<()> {
    let cli = Cli::parse();
    init_tracing()?;

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .thread_name("aster-rt")
        .build()?;

    runtime.block_on(async move {
        let options = BootstrapOptions::from(cli);
        run_async(options).await
    })
}

fn init_tracing() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(env_filter)
        .with_thread_names(true)
        .with_target(false)
        .init();
    Ok(())
}

async fn run_async(options: BootstrapOptions) -> Result<()> {
    metrics::register_version(env!("CARGO_PKG_VERSION"));

    let config = Config::load(&options.config).await?;

    info!(
        clusters = config.clusters().len(),
        names = ?config
            .clusters()
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        "configuration loaded"
    );

    if options.reload_enabled {
        warn!("configuration reload is not yet implemented; --reload will be ignored");
    }

    let metrics_handles = metrics::spawn_background_tasks(options.metrics_port);

    for cluster_cfg in config.clusters().iter().cloned() {
        let listen_addr = cluster_cfg.listen_addr.clone();
        let listener = TcpListener::bind(&listen_addr)
            .await
            .with_context(|| format!("failed to bind listener for {}", &listen_addr))?;
        let local_addr = listener
            .local_addr()
            .context("failed to obtain local listen address")?;

        let meta = derive_meta(&cluster_cfg, options.override_ip.as_deref())?;
        let cluster_label: Arc<str> = cluster_cfg.name.clone().into();

        info!(
            cluster = %cluster_cfg.name,
            listen = %local_addr,
            mode = ?cluster_cfg.cache_type,
            advertise_ip = %meta.ip(),
            "cluster listener started"
        );

        match cluster_cfg.cache_type {
            CacheType::Redis => {
                let proxy = Arc::new(StandaloneProxy::new(&cluster_cfg)?);
                let listener = listener;
                let meta = meta.clone();
                let cluster_label = cluster_label.clone();
                tokio::spawn(scope_with_meta(meta, async move {
                    accept_loop(listener, proxy, cluster_label).await;
                }));
            }
            CacheType::RedisCluster => {
                let proxy = Arc::new(ClusterProxy::new(&cluster_cfg).await?);
                let listener = listener;
                let meta = meta.clone();
                let cluster_label = cluster_label.clone();
                tokio::spawn(scope_with_meta(meta, async move {
                    accept_loop(listener, proxy, cluster_label).await;
                }));
            }
        }
    }

    info!("all clusters are running; waiting for shutdown signal");
    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for shutdown signal")?;
    info!("shutdown signal received; terminating background tasks");

    metrics_handles.http.abort();
    metrics_handles.system.abort();
    Ok(())
}

async fn accept_loop<P>(listener: TcpListener, proxy: Arc<P>, cluster: Arc<str>)
where
    P: ProxyService,
{
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let proxy = proxy.clone();
                let cluster_name = cluster.clone();
                tokio::spawn(async move {
                    if let Err(err) = proxy.handle(socket).await {
                        metrics::global_error_incr();
                        warn!(cluster = %cluster_name, peer = %addr, error = %err, "connection closed with error");
                    }
                });
            }
            Err(err) => {
                metrics::global_error_incr();
                warn!(cluster = %cluster, error = %err, "failed to accept incoming connection");
            }
        }
    }
}

#[async_trait]
pub trait ProxyService: Send + Sync + 'static {
    async fn handle(&self, socket: TcpStream) -> Result<()>;
}

#[async_trait]
impl ProxyService for StandaloneProxy {
    async fn handle(&self, socket: TcpStream) -> Result<()> {
        self.handle_connection(socket).await
    }
}

#[async_trait]
impl ProxyService for ClusterProxy {
    async fn handle(&self, socket: TcpStream) -> Result<()> {
        self.handle_connection(socket).await
    }
}
