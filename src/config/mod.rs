use std::collections::HashSet;
use std::env;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use tokio::fs;

use crate::auth::{AuthUserConfig, BackendAuthConfig, FrontendAuthConfig};

/// Environment variable controlling the default worker thread count when a
/// cluster omits the `thread` field.
pub const ENV_DEFAULT_THREADS: &str = "ASTER_DEFAULT_THREAD";

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    clusters: Vec<ClusterConfig>,
}

impl Config {
    /// Load configuration from a TOML file.
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .await
            .with_context(|| format!("failed to read config file {}", path.display()))?;

        let mut cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;
        cfg.apply_defaults();
        cfg.ensure_valid()?;
        Ok(cfg)
    }

    /// Ensure configuration correctness.
    pub fn ensure_valid(&self) -> Result<()> {
        if self.clusters.is_empty() {
            bail!("configuration must declare at least one cluster");
        }

        let mut names = HashSet::new();
        for cluster in &self.clusters {
            cluster.ensure_valid()?;
            if !names.insert(cluster.name.to_lowercase()) {
                bail!("duplicate cluster name detected: {}", cluster.name);
            }
        }
        Ok(())
    }

    /// All configured clusters.
    pub fn clusters(&self) -> &[ClusterConfig] {
        &self.clusters
    }

    /// Mutable access to clusters, used for reload scenarios.
    pub fn clusters_mut(&mut self) -> &mut [ClusterConfig] {
        &mut self.clusters
    }

    fn apply_defaults(&mut self) {
        let default_threads = default_worker_threads();
        for cluster in &mut self.clusters {
            if cluster.thread.is_none() {
                cluster.thread = Some(default_threads);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CacheType {
    Redis,
    RedisCluster,
}

impl Default for CacheType {
    fn default() -> Self {
        CacheType::RedisCluster
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub listen_addr: String,
    #[serde(default)]
    pub hash_tag: Option<String>,
    #[serde(default)]
    pub thread: Option<usize>,
    #[serde(default)]
    pub cache_type: CacheType,
    #[serde(default)]
    pub read_timeout: Option<u64>,
    #[serde(default)]
    pub write_timeout: Option<u64>,
    #[serde(default)]
    pub servers: Vec<String>,
    #[serde(default)]
    pub fetch_interval: Option<u64>,
    #[serde(default)]
    pub read_from_slave: Option<bool>,
    #[serde(default)]
    pub ping_fail_limit: Option<u8>,
    #[serde(default)]
    pub ping_interval: Option<u64>,
    #[serde(default)]
    pub ping_succ_interval: Option<u64>,

    // legacy options kept for compatibility; unused in the new implementation.
    #[serde(default)]
    pub dial_timeout: Option<u64>,
    #[serde(default)]
    pub listen_proto: Option<String>,
    #[serde(default)]
    pub node_connections: Option<usize>,
    #[serde(default)]
    pub auth: Option<FrontendAuthConfig>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub backend_auth: Option<BackendAuthConfig>,
    #[serde(default)]
    pub backend_password: Option<String>,
}

impl ClusterConfig {
    /// Worker threads assigned to this cluster.
    pub fn worker_threads(&self) -> usize {
        self.thread
            .expect("defaults should populate worker threads")
    }

    /// Validate consistency of a cluster configuration.
    pub fn ensure_valid(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("cluster name cannot be empty");
        }
        if self.listen_addr.trim().is_empty() {
            bail!("cluster {} listen_addr cannot be empty", self.name);
        }

        parse_port(&self.listen_addr).with_context(|| {
            format!(
                "cluster {} listen_addr {} is not a valid address",
                self.name, self.listen_addr
            )
        })?;

        if self.servers.is_empty() {
            bail!(
                "cluster {} must provide at least one backend server",
                self.name
            );
        }
        Ok(())
    }

    /// Extract the listening port as configured.
    pub fn listen_port(&self) -> Result<u16> {
        parse_port(&self.listen_addr).with_context(|| {
            format!(
                "cluster {} listen_addr {} is not a valid address",
                self.name, self.listen_addr
            )
        })
    }

    pub fn frontend_auth_users(&self) -> Option<Vec<AuthUserConfig>> {
        if let Some(config) = self.auth.clone() {
            return Some(config.into_users());
        }
        self.password
            .clone()
            .map(FrontendAuthConfig::Password)
            .map(|cfg| cfg.into_users())
    }

    pub fn backend_auth_config(&self) -> Option<BackendAuthConfig> {
        if let Some(config) = self.backend_auth.clone() {
            return Some(config);
        }
        self.backend_password
            .clone()
            .map(BackendAuthConfig::Password)
    }
}

fn parse_port(addr: &str) -> Result<u16> {
    if let Ok(socket) = addr.parse::<std::net::SocketAddr>() {
        return Ok(socket.port());
    }

    if let Some((_, port_str)) = addr.rsplit_once(':') {
        let port = port_str
            .trim()
            .parse::<u16>()
            .with_context(|| format!("invalid port component {}", port_str))?;
        return Ok(port);
    }

    bail!("unable to extract port from address {}", addr)
}

fn default_worker_threads() -> usize {
    if let Ok(val) = env::var(ENV_DEFAULT_THREADS) {
        if let Ok(parsed) = val.parse::<usize>() {
            if parsed > 0 {
                return parsed;
            }
        }
    }

    std::thread::available_parallelism()
        .map(|nz| nz.get())
        .unwrap_or(4)
}
