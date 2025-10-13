use std::env;
use std::future::Future;
use std::net::IpAddr;

use anyhow::Result;
use tokio::task_local;

use crate::config::ClusterConfig;

task_local! {
    static CURRENT_META: Meta;
}

#[derive(Debug, Clone)]
pub struct Meta {
    cluster: String,
    ip: String,
    port: u16,
}

impl Meta {
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    pub fn ip(&self) -> &str {
        &self.ip
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Derive meta information for a cluster.
pub fn derive_meta(cluster: &ClusterConfig, override_ip: Option<&str>) -> Result<Meta> {
    let port = cluster.listen_port()?;
    let ip = determine_ip(override_ip)?;

    Ok(Meta {
        cluster: cluster.name.clone(),
        ip,
        port,
    })
}

/// Run a future within the scope of the provided meta information.
pub async fn scope_with_meta<F, T>(meta: Meta, fut: F) -> T
where
    F: Future<Output = T>,
{
    CURRENT_META.scope(meta, fut).await
}

/// Access the current meta information if present.
pub fn current_meta() -> Option<Meta> {
    CURRENT_META.try_with(|meta| meta.clone()).ok()
}

/// Get the current cluster name if available.
pub fn current_cluster() -> Option<String> {
    current_meta().map(|m| m.cluster)
}

/// Get the advertised IP if available.
pub fn current_ip() -> Option<String> {
    current_meta().map(|m| m.ip)
}

/// Get the advertised port if available.
pub fn current_port() -> Option<u16> {
    current_meta().map(|m| m.port)
}

fn determine_ip(override_ip: Option<&str>) -> Result<String> {
    if let Some(ip) = override_ip {
        return Ok(ip.to_string());
    }

    if let Ok(env_ip) = env::var("HOST") {
        if let Ok(parsed) = env_ip.parse::<IpAddr>() {
            if !parsed.is_unspecified() && !parsed.is_multicast() {
                return Ok(env_ip);
            }
        }
    }

    // Fallback to loopback.
    Ok("127.0.0.1".to_string())
}
