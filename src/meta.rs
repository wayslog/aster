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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackupRequestConfig, CacheType, ClientCacheConfig, ClusterConfig};
    use crate::protocol::redis::RespVersion;
    use std::sync::Mutex;

    static ENV_GUARD: Mutex<()> = Mutex::new(());

    fn sample_cluster() -> ClusterConfig {
        ClusterConfig {
            name: "test".into(),
            listen_addr: "127.0.0.1:7000".into(),
            hash_tag: None,
            thread: Some(2),
            cache_type: CacheType::Redis,
            read_timeout: Some(1000),
            write_timeout: Some(1000),
            servers: vec!["127.0.0.1:6379".into()],
            fetch_interval: None,
            read_from_slave: None,
            ping_fail_limit: None,
            ping_interval: None,
            ping_succ_interval: None,
            dial_timeout: None,
            listen_proto: None,
            node_connections: None,
            auth: None,
            password: None,
            backend_auth: None,
            backend_password: None,
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            hotkey_sample_every: 200,
            hotkey_sketch_width: 256,
            hotkey_sketch_depth: 4,
            hotkey_capacity: 5_000,
            hotkey_decay: 0.5,
            backend_resp_version: RespVersion::Resp2,
            client_cache: ClientCacheConfig::default(),
            backup_request: BackupRequestConfig::default(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scope_with_meta_makes_context_available() {
        let meta = Meta {
            cluster: "cluster-a".into(),
            ip: "10.1.1.5".into(),
            port: 7111,
        };

        let value = scope_with_meta(meta.clone(), async {
            assert_eq!(current_cluster().as_deref(), Some("cluster-a"));
            assert_eq!(current_ip().as_deref(), Some("10.1.1.5"));
            assert_eq!(current_port(), Some(7111));
            current_meta().unwrap().cluster().to_string()
        })
        .await;

        assert_eq!(value, "cluster-a".to_string());
        assert!(current_meta().is_none());
    }

    #[test]
    fn derive_meta_prefers_override_ip() {
        let cluster = sample_cluster();
        let meta = derive_meta(&cluster, Some("192.168.1.10")).expect("derive meta");
        assert_eq!(meta.ip(), "192.168.1.10");
        assert_eq!(meta.port(), 7000);
        assert_eq!(meta.cluster(), "test");
    }

    #[test]
    fn determine_ip_uses_valid_host_env() {
        let _guard = ENV_GUARD.lock().unwrap();
        let prev = env::var("HOST").ok();
        env::set_var("HOST", "10.0.0.9");
        let ip = determine_ip(None).expect("determine ip");
        if let Some(value) = prev {
            env::set_var("HOST", value);
        } else {
            env::remove_var("HOST");
        }
        assert_eq!(ip, "10.0.0.9");
    }

    #[test]
    fn determine_ip_falls_back_when_env_invalid() {
        let _guard = ENV_GUARD.lock().unwrap();
        let prev = env::var("HOST").ok();
        env::set_var("HOST", "not-an-ip");
        let ip = determine_ip(None).expect("determine ip");
        if let Some(value) = prev {
            env::set_var("HOST", value);
        } else {
            env::remove_var("HOST");
        }
        assert_eq!(ip, "127.0.0.1");
    }

    #[test]
    fn current_meta_returns_none_outside_scope() {
        assert!(current_meta().is_none());
    }
}
