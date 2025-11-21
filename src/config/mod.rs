use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{info, warn};

use crate::auth::{AuthUserConfig, BackendAuthConfig, FrontendAuthConfig};
use crate::cache::ClientCache;
use crate::hotkey::{
    Hotkey, HotkeyConfig, DEFAULT_DECAY, DEFAULT_HOTKEY_CAPACITY, DEFAULT_SAMPLE_EVERY,
    DEFAULT_SKETCH_DEPTH, DEFAULT_SKETCH_WIDTH,
};
use crate::protocol::redis::{RedisCommand, RespValue, RespVersion};
use crate::slowlog::Slowlog;

/// Environment variable controlling the default worker thread count when a
/// cluster omits the `thread` field.
pub const ENV_DEFAULT_THREADS: &str = "ASTER_DEFAULT_THREAD";
const DUMP_VALUE_DEFAULT: &str = "default";

fn default_slowlog_log_slower_than() -> i64 {
    10_000
}

fn default_slowlog_max_len() -> usize {
    128
}

fn default_hotkey_sample_every() -> u64 {
    DEFAULT_SAMPLE_EVERY
}

fn default_hotkey_sketch_width() -> usize {
    DEFAULT_SKETCH_WIDTH
}

fn default_hotkey_sketch_depth() -> usize {
    DEFAULT_SKETCH_DEPTH
}

fn default_hotkey_capacity() -> usize {
    DEFAULT_HOTKEY_CAPACITY
}

fn default_hotkey_decay() -> f64 {
    DEFAULT_DECAY
}

fn default_backend_resp_version() -> RespVersion {
    RespVersion::Resp2
}

fn default_client_cache_max_entries() -> usize {
    100_000
}

fn default_client_cache_max_value_bytes() -> usize {
    512 * 1024
}

fn default_client_cache_shards() -> usize {
    32
}

fn default_client_cache_drain_batch() -> usize {
    1024
}

fn default_client_cache_drain_interval_ms() -> u64 {
    50
}

fn default_backup_multiplier() -> f64 {
    2.0
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct BackupRequestConfig {
    pub enabled: bool,
    pub trigger_slow_ms: Option<u64>,
    pub multiplier: f64,
}

impl Default for BackupRequestConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            trigger_slow_ms: None,
            multiplier: default_backup_multiplier(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientCacheConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_client_cache_max_entries")]
    pub max_entries: usize,
    #[serde(default = "default_client_cache_max_value_bytes")]
    pub max_value_bytes: usize,
    #[serde(default = "default_client_cache_shards")]
    pub shard_count: usize,
    #[serde(default = "default_client_cache_drain_batch")]
    pub drain_batch: usize,
    #[serde(default = "default_client_cache_drain_interval_ms")]
    pub drain_interval_ms: u64,
}

impl ClientCacheConfig {
    pub fn ensure_valid(&self) -> Result<()> {
        if self.max_entries == 0 {
            bail!("client cache max_entries must be > 0");
        }
        if self.max_value_bytes == 0 {
            bail!("client cache max_value_bytes must be > 0");
        }
        if self.shard_count == 0 {
            bail!("client cache shard_count must be > 0");
        }
        if self.drain_batch == 0 {
            bail!("client cache drain_batch must be > 0");
        }
        if self.drain_interval_ms == 0 {
            bail!("client cache drain_interval_ms must be > 0");
        }
        Ok(())
    }
}

impl Default for ClientCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_entries: default_client_cache_max_entries(),
            max_value_bytes: default_client_cache_max_value_bytes(),
            shard_count: default_client_cache_shards(),
            drain_batch: default_client_cache_drain_batch(),
            drain_interval_ms: default_client_cache_drain_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    #[serde(default = "default_slowlog_log_slower_than")]
    pub slowlog_log_slower_than: i64,
    #[serde(default = "default_slowlog_max_len")]
    pub slowlog_max_len: usize,
    #[serde(default = "default_hotkey_sample_every")]
    pub hotkey_sample_every: u64,
    #[serde(default = "default_hotkey_sketch_width")]
    pub hotkey_sketch_width: usize,
    #[serde(default = "default_hotkey_sketch_depth")]
    pub hotkey_sketch_depth: usize,
    #[serde(default = "default_hotkey_capacity")]
    pub hotkey_capacity: usize,
    #[serde(default = "default_hotkey_decay")]
    pub hotkey_decay: f64,
    #[serde(default = "default_backend_resp_version")]
    pub backend_resp_version: RespVersion,
    #[serde(default)]
    pub client_cache: ClientCacheConfig,
    #[serde(default)]
    pub backup_request: BackupRequestConfig,
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

        self.client_cache.ensure_valid()?;

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
        if self.slowlog_log_slower_than < -1 {
            bail!(
                "cluster {} slowlog-log-slower-than must be >= -1",
                self.name
            );
        }
        if self.hotkey_sample_every == 0 {
            bail!("cluster {} hotkey_sample_every must be > 0", self.name);
        }
        if self.hotkey_sketch_width == 0 {
            bail!("cluster {} hotkey_sketch_width must be > 0", self.name);
        }
        if self.hotkey_sketch_depth == 0 {
            bail!("cluster {} hotkey_sketch_depth must be > 0", self.name);
        }
        if self.hotkey_capacity == 0 {
            bail!("cluster {} hotkey_capacity must be > 0", self.name);
        }
        if !(self.hotkey_decay > 0.0 && self.hotkey_decay <= 1.0) {
            bail!("cluster {} hotkey_decay must be in (0, 1]", self.name);
        }
        if self.backup_request.multiplier <= 0.0 {
            bail!(
                "cluster {} backup_request.multiplier must be greater than 0",
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

#[derive(Debug)]
pub struct ClusterRuntime {
    read_timeout_ms: AtomicI64,
    write_timeout_ms: AtomicI64,
}

impl ClusterRuntime {
    fn new(read_timeout: Option<u64>, write_timeout: Option<u64>) -> Self {
        Self {
            read_timeout_ms: AtomicI64::new(option_to_atomic(read_timeout)),
            write_timeout_ms: AtomicI64::new(option_to_atomic(write_timeout)),
        }
    }

    pub fn read_timeout(&self) -> Option<u64> {
        atomic_to_option(self.read_timeout_ms.load(Ordering::Relaxed))
    }

    pub fn write_timeout(&self) -> Option<u64> {
        atomic_to_option(self.write_timeout_ms.load(Ordering::Relaxed))
    }

    pub fn set_read_timeout(&self, value: Option<u64>) {
        self.read_timeout_ms
            .store(option_to_atomic(value), Ordering::Relaxed);
    }

    pub fn set_write_timeout(&self, value: Option<u64>) {
        self.write_timeout_ms
            .store(option_to_atomic(value), Ordering::Relaxed);
    }

    pub fn request_timeout(&self, default_ms: u64) -> std::time::Duration {
        std::time::Duration::from_millis(self.request_timeout_ms(default_ms))
    }

    pub fn request_timeout_ms(&self, default_ms: u64) -> u64 {
        if let Some(value) = self.read_timeout() {
            value
        } else if let Some(value) = self.write_timeout() {
            value
        } else {
            default_ms
        }
    }
}

#[derive(Debug)]
pub struct BackupRequestRuntime {
    enabled: AtomicBool,
    threshold_ms: AtomicI64,
    multiplier_bits: AtomicU64,
}

impl BackupRequestRuntime {
    fn new(config: &BackupRequestConfig) -> Self {
        Self {
            enabled: AtomicBool::new(config.enabled),
            threshold_ms: AtomicI64::new(option_to_atomic(config.trigger_slow_ms)),
            multiplier_bits: AtomicU64::new(config.multiplier.to_bits()),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(config: BackupRequestConfig) -> Self {
        Self::new(&config)
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, value: bool) {
        self.enabled.store(value, Ordering::Relaxed);
    }

    pub fn threshold_ms(&self) -> Option<u64> {
        atomic_to_option(self.threshold_ms.load(Ordering::Relaxed))
    }

    pub fn set_threshold_ms(&self, value: Option<u64>) {
        self.threshold_ms
            .store(option_to_atomic(value), Ordering::Relaxed);
    }

    pub fn multiplier(&self) -> f64 {
        f64::from_bits(self.multiplier_bits.load(Ordering::Relaxed))
    }

    pub fn set_multiplier(&self, value: f64) {
        self.multiplier_bits
            .store(value.to_bits(), Ordering::Relaxed);
    }
}

fn option_to_atomic(value: Option<u64>) -> i64 {
    match value {
        Some(v) => v as i64,
        None => -1,
    }
}

fn atomic_to_option(value: i64) -> Option<u64> {
    if value < 0 {
        None
    } else {
        Some(value as u64)
    }
}

#[derive(Debug, Clone)]
struct ClusterEntry {
    index: usize,
    runtime: Arc<ClusterRuntime>,
    slowlog: Arc<Slowlog>,
    hotkey: Arc<Hotkey>,
    client_cache: Arc<ClientCache>,
    backup: Arc<BackupRequestRuntime>,
}

#[derive(Debug)]
pub struct ConfigManager {
    path: PathBuf,
    config: RwLock<Config>,
    clusters: HashMap<String, ClusterEntry>,
}

impl ConfigManager {
    pub fn new(path: PathBuf, config: &Config) -> Self {
        let mut clusters = HashMap::new();
        for (index, cluster) in config.clusters().iter().enumerate() {
            let key = cluster.name.to_ascii_lowercase();
            let runtime = Arc::new(ClusterRuntime::new(
                cluster.read_timeout,
                cluster.write_timeout,
            ));
            let slowlog = Arc::new(Slowlog::new(
                cluster.slowlog_log_slower_than,
                cluster.slowlog_max_len,
            ));
            let hotkey_config = HotkeyConfig {
                sample_every: cluster.hotkey_sample_every,
                sketch_width: cluster.hotkey_sketch_width,
                sketch_depth: cluster.hotkey_sketch_depth,
                capacity: cluster.hotkey_capacity,
                decay: cluster.hotkey_decay,
            };
            let hotkey = Arc::new(Hotkey::new(hotkey_config));
            let cluster_label: Arc<str> = cluster.name.clone().into();
            let client_cache = Arc::new(ClientCache::new(
                cluster_label.clone(),
                cluster.client_cache.clone(),
                cluster.backend_resp_version == RespVersion::Resp3,
            ));
            let backup = Arc::new(BackupRequestRuntime::new(&cluster.backup_request));
            clusters.insert(
                key,
                ClusterEntry {
                    index,
                    runtime,
                    slowlog,
                    hotkey,
                    client_cache,
                    backup,
                },
            );
        }

        Self {
            path,
            config: RwLock::new(config.clone()),
            clusters,
        }
    }

    pub fn runtime_for(&self, name: &str) -> Option<Arc<ClusterRuntime>> {
        self.clusters
            .get(&name.to_ascii_lowercase())
            .map(|entry| entry.runtime.clone())
    }

    pub fn slowlog_for(&self, name: &str) -> Option<Arc<Slowlog>> {
        self.clusters
            .get(&name.to_ascii_lowercase())
            .map(|entry| entry.slowlog.clone())
    }

    pub fn hotkey_for(&self, name: &str) -> Option<Arc<Hotkey>> {
        self.clusters
            .get(&name.to_ascii_lowercase())
            .map(|entry| entry.hotkey.clone())
    }

    pub fn client_cache_for(&self, name: &str) -> Option<Arc<ClientCache>> {
        self.clusters
            .get(&name.to_ascii_lowercase())
            .map(|entry| entry.client_cache.clone())
    }

    pub fn backup_request_for(&self, name: &str) -> Option<Arc<BackupRequestRuntime>> {
        self.clusters
            .get(&name.to_ascii_lowercase())
            .map(|entry| entry.backup.clone())
    }

    pub async fn handle_command(&self, command: &RedisCommand) -> Option<RespValue> {
        if !command.command_name().eq_ignore_ascii_case(b"CONFIG") {
            return None;
        }

        let args = command.args();
        if args.len() < 2 {
            return Some(err_response(
                "wrong number of arguments for 'config' command",
            ));
        }

        let sub = args[1].to_vec().to_ascii_uppercase();
        match sub.as_slice() {
            b"GET" => Some(self.handle_get(args)),
            b"SET" => Some(self.handle_set(args)),
            b"DUMP" => Some(self.handle_dump(args)),
            b"REWRITE" => Some(self.handle_rewrite(args).await),
            other => Some(err_response(format!(
                "unsupported config subcommand '{}'",
                String::from_utf8_lossy(other).to_ascii_lowercase()
            ))),
        }
    }

    fn handle_get(&self, args: &[bytes::Bytes]) -> RespValue {
        if args.len() != 3 {
            return err_response("wrong number of arguments for 'config get' command");
        }
        let pattern = String::from_utf8_lossy(&args[2]).to_string();
        let entries = self.matching_entries(&pattern);
        RespValue::array(flatten_pairs(entries))
    }

    fn handle_set(&self, args: &[bytes::Bytes]) -> RespValue {
        if args.len() != 4 {
            return err_response("wrong number of arguments for 'config set' command");
        }

        let key = String::from_utf8_lossy(&args[2]).to_string();
        let value = String::from_utf8_lossy(&args[3]).to_string();
        match self.apply_set(&key, &value) {
            Ok(()) => RespValue::simple("OK"),
            Err(err) => err_response(err.to_string()),
        }
    }

    fn handle_dump(&self, args: &[bytes::Bytes]) -> RespValue {
        if args.len() != 2 {
            return err_response("wrong number of arguments for 'config dump' command");
        }
        let entries = self.all_entries();
        RespValue::array(flatten_pairs(entries))
    }

    async fn handle_rewrite(&self, args: &[bytes::Bytes]) -> RespValue {
        if args.len() != 2 {
            return err_response("wrong number of arguments for 'config rewrite' command");
        }
        match self.rewrite().await {
            Ok(()) => RespValue::simple("OK"),
            Err(err) => {
                warn!(error = %err, "failed to rewrite configuration file");
                err_response(err.to_string())
            }
        }
    }

    fn apply_set(&self, key: &str, value: &str) -> Result<()> {
        let (cluster_name, field) = parse_key(key)?;
        let cluster_key = cluster_name.to_ascii_lowercase();
        let entry = self
            .clusters
            .get(&cluster_key)
            .ok_or_else(|| anyhow!("unknown cluster '{}'", cluster_name))?
            .clone();

        match field {
            ClusterField::ReadTimeout => {
                let parsed = parse_timeout_value(value)?;
                entry.runtime.set_read_timeout(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].read_timeout = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster read_timeout updated via CONFIG SET"
                );
            }
            ClusterField::WriteTimeout => {
                let parsed = parse_timeout_value(value)?;
                entry.runtime.set_write_timeout(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].write_timeout = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster write_timeout updated via CONFIG SET"
                );
            }
            ClusterField::SlowlogLogSlowerThan => {
                let parsed = parse_slowlog_threshold(value)?;
                entry.slowlog.set_threshold(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].slowlog_log_slower_than = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster slowlog_log_slower_than updated via CONFIG SET"
                );
            }
            ClusterField::SlowlogMaxLen => {
                let parsed = parse_slowlog_len(value)?;
                entry.slowlog.set_max_len(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].slowlog_max_len = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster slowlog_max_len updated via CONFIG SET"
                );
            }
            ClusterField::HotkeySampleEvery => {
                let parsed = parse_hotkey_sample_every(value)?;
                entry.hotkey.update_config(|cfg| cfg.sample_every = parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].hotkey_sample_every = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster hotkey_sample_every updated via CONFIG SET"
                );
            }
            ClusterField::HotkeySketchWidth => {
                let parsed = parse_hotkey_sketch_width(value)?;
                entry.hotkey.update_config(|cfg| cfg.sketch_width = parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].hotkey_sketch_width = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster hotkey_sketch_width updated via CONFIG SET"
                );
            }
            ClusterField::HotkeySketchDepth => {
                let parsed = parse_hotkey_sketch_depth(value)?;
                entry.hotkey.update_config(|cfg| cfg.sketch_depth = parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].hotkey_sketch_depth = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster hotkey_sketch_depth updated via CONFIG SET"
                );
            }
            ClusterField::HotkeyCapacity => {
                let parsed = parse_hotkey_capacity(value)?;
                entry.hotkey.update_config(|cfg| cfg.capacity = parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].hotkey_capacity = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster hotkey_capacity updated via CONFIG SET"
                );
            }
            ClusterField::HotkeyDecay => {
                let parsed = parse_hotkey_decay(value)?;
                entry.hotkey.update_config(|cfg| cfg.decay = parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].hotkey_decay = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster hotkey_decay updated via CONFIG SET"
                );
            }
            ClusterField::ClientCacheEnabled => {
                let enabled = parse_bool_flag(value, "client-cache-enabled")?;
                if enabled {
                    entry.client_cache.enable().with_context(|| {
                        format!("cluster {} failed to enable client cache", cluster_name)
                    })?;
                } else {
                    entry.client_cache.disable();
                }
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].client_cache.enabled = enabled;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster client_cache.enabled updated via CONFIG SET"
                );
            }
            ClusterField::ClientCacheMaxEntries => {
                let parsed = parse_positive_usize(value, "client-cache-max-entries")?;
                entry.client_cache.set_max_entries(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].client_cache.max_entries = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster client_cache.max_entries updated via CONFIG SET"
                );
            }
            ClusterField::ClientCacheMaxValueBytes => {
                let parsed = parse_positive_usize(value, "client-cache-max-value-bytes")?;
                entry.client_cache.set_max_value_bytes(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index]
                    .client_cache
                    .max_value_bytes = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster client_cache.max_value_bytes updated via CONFIG SET"
                );
            }
            ClusterField::ClientCacheShardCount => {
                let parsed = parse_positive_usize(value, "client-cache-shard-count")?;
                entry.client_cache.set_shard_count(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].client_cache.shard_count = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster client_cache.shard_count updated via CONFIG SET"
                );
            }
            ClusterField::ClientCacheDrainBatch => {
                let parsed = parse_positive_usize(value, "client-cache-drain-batch")?;
                entry.client_cache.set_drain_batch(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].client_cache.drain_batch = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster client_cache.drain_batch updated via CONFIG SET"
                );
            }
            ClusterField::ClientCacheDrainIntervalMs => {
                let parsed = parse_positive_u64(value, "client-cache-drain-interval-ms")?;
                entry.client_cache.set_drain_interval(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index]
                    .client_cache
                    .drain_interval_ms = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster client_cache.drain_interval_ms updated via CONFIG SET"
                );
            }
            ClusterField::BackupRequestEnabled => {
                let enabled = parse_bool_flag(value, "backup-request-enabled")?;
                entry.backup.set_enabled(enabled);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].backup_request.enabled = enabled;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster backup_request.enabled updated via CONFIG SET"
                );
            }
            ClusterField::BackupRequestThreshold => {
                let parsed = parse_timeout_value(value)?;
                entry.backup.set_threshold_ms(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index]
                    .backup_request
                    .trigger_slow_ms = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster backup_request.trigger_slow_ms updated via CONFIG SET"
                );
            }
            ClusterField::BackupRequestMultiplier => {
                let parsed = parse_backup_multiplier(value)?;
                entry.backup.set_multiplier(parsed);
                let mut guard = self.config.write();
                guard.clusters_mut()[entry.index].backup_request.multiplier = parsed;
                info!(
                    cluster = cluster_name,
                    value = value,
                    "cluster backup_request.multiplier updated via CONFIG SET"
                );
            }
        }
        Ok(())
    }

    fn matching_entries(&self, pattern: &str) -> Vec<(String, String)> {
        let pattern_lower = pattern.to_ascii_lowercase();
        self.all_entries()
            .into_iter()
            .filter(|(key, _)| wildcard_match(&pattern_lower, &key.to_ascii_lowercase()))
            .collect()
    }

    fn all_entries(&self) -> Vec<(String, String)> {
        let guard = self.config.read();
        let mut entries = Vec::new();
        for cluster in guard.clusters() {
            let name = &cluster.name;
            let key = name.to_ascii_lowercase();
            if let Some(entry) = self.clusters.get(&key) {
                let runtime = entry.runtime.clone();
                entries.push((
                    format!("cluster.{}.read-timeout", name),
                    option_to_string(runtime.read_timeout()),
                ));
                entries.push((
                    format!("cluster.{}.write-timeout", name),
                    option_to_string(runtime.write_timeout()),
                ));
                entries.push((
                    format!("cluster.{}.slowlog-log-slower-than", name),
                    entry.slowlog.threshold().to_string(),
                ));
                entries.push((
                    format!("cluster.{}.slowlog-max-len", name),
                    entry.slowlog.max_len().to_string(),
                ));
                let hotkey_cfg = entry.hotkey.config();
                entries.push((
                    format!("cluster.{}.hotkey-sample-every", name),
                    hotkey_cfg.sample_every.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.hotkey-sketch-width", name),
                    hotkey_cfg.sketch_width.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.hotkey-sketch-depth", name),
                    hotkey_cfg.sketch_depth.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.hotkey-capacity", name),
                    hotkey_cfg.capacity.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.hotkey-decay", name),
                    hotkey_cfg.decay.to_string(),
                ));
                let cache_cfg = &cluster.client_cache;
                entries.push((
                    format!("cluster.{}.client-cache-enabled", name),
                    cache_cfg.enabled.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.client-cache-max-entries", name),
                    cache_cfg.max_entries.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.client-cache-max-value-bytes", name),
                    cache_cfg.max_value_bytes.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.client-cache-shard-count", name),
                    cache_cfg.shard_count.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.client-cache-drain-batch", name),
                    cache_cfg.drain_batch.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.client-cache-drain-interval-ms", name),
                    cache_cfg.drain_interval_ms.to_string(),
                ));
                entries.push((
                    format!("cluster.{}.backup-request-enabled", name),
                    bool_to_string(entry.backup.enabled()),
                ));
                entries.push((
                    format!("cluster.{}.backup-request-threshold-ms", name),
                    option_to_string(entry.backup.threshold_ms()),
                ));
                entries.push((
                    format!("cluster.{}.backup-request-multiplier", name),
                    entry.backup.multiplier().to_string(),
                ));
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    async fn rewrite(&self) -> Result<()> {
        let snapshot = {
            let guard = self.config.read();
            toml::to_string_pretty(&*guard)?
        };
        fs::write(&self.path, snapshot)
            .await
            .with_context(|| format!("failed to persist configuration to {}", self.path.display()))
    }
}

fn parse_key(key: &str) -> Result<(String, ClusterField)> {
    let mut parts = key.splitn(3, '.');
    let scope = parts
        .next()
        .ok_or_else(|| anyhow!("invalid config parameter '{}'", key))?;
    if !scope.eq_ignore_ascii_case("cluster") {
        bail!("unsupported config parameter '{}'", key);
    }
    let cluster = parts
        .next()
        .ok_or_else(|| anyhow!("config parameter missing cluster name '{}'", key))?;
    let field = parts
        .next()
        .ok_or_else(|| anyhow!("config parameter missing field '{}'", key))?;
    let field = match field.to_ascii_lowercase().as_str() {
        "read-timeout" => ClusterField::ReadTimeout,
        "write-timeout" => ClusterField::WriteTimeout,
        "slowlog-log-slower-than" => ClusterField::SlowlogLogSlowerThan,
        "slowlog-max-len" => ClusterField::SlowlogMaxLen,
        "hotkey-sample-every" => ClusterField::HotkeySampleEvery,
        "hotkey-sketch-width" => ClusterField::HotkeySketchWidth,
        "hotkey-sketch-depth" => ClusterField::HotkeySketchDepth,
        "hotkey-capacity" => ClusterField::HotkeyCapacity,
        "hotkey-decay" => ClusterField::HotkeyDecay,
        "client-cache-enabled" => ClusterField::ClientCacheEnabled,
        "client-cache-max-entries" => ClusterField::ClientCacheMaxEntries,
        "client-cache-max-value-bytes" => ClusterField::ClientCacheMaxValueBytes,
        "client-cache-shard-count" => ClusterField::ClientCacheShardCount,
        "client-cache-drain-batch" => ClusterField::ClientCacheDrainBatch,
        "client-cache-drain-interval-ms" => ClusterField::ClientCacheDrainIntervalMs,
        "backup-request-enabled" => ClusterField::BackupRequestEnabled,
        "backup-request-threshold-ms" => ClusterField::BackupRequestThreshold,
        "backup-request-multiplier" => ClusterField::BackupRequestMultiplier,
        unknown => bail!("unknown cluster field '{}'", unknown),
    };
    Ok((cluster.to_string(), field))
}

fn parse_timeout_value(value: &str) -> Result<Option<u64>> {
    if value.eq_ignore_ascii_case(DUMP_VALUE_DEFAULT) {
        return Ok(None);
    }
    let trimmed = value.trim();
    let parsed: u64 = trimmed
        .parse()
        .with_context(|| format!("invalid timeout value '{}'", value))?;
    Ok(Some(parsed))
}

fn parse_slowlog_threshold(value: &str) -> Result<i64> {
    let parsed: i64 = value
        .trim()
        .parse()
        .with_context(|| format!("invalid slowlog-log-slower-than value '{}'", value))?;
    if parsed < -1 {
        bail!("slowlog-log-slower-than must be >= -1");
    }
    Ok(parsed)
}

fn parse_slowlog_len(value: &str) -> Result<usize> {
    let parsed: i64 = value
        .trim()
        .parse()
        .with_context(|| format!("invalid slowlog-max-len value '{}'", value))?;
    if parsed < 0 {
        bail!("slowlog-max-len must be >= 0");
    }
    usize::try_from(parsed).map_err(|_| anyhow!("slowlog-max-len is too large"))
}

fn parse_hotkey_sample_every(value: &str) -> Result<u64> {
    let parsed: u64 = value
        .trim()
        .parse()
        .with_context(|| format!("invalid hotkey-sample-every value '{}'", value))?;
    if parsed == 0 {
        bail!("hotkey-sample-every must be > 0");
    }
    Ok(parsed)
}

fn parse_hotkey_sketch_width(value: &str) -> Result<usize> {
    let parsed: usize = value
        .trim()
        .parse()
        .with_context(|| format!("invalid hotkey-sketch-width value '{}'", value))?;
    if parsed == 0 {
        bail!("hotkey-sketch-width must be > 0");
    }
    Ok(parsed)
}

fn parse_hotkey_sketch_depth(value: &str) -> Result<usize> {
    let parsed: usize = value
        .trim()
        .parse()
        .with_context(|| format!("invalid hotkey-sketch-depth value '{}'", value))?;
    if parsed == 0 {
        bail!("hotkey-sketch-depth must be > 0");
    }
    Ok(parsed)
}

fn parse_hotkey_capacity(value: &str) -> Result<usize> {
    let parsed: usize = value
        .trim()
        .parse()
        .with_context(|| format!("invalid hotkey-capacity value '{}'", value))?;
    if parsed == 0 {
        bail!("hotkey-capacity must be > 0");
    }
    Ok(parsed)
}

fn parse_hotkey_decay(value: &str) -> Result<f64> {
    let parsed: f64 = value
        .trim()
        .parse()
        .with_context(|| format!("invalid hotkey-decay value '{}'", value))?;
    if !(parsed > 0.0 && parsed <= 1.0) {
        bail!("hotkey-decay must be in (0, 1]");
    }
    Ok(parsed)
}

fn parse_bool_flag(value: &str, field: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!("invalid {} value '{}'", field, other),
    }
}

fn parse_positive_usize(value: &str, field: &str) -> Result<usize> {
    let parsed: i64 = value
        .trim()
        .parse()
        .with_context(|| format!("invalid {} value '{}'", field, value))?;
    if parsed <= 0 {
        bail!("{} must be > 0", field);
    }
    usize::try_from(parsed).map_err(|_| anyhow!("{} is too large", field))
}

fn parse_positive_u64(value: &str, field: &str) -> Result<u64> {
    let parsed: i64 = value
        .trim()
        .parse()
        .with_context(|| format!("invalid {} value '{}'", field, value))?;
    if parsed <= 0 {
        bail!("{} must be > 0", field);
    }
    Ok(parsed as u64)
}

fn parse_backup_multiplier(value: &str) -> Result<f64> {
    let parsed: f64 = value
        .trim()
        .parse()
        .with_context(|| format!("invalid backup-request-multiplier value '{}'", value))?;
    if parsed <= 0.0 {
        bail!("backup-request-multiplier must be > 0");
    }
    Ok(parsed)
}

fn option_to_string(value: Option<u64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| DUMP_VALUE_DEFAULT.to_string())
}

fn bool_to_string(value: bool) -> String {
    if value {
        "yes".to_string()
    } else {
        "no".to_string()
    }
}

fn flatten_pairs(entries: Vec<(String, String)>) -> Vec<RespValue> {
    let mut values = Vec::with_capacity(entries.len() * 2);
    for (key, value) in entries {
        values.push(RespValue::bulk(key));
        values.push(RespValue::bulk(value));
    }
    values
}

fn err_response<T: ToString>(message: T) -> RespValue {
    let payload = format!("ERR {}", message.to_string());
    RespValue::error(payload)
}

enum ClusterField {
    ReadTimeout,
    WriteTimeout,
    SlowlogLogSlowerThan,
    SlowlogMaxLen,
    HotkeySampleEvery,
    HotkeySketchWidth,
    HotkeySketchDepth,
    HotkeyCapacity,
    HotkeyDecay,
    ClientCacheEnabled,
    ClientCacheMaxEntries,
    ClientCacheMaxValueBytes,
    ClientCacheShardCount,
    ClientCacheDrainBatch,
    ClientCacheDrainIntervalMs,
    BackupRequestEnabled,
    BackupRequestThreshold,
    BackupRequestMultiplier,
}

fn wildcard_match(pattern: &str, target: &str) -> bool {
    let pattern = pattern.as_bytes();
    let target = target.as_bytes();
    let mut p = 0usize;
    let mut t = 0usize;
    let mut star = None;
    let mut match_idx = 0usize;

    while t < target.len() {
        if p < pattern.len() && (pattern[p] == target[t] || pattern[p] == b'?') {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            match_idx = t;
            p += 1;
        } else if let Some(star_idx) = star {
            p = star_idx + 1;
            match_idx += 1;
            t = match_idx;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AuthUserConfig, BackendAuthConfig, FrontendAuthConfig, FrontendAuthTable};
    use crate::protocol::redis::RespVersion;
    use std::env;
    use std::sync::Mutex;

    static ENV_GUARD: Mutex<()> = Mutex::new(());

    fn base_cluster() -> ClusterConfig {
        ClusterConfig {
            name: "alpha".into(),
            listen_addr: "127.0.0.1:7000".into(),
            hash_tag: None,
            thread: Some(2),
            cache_type: CacheType::Redis,
            read_timeout: Some(1500),
            write_timeout: Some(1500),
            servers: vec!["127.0.0.1:6379".into()],
            fetch_interval: None,
            read_from_slave: None,
            ping_fail_limit: Some(1),
            ping_interval: Some(60),
            ping_succ_interval: Some(120),
            dial_timeout: None,
            listen_proto: None,
            node_connections: Some(2),
            auth: None,
            password: None,
            backend_auth: None,
            backend_password: None,
            slowlog_log_slower_than: default_slowlog_log_slower_than(),
            slowlog_max_len: default_slowlog_max_len(),
            hotkey_sample_every: default_hotkey_sample_every(),
            hotkey_sketch_width: default_hotkey_sketch_width(),
            hotkey_sketch_depth: default_hotkey_sketch_depth(),
            hotkey_capacity: default_hotkey_capacity(),
            hotkey_decay: default_hotkey_decay(),
            backend_resp_version: RespVersion::Resp2,
            client_cache: ClientCacheConfig::default(),
            backup_request: BackupRequestConfig::default(),
        }
    }

    fn config_with_single_cluster(cluster: ClusterConfig) -> Config {
        Config {
            clusters: vec![cluster],
        }
    }

    #[test]
    fn cluster_config_validation_succeeds_for_minimal_setup() {
        let cfg = base_cluster();
        cfg.ensure_valid().expect("valid cluster");
    }

    #[test]
    fn cluster_config_validation_detects_missing_servers() {
        let mut cfg = base_cluster();
        cfg.servers.clear();
        assert!(cfg.ensure_valid().is_err());
    }

    #[test]
    fn listen_port_parses_hostname_style_addresses() {
        let mut cfg = base_cluster();
        cfg.listen_addr = "cache.example.com:8888".into();
        assert_eq!(cfg.listen_port().unwrap(), 8888);
    }

    #[test]
    fn frontend_auth_users_prefers_explicit_acl_config() {
        let mut cfg = base_cluster();
        cfg.auth = Some(FrontendAuthConfig::Detailed(FrontendAuthTable {
            password: Some("shared".into()),
            users: vec![AuthUserConfig {
                username: "extra".into(),
                password: "xyz".into(),
            }],
        }));
        cfg.password = Some("legacy".into());
        let users = cfg.frontend_auth_users().expect("auth users");
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].username, "default");
        assert_eq!(users[0].password, "shared");
    }

    #[test]
    fn frontend_auth_users_falls_back_to_password_field() {
        let mut cfg = base_cluster();
        cfg.password = Some("legacy".into());
        let users = cfg.frontend_auth_users().expect("auth users");
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "default");
        assert_eq!(users[0].password, "legacy");
    }

    #[test]
    fn backend_auth_prefers_acl_config() {
        let mut cfg = base_cluster();
        cfg.backend_auth = Some(BackendAuthConfig::Credential {
            username: "user".into(),
            password: "pw".into(),
        });
        cfg.backend_password = Some("legacy".into());
        let auth = cfg.backend_auth_config().expect("backend auth");
        match auth {
            BackendAuthConfig::Credential { username, password } => {
                assert_eq!(username, "user");
                assert_eq!(password, "pw");
            }
            BackendAuthConfig::Password(_) => panic!("expected credential variant"),
        }
    }

    #[test]
    fn backend_auth_falls_back_to_password_field() {
        let mut cfg = base_cluster();
        cfg.backend_password = Some("legacy".into());
        let auth = cfg.backend_auth_config().expect("backend auth");
        match auth {
            BackendAuthConfig::Password(password) => assert_eq!(password, "legacy"),
            _ => panic!("expected password variant"),
        }
    }

    #[test]
    fn apply_defaults_uses_env_override_for_threads() {
        let _guard = ENV_GUARD.lock().unwrap();
        let previous = env::var(ENV_DEFAULT_THREADS).ok();
        env::set_var(ENV_DEFAULT_THREADS, "11");
        let mut cfg = config_with_single_cluster(ClusterConfig {
            thread: None,
            ..base_cluster()
        });
        cfg.apply_defaults();
        let thread = cfg.clusters()[0].thread.expect("thread assigned");
        if let Some(val) = previous {
            env::set_var(ENV_DEFAULT_THREADS, val);
        } else {
            env::remove_var(ENV_DEFAULT_THREADS);
        }
        assert_eq!(thread, 11);
    }

    #[test]
    fn parse_port_handles_ipv6_endpoints() {
        let port = parse_port("[2001:db8::1]:7100").expect("ipv6 port");
        assert_eq!(port, 7100);
    }

    #[test]
    fn parse_key_recognizes_known_fields() {
        let (cluster, field) = parse_key("cluster.alpha.hotkey-decay").expect("key parsed");
        assert_eq!(cluster, "alpha");
        assert!(matches!(field, ClusterField::HotkeyDecay));
    }

    #[test]
    fn parse_key_rejects_unknown_fields() {
        assert!(parse_key("cluster.alpha.unknown").is_err());
    }

    #[test]
    fn parse_timeout_value_handles_default_marker() {
        assert_eq!(parse_timeout_value("default").unwrap(), None);
        assert_eq!(parse_timeout_value("250").unwrap(), Some(250));
        assert!(parse_timeout_value("oops").is_err());
    }

    #[test]
    fn parse_bool_flag_understands_common_aliases() {
        assert!(parse_bool_flag("YES", "flag").unwrap());
        assert!(!parse_bool_flag("0", "flag").unwrap());
        assert!(parse_bool_flag("maybe", "flag").is_err());
    }

    #[test]
    fn wildcard_match_supports_basic_patterns() {
        assert!(wildcard_match("cluster.*", "cluster.alpha"));
        assert!(wildcard_match("cache-?", "cache-a"));
        assert!(!wildcard_match("cache-?", "cache-long"));
    }

    #[test]
    fn option_atomic_roundtrip() {
        assert_eq!(atomic_to_option(option_to_atomic(Some(42))), Some(42));
        assert_eq!(atomic_to_option(option_to_atomic(None)), None);
    }
}
