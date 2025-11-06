use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{info, warn};

use crate::auth::{AuthUserConfig, BackendAuthConfig, FrontendAuthConfig};
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
            clusters.insert(
                key,
                ClusterEntry {
                    index,
                    runtime,
                    slowlog,
                    hotkey,
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

fn option_to_string(value: Option<u64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| DUMP_VALUE_DEFAULT.to_string())
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
