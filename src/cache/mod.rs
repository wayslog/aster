use std::cmp::{max, Reverse};
use std::collections::BinaryHeap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ahash::{AHasher, RandomState};
use anyhow::{bail, Result};
use arc_swap::ArcSwap;
use bytes::Bytes;
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::warn;

use crate::config::ClientCacheConfig;
use crate::metrics;
use crate::protocol::redis::{RedisCommand, RespValue};

pub mod tracker;

const STATE_DISABLED: u8 = 0;
const STATE_ENABLED: u8 = 1;
const STATE_DRAINING: u8 = 2;

const MAX_MULTI_KEYS: usize = 64;

/// Operational state for the cache, observable by trackers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheState {
    Disabled,
    Enabled,
    Draining,
}

impl CacheState {
    fn from_u8(value: u8) -> Self {
        match value {
            STATE_ENABLED => CacheState::Enabled,
            STATE_DRAINING => CacheState::Draining,
            _ => CacheState::Disabled,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            CacheState::Disabled => STATE_DISABLED,
            CacheState::Enabled => STATE_ENABLED,
            CacheState::Draining => STATE_DRAINING,
        }
    }
}

/// High-performance local cache with RESP3 invalidation support.
pub struct ClientCache {
    cluster: Arc<str>,
    state: AtomicU8,
    resp3_ready: bool,
    shards: ArcSwap<Vec<CacheShard>>,
    config: RwLock<ClientCacheConfig>,
    drain_handle: Mutex<Option<JoinHandle<()>>>,
    state_tx: watch::Sender<CacheState>,
}

impl fmt::Debug for ClientCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCache")
            .field("cluster", &self.cluster)
            .field("state", &self.state())
            .field("resp3_ready", &self.resp3_ready)
            .finish_non_exhaustive()
    }
}

impl ClientCache {
    pub fn new(cluster: Arc<str>, config: ClientCacheConfig, resp3_ready: bool) -> Self {
        let shard_count = config.shard_count.max(1);
        let per_shard = entries_per_shard(config.max_entries, shard_count);
        let shards = (0..shard_count)
            .map(|_| CacheShard::new(per_shard))
            .collect::<Vec<_>>();
        let initial_state = if config.enabled && resp3_ready {
            CacheState::Enabled
        } else {
            CacheState::Disabled
        };
        let (state_tx, _state_rx) = watch::channel(initial_state);
        let cache = Self {
            cluster,
            state: AtomicU8::new(initial_state.as_u8()),
            resp3_ready,
            shards: ArcSwap::from_pointee(shards),
            config: RwLock::new(ClientCacheConfig { enabled: initial_state == CacheState::Enabled, ..config }),
            drain_handle: Mutex::new(None),
            state_tx,
        };
        if !cache.resp3_ready && cache.config.read().enabled {
            warn!(cluster = %cache.cluster, "client cache enabled in config but backend RESP3 is unavailable; keeping disabled");
            cache.state.store(STATE_DISABLED, Ordering::SeqCst);
            cache.state_tx.send_replace(CacheState::Disabled);
        }
        cache
    }

    pub fn state(&self) -> CacheState {
        CacheState::from_u8(self.state.load(Ordering::Relaxed))
    }

    pub fn subscribe(&self) -> watch::Receiver<CacheState> {
        self.state_tx.subscribe()
    }

    pub fn enable(self: &Arc<Self>) -> Result<()> {
        if !self.resp3_ready {
            bail!("client cache requires RESP3 backend support");
        }
        let prev = self
            .state
            .swap(STATE_ENABLED, Ordering::SeqCst);
        self.stop_drain_task();
        self.state_tx.send_replace(CacheState::Enabled);
        if prev != STATE_ENABLED {
            metrics::client_cache_state(self.cluster.as_ref(), "enabled");
        }
        {
            let mut cfg = self.config.write();
            cfg.enabled = true;
        }
        Ok(())
    }

    pub fn disable(self: &Arc<Self>) {
        let prev = self
            .state
            .swap(STATE_DRAINING, Ordering::SeqCst);
        if prev == STATE_DISABLED {
            self.state_tx.send_replace(CacheState::Disabled);
            return;
        }
        self.state_tx.send_replace(CacheState::Draining);
        metrics::client_cache_state(self.cluster.as_ref(), "draining");
        {
            let mut cfg = self.config.write();
            cfg.enabled = false;
        }
        self.start_drain_task();
    }

    pub fn lookup(&self, command: &RedisCommand) -> Option<RespValue> {
        if self.state() != CacheState::Enabled {
            return None;
        }
        match classify_read(command) {
            CacheRead::Single { kind, key, field } => {
                let hit = self.lookup_single(kind, key, field);
                metrics::client_cache_lookup(
                    self.cluster.as_ref(),
                    kind.label(),
                    hit.is_some(),
                );
                hit
            }
            CacheRead::Multi { keys } => {
                let hit = self.lookup_multi(&keys);
                metrics::client_cache_lookup(self.cluster.as_ref(), "multi", hit.is_some());
                hit.map(RespValue::Array)
            }
            CacheRead::Unsupported => None,
        }
    }

    pub fn store(&self, command: &RedisCommand, response: &RespValue) {
        if self.state() != CacheState::Enabled {
            return;
        }
        if response.is_error() {
            return;
        }
        let snapshot = self.config.read().clone();
        match classify_read(command) {
            CacheRead::Single { kind, key, field } => {
                self.store_single(&snapshot, kind, key, field, response);
            }
            CacheRead::Multi { keys } => {
                self.store_multi(&snapshot, &keys, response);
            }
            CacheRead::Unsupported => {}
        }
    }

    /// Returns true if this command is a cacheable read handled by the client cache.
    pub fn is_cacheable_read(command: &RedisCommand) -> bool {
        matches!(
            classify_read(command),
            CacheRead::Single { .. } | CacheRead::Multi { .. }
        )
    }

    /// Returns true if this command can invalidate cached keys.
    pub fn is_invalidating_write(command: &RedisCommand) -> bool {
        classify_write(command).is_some()
    }

    pub fn invalidate_bytes<B: AsRef<[u8]>>(&self, keys: &[B]) {
        if keys.is_empty() {
            return;
        }
        let shards = self.shards.load();
        let shard_total = shards.len().max(1);
        drop(shards);
        let mut removed = 0usize;
        for key in keys {
            removed += self.invalidate_primary(key.as_ref(), shard_total);
        }
        if removed > 0 {
            metrics::client_cache_invalidate(self.cluster.as_ref(), removed);
        }
    }

    pub fn invalidate_command(&self, command: &RedisCommand) {
        if let Some(action) = classify_write(command) {
            match action {
                CacheWrite::FlushAll => self.flush(),
                CacheWrite::Keys(keys) => {
                    let total = self.shards.load().len().max(1);
                    for key in keys {
                        self.invalidate_primary(key.as_ref(), total);
                    }
                }
            }
        }
    }

    pub fn flush(&self) {
        let shard_count = self.shards.load().len().max(1);
        self.rebuild_shards(shard_count);
        metrics::client_cache_state(self.cluster.as_ref(), "flushed");
    }

    pub fn set_max_entries(&self, value: usize) {
        {
            let mut cfg = self.config.write();
            cfg.max_entries = value.max(1);
        }
        let shards = self.shards.load();
        let per_shard = entries_per_shard(value, shards.len().max(1));
        for shard in shards.iter() {
            shard.set_capacity(per_shard);
        }
    }

    pub fn set_max_value_bytes(&self, value: usize) {
        let mut cfg = self.config.write();
        cfg.max_value_bytes = value.max(1);
    }

    pub fn set_shard_count(self: &Arc<Self>, count: usize) {
        let count = count.max(1).min(usize::MAX / 2);
        {
            let mut cfg = self.config.write();
            cfg.shard_count = count;
        }
        self.rebuild_shards(count);
    }

    pub fn set_drain_batch(&self, value: usize) {
        let mut cfg = self.config.write();
        cfg.drain_batch = value.max(1);
    }

    pub fn set_drain_interval(&self, value: u64) {
        let mut cfg = self.config.write();
        cfg.drain_interval_ms = value.max(1);
    }

    fn lookup_single(
        &self,
        kind: CacheCommandKind,
        key: &Bytes,
        field: Option<&Bytes>,
    ) -> Option<RespValue> {
        let shards = self.shards.load();
        let index = shard_index(key.as_ref(), shards.len().max(1));
        shards[index].get(kind, key, field)
    }

    fn lookup_multi(&self, keys: &[&Bytes]) -> Option<Vec<RespValue>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.lookup_single(CacheCommandKind::Value, key, None) {
                results.push(value);
            } else {
                return None;
            }
        }
        Some(results)
    }

    fn store_single(
        &self,
        config: &ClientCacheConfig,
        kind: CacheCommandKind,
        key: &Bytes,
        field: Option<&Bytes>,
        response: &RespValue,
    ) {
        let normalized = match normalize_value(kind, response) {
            Some(value) => value,
            None => return,
        };
        if resp_size(&normalized) > config.max_value_bytes {
            return;
        }
        let entry = CacheEntry::new(normalized);
        let cache_key = CacheKey::new(kind, key.clone(), field.cloned());
        let shards = self.shards.load();
        let index = shard_index(cache_key.primary.as_ref(), shards.len().max(1));
        shards[index].put(cache_key, entry);
        metrics::client_cache_store(self.cluster.as_ref(), kind.label());
    }

    fn store_multi(
        &self,
        config: &ClientCacheConfig,
        keys: &[&Bytes],
        response: &RespValue,
    ) {
        let values = match response.as_array() {
            Some(values) if values.len() == keys.len() => values,
            _ => return,
        };
        for (key, value) in keys.iter().zip(values.iter()) {
            self.store_single(config, CacheCommandKind::Value, key, None, value);
        }
    }

    fn invalidate_primary(&self, primary: &[u8], shard_total: usize) -> usize {
        let shards = self.shards.load();
        let idx = shard_index(primary, shard_total);
        shards[idx].remove_primary(primary)
    }

    fn rebuild_shards(&self, count: usize) {
        let cfg = self.config.read().clone();
        let per_shard = entries_per_shard(cfg.max_entries, count);
        let shards = (0..count)
            .map(|_| CacheShard::new(per_shard))
            .collect::<Vec<_>>();
        self.shards.store(Arc::new(shards));
    }

    fn start_drain_task(self: &Arc<Self>) {
        let mut guard = self.drain_handle.lock();
        if let Some(handle) = guard.take() {
            handle.abort();
        }
        let weak = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            while let Some(cache) = weak.upgrade() {
                let batch = cache.config.read().drain_batch;
                let interval = cache.config.read().drain_interval_ms;
                let removed = cache.drain_once(batch);
                if removed == 0 {
                    cache.finish_draining();
                    break;
                }
                sleep(Duration::from_millis(interval)).await;
            }
        });
        *guard = Some(handle);
    }

    fn stop_drain_task(&self) {
        if let Some(handle) = self.drain_handle.lock().take() {
            handle.abort();
        }
    }

    fn finish_draining(&self) {
        self.state.store(STATE_DISABLED, Ordering::SeqCst);
        self.state_tx.send_replace(CacheState::Disabled);
        metrics::client_cache_state(self.cluster.as_ref(), "disabled");
    }

    fn drain_once(&self, batch: usize) -> usize {
        let shards = self.shards.load();
        let len = shards.len().max(1);
        let per_shard = max(1, (batch.max(1) + len - 1) / len);
        let mut removed = 0usize;
        for shard in shards.iter() {
            removed += shard.evict_batch(per_shard);
        }
        removed
    }
}

fn normalize_value(kind: CacheCommandKind, resp: &RespValue) -> Option<RespValue> {
    match kind {
        CacheCommandKind::Value => match resp {
            RespValue::BulkString(_) | RespValue::SimpleString(_) | RespValue::Null | RespValue::NullBulk =>
                Some(resp.clone()),
            _ => None,
        },
        CacheCommandKind::HashField => match resp {
            RespValue::BulkString(_) | RespValue::SimpleString(_) | RespValue::Null | RespValue::NullBulk =>
                Some(resp.clone()),
            _ => None,
        },
    }
}

fn resp_size(value: &RespValue) -> usize {
    match value {
        RespValue::SimpleString(data)
        | RespValue::BulkString(data)
        | RespValue::Error(data)
        | RespValue::BlobError(data)
        | RespValue::Double(data)
        | RespValue::BigNumber(data) => data.len(),
        RespValue::Integer(_) => std::mem::size_of::<i64>(),
        RespValue::Null
        | RespValue::NullBulk
        | RespValue::NullArray => 1,
        RespValue::Boolean(_) => 1,
        RespValue::Map(entries) | RespValue::Attribute(entries) => {
            entries.iter().map(|(k, v)| resp_size(k) + resp_size(v)).sum()
        }
        RespValue::Array(values) | RespValue::Set(values) | RespValue::Push(values) => {
            values.iter().map(resp_size).sum()
        }
        RespValue::VerbatimString { data, .. } => data.len(),
    }
}

fn shard_index(key: &[u8], shards: usize) -> usize {
    let mut hasher = AHasher::default();
    hasher.write(key);
    (hasher.finish() as usize) % shards.max(1)
}

fn entries_per_shard(entries: usize, shards: usize) -> usize {
    let shards = shards.max(1);
    let per = (entries + shards - 1) / shards;
    per.max(1)
}

fn upper_name(input: &[u8]) -> Vec<u8> {
    input.iter().map(|b| b.to_ascii_uppercase()).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CacheCommandKind {
    Value,
    HashField,
}

impl CacheCommandKind {
    fn label(self) -> &'static str {
        match self {
            CacheCommandKind::Value => "value",
            CacheCommandKind::HashField => "hash_field",
        }
    }
}

#[derive(Debug, Clone)]
struct CacheEntry {
    value: RespValue,
    access: u64,
}

impl CacheEntry {
    fn new(value: RespValue) -> Self {
        Self {
            value,
            access: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    kind: CacheCommandKind,
    primary: Bytes,
    secondary: Option<Bytes>,
}

impl CacheKey {
    fn new(kind: CacheCommandKind, primary: Bytes, secondary: Option<Bytes>) -> Self {
        Self {
            kind,
            primary,
            secondary,
        }
    }
}

#[derive(Debug)]
struct CacheShard {
    inner: Mutex<CacheShardInner>,
}

impl CacheShard {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(CacheShardInner::new(capacity)),
        }
    }

    fn get(
        &self,
        kind: CacheCommandKind,
        key: &Bytes,
        field: Option<&Bytes>,
    ) -> Option<RespValue> {
        let mut guard = self.inner.lock();
        guard.touch(&CacheKey::new(kind, key.clone(), field.cloned()))
    }

    fn put(&self, key: CacheKey, entry: CacheEntry) {
        let mut guard = self.inner.lock();
        guard.insert(key, entry);
    }

    fn remove_primary(&self, primary: &[u8]) -> usize {
        let mut guard = self.inner.lock();
        guard.remove_primary(primary)
    }

    fn set_capacity(&self, capacity: usize) {
        let mut guard = self.inner.lock();
        guard.set_capacity(capacity);
    }

    fn evict_batch(&self, batch: usize) -> usize {
        let mut guard = self.inner.lock();
        guard.evict_batch(batch)
    }
}

#[derive(Debug)]
struct CacheShardInner {
    entries: HashMap<CacheKey, CacheEntry, RandomState>,
    order: BinaryHeap<Reverse<HeapEntry>>,
    per_key: HashMap<Vec<u8>, SmallVec<[CacheKey; 4]>, RandomState>,
    counter: u64,
    capacity: usize,
}

impl CacheShardInner {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_hasher(RandomState::new()),
            order: BinaryHeap::new(),
            per_key: HashMap::with_hasher(RandomState::new()),
            counter: 0,
            capacity: capacity.max(1),
        }
    }

    fn touch(&mut self, key: &CacheKey) -> Option<RespValue> {
        if !self.entries.contains_key(key) {
            return None;
        }
        let next_access = self.next_access();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.access = next_access;
            self.order
                .push(Reverse(HeapEntry::new(next_access, key.clone())));
            Some(entry.value.clone())
        } else {
            None
        }
    }

    fn insert(&mut self, key: CacheKey, mut entry: CacheEntry) {
        entry.access = self.next_access();
        if let Some(old) = self.entries.insert(key.clone(), entry.clone()) {
            self.detach(&key);
            drop(old);
        }
        self.attach(key.clone());
        self.order
            .push(Reverse(HeapEntry::new(entry.access, key.clone())));
        self.enforce_capacity();
    }

    fn remove_primary(&mut self, primary: &[u8]) -> usize {
        let keys = match self.per_key.remove(primary) {
            Some(keys) => keys,
            None => return 0,
        };
        let mut removed = 0usize;
        for cache_key in keys {
            if self.entries.remove(&cache_key).is_some() {
                removed += 1;
            }
        }
        removed
    }

    fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity.max(1);
        self.enforce_capacity();
    }

    fn evict_batch(&mut self, batch: usize) -> usize {
        let mut removed = 0usize;
        for _ in 0..batch.max(1) {
            if self.pop_lru().is_some() {
                removed += 1;
            } else {
                break;
            }
        }
        removed
    }

    fn attach(&mut self, key: CacheKey) {
        self.per_key
            .entry(key.primary.to_vec())
            .or_default()
            .push(key);
    }

    fn detach(&mut self, key: &CacheKey) {
        if let Some(list) = self.per_key.get_mut(key.primary.as_ref()) {
            if let Some(pos) = list.iter().position(|existing| existing == key) {
                list.swap_remove(pos);
            }
            if list.is_empty() {
                self.per_key.remove(key.primary.as_ref());
            }
        }
    }

    fn enforce_capacity(&mut self) {
        while self.entries.len() > self.capacity {
            if self.pop_lru().is_none() {
                break;
            }
        }
    }

    fn pop_lru(&mut self) -> Option<(CacheKey, CacheEntry)> {
        while let Some(Reverse(entry)) = self.order.pop() {
            if let Some(stored) = self.entries.get(&entry.key) {
                if stored.access == entry.access {
                    self.detach(&entry.key);
                    return self.entries.remove(&entry.key).map(|value| (entry.key, value));
                }
            }
        }
        None
    }

    fn next_access(&mut self) -> u64 {
        self.counter = self.counter.wrapping_add(1);
        self.counter
    }
}

#[derive(Debug, Clone)]
struct HeapEntry {
    access: u64,
    key: CacheKey,
}

impl HeapEntry {
    fn new(access: u64, key: CacheKey) -> Self {
        Self { access, key }
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.access == other.access && self.key == other.key
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.access.cmp(&other.access)
    }
}

#[derive(Debug)]
enum CacheRead<'a> {
    Single {
        kind: CacheCommandKind,
        key: &'a Bytes,
        field: Option<&'a Bytes>,
    },
    Multi {
        keys: SmallVec<[&'a Bytes; MAX_MULTI_KEYS]>,
    },
    Unsupported,
}

#[derive(Debug)]
enum CacheWrite<'a> {
    FlushAll,
    Keys(SmallVec<[&'a Bytes; MAX_MULTI_KEYS]>),
}

fn classify_read(command: &RedisCommand) -> CacheRead<'_> {
    let args = command.args();
    if args.is_empty() {
        return CacheRead::Unsupported;
    }
    let upper = upper_name(command.command_name());
    match upper.as_slice() {
        b"GET" => {
            if args.len() < 2 {
                return CacheRead::Unsupported;
            }
            CacheRead::Single {
                kind: CacheCommandKind::Value,
                key: &args[1],
                field: None,
            }
        }
        b"HGET" => {
            if args.len() < 3 {
                return CacheRead::Unsupported;
            }
            CacheRead::Single {
                kind: CacheCommandKind::HashField,
                key: &args[1],
                field: Some(&args[2]),
            }
        }
        b"MGET" => {
            if args.len() < 2 || args.len() - 1 > MAX_MULTI_KEYS {
                return CacheRead::Unsupported;
            }
            let mut keys = SmallVec::<[&Bytes; MAX_MULTI_KEYS]>::new();
            for key in &args[1..] {
                keys.push(key);
            }
            CacheRead::Multi { keys }
        }
        _ => CacheRead::Unsupported,
    }
}

fn classify_write(command: &RedisCommand) -> Option<CacheWrite<'_>> {
    let args = command.args();
    if args.is_empty() {
        return None;
    }
    let name = upper_name(command.command_name());
    match name.as_slice() {
        b"FLUSHALL" | b"FLUSHDB" => Some(CacheWrite::FlushAll),
        b"DEL" | b"UNLINK" => {
            if args.len() < 2 {
                return None;
            }
            let mut keys = SmallVec::<[&Bytes; MAX_MULTI_KEYS]>::new();
            for key in &args[1..] {
                if keys.len() >= MAX_MULTI_KEYS {
                    break;
                }
                keys.push(key);
            }
            Some(CacheWrite::Keys(keys))
        }
        b"SET" | b"SETEX" | b"PSETEX" | b"SETNX" | b"HSET" | b"HDEL" => {
            if args.len() < 2 {
                return None;
            }
            let mut keys = SmallVec::<[&Bytes; MAX_MULTI_KEYS]>::new();
            keys.push(&args[1]);
            Some(CacheWrite::Keys(keys))
        }
        b"MSET" => {
            if args.len() < 3 {
                return None;
            }
            let mut keys = SmallVec::<[&Bytes; MAX_MULTI_KEYS]>::new();
            let mut idx = 1;
            while idx < args.len() {
                if keys.len() >= MAX_MULTI_KEYS {
                    break;
                }
                keys.push(&args[idx]);
                idx += 2;
            }
            Some(CacheWrite::Keys(keys))
        }
        _ => None,
    }
}
