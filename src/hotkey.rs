use std::cmp::Ordering as CmpOrdering;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use hashbrown::HashMap;

use crate::protocol::redis::{RedisCommand, RespValue};

pub const DEFAULT_SAMPLE_EVERY: u64 = 32;
pub const DEFAULT_SKETCH_WIDTH: usize = 4096;
pub const DEFAULT_SKETCH_DEPTH: usize = 4;
pub const DEFAULT_HOTKEY_CAPACITY: usize = 512;
pub const DEFAULT_DECAY: f64 = 0.925;
const HEAVY_BUCKET_SIZE: usize = 8;
const RNG_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Clone, Copy, Debug)]
pub struct HotkeyConfig {
    pub sample_every: u64,
    pub sketch_width: usize,
    pub sketch_depth: usize,
    pub capacity: usize,
    pub decay: f64,
}

impl Default for HotkeyConfig {
    fn default() -> Self {
        Self {
            sample_every: DEFAULT_SAMPLE_EVERY,
            sketch_width: DEFAULT_SKETCH_WIDTH,
            sketch_depth: DEFAULT_SKETCH_DEPTH,
            capacity: DEFAULT_HOTKEY_CAPACITY,
            decay: DEFAULT_DECAY,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HotkeySample {
    pub key: Bytes,
    pub estimated_hits: u64,
    pub error: u64,
}

pub struct Hotkey {
    enabled: AtomicBool,
    core: ArcSwap<HotkeyCore>,
}

impl std::fmt::Debug for Hotkey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let core = self.core.load();
        f.debug_struct("Hotkey")
            .field("enabled", &self.enabled.load(Ordering::Relaxed))
            .field("config", &core.config())
            .finish()
    }
}

impl Hotkey {
    pub fn new(config: HotkeyConfig) -> Self {
        let core = Arc::new(HotkeyCore::new(config));
        Self {
            enabled: AtomicBool::new(false),
            core: ArcSwap::from(core),
        }
    }

    pub fn new_default() -> Self {
        Self::new(HotkeyConfig::default())
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.core.load().reset();
    }

    pub fn config(&self) -> HotkeyConfig {
        self.core.load().config()
    }

    pub fn reconfigure(&self, config: HotkeyConfig) {
        let enabled = self.is_enabled();
        let new_core = Arc::new(HotkeyCore::new(config));
        self.core.store(new_core);
        if !enabled {
            self.disable();
        }
    }

    pub fn update_config<F>(&self, mutate: F)
    where
        F: FnOnce(&mut HotkeyConfig),
    {
        let mut config = self.config();
        mutate(&mut config);
        self.reconfigure(config);
    }

    pub fn record_command(&self, command: &RedisCommand) {
        if !self.is_enabled() {
            return;
        }
        let core = self.core.load();
        core.record_command(command);
    }

    pub fn snapshot(&self, limit: Option<usize>) -> Vec<HotkeySample> {
        self.core.load().snapshot(limit)
    }
}

struct HotkeyCore {
    config: HotkeyConfig,
    sample_counter: AtomicU64,
    sketch: CountMinSketch,
    heavy: Vec<HeavyBucket>,
    decay: f64,
    rng_state: AtomicU64,
}

impl HotkeyCore {
    fn new(config: HotkeyConfig) -> Self {
        let capacity = config.capacity.max(HEAVY_BUCKET_SIZE).next_power_of_two();
        let bucket_count = (capacity / HEAVY_BUCKET_SIZE).max(1);
        let heavy = (0..bucket_count).map(|_| HeavyBucket::new()).collect();
        Self {
            config,
            sample_counter: AtomicU64::new(0),
            sketch: CountMinSketch::new(config.sketch_width, config.sketch_depth),
            heavy,
            decay: config.decay,
            rng_state: AtomicU64::new(splitmix64(RNG_SEED)),
        }
    }

    fn config(&self) -> HotkeyConfig {
        self.config
    }

    fn reset(&self) {
        self.sketch.reset();
        for bucket in &self.heavy {
            for entry in &bucket.entries {
                entry.fingerprint.store(0, Ordering::Relaxed);
                entry.score.store(0, Ordering::Relaxed);
                entry.key.store(None);
            }
        }
        self.sample_counter.store(0, Ordering::Relaxed);
        self.rng_state
            .store(splitmix64(RNG_SEED), Ordering::Relaxed);
    }

    fn record_command(&self, command: &RedisCommand) {
        if !self.should_sample() {
            return;
        }

        let args = command.args();
        if args.len() < 2 {
            return;
        }
        if command.command_name().eq_ignore_ascii_case(b"HOTKEY") {
            return;
        }

        let name = command.command_name();
        if name.eq_ignore_ascii_case(b"MGET")
            || name.eq_ignore_ascii_case(b"DEL")
            || name.eq_ignore_ascii_case(b"UNLINK")
            || name.eq_ignore_ascii_case(b"EXISTS")
            || name.eq_ignore_ascii_case(b"TOUCH")
        {
            for key in args.iter().skip(1) {
                self.observe(key);
            }
        } else if name.eq_ignore_ascii_case(b"MSET")
            || name.eq_ignore_ascii_case(b"MSETNX")
            || name.eq_ignore_ascii_case(b"HMSET")
        {
            for key in args.iter().skip(1).step_by(2) {
                self.observe(key);
            }
        } else {
            self.observe(&args[1]);
        }
    }

    fn snapshot(&self, limit: Option<usize>) -> Vec<HotkeySample> {
        let mut candidates: HashMap<Bytes, u64> =
            HashMap::with_capacity(self.heavy.len() * HEAVY_BUCKET_SIZE);
        let error_base = self
            .sketch
            .error_bound()
            .saturating_mul(self.config.sample_every);

        for bucket in &self.heavy {
            for entry in &bucket.entries {
                let score = entry.score.load(Ordering::Relaxed);
                if score == 0 {
                    continue;
                }
                if let Some(key_arc) = entry.key.load_full() {
                    let key = (*key_arc).clone();
                    let estimate = self
                        .sketch
                        .estimate(key.as_ref())
                        .saturating_mul(self.config.sample_every);
                    candidates
                        .entry(key)
                        .and_modify(|existing| {
                            if estimate > *existing {
                                *existing = estimate;
                            }
                        })
                        .or_insert(estimate);
                }
            }
        }

        let mut entries = candidates
            .into_iter()
            .map(|(key, estimated)| HotkeySample {
                key,
                estimated_hits: estimated,
                error: error_base,
            })
            .collect::<Vec<_>>();

        entries.sort_by(|a, b| match b.estimated_hits.cmp(&a.estimated_hits) {
            CmpOrdering::Equal => a.key.cmp(&b.key),
            other => other,
        });

        if let Some(limit) = limit {
            entries.truncate(limit);
        }
        entries
    }

    fn observe(&self, key: &Bytes) {
        let fingerprint = hash_with_seed(key.as_ref(), RNG_SEED);
        let estimate = self.sketch.increment(key.as_ref(), 1);
        self.update_heavy(key, fingerprint, estimate);
    }

    fn update_heavy(&self, key: &Bytes, fingerprint: u64, estimate: u64) {
        if self.heavy.is_empty() {
            return;
        }
        let bucket_idx = (fingerprint as usize) % self.heavy.len();
        let bucket = &self.heavy[bucket_idx];

        for entry in &bucket.entries {
            if entry.fingerprint.load(Ordering::Relaxed) != fingerprint {
                continue;
            }
            if let Some(existing) = entry.key.load_full() {
                if existing.as_ref() == key {
                    self.bump_entry(entry, estimate);
                    return;
                }
            }
        }

        for entry in &bucket.entries {
            if entry.score.load(Ordering::Relaxed) == 0 {
                if entry
                    .score
                    .compare_exchange(0, estimate, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    entry.fingerprint.store(fingerprint, Ordering::Relaxed);
                    entry.key.store(Some(Arc::new(key.clone())));
                    return;
                }
            }
        }

        let mut victim_idx = 0usize;
        let mut victim_score = u64::MAX;
        for (idx, entry) in bucket.entries.iter().enumerate() {
            let current = entry.score.load(Ordering::Relaxed);
            if current < victim_score {
                victim_score = current;
                victim_idx = idx;
            }
            let chance = self.decay.powf(current as f64).clamp(0.0, 1.0);
            if self.random_unit() < chance {
                if current > 0
                    && entry
                        .score
                        .compare_exchange(
                            current,
                            current - 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                {
                    if current == 1 {
                        entry.fingerprint.store(fingerprint, Ordering::Relaxed);
                        entry.key.store(Some(Arc::new(key.clone())));
                        entry.score.store(estimate, Ordering::Relaxed);
                        return;
                    }
                }
            }
        }

        if victim_score < estimate {
            let entry = &bucket.entries[victim_idx];
            if entry
                .score
                .compare_exchange(victim_score, estimate, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                entry.fingerprint.store(fingerprint, Ordering::Relaxed);
                entry.key.store(Some(Arc::new(key.clone())));
            }
        }
    }

    fn bump_entry(&self, entry: &HeavyEntry, estimate: u64) {
        let mut current = entry.score.load(Ordering::Relaxed);
        loop {
            let target = current.max(estimate).saturating_add(1);
            match entry.score.compare_exchange(
                current,
                target,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn should_sample(&self) -> bool {
        let prev = self.sample_counter.fetch_add(1, Ordering::Relaxed);
        prev % self.config.sample_every == 0
    }

    fn random_unit(&self) -> f64 {
        let value = self.next_random();
        (value as f64) / (u64::MAX as f64)
    }

    fn next_random(&self) -> u64 {
        let mut state = self.rng_state.load(Ordering::Relaxed);
        loop {
            let next = xorshift64(state);
            match self
                .rng_state
                .compare_exchange(state, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return next,
                Err(observed) => state = observed,
            }
        }
    }
}

#[derive(Default)]
struct HeavyEntry {
    fingerprint: AtomicU64,
    score: AtomicU64,
    key: ArcSwapOption<Bytes>,
}

struct HeavyBucket {
    entries: [HeavyEntry; HEAVY_BUCKET_SIZE],
}

impl HeavyBucket {
    fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| HeavyEntry::default()),
        }
    }
}

struct CountMinSketch {
    width: usize,
    depth: usize,
    counters: Vec<AtomicU64>,
    seeds: Vec<u64>,
    total: AtomicU64,
}

impl CountMinSketch {
    fn new(width: usize, depth: usize) -> Self {
        let width = width.max(1);
        let depth = depth.max(1);
        let mut seeds = Vec::with_capacity(depth);
        let mut state = RNG_SEED;
        for _ in 0..depth {
            state = splitmix64(state);
            seeds.push(state);
        }
        let counters = (0..width * depth)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        Self {
            width,
            depth,
            counters,
            seeds,
            total: AtomicU64::new(0),
        }
    }

    fn increment(&self, key: &[u8], weight: u64) -> u64 {
        let mut min = u64::MAX;
        for depth in 0..self.depth {
            let idx = self.hash_into_index(key, depth);
            let cell = &self.counters[idx];
            let prev = cell.fetch_add(weight, Ordering::Relaxed);
            let current = prev.saturating_add(weight);
            min = min.min(current);
        }
        self.total.fetch_add(weight, Ordering::Relaxed);
        min
    }

    fn estimate(&self, key: &[u8]) -> u64 {
        let mut min = u64::MAX;
        for depth in 0..self.depth {
            let idx = self.hash_into_index(key, depth);
            let value = self.counters[idx].load(Ordering::Relaxed);
            min = min.min(value);
        }
        min
    }

    fn error_bound(&self) -> u64 {
        if self.width == 0 {
            return 0;
        }
        self.total.load(Ordering::Relaxed) / self.width as u64
    }

    fn reset(&self) {
        for cell in &self.counters {
            cell.store(0, Ordering::Relaxed);
        }
        self.total.store(0, Ordering::Relaxed);
    }

    fn hash_into_index(&self, key: &[u8], depth: usize) -> usize {
        let seed = self.seeds[depth];
        let hashed = hash_with_seed(key, seed) % self.width as u64;
        depth * self.width + hashed as usize
    }
}

pub fn handle_command(hotkey: &Hotkey, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return hotkey_error("wrong number of arguments for 'hotkey' command");
    }
    let sub = args[1].to_vec().to_ascii_uppercase();
    match sub.as_slice() {
        b"ENABLE" => handle_enable(hotkey, args),
        b"DISABLE" => handle_disable(hotkey, args),
        b"GET" => handle_get(hotkey, args),
        b"RESET" => handle_reset(hotkey, args),
        _ => hotkey_error("unknown hotkey subcommand"),
    }
}

fn handle_enable(hotkey: &Hotkey, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return hotkey_error("wrong number of arguments for 'hotkey enable' command");
    }
    hotkey.enable();
    RespValue::simple("OK")
}

fn handle_disable(hotkey: &Hotkey, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return hotkey_error("wrong number of arguments for 'hotkey disable' command");
    }
    hotkey.disable();
    RespValue::simple("OK")
}

fn handle_get(hotkey: &Hotkey, args: &[Bytes]) -> RespValue {
    if args.len() > 3 {
        return hotkey_error("wrong number of arguments for 'hotkey get' command");
    }
    let count = if args.len() == 3 {
        match parse_non_negative(&args[2]) {
            Ok(value) => Some(value),
            Err(err) => return err,
        }
    } else {
        None
    };

    let entries = hotkey.snapshot(count);
    let payload = entries
        .into_iter()
        .map(|entry| {
            RespValue::Array(vec![
                RespValue::BulkString(entry.key),
                RespValue::Integer(entry.estimated_hits.min(i64::MAX as u64) as i64),
                RespValue::Integer(entry.error.min(i64::MAX as u64) as i64),
            ])
        })
        .collect();
    RespValue::Array(payload)
}

fn handle_reset(hotkey: &Hotkey, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return hotkey_error("wrong number of arguments for 'hotkey reset' command");
    }
    hotkey.reset();
    RespValue::simple("OK")
}

fn parse_non_negative(arg: &Bytes) -> Result<usize, RespValue> {
    let text = std::str::from_utf8(arg).map_err(|_| hotkey_value_error())?;
    let value: i64 = text.parse().map_err(|_| hotkey_value_error())?;
    if value < 0 {
        return Err(hotkey_value_error());
    }
    usize::try_from(value).map_err(|_| hotkey_value_error())
}

fn hotkey_error(message: &str) -> RespValue {
    RespValue::Error(Bytes::from(format!("ERR {message}")))
}

fn hotkey_value_error() -> RespValue {
    RespValue::Error(Bytes::from_static(
        b"ERR value is not an integer or out of range",
    ))
}

fn hash_with_seed(data: &[u8], seed: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut hasher);
    data.hash(&mut hasher);
    hasher.finish()
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 7;
    x ^= x >> 9;
    x ^= x << 8;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;

    fn build_command(parts: &[&[u8]]) -> RedisCommand {
        let args = parts
            .iter()
            .map(|p| Bytes::copy_from_slice(p))
            .collect::<Vec<_>>();
        RedisCommand::new(args).unwrap()
    }

    #[test]
    fn record_and_snapshot() {
        let mut config = HotkeyConfig::default();
        config.sample_every = 1;
        let hotkey = Hotkey::new(config);
        hotkey.enable();
        for _ in 0..100 {
            let cmd = build_command(&[b"GET", b"a"]);
            hotkey.record_command(&cmd);
        }
        for _ in 0..10 {
            let cmd = build_command(&[b"GET", b"b"]);
            hotkey.record_command(&cmd);
        }
        let entries = hotkey.snapshot(Some(2));
        assert_eq!(entries[0].key, Bytes::from_static(b"a"));
        assert!(entries[0].estimated_hits >= entries[1].estimated_hits);
    }

    #[test]
    fn sampling_skips_when_disabled() {
        let hotkey = Hotkey::new_default();
        let cmd = build_command(&[b"GET", b"key"]);
        hotkey.record_command(&cmd);
        assert!(hotkey.snapshot(None).is_empty());
    }

    #[test]
    fn reset_clears_entries() {
        let mut config = HotkeyConfig::default();
        config.sample_every = 1;
        let hotkey = Hotkey::new(config);
        hotkey.enable();
        let cmd = build_command(&[b"GET", b"key"]);
        hotkey.record_command(&cmd);
        assert!(!hotkey.snapshot(None).is_empty());
        hotkey.reset();
        assert!(hotkey.snapshot(None).is_empty());
    }

    #[test]
    fn reconfigure_updates_settings() {
        let hotkey = Hotkey::new_default();
        let mut cfg = hotkey.config();
        cfg.sample_every = 4;
        hotkey.reconfigure(cfg);
        assert_eq!(hotkey.config().sample_every, 4);
    }
}
