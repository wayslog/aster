use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::Mutex;

use crate::protocol::redis::{RedisCommand, RespValue};

#[derive(Clone, Debug)]
pub struct SlowlogEntry {
    pub id: i64,
    pub timestamp: i64,
    pub duration_us: u64,
    pub command: Vec<Bytes>,
}

#[derive(Default)]
struct SlowlogState {
    next_id: i64,
    entries: VecDeque<SlowlogEntry>,
}

pub struct Slowlog {
    threshold_us: AtomicI64,
    max_len: AtomicUsize,
    state: Mutex<SlowlogState>,
}

impl std::fmt::Debug for Slowlog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slowlog")
            .field("threshold_us", &self.threshold())
            .field("max_len", &self.max_len())
            .field("len", &self.len())
            .finish()
    }
}

impl Slowlog {
    pub fn new(threshold_us: i64, max_len: usize) -> Self {
        let mut state = SlowlogState::default();
        state.next_id = 1;
        Self {
            threshold_us: AtomicI64::new(threshold_us),
            max_len: AtomicUsize::new(max_len),
            state: Mutex::new(state),
        }
    }

    pub fn threshold(&self) -> i64 {
        self.threshold_us.load(Ordering::Relaxed)
    }

    pub fn max_len(&self) -> usize {
        self.max_len.load(Ordering::Relaxed)
    }

    pub fn set_threshold(&self, value: i64) {
        self.threshold_us.store(value, Ordering::Relaxed);
    }

    pub fn set_max_len(&self, value: usize) {
        self.max_len.store(value, Ordering::Relaxed);
        self.trim_entries(value);
    }

    pub fn reset(&self) {
        let mut state = self.state.lock();
        state.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.state.lock().entries.len()
    }

    pub fn snapshot(&self, count: Option<usize>) -> Vec<SlowlogEntry> {
        let state = self.state.lock();
        let limit = count.unwrap_or(usize::MAX);
        state
            .entries
            .iter()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
    }

    pub fn maybe_record(&self, command: &RedisCommand, duration: Duration) {
        let threshold = self.threshold();
        if threshold < 0 {
            return;
        }

        let duration_us = duration.as_micros();
        if duration_us < threshold as u128 {
            return;
        }

        let clamped_duration = duration_us.min(u64::MAX as u128) as u64;
        let command_parts = command.args().iter().cloned().collect::<Vec<Bytes>>();
        let timestamp = SystemTime::now()
            .checked_sub(duration)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut state = self.state.lock();
        let id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);
        state.entries.push_front(SlowlogEntry {
            id,
            timestamp,
            duration_us: clamped_duration,
            command: command_parts,
        });

        let limit = self.max_len();
        while state.entries.len() > limit {
            state.entries.pop_back();
        }
    }

    fn trim_entries(&self, limit: usize) {
        let mut state = self.state.lock();
        while state.entries.len() > limit {
            state.entries.pop_back();
        }
    }
}

pub fn handle_command(slowlog: &Slowlog, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return slowlog_error("wrong number of arguments for 'slowlog' command");
    }

    let sub = args[1].to_vec().to_ascii_uppercase();
    match sub.as_slice() {
        b"GET" => handle_get(slowlog, args),
        b"LEN" => handle_len(slowlog, args),
        b"RESET" => handle_reset(slowlog, args),
        _ => slowlog_error("unknown slowlog subcommand"),
    }
}

fn handle_get(slowlog: &Slowlog, args: &[Bytes]) -> RespValue {
    if args.len() > 3 {
        return slowlog_error("wrong number of arguments for 'slowlog get' command");
    }
    let count = if args.len() == 3 {
        match parse_non_negative(&args[2]) {
            Ok(value) => Some(value),
            Err(err) => return err,
        }
    } else {
        None
    };
    let entries = slowlog.snapshot(count);
    let payload = entries
        .into_iter()
        .map(|entry| {
            let mut fields = Vec::with_capacity(4);
            fields.push(RespValue::Integer(entry.id));
            fields.push(RespValue::Integer(entry.timestamp));
            let duration = entry.duration_us.min(i64::MAX as u64) as i64;
            fields.push(RespValue::Integer(duration));
            let command = entry
                .command
                .into_iter()
                .map(RespValue::BulkString)
                .collect();
            fields.push(RespValue::Array(command));
            RespValue::Array(fields)
        })
        .collect();
    RespValue::Array(payload)
}

fn handle_len(slowlog: &Slowlog, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return slowlog_error("wrong number of arguments for 'slowlog len' command");
    }
    let len = slowlog.len().min(i64::MAX as usize) as i64;
    RespValue::Integer(len)
}

fn handle_reset(slowlog: &Slowlog, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return slowlog_error("wrong number of arguments for 'slowlog reset' command");
    }
    slowlog.reset();
    RespValue::simple("OK")
}

fn parse_non_negative(arg: &Bytes) -> Result<usize, RespValue> {
    let text = std::str::from_utf8(arg).map_err(|_| slowlog_value_error())?;
    let value: i64 = text.parse().map_err(|_| slowlog_value_error())?;
    if value < 0 {
        return Err(slowlog_value_error());
    }
    usize::try_from(value).map_err(|_| slowlog_value_error())
}

fn slowlog_error(message: &str) -> RespValue {
    RespValue::Error(Bytes::from(format!("ERR {message}")))
}

fn slowlog_value_error() -> RespValue {
    RespValue::Error(Bytes::from_static(
        b"ERR value is not an integer or out of range",
    ))
}
