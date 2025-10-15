use std::collections::HashMap;
use std::fmt;

use anyhow::{anyhow, bail, Result};
use bytes::{Bytes, BytesMut};

use crate::backend::pool::BackendRequest;
use crate::metrics;
use crate::utils::{crc16, trim_hash_tag};

use super::types::RespValue;

pub const SLOT_COUNT: u16 = 16384;

pub struct RedisCommand {
    parts: Vec<Bytes>,
    total_tracker: Option<metrics::Tracker>,
    remote_tracker: Option<metrics::Tracker>,
}

impl Clone for RedisCommand {
    fn clone(&self) -> Self {
        Self {
            parts: self.parts.clone(),
            total_tracker: None,
            remote_tracker: None,
        }
    }
}

impl fmt::Debug for RedisCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisCommand")
            .field(
                "parts",
                &self
                    .parts
                    .iter()
                    .map(|p| String::from_utf8_lossy(p).into_owned())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

pub type RedisResponse = RespValue;

impl RedisCommand {
    pub fn new(parts: Vec<Bytes>) -> Result<Self> {
        if parts.is_empty() {
            bail!("redis command must contain at least one element");
        }
        Ok(Self {
            parts,
            total_tracker: None,
            remote_tracker: None,
        })
    }

    pub fn from_resp(value: RespValue) -> Result<Self> {
        match value {
            RespValue::Array(values) => {
                let mut parts = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        RespValue::BulkString(data) | RespValue::SimpleString(data) => {
                            parts.push(data)
                        }
                        RespValue::Integer(int) => {
                            parts.push(Bytes::copy_from_slice(int.to_string().as_bytes()))
                        }
                        RespValue::NullBulk | RespValue::NullArray => {
                            bail!("command argument cannot be null");
                        }
                        RespValue::Error(err) => {
                            bail!(
                                "client sent RESP error frame as command argument: {}",
                                String::from_utf8_lossy(&err)
                            );
                        }
                        RespValue::Array(_) => {
                            bail!("nested array arguments are not supported");
                        }
                    }
                }
                Self::new(parts)
            }
            other => Err(anyhow!(
                "redis command must be an array frame, received {:?}",
                other
            )),
        }
    }

    pub fn into_resp(self) -> RespValue {
        let values = self.parts.into_iter().map(RespValue::BulkString).collect();
        RespValue::Array(values)
    }

    pub fn to_resp(&self) -> RespValue {
        RespValue::Array(
            self.parts
                .iter()
                .cloned()
                .map(RespValue::BulkString)
                .collect(),
        )
    }

    pub fn command_name(&self) -> &[u8] {
        self.parts.first().map(|b| b.as_ref()).unwrap_or(&[])
    }

    pub fn args(&self) -> &[Bytes] {
        &self.parts
    }

    pub fn primary_key(&self) -> Option<&[u8]> {
        self.parts.get(1).map(|b| b.as_ref())
    }

    pub fn hash_slot(&self, hash_tag: Option<&[u8]>) -> Option<u16> {
        let key = self.primary_key()?;
        let trimmed = trim_hash_tag(key, hash_tag);
        Some(crc16(trimmed) % SLOT_COUNT)
    }

    pub fn as_blocking(&self) -> BlockingKind {
        let name = uppercase_name(self.command_name());
        match name.as_slice() {
            b"BLPOP" | b"BRPOP" => BlockingKind::Queue {
                timeout_secs: parse_timeout(self.parts.last()),
            },
            b"BRPOPLPUSH" | b"BZPOPMIN" | b"BZPOPMAX" => BlockingKind::Queue {
                timeout_secs: parse_timeout(self.parts.last()),
            },
            b"XREAD" | b"XREADGROUP" => BlockingKind::Stream {
                timeout_millis: has_block_option(&self.parts),
            },
            _ => BlockingKind::None,
        }
    }

    pub fn as_subscription(&self) -> SubscriptionKind {
        let name = uppercase_name(self.command_name());
        match name.as_slice() {
            b"SUBSCRIBE" => SubscriptionKind::Channel,
            b"PSUBSCRIBE" => SubscriptionKind::Pattern,
            b"UNSUBSCRIBE" => SubscriptionKind::Unsubscribe,
            b"PUNSUBSCRIBE" => SubscriptionKind::Punsub,
            _ => SubscriptionKind::None,
        }
    }

    pub fn take_remote_tracker(&mut self) -> Option<metrics::Tracker> {
        self.remote_tracker.take()
    }

    pub fn finish(&mut self) {
        self.remote_tracker.take();
        self.total_tracker.take();
    }

    pub fn is_read_only(&self) -> bool {
        matches!(command_kind(self.command_name()), CommandKind::Read)
    }

    pub fn expand_for_multi(&self, hash_tag: Option<&[u8]>) -> Option<MultiDispatch> {
        self.expand_for_multi_with(|key| hash_slot_for_key(key, hash_tag) as u64)
    }

    pub fn expand_for_multi_with<G>(&self, mut group_for: G) -> Option<MultiDispatch>
    where
        G: FnMut(&[u8]) -> u64,
    {
        let name = uppercase_name(self.command_name());
        match name.as_slice() {
            b"MGET" if self.parts.len() > 2 => Some(expand_mget(self, &mut group_for)),
            b"MSET" if self.parts.len() > 3 && self.parts.len() % 2 == 1 => {
                Some(expand_mset(self, &mut group_for, false))
            }
            b"MSETNX" if self.parts.len() > 3 && self.parts.len() % 2 == 1 => {
                Some(expand_mset(self, &mut group_for, true))
            }
            b"DEL" | b"UNLINK" | b"EXISTS" if self.parts.len() > 2 => {
                Some(expand_simple_iter(self, name.as_slice(), &mut group_for))
            }
            _ => None,
        }
    }
}

impl BackendRequest for RedisCommand {
    type Response = RedisResponse;

    fn apply_total_tracker(&mut self, cluster: &str) {
        self.total_tracker = Some(metrics::total_tracker(cluster));
    }

    fn apply_remote_tracker(&mut self, cluster: &str) {
        if self.remote_tracker.is_none() {
            self.remote_tracker = Some(metrics::remote_tracker(cluster));
        }
    }
}

impl fmt::Display for RedisCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args: Vec<String> = self
            .parts
            .iter()
            .map(|p| String::from_utf8_lossy(p).to_string())
            .collect();
        write!(f, "{}", args.join(" "))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    Read,
    Write,
    Other,
}

fn command_kind(cmd: &[u8]) -> CommandKind {
    if cmd.is_empty() {
        return CommandKind::Other;
    }
    let mut upper = [0u8; 32];
    let len = cmd.len().min(upper.len());
    for (i, byte) in cmd.iter().take(len).enumerate() {
        upper[i] = byte.to_ascii_uppercase();
    }
    let name = &upper[..len];
    match name {
        b"GET" | b"MGET" | b"GETSET" | b"TTL" | b"PTTL" | b"STRLEN" | b"EXISTS" | b"HGET"
        | b"HMGET" | b"HGETALL" | b"SCARD" | b"SMEMBERS" | b"SRANDMEMBER" | b"ZRANGE"
        | b"ZRANK" | b"ZSCORE" | b"ZCARD" | b"ZREVRANGE" | b"ZCOUNT" | b"ZRANGEBYSCORE"
        | b"ZREVRANGEBYSCORE" | b"ZREVRANK" | b"LINDEX" | b"LLEN" | b"LRANGE" | b"GETDEL" => {
            CommandKind::Read
        }
        b"SET" | b"MSET" | b"DEL" | b"INCR" | b"DECR" | b"HSET" | b"HMSET" | b"HDEL" | b"SADD"
        | b"SREM" | b"ZADD" | b"ZREM" | b"LPUSH" | b"RPUSH" | b"LPOP" | b"RPOP" | b"FLUSHALL"
        | b"FLUSHDB" => CommandKind::Write,
        _ => CommandKind::Other,
    }
}

fn uppercase_name(input: &[u8]) -> Vec<u8> {
    input.iter().map(|b| b.to_ascii_uppercase()).collect()
}

fn parse_timeout(arg: Option<&Bytes>) -> Option<f64> {
    arg.and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|s| s.parse::<f64>().ok())
}

fn has_block_option(parts: &[Bytes]) -> Option<f64> {
    let mut idx = 0usize;
    while idx < parts.len() {
        if parts[idx].eq_ignore_ascii_case(b"BLOCK") {
            return parts
                .get(idx + 1)
                .and_then(|timeout| std::str::from_utf8(timeout).ok())
                .and_then(|s| s.parse::<f64>().ok());
        }
        idx += 1;
    }
    None
}

fn hash_slot_for_key(key: &[u8], hash_tag: Option<&[u8]>) -> u16 {
    let trimmed = trim_hash_tag(key, hash_tag);
    crc16(trimmed) % SLOT_COUNT
}

fn expand_mget<G>(command: &RedisCommand, group_for: &mut G) -> MultiDispatch
where
    G: FnMut(&[u8]) -> u64,
{
    let mut groups: HashMap<u64, Vec<(usize, Bytes)>> = HashMap::new();
    for (index, key) in command.parts.iter().enumerate().skip(1) {
        let group = group_for(key.as_ref());
        groups.entry(group).or_default().push((index - 1, key.clone()));
    }

    let mut subcommands = Vec::with_capacity(groups.len());
    for keys in groups.into_values() {
        let mut parts = Vec::with_capacity(keys.len() + 1);
        parts.push(Bytes::from_static(b"MGET"));
        let mut positions = Vec::with_capacity(keys.len());
        for (position, key) in keys {
            positions.push(position);
            parts.push(key);
        }
        let sub = RedisCommand::new(parts).expect("MGET command must be valid");
        subcommands.push(SubCommand {
            positions,
            command: sub,
        });
    }

    MultiDispatch {
        subcommands,
        aggregator: Aggregator::Array {
            key_count: command.parts.len().saturating_sub(1),
        },
    }
}

fn expand_mset<G>(command: &RedisCommand, group_for: &mut G, nx: bool) -> MultiDispatch
where
    G: FnMut(&[u8]) -> u64,
{
    let mut groups: HashMap<u64, Vec<(usize, Bytes, Bytes)>> = HashMap::new();
    let mut position = 0usize;
    let mut iter = command.parts.iter().skip(1);
    while let Some(key) = iter.next() {
        if let Some(value) = iter.next() {
            let group = group_for(key.as_ref());
            groups
                .entry(group)
                .or_default()
                .push((position, key.clone(), value.clone()));
            position += 1;
        }
    }

    let mut subcommands = Vec::with_capacity(groups.len());
    for entries in groups.into_values() {
        let mut parts = Vec::with_capacity(entries.len() * 2 + 1);
        if nx {
            parts.push(Bytes::from_static(b"MSETNX"));
        } else {
            parts.push(Bytes::from_static(b"MSET"));
        }
        let mut positions = Vec::with_capacity(entries.len());
        for (pos, key, value) in entries {
            positions.push(pos);
            parts.push(key);
            parts.push(value);
        }
        let sub = RedisCommand::new(parts).expect("SET command must be valid");
        subcommands.push(SubCommand {
            positions,
            command: sub,
        });
    }

    MultiDispatch {
        subcommands,
        aggregator: if nx {
            Aggregator::SetnxAll
        } else {
            Aggregator::OkAll
        },
    }
}

fn expand_simple_iter(
    command: &RedisCommand,
    name: &[u8],
    group_for: &mut impl FnMut(&[u8]) -> u64,
) -> MultiDispatch {
    let mut groups: HashMap<u64, Vec<Bytes>> = HashMap::new();
    for key in command.parts.iter().skip(1) {
        let group = group_for(key.as_ref());
        groups.entry(group).or_default().push(key.clone());
    }

    let mut subcommands = Vec::with_capacity(groups.len());
    for keys in groups.into_values() {
        let mut parts = Vec::with_capacity(keys.len() + 1);
        parts.push(BytesMut::from(name).freeze());
        for key in keys {
            parts.push(key);
        }
        let sub = RedisCommand::new(parts).expect("single key command must be valid");
        subcommands.push(SubCommand {
            positions: Vec::new(),
            command: sub,
        });
    }
    MultiDispatch {
        subcommands,
        aggregator: Aggregator::IntegerSum,
    }
}

#[derive(Debug, Clone)]
pub struct MultiDispatch {
    pub subcommands: Vec<SubCommand>,
    pub aggregator: Aggregator,
}

#[derive(Debug, Clone)]
pub struct SubCommand {
    pub positions: Vec<usize>,
    pub command: RedisCommand,
}

#[derive(Debug, Clone)]
pub enum Aggregator {
    Array { key_count: usize },
    IntegerSum,
    OkAll,
    SetnxAll,
}

#[derive(Debug, Clone)]
pub struct SubResponse {
    pub positions: Vec<usize>,
    pub response: RespValue,
}

impl Aggregator {
    pub fn combine(&self, responses: Vec<SubResponse>) -> Result<RespValue> {
        match self {
            Aggregator::Array { key_count } => {
                let mut ordered: Vec<Option<RespValue>> = vec![None; *key_count];
                for sub in responses {
                    if let RespValue::Error(_) = sub.response {
                        return Ok(sub.response);
                    }
                    if sub.positions.is_empty() {
                        bail!("multi response missing positions");
                    }
                    match sub.response {
                        RespValue::Array(items) => {
                            if items.len() != sub.positions.len() {
                                bail!(
                                    "unexpected response item count: expected {}, got {}",
                                    sub.positions.len(),
                                    items.len()
                                );
                            }
                            for (pos, item) in sub.positions.into_iter().zip(items.into_iter()) {
                                if pos >= *key_count {
                                    bail!("unexpected response position {}", pos);
                                }
                                if ordered[pos].is_some() {
                                    bail!("duplicate response position {}", pos);
                                }
                                ordered[pos] = Some(item);
                            }
                        }
                        other => {
                            if sub.positions.len() != 1 {
                                bail!(
                                    "unexpected scalar response for multiple positions: {:?}",
                                    other
                                );
                            }
                            let pos = sub.positions[0];
                            if pos >= *key_count {
                                bail!("unexpected response position {}", pos);
                            }
                            if ordered[pos].is_some() {
                                bail!("duplicate response position {}", pos);
                            }
                            ordered[pos] = Some(other);
                        }
                    }
                }
                let collected = ordered
                    .into_iter()
                    .map(|item| item.unwrap_or(RespValue::NullBulk))
                    .collect();
                Ok(RespValue::Array(collected))
            }
            Aggregator::IntegerSum => {
                let mut sum = 0i64;
                for sub in responses {
                    match sub.response {
                        RespValue::Integer(value) => sum += value,
                        RespValue::Error(_) => return Ok(sub.response),
                        other => bail!("unexpected response type: {:?}", other),
                    }
                }
                Ok(RespValue::Integer(sum))
            }
            Aggregator::OkAll => {
                for sub in responses {
                    match sub.response {
                        RespValue::SimpleString(ref s) if s.as_ref() == b"OK" => {}
                        RespValue::Error(_) => return Ok(sub.response),
                        other => bail!("unexpected response type: {:?}", other),
                    }
                }
                Ok(RespValue::SimpleString(Bytes::from_static(b"OK")))
            }
            Aggregator::SetnxAll => {
                let mut all_success = true;
                for sub in responses {
                    match sub.response {
                        RespValue::Integer(value) => {
                            if value == 0 {
                                all_success = false;
                            }
                        }
                        RespValue::Error(_) => return Ok(sub.response),
                        other => bail!("unexpected response type: {:?}", other),
                    }
                }
                Ok(RespValue::Integer(if all_success { 1 } else { 0 }))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BlockingKind {
    None,
    Queue { timeout_secs: Option<f64> },
    Stream { timeout_millis: Option<f64> },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SubscriptionKind {
    None,
    Channel,
    Pattern,
    Unsubscribe,
    Punsub,
}
