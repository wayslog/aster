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

    pub fn expand_for_multi(&self) -> Option<MultiDispatch> {
        let name = uppercase_name(self.command_name());
        match name.as_slice() {
            b"MGET" if self.parts.len() > 2 => Some(expand_mget(self)),
            b"MSET" if self.parts.len() > 3 && self.parts.len() % 2 == 1 => {
                Some(expand_mset(self, false))
            }
            b"MSETNX" if self.parts.len() > 3 && self.parts.len() % 2 == 1 => {
                Some(expand_mset(self, true))
            }
            b"DEL" | b"UNLINK" | b"EXISTS" if self.parts.len() > 2 => {
                Some(expand_simple_iter(self, name.as_slice()))
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

fn expand_mget(command: &RedisCommand) -> MultiDispatch {
    let mut subcommands = Vec::new();
    for (index, key) in command.parts.iter().enumerate().skip(1) {
        let sub = RedisCommand::new(vec![Bytes::from_static(b"GET"), key.clone()])
            .expect("GET command must be valid");
        subcommands.push(SubCommand {
            position: index - 1,
            command: sub,
        });
    }
    MultiDispatch {
        subcommands,
        aggregator: Aggregator::Array {
            key_count: command.parts.len() - 1,
        },
    }
}

fn expand_mset(command: &RedisCommand, nx: bool) -> MultiDispatch {
    let mut subcommands = Vec::new();
    let mut position = 0usize;
    let mut iter = command.parts.iter().skip(1);
    while let Some(key) = iter.next() {
        if let Some(value) = iter.next() {
            let mut parts = Vec::with_capacity(3);
            if nx {
                parts.push(Bytes::from_static(b"SETNX"));
            } else {
                parts.push(Bytes::from_static(b"SET"));
            }
            parts.push(key.clone());
            parts.push(value.clone());
            let sub = RedisCommand::new(parts).expect("SET command must be valid");
            subcommands.push(SubCommand {
                position,
                command: sub,
            });
            position += 1;
        }
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

fn expand_simple_iter(command: &RedisCommand, name: &[u8]) -> MultiDispatch {
    let mut subcommands = Vec::new();
    for (position, key) in command.parts.iter().enumerate().skip(1) {
        let mut parts = Vec::with_capacity(2);
        parts.push(BytesMut::from(name).freeze());
        parts.push(key.clone());
        let sub = RedisCommand::new(parts).expect("single key command must be valid");
        subcommands.push(SubCommand {
            position: position - 1,
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
    pub position: usize,
    pub command: RedisCommand,
}

#[derive(Debug, Clone)]
pub enum Aggregator {
    Array { key_count: usize },
    IntegerSum,
    OkAll,
    SetnxAll,
}

impl Aggregator {
    pub fn combine(&self, responses: Vec<(usize, RespValue)>) -> Result<RespValue> {
        match self {
            Aggregator::Array { key_count } => {
                let mut ordered: Vec<Option<RespValue>> = vec![None; *key_count];
                for (index, resp) in responses {
                    if let RespValue::Error(_) = resp {
                        return Ok(resp);
                    }
                    if index >= *key_count {
                        bail!("unexpected response position {}", index);
                    }
                    ordered[index] = Some(resp);
                }
                let collected = ordered
                    .into_iter()
                    .map(|item| item.unwrap_or(RespValue::NullBulk))
                    .collect();
                Ok(RespValue::Array(collected))
            }
            Aggregator::IntegerSum => {
                let mut sum = 0i64;
                for (_idx, resp) in responses {
                    match resp {
                        RespValue::Integer(value) => sum += value,
                        RespValue::Error(_) => return Ok(resp),
                        other => bail!("unexpected response type: {:?}", other),
                    }
                }
                Ok(RespValue::Integer(sum))
            }
            Aggregator::OkAll => {
                for (_idx, resp) in responses {
                    match resp {
                        RespValue::SimpleString(ref s) if s.as_ref() == b"OK" => {}
                        RespValue::Error(_) => return Ok(resp),
                        other => bail!("unexpected response type: {:?}", other),
                    }
                }
                Ok(RespValue::SimpleString(Bytes::from_static(b"OK")))
            }
            Aggregator::SetnxAll => {
                let mut all_success = true;
                for (_idx, resp) in responses {
                    match resp {
                        RespValue::Integer(value) => {
                            if value == 0 {
                                all_success = false;
                            }
                        }
                        RespValue::Error(_) => return Ok(resp),
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
