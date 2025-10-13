use std::fmt;

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;

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
