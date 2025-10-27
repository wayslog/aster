use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tokio_util::codec::{Decoder, Encoder};

use super::types::RespValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespVersion {
    Resp2,
    Resp3,
}

impl RespVersion {
    fn as_u8(self) -> u8 {
        match self {
            RespVersion::Resp2 => 2,
            RespVersion::Resp3 => 3,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            3 => RespVersion::Resp3,
            _ => RespVersion::Resp2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RespCodec {
    version: Arc<AtomicU8>,
}

impl Default for RespCodec {
    fn default() -> Self {
        Self {
            version: Arc::new(AtomicU8::new(RespVersion::Resp2.as_u8())),
        }
    }
}

impl RespCodec {
    pub fn version(&self) -> RespVersion {
        RespVersion::from_u8(self.version.load(Ordering::SeqCst))
    }

    pub fn set_version(&self, version: RespVersion) {
        self.version.store(version.as_u8(), Ordering::SeqCst);
    }

    pub fn upgrade_to_resp3(&self) {
        self.set_version(RespVersion::Resp3);
    }
}

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let mut pos = 0usize;
        match parse_value(&src[..], &mut pos)? {
            Some(frame) => {
                src.advance(pos);
                Ok(Some(frame))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<RespValue> for RespCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<()> {
        let version = self.version();
        write_value(&item, version, dst);
        Ok(())
    }
}

fn parse_value(src: &[u8], pos: &mut usize) -> Result<Option<RespValue>> {
    if *pos >= src.len() {
        return Ok(None);
    }
    let start = *pos;
    let prefix = src[*pos];
    *pos += 1;

    match prefix {
        b'+' => {
            let line = match read_line(src, pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            Ok(Some(RespValue::SimpleString(Bytes::copy_from_slice(line))))
        }
        b'-' => {
            let line = match read_line(src, pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            Ok(Some(RespValue::Error(Bytes::copy_from_slice(line))))
        }
        b':' => {
            let line = match read_line(src, pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            let int_str = std::str::from_utf8(line)?;
            let value = int_str
                .parse::<i64>()
                .map_err(|err| anyhow!("invalid integer: {err}"))?;
            Ok(Some(RespValue::Integer(value)))
        }
        b'$' => parse_bulk_string(src, pos, start, false),
        b'*' => parse_array(src, pos, start),
        b'_' => {
            let line = match read_line(src, pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            if !line.is_empty() {
                return Err(anyhow!("invalid null frame payload"));
            }
            Ok(Some(RespValue::Null))
        }
        b'#' => {
            let line = match read_line(src, pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            match line {
                b"t" | b"T" => Ok(Some(RespValue::Boolean(true))),
                b"f" | b"F" => Ok(Some(RespValue::Boolean(false))),
                _ => Err(anyhow!("invalid boolean literal '{:?}'", line)),
            }
        }
        b',' => {
            let line = match read_line(src, pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            Ok(Some(RespValue::Double(Bytes::copy_from_slice(line))))
        }
        b'(' => {
            let line = match read_line(src, pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            Ok(Some(RespValue::BigNumber(Bytes::copy_from_slice(line))))
        }
        b'=' => parse_verbatim_string(src, pos, start),
        b'!' => parse_bulk_string(src, pos, start, true),
        b'%' => parse_map(src, pos, start, RespValue::Map),
        b'~' => parse_collection(src, pos, start, RespValue::Set),
        b'>' => parse_collection(src, pos, start, RespValue::Push),
        b'|' => parse_map(src, pos, start, RespValue::Attribute),
        _ => Err(anyhow!("unsupported RESP prefix '{}'.", prefix as char)),
    }
}

fn parse_bulk_string(
    src: &[u8],
    pos: &mut usize,
    start: usize,
    as_error: bool,
) -> Result<Option<RespValue>> {
    let line = match read_line(src, pos)? {
        Some(line) => line,
        None => {
            *pos = start;
            return Ok(None);
        }
    };
    let len = parse_length(line, "bulk string")?;
    if len < 0 {
        return Ok(Some(RespValue::NullBulk));
    }
    let len = len as usize;
    if *pos + len + 2 > src.len() {
        *pos = start;
        return Ok(None);
    }
    let data = &src[*pos..*pos + len];
    *pos += len + 2;
    let payload = Bytes::copy_from_slice(data);
    if as_error {
        Ok(Some(RespValue::BlobError(payload)))
    } else {
        Ok(Some(RespValue::BulkString(payload)))
    }
}

fn parse_verbatim_string(src: &[u8], pos: &mut usize, start: usize) -> Result<Option<RespValue>> {
    let line = match read_line(src, pos)? {
        Some(line) => line,
        None => {
            *pos = start;
            return Ok(None);
        }
    };
    let len = parse_length(line, "verbatim string")?;
    if len < 0 {
        return Err(anyhow!("verbatim string must not be null"));
    }
    let len = len as usize;
    if *pos + len + 2 > src.len() {
        *pos = start;
        return Ok(None);
    }
    let data = &src[*pos..*pos + len];
    *pos += len + 2;
    if len < 4 || data[3] != b':' {
        return Err(anyhow!("invalid verbatim string header"));
    }
    let mut format = [0u8; 3];
    format.copy_from_slice(&data[..3]);
    let payload = Bytes::copy_from_slice(&data[4..]);
    Ok(Some(RespValue::VerbatimString { format, data: payload }))
}

fn parse_array(src: &[u8], pos: &mut usize, start: usize) -> Result<Option<RespValue>> {
    let mut local_pos = *pos;
    let line = match read_line(src, &mut local_pos)? {
        Some(line) => line,
        None => {
            *pos = start;
            return Ok(None);
        }
    };
    let len = parse_length(line, "array")?;
    if len < 0 {
        *pos = local_pos;
        return Ok(Some(RespValue::NullArray));
    }
    let mut values = Vec::with_capacity(len as usize);
    let mut element_pos = local_pos;
    for _ in 0..len {
        match parse_value(src, &mut element_pos)? {
            Some(value) => values.push(value),
            None => {
                *pos = start;
                return Ok(None);
            }
        }
    }
    *pos = element_pos;
    Ok(Some(RespValue::Array(values)))
}

fn parse_collection<F>(
    src: &[u8],
    pos: &mut usize,
    start: usize,
    ctor: F,
) -> Result<Option<RespValue>>
where
    F: FnOnce(Vec<RespValue>) -> RespValue,
{
    let mut local_pos = *pos;
    let line = match read_line(src, &mut local_pos)? {
        Some(line) => line,
        None => {
            *pos = start;
            return Ok(None);
        }
    };
    let len = parse_length(line, "collection")?;
    if len < 0 {
        *pos = local_pos;
        return Ok(Some(RespValue::Null));
    }
    let mut values = Vec::with_capacity(len as usize);
    let mut element_pos = local_pos;
    for _ in 0..len {
        match parse_value(src, &mut element_pos)? {
            Some(value) => values.push(value),
            None => {
                *pos = start;
                return Ok(None);
            }
        }
    }
    *pos = element_pos;
    Ok(Some(ctor(values)))
}

fn parse_map<F>(
    src: &[u8],
    pos: &mut usize,
    start: usize,
    ctor: F,
) -> Result<Option<RespValue>>
where
    F: FnOnce(Vec<(RespValue, RespValue)>) -> RespValue,
{
    let mut local_pos = *pos;
    let line = match read_line(src, &mut local_pos)? {
        Some(line) => line,
        None => {
            *pos = start;
            return Ok(None);
        }
    };
    let len = parse_length(line, "map")?;
    if len < 0 {
        *pos = local_pos;
        return Ok(Some(RespValue::Null));
    }
    let mut entries = Vec::with_capacity(len as usize);
    let mut element_pos = local_pos;
    for _ in 0..len {
        let key = match parse_value(src, &mut element_pos)? {
            Some(value) => value,
            None => {
                *pos = start;
                return Ok(None);
            }
        };
        let value = match parse_value(src, &mut element_pos)? {
            Some(value) => value,
            None => {
                *pos = start;
                return Ok(None);
            }
        };
        entries.push((key, value));
    }
    *pos = element_pos;
    Ok(Some(ctor(entries)))
}

fn parse_length(bytes: &[u8], kind: &str) -> Result<isize> {
    let text = std::str::from_utf8(bytes)?;
    text.parse::<isize>()
        .map_err(|err| anyhow!("invalid {kind} length: {err}"))
}

fn read_line<'a>(src: &'a [u8], pos: &mut usize) -> Result<Option<&'a [u8]>> {
    if *pos >= src.len() {
        return Ok(None);
    }
    let mut idx = *pos;
    while idx + 1 < src.len() {
        if src[idx] == b'\r' && src[idx + 1] == b'\n' {
            let line = &src[*pos..idx];
            *pos = idx + 2;
            return Ok(Some(line));
        }
        idx += 1;
    }
    Ok(None)
}

fn write_value(value: &RespValue, version: RespVersion, dst: &mut BytesMut) {
    match value {
        RespValue::SimpleString(data) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(data);
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Error(data) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(data);
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(value) => write_integer(*value, dst),
        RespValue::BulkString(data) => write_bulk(data, dst),
        RespValue::NullBulk => dst.extend_from_slice(b"$-1\r\n"),
        RespValue::Array(values) => write_aggregate(b'*', values.len(), values, version, dst),
        RespValue::NullArray => dst.extend_from_slice(b"*-1\r\n"),
        RespValue::Null => match version {
            RespVersion::Resp3 => dst.extend_from_slice(b"_\r\n"),
            RespVersion::Resp2 => dst.extend_from_slice(b"$-1\r\n"),
        },
        RespValue::Boolean(flag) => match version {
            RespVersion::Resp3 => {
                dst.extend_from_slice(b"#");
                dst.extend_from_slice(if *flag { b"t" } else { b"f" });
                dst.extend_from_slice(b"\r\n");
            }
            RespVersion::Resp2 => write_integer(if *flag { 1 } else { 0 }, dst),
        },
        RespValue::Double(data) => match version {
            RespVersion::Resp3 => {
                dst.extend_from_slice(b",");
                dst.extend_from_slice(data);
                dst.extend_from_slice(b"\r\n");
            }
            RespVersion::Resp2 => write_bulk(data, dst),
        },
        RespValue::BigNumber(data) => match version {
            RespVersion::Resp3 => {
                dst.extend_from_slice(b"(");
                dst.extend_from_slice(data);
                dst.extend_from_slice(b"\r\n");
            }
            RespVersion::Resp2 => write_bulk(data, dst),
        },
        RespValue::VerbatimString { format, data } => match version {
            RespVersion::Resp3 => {
                let total_len = 4 + data.len();
                dst.extend_from_slice(b"=");
                dst.extend_from_slice(total_len.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(format);
                dst.extend_from_slice(b":");
                dst.extend_from_slice(data);
                dst.extend_from_slice(b"\r\n");
            }
            RespVersion::Resp2 => write_bulk(data, dst),
        },
        RespValue::BlobError(data) => match version {
            RespVersion::Resp3 => {
                dst.extend_from_slice(b"!");
                dst.extend_from_slice(data.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(data);
                dst.extend_from_slice(b"\r\n");
            }
            RespVersion::Resp2 => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(data);
                dst.extend_from_slice(b"\r\n");
            }
        },
        RespValue::Map(entries) => match version {
            RespVersion::Resp3 => write_map(b'%', entries, version, dst),
            RespVersion::Resp2 => write_map_as_array(entries, version, dst),
        },
        RespValue::Set(values) => match version {
            RespVersion::Resp3 => write_aggregate(b'~', values.len(), values, version, dst),
            RespVersion::Resp2 => write_aggregate(b'*', values.len(), values, version, dst),
        },
        RespValue::Push(values) => match version {
            RespVersion::Resp3 => write_aggregate(b'>', values.len(), values, version, dst),
            RespVersion::Resp2 => write_aggregate(b'*', values.len(), values, version, dst),
        },
        RespValue::Attribute(entries) => match version {
            RespVersion::Resp3 => write_map(b'|', entries, version, dst),
            RespVersion::Resp2 => write_map_as_array(entries, version, dst),
        },
    }
}

fn write_integer(value: i64, dst: &mut BytesMut) {
    dst.extend_from_slice(b":");
    dst.extend_from_slice(value.to_string().as_bytes());
    dst.extend_from_slice(b"\r\n");
}

fn write_bulk(data: &[u8], dst: &mut BytesMut) {
    dst.extend_from_slice(b"$");
    dst.extend_from_slice(data.len().to_string().as_bytes());
    dst.extend_from_slice(b"\r\n");
    dst.extend_from_slice(data);
    dst.extend_from_slice(b"\r\n");
}

fn write_aggregate(
    prefix: u8,
    len: usize,
    values: &[RespValue],
    version: RespVersion,
    dst: &mut BytesMut,
) {
    dst.extend_from_slice(&[prefix]);
    dst.extend_from_slice(len.to_string().as_bytes());
    dst.extend_from_slice(b"\r\n");
    for value in values {
        write_value(value, version, dst);
    }
}

fn write_map(
    prefix: u8,
    entries: &[(RespValue, RespValue)],
    version: RespVersion,
    dst: &mut BytesMut,
) {
    dst.extend_from_slice(&[prefix]);
    dst.extend_from_slice(entries.len().to_string().as_bytes());
    dst.extend_from_slice(b"\r\n");
    for (key, value) in entries {
        write_value(key, version, dst);
        write_value(value, version, dst);
    }
}

fn write_map_as_array(entries: &[(RespValue, RespValue)], version: RespVersion, dst: &mut BytesMut) {
    dst.extend_from_slice(b"*");
    dst.extend_from_slice((entries.len() * 2).to_string().as_bytes());
    dst.extend_from_slice(b"\r\n");
    for (key, value) in entries {
        write_value(key, version, dst);
        write_value(value, version, dst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_entry() -> RespValue {
        RespValue::Map(vec![(
            RespValue::SimpleString(Bytes::from_static(b"mode")),
            RespValue::BulkString(Bytes::from_static(b"standalone")),
        )])
    }

    #[test]
    fn encodes_map_with_resp3_prefix() {
        let mut codec = RespCodec::default();
        codec.upgrade_to_resp3();
        let mut buf = BytesMut::new();
        codec.encode(map_entry(), &mut buf).unwrap();
        assert_eq!(
            buf.as_ref(),
            b"%1\r\n+mode\r\n$10\r\nstandalone\r\n"
        );
    }

    #[test]
    fn encodes_map_as_array_in_resp2() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::new();
        codec.encode(map_entry(), &mut buf).unwrap();
        assert_eq!(
            buf.as_ref(),
            b"*2\r\n+mode\r\n$10\r\nstandalone\r\n"
        );
    }
}
