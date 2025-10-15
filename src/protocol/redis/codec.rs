use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::types::RespValue;

#[derive(Debug, Default, Clone, Copy)]
pub struct RespCodec;

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
        write_value(&item, dst);
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
        b'$' => {
            let line = match read_line(src, pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            let len_str = std::str::from_utf8(line)?;
            let len = len_str
                .parse::<isize>()
                .map_err(|err| anyhow!("invalid bulk length: {err}"))?;
            if len < 0 {
                return Ok(Some(RespValue::NullBulk));
            }
            let len = len as usize;
            if *pos + len + 2 > src.len() {
                *pos = start;
                return Ok(None);
            }
            let data = &src[*pos..*pos + len];
            *pos += len + 2; // skip data and CRLF
            Ok(Some(RespValue::BulkString(Bytes::copy_from_slice(data))))
        }
        b'*' => {
            let mut local_pos = *pos;
            let line = match read_line(src, &mut local_pos)? {
                Some(line) => line,
                None => {
                    *pos = start;
                    return Ok(None);
                }
            };
            let len_str = std::str::from_utf8(line)?;
            let len = len_str
                .parse::<isize>()
                .map_err(|err| anyhow!("invalid array length: {err}"))?;
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
        _ => Err(anyhow!("unsupported RESP prefix '{}'.", prefix as char)),
    }
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

fn write_value(value: &RespValue, dst: &mut BytesMut) {
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
        RespValue::Integer(value) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(value.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(data) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(data.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(data);
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::NullBulk => {
            dst.extend_from_slice(b"$-1\r\n");
        }
        RespValue::Array(values) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(values.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for value in values {
                write_value(value, dst);
            }
        }
        RespValue::NullArray => {
            dst.extend_from_slice(b"*-1\r\n");
        }
    }
}
