use bytes::{Bytes, BytesMut};

use crate::com::AsError;

use super::{Codec, Message};

pub const RESP_SIMPLE_STRING: u8 = b'+';
pub const RESP_SIMPLE_NUMBER: u8 = b':';
pub const RESP_SIMPLE_ERROR: u8 = b'-';
pub const RESP_BLOB_STRING: u8 = b'$';
pub const RESP_ARRAY: u8 = b'*';
pub const RESP_NULL: u8 = b'_';
pub const RESP_DOUBLE: u8 = b',';
pub const RESP_BOOLEAN: u8 = b'#';
pub const RESP_BLOB_ERROR: u8 = b'!';
pub const RESP_VERBATIM_STRING: u8 = b'=';
pub const RESP_MAP: u8 = b'%';
pub const RESP_SET: u8 = b'~';
pub const RESP_ATTR: u8 = b'|';
pub const RESP_PUSH: u8 = b'>';
pub const RESP_BIG_NUMBER: u8 = b'(';
pub const BYTE_CR: u8 = b'\r';
pub const BYTE_LF: u8 = b'\n';

#[derive(Clone)]
pub enum RedisObject {
    Array(Vec<RedisObject>),
    // for bulk string in resp2
    BlobString(Bytes),
    SimpleString(Bytes),
    SimpleError(Bytes),
    SimpleNumber(i64),
    // replace '*-1' and '$-1' with newer
    Null,
    Double(f64),
    Boolean(bool),
    BlobError(Bytes),
    // for (doc_type, content)
    VerbatimString(Bytes, Bytes),
    // Map, for key-value
    Map(Vec<RedisObject>),
    Set(Vec<RedisObject>),
    Attribute(Vec<RedisObject>),
    Push(Bytes, Vec<RedisObject>),
    BigNumber(Bytes),
}

impl Message for RedisObject {}

pub struct Resp3Codec {}

impl Resp3Codec {
    fn deserialize(&mut self, src: &mut BytesMut) -> Result<RedisObject, AsError> {
        // self.deserialize(ParseState::DetectType, src)
        unimplemented!()
    }

    fn deserialize_by_state(
        &mut self,
        state: ParseState,
        src: &mut BytesMut,
    ) -> Result<(usize, RedisObject), AsError> {
        if src.is_empty() {
            return Err(AsError::More);
        }

        match src[0] {
            RESP_ARRAY => {}
            RESP_SIMPLE_STRING => {}
            RESP_SIMPLE_NUMBER => {}
            RESP_SIMPLE_ERROR => {}
            RESP_BLOB_STRING => {}
            RESP_ARRAY => {}
            RESP_NULL => {}
            RESP_DOUBLE => {}
            RESP_BOOLEAN => {}
            RESP_BLOB_ERROR => {}
            RESP_VERBATIM_STRING => {}
            RESP_MAP => {}
            RESP_SET => {}
            RESP_ATTR => {}
            RESP_PUSH => {}
            RESP_BIG_NUMBER => {}
        }
        todo!()
    }
}

impl Codec for Resp3Codec {
    type Item = RedisObject;
    type Error = AsError;

    fn deserialize(&mut self, src: &mut BytesMut) -> Result<Self::Item, Self::Error> {
        unimplemented!()
    }

    fn serialize(&mut self, item: &Self::Item, dst: &mut BytesMut) -> Result<usize, Self::Error> {
        unimplemented!()
    }
}
