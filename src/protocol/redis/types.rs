use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    BulkString(Bytes),
    NullBulk,
    Array(Vec<RespValue>),
    NullArray,
    Null,
    Boolean(bool),
    Double(Bytes),
    BigNumber(Bytes),
    VerbatimString { format: [u8; 3], data: Bytes },
    BlobError(Bytes),
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    Push(Vec<RespValue>),
    Attribute(Vec<(RespValue, RespValue)>),
}

impl RespValue {
    pub fn simple<T: AsRef<[u8]>>(value: T) -> Self {
        RespValue::SimpleString(Bytes::copy_from_slice(value.as_ref()))
    }

    pub fn error<T: AsRef<[u8]>>(value: T) -> Self {
        RespValue::Error(Bytes::copy_from_slice(value.as_ref()))
    }

    pub fn bulk<T: AsRef<[u8]>>(value: T) -> Self {
        RespValue::BulkString(Bytes::copy_from_slice(value.as_ref()))
    }

    pub fn array(values: Vec<RespValue>) -> Self {
        RespValue::Array(values)
    }

    pub fn map(entries: Vec<(RespValue, RespValue)>) -> Self {
        RespValue::Map(entries)
    }

    pub fn null() -> Self {
        RespValue::Null
    }

    pub fn boolean(value: bool) -> Self {
        RespValue::Boolean(value)
    }

    pub fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_) | RespValue::BlobError(_))
    }

    pub fn as_array(&self) -> Option<&[RespValue]> {
        match self {
            RespValue::Array(values) => Some(values.as_slice()),
            _ => None,
        }
    }
}
