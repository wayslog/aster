use crate::com::*;
use crate::utils::simdfind;

use bytes::{Bytes, BytesMut};

use std::usize;

pub const RESP_INLINE: u8 = 0u8;
pub const RESP_STRING: u8 = b'+';
pub const RESP_INT: u8 = b':';
pub const RESP_ERROR: u8 = b'-';
pub const RESP_BULK: u8 = b'$';
pub const RESP_ARRAY: u8 = b'*';

pub const BYTE_CR: u8 = b'\r';
pub const BYTE_LF: u8 = b'\n';

#[derive(Clone, Copy, Debug)]
pub struct Range {
    pub begin: u32,
    pub end: u32,
}

impl Range {
    pub fn new(begin: usize, end: usize) -> Range {
        Range {
            begin: begin as u32,
            end: end as u32,
        }
    }

    #[inline(always)]
    pub fn begin(&self) -> usize {
        self.begin as usize
    }

    #[inline(always)]
    pub fn end(&self) -> usize {
        self.end as usize
    }

    #[inline]
    pub fn range(&self) -> usize {
        (self.end - self.begin) as usize
    }
}

// contains Range means body cursor range [begin..end] for non-array type
#[derive(Debug, Clone)]
pub enum RespType {
    String(Range),
    Error(Range),
    Integer(Range),
    // contains head range and bulk body range
    Bulk(Range, Range),
    // contains head range and sub vecs
    Array(Range, Vec<RespType>),
}

impl RespType {
    pub fn array(self) -> Option<Vec<RespType>> {
        match self {
            RespType::Array(_, rv) => Some(rv),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageMut {
    pub rtype: RespType,
    pub data: BytesMut,
}

impl MessageMut {
    fn parse_inner(cursor: usize, src: &[u8]) -> Result<Option<MsgPack>, Error> {
        let pos = if let Some(p) = simdfind::find_lf_simd(&src[cursor..]) {
            p
        } else {
            return Ok(None);
        };

        if pos == 0 {
            return Err(RespError::BadMessage.into());
        }

        // detect pos -1 is CR
        if src[cursor + pos - 1] != BYTE_CR {
            // should detect inline
            return Err(RespError::BadMessage.into());
        }

        match src[cursor] {
            RESP_STRING => {
                return Ok(Some(MsgPack {
                    rtype: RespType::String(Range::new(cursor, cursor + pos + 1)),
                    size: pos + 1,
                }));
            }
            RESP_INT => {
                return Ok(Some(MsgPack {
                    rtype: RespType::Integer(Range::new(cursor, cursor + pos + 1)),
                    size: pos + 1,
                }));
            }
            RESP_ERROR => {
                return Ok(Some(MsgPack {
                    rtype: RespType::Error(Range::new(cursor, cursor + pos + 1)),
                    size: pos + 1,
                }));
            }
            RESP_BULK => {
                let csize = match btoi::btoi::<isize>(&src[cursor + 1..cursor + pos - 1]) {
                    Ok(csize) => csize,
                    Err(_err) => return Err(RespError::BadMessage.into()),
                };

                if csize == -1 {
                    return Ok(Some(MsgPack {
                        rtype: RespType::Bulk(Range::new(cursor, cursor + 5), Range::new(0, 0)),
                        size: 5,
                    }));
                } else if csize < 0 {
                    return Err(RespError::BadMessage.into());
                }

                let total_size = (pos + 1) + (csize as usize) + 2;

                if src.len() >= cursor + total_size {
                    return Ok(Some(MsgPack {
                        rtype: RespType::Bulk(
                            Range::new(cursor, cursor + pos + 1),
                            Range::new(cursor + pos + 1, cursor + total_size),
                        ),
                        size: total_size,
                    }));
                }
            }
            RESP_ARRAY => {
                let csize = match btoi::btoi::<isize>(&src[cursor + 1..cursor + pos - 1]) {
                    Ok(csize) => csize,
                    Err(_err) => return Err(RespError::BadMessage.into()),
                };
                if csize == -1 {
                    return Ok(Some(MsgPack {
                        rtype: RespType::Array(Range::new(cursor, cursor + 5), vec![]),
                        size: 5,
                    }));
                } else if csize < 0 {
                    return Err(RespError::BadMessage.into());
                }
                let mut mycursor = cursor + pos + 1;
                let mut items = Vec::new();
                for _ in 0..csize {
                    if let Some(MsgPack { rtype, size }) = Self::parse_inner(mycursor, &src[..])? {
                        mycursor += size;
                        items.push(rtype);
                    } else {
                        return Ok(None);
                    }
                }
                return Ok(Some(MsgPack {
                    rtype: RespType::Array(Range::new(cursor, cursor + pos + 1), items),
                    size: mycursor - cursor,
                }));
            }
            _ => {
                return Err(RespError::BadMessage.into());
            }
        }

        Ok(None)
    }

    pub fn parse(src: &mut BytesMut) -> Result<Option<MessageMut>, Error> {
        let rslt = match Self::parse_inner(0, &src[..]) {
            Ok(r) => r,
            Err(err) => {
                if let Some(pos) = simdfind::find_lf_simd(&src[..]) {
                    src.advance(pos + 1);
                }
                return Err(err);
            }
        };

        if let Some(MsgPack { size, rtype }) = rslt {
            let data = src.split_to(size);
            return Ok(Some(MessageMut { data, rtype }));
        }
        Ok(None)
    }
}

#[test]
fn test_parse() {
    let data = b"*2\r\n$3\r\nget\r\n$4\r\nab\nc\r\n";
    let mut src = BytesMut::from(&data[..]);
    let msg = MessageMut::parse(&mut src).unwrap().unwrap();
    assert_eq!(msg.data.len(), data.len());
    assert_eq!(msg.nth(0).unwrap(), b"get");
    assert_eq!(msg.nth(1).unwrap(), b"ab\nc");
    match msg.rtype {
        RespType::Array(head, vals) => {
            assert_eq!(head.begin, 0);
            assert_eq!(head.end, 4);

            if let RespType::Bulk(h, body) = vals[0] {
                assert_eq!(h.begin, 4);
                assert_eq!(h.end, 8);

                assert_eq!(body.begin, 8);
                assert_eq!(body.end, 13);
            } else {
                panic!("fail to load bulk string");
            }

            if let RespType::Bulk(h, body) = vals[1] {
                assert_eq!(h.begin, 13);
                assert_eq!(h.end, 17);

                assert_eq!(body.begin, 17);
                assert_eq!(body.end, 23);
            } else {
                panic!("fail to load bulk string");
            }
        }
        other => {
            panic!("fail to parse {:?}", other);
        }
    }
}

impl MessageMut {
    pub fn nth_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        if let Some(range) = self.get_nth_data_range(index) {
            Some(&mut self.data.as_mut()[range.begin()..range.end()])
        } else {
            None
        }
    }

    pub fn nth(&self, index: usize) -> Option<&[u8]> {
        if let Some(range) = self.get_nth_data_range(index) {
            Some(&self.data.as_ref()[range.begin()..range.end()])
        } else {
            None
        }
    }

    fn get_nth_data_range(&self, index: usize) -> Option<Range> {
        if let RespType::Array(_, items) = &self.rtype {
            if let Some(item) = items.get(index) {
                match item {
                    RespType::String(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        })
                    }
                    RespType::Error(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        })
                    }
                    RespType::Integer(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        })
                    }
                    RespType::Bulk(_, Range { begin, end }) => {
                        return Some(Range {
                            begin: *begin,
                            end: end - 2,
                        })
                    }
                    _ => return None,
                }
            }
        }
        None
    }
}

struct MsgPack {
    rtype: RespType,
    size: usize,
}

impl From<MessageMut> for Message {
    fn from(MessageMut { rtype, data }: MessageMut) -> Message {
        Message {
            data: data.freeze(),
            rtype,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    pub rtype: RespType,
    pub data: Bytes,
}

impl Message {
    pub fn save(&self, buf: &mut BytesMut) -> usize {
        self.save_by_rtype(&self.rtype, buf)
    }

    pub fn save_by_rtype(&self, rtype: &RespType, buf: &mut BytesMut) -> usize {
        match rtype {
            RespType::String(rg) => {
                buf.extend_from_slice(&self.data.as_ref()[rg.begin()..rg.end()]);
                rg.range()
            }
            RespType::Error(rg) => {
                buf.extend_from_slice(&self.data.as_ref()[rg.begin()..rg.end()]);
                rg.range()
            }
            RespType::Integer(rg) => {
                buf.extend_from_slice(&self.data.as_ref()[rg.begin()..rg.end()]);
                rg.range()
            }
            RespType::Bulk(head, body) => {
                buf.extend_from_slice(&self.data.as_ref()[head.begin()..body.end()]);
                (body.end - head.begin) as usize
            }
            RespType::Array(head, subs) => {
                buf.extend_from_slice(&self.data.as_ref()[head.begin()..head.end()]);
                let mut size = head.range();
                for sub in subs {
                    size += self.save_by_rtype(sub, buf);
                }
                size
            }
        }
    }

    pub fn data(&self) -> &[u8] {
        self.data.as_ref()
    }

    pub fn nth(&self, index: usize) -> Option<&[u8]> {
        if let Some(range) = self.get_nth_data_range(index) {
            Some(&self.data.as_ref()[range.begin()..range.end()])
        } else {
            None
        }
    }

    fn get_nth_data_range(&self, index: usize) -> Option<Range> {
        if let RespType::Array(_, items) = &self.rtype {
            if let Some(item) = items.get(index) {
                match item {
                    RespType::String(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        })
                    }
                    RespType::Error(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        })
                    }
                    RespType::Integer(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        })
                    }
                    RespType::Bulk(_, Range { begin, end }) => {
                        return Some(Range {
                            begin: *begin,
                            end: end - 2,
                        })
                    }
                    _ => return None,
                }
            }
        }
        None
    }
}
