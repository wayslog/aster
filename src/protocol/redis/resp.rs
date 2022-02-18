use crate::com::*;
use crate::proxy::cluster::Redirect;
use crate::utils::simdfind;
use crate::utils::Range;

use aho_corasick::AhoCorasick;
use bytes::{BufMut, Bytes, BytesMut};

pub const RESP_STRING: u8 = b'+';
pub const RESP_INT: u8 = b':';
pub const RESP_ERROR: u8 = b'-';
pub const RESP_BULK: u8 = b'$';
pub const RESP_ARRAY: u8 = b'*';

pub const BYTE_CR: u8 = b'\r';
pub const BYTE_LF: u8 = b'\n';

pub const BYTES_CMD_CLUSTER_SLOTS: &[u8] = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";
pub const BYTES_CMD_CLUSTER_NODES: &[u8] = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n";

// contains Range means body cursor range [begin..end] for non-array type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespType {
    String(Range),
    Error(Range),
    Integer(Range),
    // contains head range and bulk body range
    Bulk(Range, Range),
    // contains head range and sub vecs
    Array(Range, Vec<RespType>),

    Inline(Vec<Range>),
}

impl RespType {
    pub fn array(self) -> Option<Vec<RespType>> {
        match self {
            RespType::Array(_, rv) => Some(rv),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMut {
    pub rtype: RespType,
    pub data: BytesMut,
}

impl MessageMut {
    fn try_parse_inline(src: &[u8]) -> Result<Option<MsgPack>, AsError> {
        let mut cursor = 0;
        let mut fields = Vec::new();
        for data in src.split(|x| *x == BYTE_SPACE) {
            fields.push(Range::new(cursor, cursor + data.len()));
            cursor = cursor + data.len() + 1;
        }
        Ok(Some(MsgPack {
            rtype: RespType::Inline(fields),
            size: src.len(),
        }))
    }

    fn parse_inner(cursor: usize, src: &[u8]) -> Result<Option<MsgPack>, AsError> {
        let pos = if let Some(p) = simdfind::find_lf_simd(&src[cursor..]) {
            p
        } else {
            return Ok(None);
        };

        if pos == 0 {
            return Err(AsError::BadMessage);
        }

        // detect pos -1 is CR
        if src[cursor + pos - 1] != BYTE_CR {
            // should detect inline
            return Self::try_parse_inline(&src[cursor..cursor + pos + 1]);
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
                    Err(_err) => return Err(AsError::BadMessage),
                };

                if csize == -1 {
                    return Ok(Some(MsgPack {
                        rtype: RespType::Bulk(
                            Range::new(cursor, cursor + 5),
                            Range::new(cursor, cursor + 5),
                        ),
                        size: 5,
                    }));
                } else if csize < 0 {
                    return Err(AsError::BadMessage);
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
                    Err(_err) => return Err(AsError::BadMessage),
                };
                if csize == -1 {
                    return Ok(Some(MsgPack {
                        rtype: RespType::Array(Range::new(cursor, cursor + 5), vec![]),
                        size: 5,
                    }));
                } else if csize < 0 {
                    return Err(AsError::BadMessage);
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
                if cursor != 0 {
                    return Err(AsError::BadMessage);
                }
                // inline command
                return Self::try_parse_inline(&src[cursor..cursor + pos + 1]);
            }
        }

        Ok(None)
    }

    pub fn parse(src: &mut BytesMut) -> Result<Option<MessageMut>, AsError> {
        let rslt = match Self::parse_inner(0, &src[..]) {
            Ok(r) => r,
            Err(err) => {
                // TODO: should change it as wrong bad command error
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
                        });
                    }
                    RespType::Error(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        });
                    }
                    RespType::Integer(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        });
                    }
                    RespType::Bulk(_, Range { begin, end }) => {
                        return Some(Range {
                            begin: *begin,
                            end: end - 2,
                        });
                    }
                    _ => return None,
                }
            }
        }
        if let RespType::Inline(fields) = &self.rtype {
            if let Some(rng) = fields.get(index) {
                let mut end = rng.end();
                let len = rng.range();
                if len == 0 {
                    return Some(*rng);
                }
                if len > 0 && self.data[len - 1] == BYTE_LF {
                    end -= 1;
                    if len > 1 && self.data[len - 2] == BYTE_CR {
                        end -= 1;
                    }
                }
                return Some(Range::new(rng.begin(), end));
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

pub struct MessageIter<'a> {
    msg: &'a Message,
    index: usize,
}

impl<'a> Iterator for MessageIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.index;
        self.index += 1;
        self.msg.nth(current)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub rtype: RespType,
    pub data: Bytes,
}

impl Message {
    pub fn new_cluster_slots() -> Message {
        Message {
            data: Bytes::from(BYTES_CMD_CLUSTER_SLOTS),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![
                    RespType::Bulk(Range::new(4, 8), Range::new(8, 17)),
                    RespType::Bulk(Range::new(17, 21), Range::new(21, 28)),
                ],
            ),
        }
    }

    pub fn new_read_only() -> Message {
        Message {
            data: Bytes::from("*1\r\n$8\r\nREADONLY\r\n"),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![RespType::Bulk(Range::new(4, 8), Range::new(8, 18))],
            ),
        }
    }

    pub fn new_ping_request() -> Message {
        Message {
            data: Bytes::from("*1\r\n$4\r\nPING\r\n"),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![RespType::Bulk(Range::new(4, 8), Range::new(8, 14))],
            ),
        }
    }

    pub fn new_auth(auth: &str) -> Message {
        let len = auth.len();
        let len_len = len.to_string().len();
        let s = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", len, auth);
        Message {
            data: Bytes::from(s),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![
                    RespType::Bulk(Range::new(4, 8), Range::new(8, 14)),
                    RespType::Bulk(Range::new(14, 17 + len_len), Range::new(17 + len_len, 17 + len_len + len + 2)),
                ],
            ),
        }
    }

    pub fn inline_raw(data: Bytes) -> Message {
        let rngs = vec![Range::new(0, data.len())];
        Message {
            rtype: RespType::Inline(rngs),
            data,
        }
    }

    pub fn plain<I: Into<Bytes>>(data: I, resp_type: u8) -> Message {
        let bytes = data.into();
        let mut rdata = BytesMut::new();
        let total_len = 1 /* resp_type */ + bytes.len() + 2 /*\r\n*/;
        rdata.reserve(total_len);
        rdata.put_u8(resp_type);
        rdata.put(&bytes);
        rdata.put_u8(BYTE_CR);
        rdata.put_u8(BYTE_LF);

        let rtype = if resp_type == RESP_STRING {
            RespType::String(Range::new(0, total_len))
        } else if resp_type == RESP_INT {
            RespType::Integer(Range::new(0, total_len))
        } else if resp_type == RESP_ERROR {
            RespType::Error(Range::new(0, total_len))
        } else {
            unreachable!("fail to create uon plain message");
        };

        Message {
            data: rdata.into(),
            rtype,
        }
    }

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
                let data = &self.data.as_ref()[head.begin()..body.end()];
                buf.extend_from_slice(data);
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
            RespType::Inline(fields) => {
                let first_begin = fields.first().map(|x| x.begin()).unwrap_or(0);
                let last_end = fields.last().map(|x| x.end()).unwrap_or(0);
                if first_begin != last_end {
                    buf.extend_from_slice(&self.data.as_ref()[first_begin..last_end]);
                }
                last_end - first_begin
            }
        }
    }

    pub fn raw_data(&self) -> &[u8] {
        self.data.as_ref()
    }

    pub fn data(&self) -> Option<&[u8]> {
        let range = self.get_range(Some(&self.rtype));
        range.map(|rg| &self.data.as_ref()[rg.begin()..rg.end()])
    }

    pub fn nth(&self, index: usize) -> Option<&[u8]> {
        if let Some(range) = self.get_nth_data_range(index) {
            return Some(&self.data.as_ref()[range.begin()..range.end()]);
        }

        if index == 0 {
            // only zero shot path to data
            return self.data();
        }
        None
    }

    fn get_nth_data_range(&self, index: usize) -> Option<Range> {
        if let RespType::Array(_, items) = &self.rtype {
            return self.get_range(items.get(index));
        }
        if let RespType::Inline(fields) = &self.rtype {
            if let Some(rng) = fields.get(index) {
                let mut end = rng.end();
                let len = rng.range();
                if rng.begin() == rng.end() {
                    return Some(*rng);
                }
                if len > 0 && self.data[len - 1] == BYTE_LF {
                    end -= 1;
                    if len > 1 && self.data[len - 2] == BYTE_CR {
                        end -= 1;
                    }
                }
                return Some(Range::new(rng.begin(), end));
            }
        }
        None
    }

    pub(super) fn get_data_of_range(&self, rg: Range) -> &[u8] {
        &self.data.as_ref()[rg.begin()..rg.end()]
    }

    pub(super) fn get_range(&self, rtype: Option<&RespType>) -> Option<Range> {
        if let Some(item) = rtype {
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

        None
    }

    pub fn iter(&self) -> MessageIter {
        MessageIter {
            msg: &self,
            index: 0,
        }
    }

    pub fn check_redirect(&self) -> Option<Redirect> {
        match self.rtype {
            RespType::Error(_) => {}
            _ => return None,
        }
        if let Some(data) = self.data().as_ref() {
            return parse_redirect(data);
        }
        None
    }
}

const BYTE_SPACE: u8 = b' ';
const PATTERNS: &[&str] = &["ASK", "MOVED"];

lazy_static! {
    static ref FINDER: AhoCorasick = AhoCorasick::new(PATTERNS);
}

fn parse_redirect(data: &[u8]) -> Option<Redirect> {
    if let Some(mat) = FINDER.find(data) {
        let pat = mat.pattern();
        let end = mat.end();
        let rdata = &data[end + 1..];

        let pos = rdata.iter().position(|&x| x == BYTE_SPACE)?;

        let sdata = &rdata[..pos];
        let tdata = &rdata[pos + 1..];
        if let Ok(slot) = btoi::btoi::<usize>(sdata) {
            let to = String::from_utf8_lossy(tdata);
            let to = to.to_string();
            if pat == 0 {
                return Some(Redirect::Ask { slot, to });
            } else {
                // moved
                return Some(Redirect::Move { slot, to });
            }
        }
    }
    None
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

#[cfg(test)]
mod test {
    use self::super::*;
    use assert2::{assert, check};

    #[test]
    fn test_iter() {
        let data = b"*2\r\n$3\r\nget\r\n$4\r\nab\nc\r\n";
        let mut src = BytesMut::from(&data[..]);
        let msg: Message = MessageMut::parse(&mut src).unwrap().unwrap().into();
        assert!(msg.raw_data().len() == data.len());
        let mut iter = msg.iter();
        assert!(iter.next() == Some(b"get".as_ref()));
        assert!(iter.next() == Some(b"ab\nc".as_ref()));
    }

    #[test]
    fn test_iter_plain() {
        let data = b"+abcdef\r\n";
        let mut src = BytesMut::from(&data[..]);
        let msg: Message = MessageMut::parse(&mut src).unwrap().unwrap().into();
        assert!(msg.raw_data().len() == data.len());
        let mut iter = msg.iter();
        assert!(iter.next() == Some("abcdef".as_bytes()));
        assert!(iter.next() == None);
    }

    #[test]
    fn test_iter_bulk() {
        let data = b"$3\r\nabc\r\n";
        let mut src = BytesMut::from(&data[..]);
        let msg: Message = MessageMut::parse(&mut src).unwrap().unwrap().into();
        assert!(msg.raw_data().len() == data.len());
        let mut iter = msg.iter();
        assert!(iter.next() == Some("abc".as_bytes()));
        assert!(iter.next() == None);
    }

    #[test]
    fn test_parse_cluster_slots() {
        let data = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";
        let mut src = BytesMut::from(&data[..]);
        let msg: Message = MessageMut::parse(&mut src).unwrap().unwrap().into();
        assert!(
            msg == Message {
                data: Bytes::from("*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n"),
                rtype: RespType::Array(
                    Range::new(0, 4),
                    vec![
                        RespType::Bulk(Range::new(4, 8), Range::new(8, 17)),
                        RespType::Bulk(Range::new(17, 21), Range::new(21, 28)),
                    ],
                ),
            }
        );
    }

    #[test]
    fn test_parse_inline() {
        let data = b"GET a\n";
        let mut src = BytesMut::from(&data[..]);
        let msg: Message = MessageMut::parse(&mut src).unwrap().unwrap().into();
        assert!(
            msg == Message {
                data: Bytes::from(&data[..]),
                rtype: RespType::Inline(vec![Range::new(0, 3), Range::new(4, 6),]),
            }
        )
    }

    // ---------------------- test copy from redis/tests/unit/protocol.tcl ------------------------------------------ //

    #[test]
    fn test_handle_empy_query() {
        let data = b"\r\n";
        let mut src = BytesMut::from(data.as_ref());
        let msg = MessageMut::parse(&mut src).unwrap().unwrap();
        assert!(
            msg == MessageMut {
                data: BytesMut::from(data.as_ref()),
                rtype: RespType::Inline(vec![Range::new(0, 2)]),
            }
        );
        assert_eq!(src.len(), 0);
    }

    #[test]
    fn test_negative_multibulk_length() {
        let data = b"*-10\r\n";
        let mut src = BytesMut::from(data.as_ref());
        assert!(MessageMut::parse(&mut src).unwrap_err() == AsError::BadMessage);
    }

    #[test]
    fn test_wrong_mutlti_bulk_payload_header() {
        let data = "*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfooz\r\n";
        let mut src = BytesMut::from(data.as_bytes());
        check!(MessageMut::parse(&mut src).unwrap_err() == AsError::BadMessage);
        check!(src.len() == data.len() - 4);
    }

    #[test]
    fn test_negative_mutiblk_payload_length() {
        let data = "*3\r\n\\$3\r\nSET\r\n\\$1\r\nx\r\nfooz\r\n";
        let mut src = BytesMut::from(data.as_bytes());
        assert!(MessageMut::parse(&mut src).unwrap_err() == AsError::BadMessage);
    }

    #[test]
    fn test_non_number_multibulk_payload_length() {
        let data = "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$blabla\r\n";
        let mut src = BytesMut::from(data.as_bytes());
        assert!(MessageMut::parse(&mut src).unwrap_err() == AsError::BadMessage);
    }
}
