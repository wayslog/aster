use bitflags::bitflags;
use btoi;
use bytes::BufMut;
use bytes::BytesMut;
use log::Level;
use tokio_codec::{Decoder, Encoder};

use std::char;
use std::collections::LinkedList;
use std::rc::Rc;

use crate::com::*;
// pub const SLOTS_COUNT: usize = 16384;
// pub static LF_STR: &'static str = "\n";

pub type RespType = u8;
pub const RESP_INLINE: RespType = 0u8;
pub const RESP_STRING: RespType = b'+';
pub const RESP_INT: RespType = b':';
pub const RESP_ERROR: RespType = b'-';
pub const RESP_BULK: RespType = b'$';
pub const RESP_ARRAY: RespType = b'*';

pub const BYTE_CR: u8 = b'\r';
pub const BYTE_LF: u8 = b'\n';

pub const BYTES_CRLF: &[u8] = b"\r\n";
pub const BYTES_NULL_RESP: &[u8] = b"-1\r\n";

#[test]
fn test_resp_parse_plain() {
    let sdata = "+baka for you\r\n";
    let resp = Resp::parse(sdata.as_bytes()).unwrap();
    assert_eq!(RESP_STRING, resp.rtype);
    assert_eq!(Some(b"baka for you".to_vec()), resp.data);

    let edata = "-boy next door\r\n";
    let resp = Resp::parse(edata.as_bytes()).unwrap();
    assert_eq!(RESP_ERROR, resp.rtype);
    assert_eq!(Some(b"boy next door".to_vec()), resp.data);

    let idata = ":1024\r\n";
    let resp = Resp::parse(idata.as_bytes()).unwrap();
    assert_eq!(RESP_INT, resp.rtype);
    assert_eq!(Some(b"1024".to_vec()), resp.data);
}

#[test]
fn test_resp_parse_bulk_ok() {
    let data = "$5\r\nojbK\n\r\n";
    let resp = Resp::parse(data.as_bytes()).unwrap();
    assert_eq!(RESP_BULK, resp.rtype);
    assert_eq!(Some(b"ojbK\n".to_vec()), resp.data);
}

#[test]
fn test_resp_parse_array_ok() {
    let data = "*2\r\n$1\r\na\r\n$5\r\nojbK\n\r\n";
    let resp = Resp::parse(data.as_bytes()).unwrap();
    assert_eq!(RESP_ARRAY, resp.rtype);
    assert_eq!(Some(b"2".to_vec()), resp.data);
    assert_eq!(2, resp.array.as_ref().unwrap().len());
}

#[test]
fn test_resp_parse_write_array_the_same_ok() {
    let data = "*2\r\n$1\r\na\r\n$5\r\nojbK\n\r\n";
    let resp = Resp::parse(data.as_bytes()).unwrap();
    assert_eq!(RESP_ARRAY, resp.rtype);
    assert_eq!(Some(b"2".to_vec()), resp.data);
    assert_eq!(2, resp.array.as_ref().unwrap().len());
    let mut buf = BytesMut::with_capacity(100);
    resp.write(&mut buf).unwrap();
    let rbuf = buf.freeze();
    assert_eq!(data.as_bytes(), &rbuf);
}

#[derive(Clone, Debug)]
pub struct Resp {
    pub rtype: RespType,
    pub data: Option<Vec<u8>>,
    pub array: Option<Vec<Resp>>,
}

impl Resp {
    pub fn new_plain(rtype: RespType, data: Option<Vec<u8>>) -> Resp {
        Resp {
            rtype,
            data,
            array: None,
        }
    }

    pub fn new_array(array: Option<Vec<Resp>>) -> Resp {
        let data: Option<Vec<u8>> = if array.is_some() {
            let array_len = array.as_ref().unwrap().len();
            Some(format!("{}", array_len).as_bytes().to_vec())
        } else {
            None
        };

        Resp {
            rtype: RESP_ARRAY,
            data,
            array,
        }
    }

    #[inline]
    fn parse_inline(line: &[u8]) -> AsResult<Self> {
        let mut line_size = line.len();
        if line[line_size - 1] == BYTE_CR {
            line_size -= 1;
        }
        Ok(Resp {
            rtype: RESP_INLINE,
            data: Some(line[..line_size].to_vec()),
            array: None,
        })
    }

    pub fn parse(src: &[u8]) -> AsResult<Self> {
        if src.is_empty() {
            return Err(Error::MoreData);
        }

        let mut iter = src.splitn(2, |x| *x == BYTE_LF);
        let line = iter.next().ok_or(Error::MoreData)?;

        // line is not end with addtional '\n'
        if line.len() == src.len() {
            return Err(Error::MoreData);
        }

        let rtype = line[0];
        match rtype {
            RESP_STRING | RESP_INT | RESP_ERROR | RESP_BULK | RESP_ARRAY => {}
            _ => {
                // trying to parse inline
                return Self::parse_inline(line);
            }
        }

        if line[line.len() - 1] != BYTE_CR {
            return Err(Error::MoreData);
        }

        let line_size = line.len() + 1;

        match rtype {
            RESP_STRING | RESP_INT | RESP_ERROR => {
                let resp = Resp {
                    rtype,
                    data: Some(line[1..line_size - 2].to_vec()),
                    array: None,
                };
                debug_assert_eq!(resp.binary_size(), line_size);
                Ok(resp)
            }
            RESP_BULK => {
                let count = btoi::btoi::<isize>(&line[1..line_size - 2])?;
                if count == -1 {
                    return Ok(Resp {
                        rtype,
                        data: None,
                        array: None,
                    });
                }
                let size = count as usize + 2;
                // BUG: if the body contains '\n'
                // the request is always NeedMoreData.
                if src.len() < line_size + size {
                    return Err(Error::MoreData);
                }
                let data = &src[line_size..line_size + size];

                let resp = Resp {
                    rtype,
                    data: Some(data[..size - 2].to_vec()),
                    array: None,
                };
                debug_assert_eq!(resp.binary_size(), size + line_size);
                Ok(resp)
            }

            RESP_ARRAY => {
                let count_bs = &line[1..line_size - 2];
                let count = btoi::btoi::<isize>(count_bs)?;
                if count == -1 {
                    return Ok(Resp {
                        rtype,
                        data: None,
                        array: None,
                    });
                }

                let mut items = Vec::with_capacity(count as usize);
                let mut parsed = line_size;
                for _ in 0..count {
                    if src.len() <= parsed {
                        return Err(Error::MoreData);
                    }

                    let item = Self::parse(&src[parsed..])?;
                    parsed += item.binary_size();
                    items.push(item);
                }

                let resp = Resp {
                    rtype,
                    data: Some(count_bs.to_vec()),
                    array: Some(items),
                };

                debug_assert_eq!(resp.binary_size(), parsed);
                Ok(resp)
            }
            _ => {
                // trying to parse inline protocol
                unreachable!();
            }
        }
    }

    fn write(&self, dst: &mut BytesMut) -> AsResult<usize> {
        match self.rtype {
            RESP_STRING | RESP_ERROR | RESP_INT => {
                let data = self.data.as_ref().expect("never empty");
                let my_len = 1 + 2 + data.len();
                if dst.remaining_mut() < my_len {
                    dst.reserve(my_len);
                }
                dst.put_u8(self.rtype);
                dst.extend_from_slice(data);
                dst.extend_from_slice(BYTES_CRLF);
                Ok(1 + 2 + data.len())
            }
            RESP_BULK => {
                if !dst.has_remaining_mut() {
                    dst.reserve(1);
                }
                dst.put_u8(self.rtype);
                if self.is_null() {
                    dst.extend_from_slice(BYTES_NULL_RESP);
                    return Ok(5);
                }

                let data = self.data.as_ref().expect("bulk never nulll");
                let data_len = data.len();
                let len_len = Self::write_len(dst, data_len)?;
                // let len_len = itoa::write(&mut dst[1..], data_len)?;
                dst.extend_from_slice(BYTES_CRLF);
                dst.extend_from_slice(data);
                dst.extend_from_slice(BYTES_CRLF);
                Ok(1 + len_len + 2 + data_len + 2)
            }
            RESP_ARRAY => {
                if dst.remaining_mut() < 5 {
                    dst.reserve(5);
                }

                dst.put_u8(self.rtype);
                if self.is_null() {
                    dst.put(BYTES_NULL_RESP);
                    return Ok(5);
                }

                let data = self.data.as_ref().expect("array never null");
                dst.extend_from_slice(data);
                dst.extend_from_slice(BYTES_CRLF);
                let mut size = 1 + data.len() + 2;
                for item in self
                    .array
                    .as_ref()
                    .expect("non-null array item never empty")
                {
                    size += item.write(dst)?;
                }
                Ok(size)
            }
            _ => unreachable!(),
        }
    }

    pub fn write_len(dst: &mut BytesMut, len: usize) -> AsResult<usize> {
        // TODO make it more faster
        let buf = format!("{}", len);
        let buf_len = buf.len();
        dst.extend_from_slice(buf.as_bytes());
        Ok(buf_len)
    }

    pub fn cmd_bytes(&self) -> &[u8] {
        let arr = self.array.as_ref().expect("must cmd");
        let resp = arr.get(0).expect("array contains more than 1 item");
        resp.data.as_ref().expect("data must exists")
    }

    fn is_null(&self) -> bool {
        match self.rtype {
            RESP_BULK => self.data.is_none(),
            RESP_ARRAY => self.array.is_none(),
            _ => false,
        }
    }

    fn ascii_len(mut n: usize) -> usize {
        let mut len = 0;
        loop {
            if n == 0 {
                return len;
            } else if n < 10 {
                return len + 1;
            } else if n < 100 {
                return len + 2;
            } else if n < 1000 {
                return len + 3;
            } else {
                n /= 1000;
                len += 3;
            }
        }
    }

    fn binary_size(&self) -> usize {
        match self.rtype {
            RESP_STRING | RESP_ERROR | RESP_INT => {
                3 + self.data.as_ref().expect("never be empty").len()
            }
            RESP_BULK => {
                if self.is_null() {
                    return 5;
                }

                let dlen = self.data.as_ref().expect("never null").len();
                1 + Self::ascii_len(dlen) + 2 + dlen + 2
            }
            RESP_ARRAY => {
                if self.is_null() {
                    return 5;
                }
                let mut size = 1 + self.data.as_ref().expect("never null").len() + 2;
                for item in self.array.as_ref().expect("never empty") {
                    size += item.binary_size();
                }
                size
            }
            RESP_INLINE => self.data.as_ref().expect("inline always have data").len() + 2,
            _ => unreachable!(),
        }
    }

    pub fn get(&self, i: usize) -> Option<&Self> {
        self.array
            .as_ref()
            .map(|x| x.get(i))
            .expect("must be array")
    }

    pub fn get_mut(&mut self, i: usize) -> Option<&mut Self> {
        self.array
            .as_mut()
            .map(|x| x.get_mut(i))
            .expect("must be array")
    }

    pub fn is_inline(&self) -> bool {
        self.rtype == RESP_INLINE
    }

    pub fn unwrap_data(self) -> Option<Vec<u8>> {
        let Resp {
            rtype: _rtype,
            data: d,
            array: _array,
        } = self;
        d
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RespObj {
    pub rtype: RespType,
    pub data: Option<BytesMut>,
    pub array: Option<Vec<RespObj>>,
}

#[allow(unused)]
impl RespObj {
    fn empty(rtype: RespType) -> RespObj {
        RespObj {
            data: None,
            array: None,
            rtype,
        }
    }

    fn get_mut(&mut self, cursor: usize) -> Option<&mut RespObj> {
        if let Some(arr) = self.array.as_mut() {
            return arr.get_mut(cursor);
        }
        None
    }

    fn set_data(&mut self, data: BytesMut) {
        self.data.replace(data);
    }

    fn push(&mut self, resp: RespObj) {
        if self.array.is_none() {
            self.array = Some(Vec::new());
        }
        let vec = self.array.as_mut().expect("never be empty");
        vec.push(resp);
    }
}

#[test]
fn test_fsm_inline() {
    let sdata = b"get baka\nget kaba qiu\r\n";
    let mut codec = RespFSMCodec::default();
    let mut buf = BytesMut::from(&sdata[..]);
    let resp = codec.parse(&mut buf).unwrap().unwrap();
    assert_eq!(resp.rtype, RESP_ARRAY);
    assert_eq!(resp.array.as_ref().map(|x| x.len()), Some(2));

    let resp = codec.parse(&mut buf).unwrap().unwrap();
    assert_eq!(resp.array.as_ref().map(|x| x.len()), Some(3));
}


#[test]
fn test_fsm_palin_ok() {
    let sdata = b"+baka for you\r\n";
    let mut codec = RespFSMCodec::default();
    let resp = codec
        .parse(&mut BytesMut::from(&sdata[..]))
        .unwrap()
        .unwrap();
    assert_eq!(RESP_STRING, resp.rtype);
    assert_eq!(Some(BytesMut::from(&b"baka for you"[..])), resp.data);

    let sdata = b"-boy next door\r\n";
    let mut codec = RespFSMCodec::default();
    let resp = codec
        .parse(&mut BytesMut::from(&sdata[..]))
        .unwrap()
        .unwrap();
    assert_eq!(RESP_ERROR, resp.rtype);
    assert_eq!(Some(BytesMut::from(&sdata[1..sdata.len() - 2])), resp.data);

    let sdata = b":-1024\r\n";
    let mut codec = RespFSMCodec::default();
    let resp = codec
        .parse(&mut BytesMut::from(&sdata[..]))
        .unwrap()
        .unwrap();
    assert_eq!(RESP_INT, resp.rtype);
    assert_eq!(Some(BytesMut::from(&sdata[1..sdata.len() - 2])), resp.data);
}

#[test]
fn test_fsm_bulk_ok() {
    let sdata = "$5\r\nojbK\n\r\n";
    let mut codec = RespFSMCodec::default();
    let resp = codec
        .parse(&mut BytesMut::from(&sdata[..]))
        .unwrap()
        .unwrap();
    assert_eq!(RESP_BULK, resp.rtype);
    assert_eq!(Some(BytesMut::from(&b"ojbK\n"[..])), resp.data);
}

#[test]
fn test_fsm_ends_with_half_crlf() {
    let sdata = "$5\r\nojbK\n\r";
    let mut codec = RespFSMCodec::default();
    let mut buf = BytesMut::from(&sdata[..]);

    assert_eq!(codec.parse(&mut buf).unwrap(), None);
    buf.put_u8(10u8); // write \n
    let resp = codec.parse(&mut buf).unwrap().unwrap();
    assert_eq!(RESP_BULK, resp.rtype);
    assert_eq!(Some(BytesMut::from(&b"ojbK\n"[..])), resp.data);
}

#[test]
fn test_fsm_array_ok() {
    let sdata = "*2\r\n$1\r\na\r\n$5\r\nojbK\n\r\n";

    let mut codec = RespFSMCodec::default();
    let resp = codec
        .parse(&mut BytesMut::from(&sdata[..]))
        .unwrap()
        .unwrap();

    assert_eq!(RESP_ARRAY, resp.rtype);
    assert_eq!(Some(BytesMut::from(&b"2"[..])), resp.data);
    assert_eq!(2, resp.array.as_ref().unwrap().len());
    assert_eq!(
        resp.array,
        Some(vec![
            RespObj {
                rtype: RESP_BULK,
                data: Some(BytesMut::from(&b"a"[..])),
                array: None,
            },
            RespObj {
                rtype: RESP_BULK,
                data: Some(BytesMut::from(&b"ojbK\n"[..])),
                array: None,
            }
        ])
    );
}

bitflags! {
    struct Flag: u8 {
        const KIND         = 0b00000001;
        const PLAIN_BODY   = 0b00000010;
        const BULK_SIZE    = 0b00000100;
        const BULK_BODY    = 0b00001000;
        const ARRAY_SIZE   = 0b00010000;
        const INLINE       = 0b00100000;
    }
}

pub struct RespFSMCodec {
    buf: BytesMut,

    stack: LinkedList<RespObj>,
    cstack: LinkedList<isize>,
    count: isize,

    current: Option<RespObj>,
    size: isize,

    flags: Flag,
}

impl Default for RespFSMCodec {
    fn default() -> RespFSMCodec {
        RespFSMCodec {
            buf: BytesMut::new(),
            stack: LinkedList::new(),
            cstack: LinkedList::new(),
            count: 0,
            size: 0,
            current: None,
            flags: Flag::KIND,
        }
    }
}

#[allow(unused)]
impl RespFSMCodec {
    pub fn parse(&mut self, src: &mut BytesMut) -> Result<Option<RespObj>, Error> {
        loop {
            if self.flags == Flag::KIND {
                if src.is_empty() {
                    return Ok(None);
                }
                let rtype = src[0];
                src.advance(1);
                match rtype {
                    RESP_BULK => {
                        self.current = Some(RespObj::empty(rtype));
                        self.flags = Flag::BULK_SIZE;
                    }
                    RESP_INT | RESP_STRING | RESP_ERROR => {
                        self.current = Some(RespObj::empty(rtype));
                        self.flags = Flag::PLAIN_BODY;
                    }
                    RESP_ARRAY => {
                        self.flags = Flag::ARRAY_SIZE;
                        self.stack.push_back(RespObj::empty(rtype));
                        self.cstack.push_back(self.count);
                        self.count = 0;
                    }
                    _ => {
                        self.flags = Flag::INLINE;
                    }
                };
            } else if self.flags == Flag::INLINE {
                if let None = self.read_until_crlf(src) {
                    return Ok(None);
                }
                let len = self.buf.len();
                if len < 1 {
                    return Err(Error::BadCmd);
                }
                self.flags = Flag::KIND;

                let mut trim = 1;
                if self.buf[len - 1] == BYTE_CR {
                    trim = 2;
                }
                let buf = self.buf.as_ref();
                let line = String::from_utf8_lossy(&buf[..len - trim]);
                let cmds: Vec<_> = line
                    .split(char::is_whitespace)
                    .filter(|x| x.len() != 0)
                    .map(|x| RespObj {
                        rtype: RESP_BULK,
                        data: Some(BytesMut::from(x.as_bytes())),
                        array: None,
                    })
                    .collect();
                self.buf.take();
                return Ok(Some(RespObj {
                    rtype: RESP_ARRAY,
                    data: Some(BytesMut::from(format!("{}", cmds.len()).as_bytes())),
                    array: Some(cmds),
                }));
            } else if self.flags == Flag::PLAIN_BODY {
                if let None = self.read_until_crlf(src) {
                    return Ok(None);
                }
                let len = self.buf.len();
                unsafe {
                    self.buf.set_len(len - 2);
                }

                self.next_kind();
                let ret = self.pop_array();
                if ret.is_some() {
                    return Ok(ret);
                }
            } else if self.flags == Flag::BULK_SIZE {
                if let None = self.read_until_crlf(src) {
                    return Ok(None);
                }
                let len = self.buf.len();
                unsafe {
                    self.buf.set_len(len - 2);
                }

                self.flags = Flag::BULK_BODY;
                let size = btoi::btoi::<isize>(self.buf.take().as_ref())?;
                if size == -1 {
                    self.flags = Flag::KIND;
                    let ret = self.pop_array();
                    if ret.is_some() {
                        return Ok(ret);
                    }
                }
                self.size = size + 2;
            } else if self.flags == Flag::BULK_BODY {
                let left = self.size as usize - self.buf.len();
                if src.len() >= left {
                    self.buf.extend_from_slice(src.split_to(left).as_ref());
                } else {
                    self.buf.extend_from_slice(src.take().as_ref());
                    return Ok(None);
                }
                let len = self.buf.len();
                unsafe {
                    self.buf.set_len(len - 2);
                }

                self.next_kind();
                let ret = self.pop_array();
                if ret.is_some() {
                    return Ok(ret);
                }
            } else if self.flags == Flag::ARRAY_SIZE {
                if let None = self.read_until_crlf(src) {
                    return Ok(None);
                }
                let len = self.buf.len();
                unsafe {
                    self.buf.set_len(len - 2);
                }

                self.flags = Flag::KIND;

                let count = btoi::btoi::<isize>(self.buf.as_ref())?;
                // set buf into stack top resp data
                let handle = self.stack.back_mut().unwrap();
                handle.set_data(self.buf.take());

                self.count = count;
                if count == -1 {
                    self.count -= 1;
                    let poped = self.stack.pop_back();
                    if self.stack.is_empty() {
                        self.count = 0;
                        return Ok(poped);
                    }

                    let top = self.stack.back_mut().expect("parse stack.last_mut");
                    top.push(poped.expect("parse poped"));
                    self.cstack_add(-1);
                }
            } else {
                unreachable!();
            }
        }

        // Ok(None)
    }

    #[inline]
    fn read_until_crlf(&mut self, src: &mut BytesMut) -> Option<()> {
        if let Some(pos) = src.as_ref().iter().position(|&x| x == BYTE_LF) {
            self.buf.extend_from_slice(src.split_to(pos + 1).as_ref());
            Some(())
        } else {
            self.buf.extend_from_slice(src.take().as_ref());
            None
        }
    }

    #[inline]
    fn next_kind(&mut self) {
        self.flags = Flag::KIND;
        let buf = self.buf.take();
        let hd = self.current.as_mut().expect("parse current.as_mut");
        hd.set_data(buf);
    }

    #[inline]
    fn pop_array(&mut self) -> Option<RespObj> {
        if self.count <= 0 {
            return self.current.take();
        }

        let current_take = self.current.take().unwrap();
        self.push_top(current_take);
        if self.count == 1 {
            // pop top and push it into next top level or return
            let poped = self.stack.pop_back();
            if self.stack.is_empty() {
                return poped;
            }
            self.push_top(poped.unwrap());
            self.count = self.cstack.pop_back().unwrap() - 1;
        } else if self.count > 0 {
            self.count -= 1;
        }
        None
    }

    #[inline]
    fn push_top(&mut self, resp: RespObj) {
        let top = self
            .stack
            .back_mut()
            .expect("parse stack.last_mut plain_body");
        top.push(resp);
    }

    #[inline]
    fn cstack_add(&mut self, count: isize) {
        let handle = self.cstack.back_mut().expect("cstack_add cstack.last_mut");
        *handle += count;
    }
}

impl Decoder for RespFSMCodec {
    type Item = RespObj;
    type Error = Error;

    fn decode(&mut self, _src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        unimplemented!()
    }
}

pub struct RespCodec {}

impl Decoder for RespCodec {
    type Item = Resp;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let item = Resp::parse(&src).map(Some).or_else(|err| match err {
            Error::MoreData => Ok(None),
            ev => Err(ev),
        })?;
        if let Some(resp) = item {
            let bsize = resp.binary_size();
            if bsize > src.len() {
                trace!(
                    "decode read bytes size={} and remaining_mut={} for bytes={:?}",
                    bsize,
                    src.len(),
                    &src[..],
                );
            }
            src.advance(bsize);
            return Ok(Some(resp));
        }
        Ok(None)
    }
}

impl Encoder for RespCodec {
    type Item = Rc<Resp>;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let size = item.write(dst)?;
        if log_enabled!(Level::Trace) {
            trace!("encode write bytes size {}", size);
        }
        Ok(())
    }
}

#[test]
fn test_resp_ascii_len() {
    assert_eq!(Resp::ascii_len(11111), 5);
    assert_eq!(Resp::ascii_len(1000), 4);
    assert_eq!(Resp::ascii_len(100), 3);
    assert_eq!(Resp::ascii_len(10), 2);
    assert_eq!(Resp::ascii_len(2), 1);
}
