use btoi;
use bytes::BufMut;
use bytes::BytesMut;
use log::Level;
use tokio_codec::{Decoder, Encoder};

use std::rc::Rc;

use com::*;
// pub const SLOTS_COUNT: usize = 16384;
// pub static LF_STR: &'static str = "\n";

pub type RespType = u8;
pub const RESP_STRING: RespType = '+' as u8;
pub const RESP_INT: RespType = ':' as u8;
pub const RESP_ERROR: RespType = '-' as u8;
pub const RESP_BULK: RespType = '$' as u8;
pub const RESP_ARRAY: RespType = '*' as u8;

pub const BYTE_CR: u8 = '\r' as u8;
pub const BYTE_LF: u8 = '\n' as u8;

pub const BYTES_CRLF: &'static [u8] = b"\r\n";
pub const BYTES_NULL_RESP: &'static [u8] = b"-1\r\n";

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
            rtype: rtype,
            data: data,
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
            data: data,
            array: array,
        }
    }

    pub fn parse(src: &[u8]) -> AsResult<Self> {
        if src.len() == 0 {
            return Err(Error::MoreData);
        }

        let mut iter = src.splitn(2, |x| *x == BYTE_LF);
        let line = iter.next().ok_or(Error::MoreData)?;
        if line[line.len() - 1] != BYTE_CR || line.len() == src.len() {
            return Err(Error::MoreData);
        }

        let line_size = line.len() + 1;
        let rtype = line[0];

        match rtype {
            RESP_STRING | RESP_INT | RESP_ERROR => {
                let resp = Resp {
                    rtype: rtype,
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
                        rtype: rtype,
                        data: None,
                        array: None,
                    });
                }
                let size = count as usize + 2;
                let data = iter.next().ok_or(Error::MoreData)?;
                if data.len() < size {
                    return Err(Error::MoreData);
                }

                let resp = Resp {
                    rtype: rtype,
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
                        rtype: rtype,
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
                    rtype: rtype,
                    data: Some(count_bs.to_vec()),
                    array: Some(items),
                };

                debug_assert_eq!(resp.binary_size(), parsed);
                Ok(resp)
            }
            _ => unreachable!(),
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

                let data = self.data.as_ref().expect("never nulll");
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

                let data = self.data.as_ref().expect("never null");
                dst.extend_from_slice(data);
                dst.extend_from_slice(BYTES_CRLF);
                let mut size = 1 + data.len() + 2;
                for item in self.array.as_ref().expect("never empty") {
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
}

pub struct RespCodec {}

impl Decoder for RespCodec {
    type Item = Resp;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let item = Resp::parse(&src)
            .map(|x| Some(x))
            .or_else(|err| match err {
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
