use btoi;
use bytes::BufMut;
use bytes::BytesMut;
use com::*;
use itoa;
use tokio_codec::{Decoder, Encoder};

pub const SLOTS_COUNT: usize = 16384;
pub static LF_STR: &'static str = "\n";

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
        Resp {
            rtype: RESP_ARRAY,
            data: None,
            array: array,
        }
    }

    pub fn parse(src: &[u8]) -> AsResult<Self> {
        let mut iter = src.splitn(2, |x| *x == BYTE_LF);
        let line = iter.next().ok_or(Error::MoreData)?;

        let line_size = line.len();
        let rtype = line[0];

        match rtype {
            RESP_STRING | RESP_INT | RESP_ERROR => Ok(Resp {
                rtype: rtype,
                data: Some(line[1..line_size - 2].to_vec()),
                array: None,
            }),
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

                Ok(Resp {
                    rtype: rtype,
                    data: Some(data[..size].to_vec()),
                    array: None,
                })
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
                    let item = Self::parse(&src[parsed..])?;
                    parsed += item.binary_size();
                    items.push(item);
                }

                Ok(Resp {
                    rtype: rtype,
                    data: Some(count_bs.to_vec()),
                    array: Some(items),
                })
            }
            _ => unreachable!(),
        }
    }

    fn write(&mut self, dst: &mut BytesMut) -> AsResult<usize> {
        match self.rtype {
            RESP_STRING | RESP_ERROR | RESP_INT => {
                dst.put_u8(self.rtype);
                let data = self.data.as_ref().expect("never empty");
                dst.put(data);
                dst.put(BYTES_CRLF);
                Ok(1 + 2 + data.len())
            }
            RESP_BULK => {
                dst.put_u8(self.rtype);
                if self.is_null() {
                    dst.put(BYTES_NULL_RESP);
                    return Ok(5);
                }

                let data = self.data.as_ref().expect("never nulll");
                let data_len = data.len();
                let len_len = itoa::write(&mut dst[1..], data_len)?;
                dst.put(BYTES_CRLF);
                dst.put(data);
                dst.put(BYTES_CRLF);
                Ok(1 + len_len + 2 + data_len + 2)
            }
            RESP_ARRAY => {
                dst.put_u8(self.rtype);
                if self.is_null() {
                    dst.put(BYTES_NULL_RESP);
                    return Ok(5);
                }

                let data = self.data.as_ref().expect("never null");
                dst.put(data);
                dst.put(BYTES_CRLF);
                let mut size = 1 + data.len() + 2;

                for item in self.array.as_mut().expect("never empty") {
                    size += item.write(dst)?;
                }
                Ok(size)
            }
            _ => unreachable!(),
        }
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
                len += 4;
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
            src.advance(resp.binary_size());
            return Ok(Some(resp));
        }
        Ok(None)
    }
}

impl Encoder for RespCodec {
    type Item = Resp;
    type Error = Error;

    fn encode(&mut self, mut item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let size = item.write(dst)?;
        trace!("encode write bytes size {}", size);
        Ok(())
    }
}
