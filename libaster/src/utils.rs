use bytes::{BufMut, BytesMut};
use itoa;

pub mod crc;
pub mod notify;
pub mod simdfind;

const LOWER_BEGIN: u8 = b'a';
const LOWER_END: u8 = b'z';
const UPPER_TO_LOWER: u8 = b'a' - b'A';

pub(crate) fn upper(input: &mut [u8]) {
    for b in input {
        if *b < LOWER_BEGIN || *b > LOWER_END {
            continue;
        }
        *b -= UPPER_TO_LOWER;
    }
}

#[test]
fn test_itoa_ok() {
    let mut buf = BytesMut::with_capacity(1);

    myitoa(10, &mut buf);
    assert_eq!("10", String::from_utf8_lossy(buf.as_ref()));
    buf.clear();

    myitoa(std::usize::MAX, &mut buf);
    assert_eq!(
        "18446744073709551615",
        String::from_utf8_lossy(buf.as_ref())
    );
    buf.clear();
}

pub(crate) fn myitoa(input: usize, buf: &mut BytesMut) {
    loop {
        if !myitoa_ok(input, buf) {
            let alen = ascii_len(input);
            if buf.remaining_mut() < alen {
                buf.reserve(alen); // alloc more size
            }
        } else {
            break;
        }
    }
}

#[inline(always)]
fn myitoa_ok(input: usize, buf: &mut BytesMut) -> bool {
    let mut writer = buf.writer();
    itoa::write(&mut writer, input).is_ok()
}

fn ascii_len(mut input: usize) -> usize {
    let mut len = 0;
    while input != 0 {
        if input < 10 {
            return len + 1;
        } else if input < 100 {
            return len + 2;
        } else if input < 1000 {
            return len + 3;
        }
        input %= 1000;
        len += 3;
    }
    len
}

#[inline]
pub fn trim_hash_tag<'a, 'b>(key: &'a [u8], hash_tag: &'b [u8]) -> &'a [u8] {
    if hash_tag.len() != 2 {
        return key;
    }
    if let Some(begin) = key.iter().position(|x| *x == hash_tag[0]) {
        if let Some(end_offset) = key[begin..].iter().position(|x| *x == hash_tag[1]) {
            // to avoid abc{}de
            if end_offset > 1 {
                return &key[begin + 1..begin + end_offset];
            }
        }
    }
    key
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    pub fn set_begin(&mut self, begin: usize) {
        self.begin = begin as u32;
    }

    #[inline(always)]
    pub fn set_end(&mut self, end: usize) {
        self.end = end as u32;
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

impl Default for Range {
    fn default() -> Range {
        Range::new(0, 0)
    }
}
