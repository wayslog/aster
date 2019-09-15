use bytes::{BufMut, BytesMut};
use itoa;

pub mod crc;
pub mod notify;
pub mod simdfind;

const LOWER_BEGIN: u8 = b'a';
const LOWER_END: u8 = b'z';
const UPPER_BEGIN: u8 = b'A';
const UPPER_END: u8 = b'Z';
const UPPER_TO_LOWER: u8 = b'a' - b'A';

const ASCII_0: u8 = b'0';

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
    let mut writer = buf.writer();
    itoa::write(&mut writer, input).unwrap();
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
