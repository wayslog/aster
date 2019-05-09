pub mod simdfind;
pub mod notify;

const LOWER_BEGIN: u8 = b'a';
const LOWER_END: u8 = b'z';
const UPPER_BEGIN: u8 = b'A';
const UPPER_END: u8 = b'Z';
const UPPER_TO_LOWER: u8 = b'a' - b'A';

pub(crate) fn upper(input: &mut [u8]) {
    for b in input {
        if *b < LOWER_BEGIN || *b > LOWER_END {
            continue;
        }
        *b -= UPPER_TO_LOWER;
    }
}
