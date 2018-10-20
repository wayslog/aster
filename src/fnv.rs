#![allow(unused)]

use std::hash::Hasher;

const FNV_64_PRIME: u64 = 0x100000001b3 &  0x0000ffff;

pub struct Fnv1a64(u64);

impl Hasher for Fnv1a64 {
    fn write(&mut self, data: &[u8]) {
        let mut val = self.0 as u32;
        for b in data {
            val = val ^ (*b as u32);
            val.wrapping_mul(FNV_64_PRIME as u32);
        }
        self.0 = val as u64;
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

impl Default for Fnv1a64 {
    fn default() -> Fnv1a64 {
        Fnv1a64(0xcbf29ce484222325u64)
    }
}

#[cfg(test)]
mod test_fnv1a {
    use super::*;

    #[test]
    fn fnv1a_hash() {
        let input = b"abcdefg";
        let mut hash = Fnv1a64::default();
        hash.write(input);
        assert_eq!(hash.finish(), 2216829765);
    }
}
