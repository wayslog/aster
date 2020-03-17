use std::hash::Hasher;

const FNV_64_PRIME: u32 = (1_099_511_628_211u64 & 0x0000_ffffu64) as u32;

pub struct Fnv1a64(u64);

impl Hasher for Fnv1a64 {
    fn write(&mut self, data: &[u8]) {
        let mut val = self.0 as u32;
        for b in data {
            val ^= u32::from(*b);
            val = val.wrapping_mul(FNV_64_PRIME);
        }
        self.0 = u64::from(val);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

impl Default for Fnv1a64 {
    fn default() -> Fnv1a64 {
        Fnv1a64(14_695_981_039_346_656_037)
    }
}

pub fn fnv1a64(data: &[u8]) -> u64 {
    let mut hasher = Fnv1a64::default();
    hasher.write(data);
    hasher.finish()
}

#[cfg(test)]
mod test_fnv1a {
    use super::*;

    #[test]
    fn fnv1a_hash() {
        let input = b"abcdefg";
        let mut hash = Fnv1a64::default();
        hash.write(input);
        assert_eq!(hash.finish(), 397047607);
    }
}
