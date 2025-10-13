/// Compute CRC16 (XMODEM) hash over the provided bytes.
pub fn crc16(data: &[u8]) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(data)
}

/// Trim hash tags according to Redis Cluster specification.
pub fn trim_hash_tag<'a>(key: &'a [u8], hash_tag: Option<&[u8]>) -> &'a [u8] {
    let tag = match hash_tag {
        Some(tag) if tag.len() == 2 => tag,
        _ => return key,
    };

    let (start, end) = (tag[0], tag[1]);

    if let Some(begin) = key.iter().position(|&b| b == start) {
        if let Some(offset) = key[begin + 1..].iter().position(|&b| b == end) {
            if offset > 0 {
                let start_idx = begin + 1;
                return &key[start_idx..start_idx + offset];
            }
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::trim_hash_tag;

    #[test]
    fn trim_hash_tag_extracts_segment() {
        let result = trim_hash_tag(b"user:{42}:profile", Some(b"{}"));
        assert_eq!(result, b"42");
    }

    #[test]
    fn trim_hash_tag_returns_key_when_missing() {
        let key = b"plain-key";
        let result = trim_hash_tag(key, Some(b"{}"));
        assert_eq!(result, key);
    }

    #[test]
    fn trim_hash_tag_ignores_invalid_hash_tag() {
        let key = b"foo";
        assert_eq!(trim_hash_tag(key, Some(b"{")), key);
    }
}
