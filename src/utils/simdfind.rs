use std::arch::x86_64::*;

const NEEDLE: &'static [u8] = b"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n";

#[inline(always)]
pub fn find_lf_simd(data: &[u8]) -> Option<usize> {
    let len = data.len();
    if len < 16 {
        return find_lf_iter(data);
    }
    let mut cursor = 0;
    let needle_mm = unsafe { _mm_loadu_si128(NEEDLE.as_ptr() as *const _) };
    loop {
        if len == cursor {
            return None;
        }

        if len - cursor < 16 {
            cursor = len - 16;
        }

        unsafe {
            let b = _mm_loadu_si128((&data[cursor..]).as_ptr() as *const _);
            let idx = _mm_cmpestri(needle_mm, 1, b, 16, _SIDD_CMP_EQUAL_ORDERED);
            cursor += idx as usize;
            if idx != 16 {
                return Some(cursor);
            }
        }
    }
}

#[inline(always)]
fn find_lf_iter(data: &[u8]) -> Option<usize> {
    data.iter().position(|&x| x == '\n' as u8)
}

#[allow(unused)]
#[inline(always)]
pub fn find_lf_simd2(data: &[u8]) -> Option<usize> {
    let needle_mm = unsafe { _mm_loadu_si128(NEEDLE.as_ptr() as *const _) };

    let mut cursor = 0;
    let len = data.len();
    let mut dummy = 0u128;

    while len - cursor >= 16 {
        unsafe {
            let b = _mm_loadu_si128((&data[cursor..]).as_ptr() as *const _);
            let intv = _mm_cmpeq_epi8(needle_mm, b);
            _mm_store_si128(std::mem::transmute(&mut dummy as *mut _), intv);
            let idx = dummy.trailing_zeros() as usize >> 3;
            cursor += idx as usize;
            if idx != 16 {
                return Some(cursor);
            }
        }
    }
    find_lf_iter(&data[cursor..]).map(|x| x + cursor)
}

#[cfg(test)]
fn create_data() -> Vec<u8> {
    let ds: String = std::iter::repeat(('a' as u8..'z' as u8).into_iter())
        .flatten()
        .take(3)
        .map(|x| x as char)
        .collect();
    let mut data = ds.as_bytes().to_vec();
    data.extend(b"\r\n\t");
    data
}

#[test]
fn test_simd() {
    let data = create_data();
    assert_eq!(find_lf_iter(&data), find_lf_simd(&data));
    assert_eq!(find_lf_iter(&data), find_lf_simd2(&data));
}
