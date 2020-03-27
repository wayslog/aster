#[inline(always)]
pub fn find_lf_iter(data: &[u8]) -> Option<usize> {
    data.iter().position(|&x| x == b'\n')
}
