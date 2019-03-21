use std::ops::Deref;

pub struct StringView {
    ptr: *const u8,
    len: usize,
}

impl StringView {
    pub fn from_str(v: &str) -> StringView {
        let ptr = v.as_ptr();
        let len = v.len();
        StringView {
            ptr, len,
        }
    }
}

impl Deref for StringView {
    type Target = str;

    fn deref(&self) -> &str {
        use std::slice;
        use std::str;

        unsafe {
            let s = slice::from_raw_parts(self.ptr, self.len);
            str::from_utf8(s).unwrap()
        }
    }
}
