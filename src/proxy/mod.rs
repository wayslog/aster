/// proxy is the mod which contains genneral proxy

use std::marker::PhantomData;
use std::hash::Hasher;

pub struct Proxy<H: Hasher> {
    _placeholder: PhantomData<H>,
}
