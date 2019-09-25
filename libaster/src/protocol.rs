pub mod mc;
pub mod redis;

pub trait IntoReply<R> {
    fn into_reply(self) -> R;
}

impl<T> IntoReply<T> for T {
    fn into_reply(self) -> T {
        self
    }
}
