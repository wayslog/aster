use bitflags::bitflags;

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

bitflags! {
    pub struct CmdFlags: u8 {
        const DONE     = 0b00000001;
        // redis cluster only
        const ASK      = 0b00000010;
        const MOVED    = 0b00000100;
        // mc only
        const NOREPLY  = 0b00001000;
        const QUIET    = 0b00010000;

        // retry
        // const RETRY    = 0b00100000;

        const ERROR    = 0b10000000;
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum CmdType {
    Read,
    Write,
    Ctrl,
    NotSupport,

    // for redis only
    MSet,   // Write
    MGet,   // Read
    Exists, // Read
    Eval,   // Write
    Del,    // Write
}
