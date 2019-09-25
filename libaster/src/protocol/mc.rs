use bitflags::bitflags;
use bytes::BytesMut;
use futures::task::{self, Task};

use tokio::codec::{Decoder, Encoder};

use crate::com::AsError;
use crate::protocol::IntoReply;
use crate::proxy::standalone::Request;
use crate::utils::notify::Notify;
use crate::utils::trim_hash_tag;

use std::cell::RefCell;
use std::rc::Rc;

pub mod msg;
use self::msg::Message;

#[derive(Clone, Debug)]
pub struct Cmd {
    cmd: Rc<RefCell<Command>>,
    notify: Notify,
}

impl Drop for Cmd {
    fn drop(&mut self) {
        let expect = self.notify.expect();
        let origin = self.notify.fetch_sub(1);
        // TODO: sub command maybe notify multiple
        // trace!("cmd drop strong ref {} and expect {}", origin, expect);
        if origin - 1 == expect {
            self.notify.notify();
        }
    }
}

impl Request for Cmd {
    type Reply = Message;
    type FrontCodec = FrontCodec;
    type BackCodec = BackCodec;

    fn ping_request() -> Self {
        let cmd = Command {
            ctype: CmdType::Read,
            flags: Flags::empty(),

            req: Message::version_request(),
            reply: None,
            subs: None,
        };
        Cmd {
            cmd: Rc::new(RefCell::new(cmd)),
            notify: Notify::empty(),
        }
    }

    fn reregister(&mut self, task: Task) {
        self.notify.set_task(task);
    }

    fn key_hash(&self, hash_tag: &[u8], hasher: fn(&[u8]) -> u64) -> u64 {
        let cmd = self.cmd.borrow();
        let key = cmd.req.get_key();
        hasher(trim_hash_tag(key, hash_tag))
    }

    fn subs(&self) -> Option<Vec<Self>> {
        self.cmd.borrow().subs.clone()
    }

    fn is_done(&self) -> bool {
        if let Some(subs) = self.subs() {
            subs.into_iter().all(|x| x.is_done())
        } else {
            self.cmd.borrow().is_done()
        }
    }

    fn is_error(&self) -> bool {
        self.cmd.borrow().is_error()
    }

    fn valid(&self) -> bool {
        !self.is_done()
    }

    fn set_reply<R: IntoReply<Message>>(&self, t: R) {
        let reply = t.into_reply();
        self.cmd.borrow_mut().reply = Some(reply);
        self.cmd.borrow_mut().flags |= Flags::DONE;
    }

    fn set_error(&self, t: &AsError) {
        let reply: Message = t.into_reply();
        self.cmd.borrow_mut().reply = Some(reply);
        self.cmd.borrow_mut().flags |= Flags::DONE;
        self.cmd.borrow_mut().flags |= Flags::ERROR;
    }
}

impl Cmd {
    fn from_msg(msg: Message, notify: Notify) -> Cmd {
        let subcmds: Vec<_> = msg
            .mk_subs()
            .into_iter()
            .map(|x| Cmd::from_msg(x, notify.clone()))
            .collect();
        let subs = if subcmds.is_empty() {
            Some(subcmds)
        } else {
            None
        };
        let command = Command {
            ctype: CmdType::Read,
            flags: Flags::empty(),
            req: msg,
            reply: None,
            subs,
        };
        Cmd {
            cmd: Rc::new(RefCell::new(command)),
            notify,
        }
    }
}

impl From<Message> for Cmd {
    fn from(msg: Message) -> Cmd {
        Cmd::from_msg(msg, Notify::empty())
    }
}

bitflags! {
    pub struct Flags: u8 {
        const DONE = 0b00000001;
        const ERROR = 0b00000010;
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmdType {
    Read,
    Write,
    Ctrl,
    NotSupport,
}

#[derive(Clone, Debug)]
pub struct Command {
    ctype: CmdType,
    flags: Flags,

    req: Message,
    reply: Option<Message>,

    subs: Option<Vec<Cmd>>,
}

impl Command {
    fn is_done(&self) -> bool {
        self.flags & Flags::DONE == Flags::DONE
    }

    fn is_error(&self) -> bool {
        self.flags & Flags::ERROR == Flags::ERROR
    }
}

#[derive(Default)]
pub struct FrontCodec {}

impl Decoder for FrontCodec {
    type Item = Cmd;
    type Error = AsError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Message::parse(src).map(|x| x.map(Into::into))
    }
}

impl Encoder for FrontCodec {
    type Item = Cmd;
    type Error = AsError;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut cmd = item.cmd.borrow_mut();
        if let Some(subs) = cmd.subs.as_ref().cloned() {
            for sub in subs {
                self.encode(sub, dst)?;
            }
            cmd.req.try_save_ends(dst);
        } else {
            let reply = cmd.reply.take().expect("reply must exits");
            cmd.req.save_reply(reply, dst)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct BackCodec {}

impl Decoder for BackCodec {
    type Item = Message;
    type Error = AsError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Message::parse(src)
    }
}

impl Encoder for BackCodec {
    type Item = Cmd;
    type Error = AsError;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.cmd.borrow().req.save_req(dst)
    }
}
