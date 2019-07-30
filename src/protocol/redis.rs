use bitflags::bitflags;
use bytes::BytesMut;
use failure::Error;
use futures::task::Task;
use tokio::codec::{Decoder, Encoder};

use crate::com::AsError;
use crate::utils::myitoa;
use crate::utils::notify::Notify;
use crate::utils::upper;

bitflags! {
    struct CFlags: u8 {
        const DONE     = 0b00000001;
        const ASK      = 0b00000010;
    }
}

pub mod cmd;
pub mod resp;

pub use cmd::CmdType;
pub use resp::{Message, MessageMut, RespType};

pub struct Command {
    flags: CFlags,
    ctype: CmdType,
    // Command redirect count
    cycle: u8,

    req: Message,
    reply: Option<Message>,

    notify: Notify,
    subs: Option<Vec<Command>>,
}

const BYTES_JUSTOK: &[u8] = b"+OK\r\n";
const BYTES_NULL_ARRAY: &[u8] = b"*-1\r\n";
const BYTES_ZERO_INT: &[u8] = b":0\r\n";

const BYTES_CRLF: &[u8] = b"\r\n";

const BYTES_ARRAY: &[u8] = b"*";
const BYTES_INTEGER: &[u8] = b":";

const DEFAULT_CYCLE: u8 = 0;
const MAX_CYCLE: u8 = 0b11111111;

// for front end
impl Command {
    pub fn parse_cmd(buf: &mut BytesMut) -> Result<Option<Command>, Error> {
        let msg = MessageMut::parse(buf)?;
        Ok(msg.map(Into::into))
    }

    pub fn reply_cmd(&self, buf: &mut BytesMut) -> Result<usize, Error> {
        if self.ctype.is_mset() {
            buf.extend_from_slice(BYTES_JUSTOK);
            return Ok(BYTES_JUSTOK.len());
        } else if self.ctype.is_mget() {
            if let Some(subs) = self.subs.as_ref() {
                buf.extend_from_slice(BYTES_ARRAY);

                let begin = buf.len();
                let len = subs.len();
                myitoa(len, buf);
                buf.extend_from_slice(BYTES_CRLF);

                for sub in subs {
                    sub.reply_cmd(buf)?;
                }
                return Ok(buf.len() - begin);
            } else {
                buf.extend_from_slice(BYTES_NULL_ARRAY);
                return Ok(BYTES_NULL_ARRAY.len());
            }
        } else if self.ctype.is_del() || self.ctype.is_exists() {
            if let Some(subs) = self.subs.as_ref() {
                let begin = buf.len();
                buf.extend_from_slice(BYTES_INTEGER);
                let len = subs.len();
                myitoa(len, buf);
                buf.extend_from_slice(BYTES_CRLF);
                return Ok(buf.len() - begin);
            } else {
                buf.extend_from_slice(BYTES_ZERO_INT);
                return Ok(BYTES_ZERO_INT.len());
            }
        } else {
            if let Some(size) = self.reply.as_ref().map(|x| x.save(buf)) {
                return Ok(size);
            } else {
                Err(AsError::BadReply.into())
            }
        }
    }
}

// const BYTES_ASK: &[u8] = b"*1/r/n$3/r/nASK/r/n";

const BYTES_GET: &[u8] = b"*3\r\nGET\r\n";
const BYTES_LEN2_HEAD: &[u8] = b"*2/r/n";
const BYTES_LEN3_HEAD: &[u8] = b"*3/r/n";

// for back end
impl Command {
    /// save redis Command into given BytesMut
    pub fn send_req(&self, buf: &mut BytesMut) -> Result<(), Error> {
        if self.ctype.is_exists() && self.ctype.is_del() {
            buf.extend_from_slice(BYTES_LEN2_HEAD);
            if let RespType::Array(_, arrs) = &self.req.rtype {
                for rtype in arrs {
                    self.req.save_by_rtype(rtype, buf);
                }
            }
            return Ok(());
        } else if self.ctype.is_mset() {
            buf.extend_from_slice(BYTES_LEN3_HEAD);
            if let RespType::Array(_, arrs) = &self.req.rtype {
                for rtype in arrs {
                    self.req.save_by_rtype(rtype, buf);
                }
            }
            return Ok(());
        } else if self.ctype.is_mget() {
            buf.extend_from_slice(BYTES_LEN2_HEAD);
            buf.extend_from_slice(BYTES_GET);

            if let RespType::Array(_, arrs) = &self.req.rtype {
                for rtype in &arrs[1..] {
                    self.req.save_by_rtype(rtype, buf);
                }
            }
            return Ok(());
        }
        self.req.save(buf);
        Ok(())
    }
}

/// parse redis Command from BytesMut buffer ignore if that's ask
pub fn recv_reply(buf: &mut BytesMut) -> Result<Option<Message>, Error> {
    let reply = MessageMut::parse(buf)?;
    Ok(reply.map(Into::into))
}

impl Command {
    pub fn key_hash<T>(&self, method: T) -> u64
    where
        T: Fn(&[u8]) -> u64,
    {
        let pos = self.key_pos();

        if let Some(cmd_data) = self.req.nth(pos) {
            method(cmd_data)
        } else {
            // TODO: set bad request error
            unreachable!()
        }
    }

    #[inline(always)]
    fn key_pos(&self) -> usize {
        if self.ctype.is_eval() {
            return KEY_EVAL_POS;
        }
        KEY_RAW_POS
    }

    pub fn reregister(&self, task: Task) {
        self.notify.set_task(task);
    }

    pub fn is_done(&self) -> bool {
        self.flags & CFlags::DONE == CFlags::DONE
    }

    pub fn cycle(&self) -> u8 {
        self.cycle
    }

    pub fn can_cycle(&mut self) -> bool {
        self.cycle < MAX_CYCLE
    }

    pub fn add_cycle(&mut self) {
        self.cycle += 1;
    }

    pub fn set_done(&mut self) {
        self.flags |= CFlags::DONE;
    }

    pub fn is_ask(&self) -> bool {
        self.flags & CFlags::ASK == CFlags::ASK
    }

    pub fn set_ask(&mut self) {
        self.flags |= CFlags::ASK;
    }
}

impl Command {
    fn mk_mset(flags: CFlags, ctype: CmdType, mut notify: Notify, msg: Message) -> Command {
        let Message { rtype, data } = msg.clone();
        if let RespType::Array(head, array) = rtype {
            let array_len = array.len();

            if array_len > MAX_KEY_COUNT {
                // TODO: forbidden large request
                unimplemented!();
            }

            let cmd_count = array_len / 2;
            notify.set_expect(cmd_count + 1);
            let mut subs = Vec::with_capacity(cmd_count / 2);

            for chunk in (&array[1..]).chunks(2) {
                let key = chunk[0].clone();
                let val = chunk[1].clone();

                let sub = Message {
                    rtype: RespType::Array(head, vec![array[0].clone(), key, val]),
                    data: data.clone(),
                };
                let subcmd = Command {
                    flags,
                    ctype,
                    cycle: DEFAULT_CYCLE,
                    req: sub,
                    reply: None,
                    notify: notify.clone(),
                    subs: None,
                };

                subs.push(subcmd);
            }
            Command {
                flags,
                ctype,
                notify,
                cycle: DEFAULT_CYCLE,
                subs: Some(subs),
                req: msg,
                reply: None,
            }
        } else {
            unreachable!()
        }
    }

    fn mk_subs(flags: CFlags, ctype: CmdType, mut notify: Notify, msg: Message) -> Command {
        let Message { rtype, data } = msg.clone();
        if let RespType::Array(head, array) = rtype {
            let array_len = array.len();
            if array.len() > MAX_KEY_COUNT {
                // TODO: forbidden large request
                unimplemented!();
            }
            notify.set_expect(array_len);

            let mut subs = Vec::with_capacity(array_len - 1);
            for key in &array[1..] {
                let sub = Message {
                    rtype: RespType::Array(head, vec![array[0].clone(), key.clone()]),
                    data: data.clone(),
                };

                let subcmd = Command {
                    flags,
                    ctype,
                    cycle: DEFAULT_CYCLE,
                    req: sub,
                    reply: None,
                    notify: notify.clone(),
                    subs: None,
                };

                subs.push(subcmd);
            }

            Command {
                flags,
                cycle: DEFAULT_CYCLE,
                ctype,
                req: msg,
                reply: None,
                notify,
                subs: Some(subs),
            }
        } else {
            unreachable!();
        }
    }
}

const COMMAND_POS: usize = 0;
const KEY_EVAL_POS: usize = 3;
const KEY_RAW_POS: usize = 1;

// const CMD_BYTES_MGET: &[u8] = b"MGET";
// const CMD_BYTES_MSET: &[u8] = b"MSET";
// const CMD_BYTES_DEL: &[u8] = b"DEL";
// const CMD_BYTES_EXISTS: &[u8] = b"EXISTS";
// const CMD_BYTES_EVAL: &[u8] = b"EVAL";

const MAX_KEY_COUNT: usize = 10000;

impl From<MessageMut> for Command {
    fn from(mut msg_mut: MessageMut) -> Command {
        let mut notify = Notify::empty();
        // upper the given command
        if let Some(data) = msg_mut.nth_mut(COMMAND_POS) {
            upper(data);
        } else {
            // TODO: add error message
            unimplemented!();
        }

        let msg = msg_mut.into();
        let ctype = CmdType::get_cmd_type(&msg);

        let flags = CFlags::empty();

        if ctype.is_exists() || ctype.is_del() || ctype.is_mget() {
            return Command::mk_subs(flags, ctype, notify, msg);
        } else if ctype.is_mset() {
            return Command::mk_mset(flags, ctype, notify, msg);
        }

        notify.set_expect(1);
        Command {
            flags,
            ctype,
            cycle: DEFAULT_CYCLE,
            req: msg,
            reply: None,
            notify,
            subs: None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RedisHandleCodec {}

impl Decoder for RedisHandleCodec {
    type Item = Command;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Command::parse_cmd(src)
    }
}

impl Encoder for RedisHandleCodec {
    type Item = Command;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let _ = item.reply_cmd(dst)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RedisNodeCodec {}

impl Decoder for RedisNodeCodec {
    type Item = Message;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        recv_reply(src)
    }
}

impl Encoder for RedisNodeCodec {
    type Item = Command;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.send_req(dst)
    }
}
