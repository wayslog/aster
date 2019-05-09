use bitflags::bitflags;
use bytes::BytesMut;
use failure::Error;
use futures::task::Task;

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

use cmd::CmdType;
use resp::{Message, MessageMut, RespType};

pub struct Command {
    flags: CFlags,
    ctype: CmdType,

    req: Message,
    reply: Option<Message>,

    notify: Notify,
    subs: Option<Vec<Command>>,
}

// for front end
impl Command {
    pub fn parse_cmd(buf: &mut BytesMut) -> Result<Option<Command>, Error> {
        let msg = MessageMut::parse(buf)?;
        Ok(msg.map(Into::into))
    }

    pub fn reply_cmd(&mut self, buf: &mut BytesMut) -> Result<usize, Error> {
        unimplemented!()
    }
}

const BYTES_ASK: &[u8] = b"*1/r/n$3/r/nASK/r/n";

const BYTES_LEN2_HEAD: &[u8] = b"*2/r/n";
const BYTES_LEN3_HEAD: &[u8] = b"*3/r/n";

// for back end
impl Command {
    /// save redis Command into given BytesMut
    pub fn send_req(&self, buf: &mut BytesMut) -> Result<usize, Error> {
        let mut size = 0;
        if self.ctype.is_mget() && self.ctype.is_exists() && self.ctype.is_del() {
            size += BYTES_LEN2_HEAD.len();
            buf.extend_from_slice(BYTES_LEN2_HEAD);
            if let RespType::Array(_, arrs) = &self.req.rtype {
                for rtype in arrs {
                    size += self.req.save_by_rtype(rtype, buf);
                }
            }
            return Ok(size);
        } else if self.ctype.is_mset() {
            size += BYTES_LEN3_HEAD.len();
            buf.extend_from_slice(BYTES_LEN3_HEAD);
            if let RespType::Array(_, arrs) = &self.req.rtype {
                for rtype in arrs {
                    size += self.req.save_by_rtype(rtype, buf);
                }
            }
            return Ok(size);
        }
        Ok(size + self.req.save(buf))
    }

    /// parse redis Command from BytesMut buffer ignore if that's ask
    pub fn recv_reply(buf: &mut BytesMut) -> Result<Option<Message>, Error> {
        let reply = MessageMut::parse(buf)?;
        Ok(reply.map(Into::into))
    }
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
            // let array = rtype.array().expect("mget sub never none");
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

const CMD_BYTES_MGET: &[u8] = b"MGET";
const CMD_BYTES_MSET: &[u8] = b"MSET";
const CMD_BYTES_DEL: &[u8] = b"DEL";
const CMD_BYTES_EXISTS: &[u8] = b"EXISTS";
const CMD_BYTES_EVAL: &[u8] = b"EVAL";

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
            req: msg,
            reply: None,
            notify,
            subs: None,
        }
    }
}
