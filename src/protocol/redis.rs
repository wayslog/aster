use bytes::{Bytes, BytesMut};
use futures::task::Task;
use tokio::codec::{Decoder, Encoder};

use crate::metrics::*;

use crate::com::{meta, AsError};
use crate::protocol::IntoReply;
use crate::protocol::{CmdFlags, CmdType};
use crate::proxy::standalone::Request;
use crate::utils::notify::Notify;
use crate::utils::{myitoa, trim_hash_tag, upper};

use std::cell::{Ref, RefCell, RefMut};
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;
use std::u64;

use std::fmt::Debug;
use std::time::Instant;

pub const SLOTS_COUNT: usize = 16384;

pub mod cmd;
pub mod resp;

pub use resp::{Message, MessageIter, MessageMut, RespType};
pub use resp::{RESP_ERROR, RESP_INT, RESP_STRING};

const BYTES_CMD_CLUSTER: &[u8] = b"CLUSTER";
const BYTES_CMD_QUIT: &[u8] = b"QUIT";
const BYTES_SLOTS: &[u8] = b"SLOTS";
const BYTES_NODES: &[u8] = b"NODES";

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
    type FrontCodec = RedisHandleCodec;
    type BackCodec = RedisNodeCodec;

    fn ping_request() -> Self {
        let msg = Message::new_ping_request();
        let flags = CmdFlags::empty();
        let mut notify = Notify::empty();
        notify.set_expect(1);
        let ctype = CmdType::get_cmd_type(&msg);

        let cmd = Command {
            flags,
            ctype,
            cycle: DEFAULT_CYCLE,
            req: msg,
            reply: None,
            subs: None,

            total_tracker: None,

            remote_tracker: None,
        };
        cmd.into_cmd(notify)
    }

    fn reregister(&mut self, task: Task) {
        self.notify.set_task(task);
    }

    fn key_hash(&self, hash_tag: &[u8], hasher: fn(&[u8]) -> u64) -> u64 {
        self.cmd.borrow().key_hash(hash_tag, hasher)
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

    fn add_cycle(&self) {
        self.borrow_mut().add_cycle()
    }
    fn can_cycle(&self) -> bool {
        self.borrow().can_cycle()
    }

    fn valid(&self) -> bool {
        self.check_valid()
    }

    fn set_reply<R: IntoReply<Message>>(&self, t: R) {
        let reply = t.into_reply();
        let mut cmd = self.cmd.borrow_mut();
        cmd.set_reply(reply);
    }

    fn set_error(&self, t: &AsError) {
        let reply: Message = t.into_reply();
        let mut cmd = self.cmd.borrow_mut();
        cmd.set_reply(reply);
        cmd.set_error();

        global_error_incr();
    }

    fn mark_total(&self, cluster: &str) {
        let timer = total_tracker(cluster);
        self.cmd.borrow_mut().total_tracker.replace(timer);
    }

    fn mark_remote(&self, cluster: &str) {
        let timer = remote_tracker(cluster);
        self.cmd.borrow_mut().remote_tracker.replace(timer);
    }

    fn get_sendtime(&self) -> Option<Instant> {
        let mut c = self.borrow_mut();
        match c.remote_tracker.take() {
            Some(t) => {
                let s = t.start.clone();
                c.remote_tracker = Some(t);
                Some(s)
            }
            None => None,
        }
    }
}

impl Cmd {
    pub fn cluster_mark_total(&self, cluster: &str) {
        let timer = total_tracker(cluster);
        self.cmd.borrow_mut().total_tracker.replace(timer);
    }

    pub fn cluster_mark_remote(&self, cluster: &str) {
        let timer = remote_tracker(cluster);
        if self.cmd.borrow().remote_tracker.is_none() {
            self.cmd.borrow_mut().remote_tracker.replace(timer);
        }
    }

    pub fn incr_notify(&self, count: u16) {
        self.notify.fetch_add(count);
    }

    pub fn borrow(&self) -> Ref<Command> {
        self.cmd.borrow()
    }

    pub fn borrow_mut(&self) -> RefMut<Command> {
        self.cmd.borrow_mut()
    }

    pub fn unset_error(&self) {
        self.cmd.borrow_mut().unset_error();
    }

    pub fn unset_done(&self) {
        self.cmd.borrow_mut().unset_done();
    }

    pub fn set_reply<T: IntoReply<Message>>(&self, reply: T) {
        self.borrow_mut().set_reply(reply);
    }

    pub fn set_error<T: IntoReply<Message>>(&self, reply: T) {
        self.borrow_mut().set_reply(reply);
        self.borrow_mut().set_error();
    }

    pub fn reregister(&mut self, task: Task) {
        self.notify.set_task(task);
    }

    pub fn check_valid(&self) -> bool {
        if self.borrow().ctype.is_not_support() {
            self.borrow_mut().set_reply(AsError::RequestNotSupport);
            return false;
        }
        if self.borrow().is_done() {
            return true;
        }

        if self.borrow().ctype.is_ctrl() {
            let is_quit = self
                .borrow()
                .req
                .nth(0)
                .map(|x| x == BYTES_CMD_QUIT)
                .unwrap_or(false);
            if is_quit {
                self.borrow_mut()
                    .set_reply(Message::inline_raw(Bytes::new()));
                return false;
            }

            // check if is cluster
            let is_cluster = self
                .borrow()
                .req
                .nth(0)
                .map(|x| x == BYTES_CMD_CLUSTER)
                .unwrap_or(false);
            if is_cluster {
                let sub_cmd = self.borrow().req.nth(1).map(|x| x.to_vec());
                if let Some(mut sub_cmd) = sub_cmd {
                    upper(&mut sub_cmd);
                    if sub_cmd == BYTES_SLOTS {
                        let mut data = build_cluster_slots_reply();
                        if let Ok(Some(msg)) =
                            MessageMut::parse(&mut data).map(|x| x.map(|y| y.into()))
                        {
                            let msg: Message = msg;
                            self.borrow_mut().set_reply(msg);
                            return false;
                        };
                    } else if sub_cmd == BYTES_NODES {
                        let mut data = build_cluster_nodes_reply();
                        if let Ok(Some(msg)) =
                            MessageMut::parse(&mut data).map(|x| x.map(|y| y.into()))
                        {
                            let msg: Message = msg;
                            self.borrow_mut().set_reply(msg);
                            return false;
                        };
                    }
                }
            }
            self.borrow_mut().set_reply(AsError::RequestNotSupport);
            return false;
        }
        // and other conditions
        true
    }
}

#[derive(Debug)]
pub struct Command {
    flags: CmdFlags,
    ctype: CmdType,
    // Command redirect count
    cycle: u8,

    req: Message,
    pub reply: Option<Message>,

    subs: Option<Vec<Cmd>>,

    total_tracker: Option<Tracker>,

    remote_tracker: Option<Tracker>,
}

const BYTES_JUSTOK: &[u8] = b"+OK\r\n";
const BYTES_NULL_ARRAY: &[u8] = b"*-1\r\n";
const BYTES_ZERO_INT: &[u8] = b":0\r\n";
const BYTES_CMD_PING: &[u8] = b"PING";
const BYTES_CMD_COMMAND: &[u8] = b"COMMAND";
const BYTES_REPLY_NULL_ARRAY: &[u8] = b"*-1\n";
const STR_REPLY_PONG: &str = "PONG";

const BYTES_CRLF: &[u8] = b"\r\n";

const BYTES_ARRAY: &[u8] = b"*";
const BYTES_INTEGER: &[u8] = b":";

const DEFAULT_CYCLE: u8 = 0;
const MAX_CYCLE: u8 = 1;

// for front end
impl Command {
    pub fn into_cmd(self, notify: Notify) -> Cmd {
        Cmd {
            cmd: Rc::new(RefCell::new(self)),
            notify,
        }
    }

    pub fn parse_cmd(buf: &mut BytesMut) -> Result<Option<Cmd>, AsError> {
        let msg = MessageMut::parse(buf)?;
        Ok(msg.map(Into::into))
    }

    pub fn reply_cmd(&self, buf: &mut BytesMut) -> Result<usize, AsError> {
        if self.ctype.is_mset() {
            buf.extend_from_slice(BYTES_JUSTOK);
            Ok(BYTES_JUSTOK.len())
        } else if self.ctype.is_mget() {
            if let Some(subs) = self.subs.as_ref() {
                buf.extend_from_slice(BYTES_ARRAY);

                let begin = buf.len();
                let len = subs.len();
                myitoa(len, buf);
                buf.extend_from_slice(BYTES_CRLF);

                for sub in subs {
                    sub.borrow().reply_raw(buf)?;
                }
                Ok(buf.len() - begin)
            } else {
                // debug!("subs is empty");
                buf.extend_from_slice(BYTES_NULL_ARRAY);
                Ok(BYTES_NULL_ARRAY.len())
            }
        } else if self.ctype.is_del() || self.ctype.is_exists() {
            if let Some(subs) = self.subs.as_ref() {
                let begin = buf.len();
                buf.extend_from_slice(BYTES_INTEGER);
                let mut total = 0usize;
                for sub in subs {
                    if let Some(Some(data)) = sub.borrow().reply.as_ref().map(|x| x.nth(0)) {
                        let ecount = btoi::btoi::<usize>(data).unwrap_or(0);
                        total += ecount;
                    }
                }
                myitoa(total, buf);
                buf.extend_from_slice(BYTES_CRLF);
                Ok(buf.len() - begin)
            } else {
                buf.extend_from_slice(BYTES_ZERO_INT);
                Ok(BYTES_ZERO_INT.len())
            }
        } else {
            self.reply_raw(buf)
        }
    }

    fn reply_raw(&self, buf: &mut BytesMut) -> Result<usize, AsError> {
        self.reply
            .as_ref()
            .map(|x| x.save(buf))
            .ok_or_else(|| AsError::BadReply)
    }
}

const BYTES_ASK: &[u8] = b"*1\r\n$3\r\nASK\r\n";

const BYTES_GET: &[u8] = b"$3\r\nGET\r\n";
const BYTES_LEN2_HEAD: &[u8] = b"*2\r\n";
const BYTES_LEN3_HEAD: &[u8] = b"*3\r\n";

// for back end
impl Command {
    /// save redis Command into given BytesMut
    pub fn send_req(&self, buf: &mut BytesMut) -> Result<(), AsError> {
        if self.is_ask() {
            buf.extend_from_slice(BYTES_ASK);
        }

        if self.ctype.is_exists() || self.ctype.is_del() {
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

impl Command {
    pub fn key_hash<T>(&self, hash_tag: &[u8], method: T) -> u64
    where
        T: Fn(&[u8]) -> u64,
    {
        let pos = self.key_pos();

        if let Some(key_data) = self.req.nth(pos) {
            method(trim_hash_tag(key_data, hash_tag)) as u64
        } else {
            return u64::MAX;
        }
    }

    #[inline(always)]
    fn key_pos(&self) -> usize {
        if self.ctype.is_eval() {
            return KEY_EVAL_POS;
        }
        KEY_RAW_POS
    }

    pub fn subs(&self) -> Option<Vec<Cmd>> {
        self.subs.as_ref().cloned()
    }

    pub fn is_done(&self) -> bool {
        if self.subs.is_some() {
            return self
                .subs
                .as_ref()
                .map(|x| x.iter().all(|y| y.borrow().is_done()))
                .unwrap_or(false);
        }
        self.flags & CmdFlags::DONE == CmdFlags::DONE
    }

    fn set_reply<T: IntoReply<Message>>(&mut self, reply: T) {
        self.reply = Some(reply.into_reply());
        self.set_done();

        let _ = self.remote_tracker.take();
    }

    fn set_done(&mut self) {
        self.flags |= CmdFlags::DONE;
    }

    fn unset_done(&mut self) {
        self.flags &= !CmdFlags::DONE;
    }

    fn unset_error(&mut self) {
        self.flags &= !CmdFlags::ERROR;
    }

    fn set_error(&mut self) {
        self.flags |= CmdFlags::ERROR;
    }

    pub fn cycle(&self) -> u8 {
        self.cycle
    }

    pub fn can_cycle(&self) -> bool {
        self.cycle < MAX_CYCLE
    }

    pub fn add_cycle(&mut self) {
        self.cycle += 1;
    }

    pub fn is_ask(&self) -> bool {
        self.flags & CmdFlags::ASK == CmdFlags::ASK
    }

    pub fn unset_ask(&mut self) {
        self.flags &= !CmdFlags::ASK;
    }

    pub fn set_ask(&mut self) {
        self.flags |= CmdFlags::ASK;
    }

    pub fn is_moved(&mut self) -> bool {
        self.flags & CmdFlags::MOVED == CmdFlags::MOVED
    }

    pub fn set_moved(&mut self) {
        self.flags |= CmdFlags::MOVED;
    }

    pub fn unset_moved(&mut self) {
        self.flags &= !CmdFlags::MOVED;
    }

    pub fn is_error(&self) -> bool {
        if self.subs.is_some() {
            return self
                .subs
                .as_ref()
                .map(|x| x.iter().any(|y| y.borrow().is_error()))
                .unwrap_or(false);
        }
        self.flags & CmdFlags::ERROR == CmdFlags::ERROR
    }

    pub fn is_read(&self) -> bool {
        self.ctype.is_read()
    }
}

impl Command {
    fn mk_mset(flags: CmdFlags, ctype: CmdType, mut notify: Notify, msg: Message) -> Cmd {
        let Message { rtype, data } = msg.clone();
        if let RespType::Array(head, array) = rtype {
            let array_len = array.len();

            if array_len > MAX_KEY_COUNT {
                // TODO: forbidden large request
                unimplemented!();
            }

            let cmd_count = array_len / 2;
            notify.set_expect((cmd_count + 1) as u16);
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
                    subs: None,

                    total_tracker: None,

                    remote_tracker: None,
                };

                subs.push(subcmd.into_cmd(notify.clone()));
            }
            let command = Command {
                flags,
                ctype,
                cycle: DEFAULT_CYCLE,
                subs: Some(subs),
                req: msg,
                reply: None,

                total_tracker: None,

                remote_tracker: None,
            };
            command.into_cmd(notify)
        } else {
            let cmd = Command {
                flags,
                cycle: DEFAULT_CYCLE,
                ctype,
                req: msg,
                reply: None,
                subs: None,

                total_tracker: None,

                remote_tracker: None,
            };
            let cmd = cmd.into_cmd(notify);
            cmd.set_reply(&AsError::RequestInlineWithMultiKeys);
            cmd
        }
    }

    fn mk_subs(flags: CmdFlags, ctype: CmdType, mut notify: Notify, msg: Message) -> Cmd {
        let Message { rtype, data } = msg.clone();
        if let RespType::Array(head, array) = rtype {
            let array_len = array.len();
            // if array.len() > MAX_KEY_COUNT {
            //     // TODO: forbidden large request
            //     unimplemented!();
            // }

            notify.set_expect((array_len - 1 + 1) as u16);
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
                    subs: None,

                    total_tracker: None,

                    remote_tracker: None,
                };

                subs.push(subcmd.into_cmd(notify.clone()));
            }

            let cmd = Command {
                flags,
                cycle: DEFAULT_CYCLE,
                ctype,
                req: msg,
                reply: None,
                subs: Some(subs),

                total_tracker: None,

                remote_tracker: None,
            };
            cmd.into_cmd(notify)
        } else {
            let cmd = Command {
                flags,
                cycle: DEFAULT_CYCLE,
                ctype,
                req: msg,
                reply: None,
                subs: None,

                total_tracker: None,

                remote_tracker: None,
            };
            let cmd = cmd.into_cmd(notify);
            cmd.set_reply(&AsError::RequestInlineWithMultiKeys);
            cmd
        }
    }
}

const COMMAND_POS: usize = 0;
const KEY_EVAL_POS: usize = 3;
const KEY_RAW_POS: usize = 1;

const MAX_KEY_COUNT: usize = 10000;

impl From<MessageMut> for Cmd {
    fn from(mut msg_mut: MessageMut) -> Cmd {
        let mut notify = Notify::empty();
        notify.set_expect(1);
        // upper the given command
        if let Some(data) = msg_mut.nth_mut(COMMAND_POS) {
            upper(data);
        } else {
            let msg = msg_mut.into();
            let ctype = CmdType::NotSupport;
            let flags = CmdFlags::empty();
            let command = Command {
                flags,
                ctype,
                cycle: DEFAULT_CYCLE,
                req: msg,
                reply: None,
                subs: None,

                total_tracker: None,

                remote_tracker: None,
            };
            let cmd: Cmd = command.into_cmd(notify);
            cmd.set_reply(AsError::RequestNotSupport);
            return cmd;
        }

        let msg = msg_mut.into();
        let ctype = CmdType::get_cmd_type(&msg);
        let flags = CmdFlags::empty();

        if ctype.is_exists() || ctype.is_del() || ctype.is_mget() {
            return Command::mk_subs(flags, ctype, notify, msg);
        } else if ctype.is_mset() {
            return Command::mk_mset(flags, ctype, notify, msg);
        }

        let mut cmd = Command {
            flags,
            ctype,
            cycle: DEFAULT_CYCLE,
            req: msg.clone(),
            reply: None,
            subs: None,

            total_tracker: None,

            remote_tracker: None,
        };
        if ctype.is_ctrl() {
            if let Some(data) = msg.nth(COMMAND_POS) {
                if data == BYTES_CMD_PING {
                    cmd.set_reply(STR_REPLY_PONG);
                    cmd.unset_error();
                } else if data == BYTES_CMD_COMMAND {
                    cmd.set_reply(BYTES_REPLY_NULL_ARRAY);
                    cmd.unset_error();
                } else {
                    // unsupport commands
                }
            }
        }
        cmd.into_cmd(notify)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RedisHandleCodec {}

impl Decoder for RedisHandleCodec {
    type Item = Cmd;
    type Error = AsError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Command::parse_cmd(src)
    }
}

impl Encoder for RedisHandleCodec {
    type Item = Cmd;
    type Error = AsError;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let _ = item.borrow().reply_cmd(dst)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RedisNodeCodec {}

impl Decoder for RedisNodeCodec {
    type Item = Message;
    type Error = AsError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let reply = MessageMut::parse(src)?;
        Ok(reply.map(Into::into))
    }
}

impl Encoder for RedisNodeCodec {
    type Item = Cmd;
    type Error = AsError;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.borrow().send_req(dst)
    }
}

pub fn new_read_only_cmd() -> Cmd {
    let msg = Message::new_read_only();
    let flags = CmdFlags::empty();
    let mut notify = Notify::empty();
    notify.set_expect(1);
    let ctype = CmdType::get_cmd_type(&msg);

    let cmd = Command {
        flags,
        ctype,
        cycle: DEFAULT_CYCLE,
        req: msg,
        reply: None,
        subs: None,

        total_tracker: None,

        remote_tracker: None,
    };
    cmd.into_cmd(notify)
}

pub fn new_cluster_slots_cmd() -> Cmd {
    let msg = Message::new_cluster_slots();
    let flags = CmdFlags::empty();
    let mut notify = Notify::empty();
    notify.set_expect(1);
    let ctype = CmdType::get_cmd_type(&msg);

    let cmd = Command {
        flags,
        ctype,
        cycle: DEFAULT_CYCLE,
        req: msg,
        reply: None,
        subs: None,

        total_tracker: None,

        remote_tracker: None,
    };
    cmd.into_cmd(notify)
}

pub type ReplicaLayout = (Vec<String>, Vec<Vec<String>>);

pub fn slots_reply_to_replicas(cmd: Cmd) -> Result<Option<ReplicaLayout>, AsError> {
    let msg = cmd
        .borrow_mut()
        .reply
        .take()
        .expect("reply must be non-empty");
    match msg.rtype {
        RespType::Array(_, ref subs) => {
            let mut masters = BTreeMap::<usize, String>::new();
            let mut replicas = BTreeMap::<usize, HashSet<String>>::new();

            let get_data = |index: usize, each: &[RespType]| -> Result<&[u8], AsError> {
                let frg = msg
                    .get_range(each.get(index))
                    .ok_or(AsError::WrongClusterSlotsReplySlot)?;
                Ok(msg.get_data_of_range(frg))
            };

            let get_number = |index: usize, each: &[RespType]| -> Result<usize, AsError> {
                let data = get_data(index, each)?;
                let num = btoi::btoi::<usize>(data)?;
                Ok(num)
            };

            let get_addr = |each: &RespType| -> Result<String, AsError> {
                if let RespType::Array(_, ref inner) = each {
                    let port = get_number(1, inner)?;
                    let addr = String::from_utf8_lossy(get_data(0, inner)?);
                    Ok(format!("{}:{}", addr, port))
                } else {
                    Err(AsError::WrongClusterSlotsReplyType)
                }
            };

            for sub in subs {
                if let RespType::Array(_, ref subs) = sub {
                    let begin = get_number(0, subs)?;
                    let end = get_number(1, subs)?;
                    let master = get_addr(subs.get(2).ok_or(AsError::WrongClusterSlotsReplyType)?)?;
                    for i in begin..=end {
                        masters.insert(i, master.clone());
                    }
                    for i in begin..=end {
                        replicas.insert(i, HashSet::new());
                    }
                    if subs.len() > 3 {
                        let mut replica_set = HashSet::new();
                        for resp in subs.iter().skip(3) {
                            let replica = get_addr(resp)?;
                            replica_set.insert(replica);
                        }
                        for i in begin..=end {
                            replicas.insert(i, replica_set.clone());
                        }
                    }
                } else {
                    return Err(AsError::WrongClusterSlotsReplyType);
                }
            }
            if masters.len() != SLOTS_COUNT {
                warn!("slots is not full covered but ignore it");
            }
            let master_list = masters.into_iter().map(|(_, v)| v).collect();
            let replicas_list = replicas
                .into_iter()
                .map(|(_, v)| v.into_iter().collect())
                .collect();
            Ok(Some((master_list, replicas_list)))
        }
        _ => Err(AsError::WrongClusterSlotsReplyType),
    }
}

impl From<AsError> for Message {
    fn from(err: AsError) -> Message {
        err.into_reply()
    }
}

impl IntoReply<Message> for AsError {
    fn into_reply(self) -> Message {
        let value = format!("{}", self);
        Message::plain(value.as_bytes(), RESP_ERROR)
    }
}

impl<'a> IntoReply<Message> for &'a AsError {
    fn into_reply(self) -> Message {
        let value = format!("{}", self);
        Message::plain(value.as_bytes(), RESP_ERROR)
    }
}

impl<'a> IntoReply<Message> for &'a str {
    fn into_reply(self) -> Message {
        let value = self.to_string();
        Message::plain(value.as_bytes(), RESP_STRING)
    }
}

impl<'a> IntoReply<Message> for &'a [u8] {
    fn into_reply(self) -> Message {
        let bytes = Bytes::from(self);
        Message::inline_raw(bytes)
    }
}

impl<'a> IntoReply<Message> for &'a usize {
    fn into_reply(self) -> Message {
        let value = format!("{}", self);
        Message::plain(value.as_bytes(), RESP_INT)
    }
}

impl IntoReply<Message> for usize {
    fn into_reply(self) -> Message {
        let value = format!("{}", self);
        Message::plain(value.as_bytes(), RESP_INT)
    }
}

fn build_cluster_nodes_reply() -> BytesMut {
    let port = meta::get_port();
    let ip = meta::get_ip();
    let reply = format!("0000000000000000000000000000000000000001 {ip}:{port} master,myself - 0 0 1 connected 0-5460\n0000000000000000000000000000000000000002 {ip}:{port} master - 0 0 2 connected 5461-10922\n0000000000000000000000000000000000000003 {ip}:{port} master - 0 0 3 connected 10923-16383\n", ip = ip, port = port);
    let reply = format!("${}\r\n{}\r\n", reply.len(), reply);
    let mut data = BytesMut::new();
    data.extend_from_slice(reply.as_bytes());
    data
}

fn build_cluster_slots_reply() -> BytesMut {
    let port = meta::get_port();
    let ip = meta::get_ip();
    let reply = format!("*3\r\n*3\r\n:0\r\n:5460\r\n*3\r\n${iplen}\r\n{ip}\r\n:{port}\r\n$40\r\n0000000000000000000000000000000000000001\r\n*3\r\n:5461\r\n:10922\r\n*3\r\n${iplen}\r\n{ip}\r\n:{port}\r\n$40\r\n0000000000000000000000000000000000000002\r\n*3\r\n:10923\r\n:16383\r\n*3\r\n${iplen}\r\n{ip}\r\n:{port}\r\n$40\r\n0000000000000000000000000000000000000003\r\n", iplen=ip.len(), ip = ip, port = port);
    let mut data = BytesMut::new();
    data.extend_from_slice(reply.as_bytes());
    data
}

#[test]
fn test_redis_parse_wrong_case() {
    use std::fs::{self, File};
    use std::io::prelude::*;
    use std::io::BufReader;

    let prefix = "../fuzz/corpus/fuzz_redis_parser/";

    if let Ok(dir) = fs::read_dir(prefix) {
        for entry in dir {
            let entry = entry.unwrap();
            let path = entry.path();
            println!("parsing abs_path: {:?}", path);
            let fd = File::open(path).unwrap();
            let mut buffer = BufReader::new(fd);
            let mut data = Vec::new();
            buffer.read_to_end(&mut data).unwrap();
            let mut src = BytesMut::from(&data[..]);

            loop {
                let result = Command::parse_cmd(&mut src);
                match result {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_err) => break,
                }
            }
        }
    }
}
