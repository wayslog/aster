// use bytes::BufMut;
use btoi;
use com::*;
use crc::crc16 as crc;
use notify::Notify;
use proxy::Request;
use redis::resp::{Resp, RespCodec};
use redis::resp::{BYTES_CRLF, RESP_ARRAY, RESP_BULK, RESP_ERROR, RESP_INT, RESP_STRING};

use bytes::{BufMut, BytesMut};
use futures::task::Task;
use hashbrown::HashMap;
use tokio_codec::{Decoder, Encoder};

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::mem;
use std::rc::Rc;

pub const MUSK: u16 = 0x3fff;

#[derive(Clone, Copy, Debug)]
pub enum CmdType {
    Read,
    Write,
    Ctrl,
    NotSupport,
    IngnoreReply,
}

impl Request for Cmd {
    type Reply = Resp;
    type HandleCodec = HandleCodec;
    type NodeCodec = NodeCodec;

    fn reregister(&self, task: Task) {
        self.cmd_reregister(task)
    }

    fn handle_codec() -> Self::HandleCodec {
        HandleCodec::default()
    }

    fn node_codec() -> Self::NodeCodec {
        NodeCodec::default()
    }

    fn ping_request() -> Self {
        new_ping_request()
    }

    fn key(&self) -> Vec<u8> {
        self.cmd.borrow().key()
    }
    fn subs(&self) -> Option<Vec<Self>> {
        self.sub_reqs()
    }

    fn is_done(&self) -> bool {
        self.is_done()
    }

    fn valid(&self) -> bool {
        let cmd_type = self.cmd_type();
        match cmd_type {
            CmdType::NotSupport => {
                self.done_with_error(&RESP_OBJ_ERROR_NOT_SUPPORT);
                false
            }
            _ => true,
        }
    }

    fn done(&self, data: Resp) {
        self.done(data)
    }

    fn done_with_error(&self, err: Error) {
        let err_str = format!("{:?}", err);
        let err_bs = err_str.as_bytes().to_vec();
        let resp = Resp::new_plain(RESP_ERROR, Some(err_bs));
        self.done_with_error(&resp);
    }
}

#[derive(Debug)]
pub struct Cmd {
    cmd: Rc<RefCell<Command>>,
}

impl Clone for Cmd {
    fn clone(&self) -> Cmd {
        self.cmd.borrow().notify.add(1);
        Cmd {
            cmd: self.cmd.clone(),
        }
    }
}

impl Drop for Cmd {
    fn drop(&mut self) {
        self.cmd.borrow().notify.notify();
    }
}

impl From<Resp> for Cmd {
    fn from(resp: Resp) -> Cmd {
        let command = Command::from_resp(resp);
        Cmd::new(command)
    }
}

impl Cmd {
    fn new(command: Command) -> Cmd {
        Cmd {
            cmd: Rc::new(RefCell::new(command)),
        }
    }

    pub fn cmd_reregister(&self, task: Task) {
        if self.is_complex() {
            if let Some(sub_reqs) = self.sub_reqs() {
                for req in sub_reqs {
                    req.cmd_reregister(task.clone());
                }
            }
        }
        self.cmd.borrow_mut().notify.reregister(task)
    }

    pub fn set_is_ask(&self, is_ask: bool) {
        self.cmd.borrow_mut().is_ask = is_ask;
    }

    pub fn is_ignore_reply(&self) -> bool {
        match self.cmd.borrow().get_cmd_type() {
            CmdType::IngnoreReply => true,
            _ => false,
        }
    }

    pub fn is_ask(&self) -> bool {
        self.cmd.borrow().is_ask
    }

    pub fn is_complex(&self) -> bool {
        self.cmd.borrow().is_complex()
    }

    pub fn set_is_complex(&self, is_complex: bool) {
        self.cmd.borrow_mut().is_complex = is_complex;
    }

    pub fn cmd_type(&self) -> CmdType {
        self.cmd.borrow().get_cmd_type()
    }

    pub fn crc(&self) -> u16 {
        self.cmd.borrow().crc()
    }

    pub fn is_done(&self) -> bool {
        self.cmd.borrow().is_done()
    }

    pub fn done_with_error(&self, err: &Resp) {
        self.cmd.borrow_mut().done_with_error(err)
    }

    pub fn done(&self, resp: Resp) {
        self.cmd.borrow_mut().done(resp)
    }

    pub fn rc_req(&self) -> Rc<Resp> {
        self.cmd.borrow().req.clone()
    }

    pub fn sub_reqs(&self) -> Option<Vec<Cmd>> {
        self.cmd.borrow().sub_reqs.as_ref().cloned()
    }

    pub fn swap_reply(&self) -> Option<Resp> {
        let mut cmd_mut = self.cmd.borrow_mut();
        let mut empty = None;
        mem::swap(&mut empty, &mut cmd_mut.reply);
        empty
    }
}

// pub type Cmd = Rc<RefCell<Command>>;

/// Command is a type for Redis Command.
#[derive(Debug)]
pub struct Command {
    pub is_done: bool,
    pub is_ask: bool,
    pub is_inline: bool,

    pub is_complex: bool,
    pub cmd_type: CmdType,

    pub crc: u16,
    pub notify: Notify,

    pub req: Rc<Resp>,
    pub sub_reqs: Option<Vec<Cmd>>,
    pub reply: Option<Resp>,
}

impl From<Resp> for Command {
    fn from(resp: Resp) -> Command {
        Command::from_resp(resp)
    }
}

impl Command {
    fn inner_from_resp(mut resp: Resp, notify: Notify) -> Command {
        Self::cmd_to_upper(&mut resp);
        let cmd_type = Self::get_resp_cmd_type(&resp);
        let is_complex = Self::is_resp_complex(&resp);
        let crc = Self::crc16(&resp);

        Command {
            is_done: false,
            is_ask: false,
            is_inline: false,

            is_complex,
            cmd_type,

            crc,
            notify,
            req: Rc::new(resp),
            sub_reqs: None,
            reply: None,
        }
    }

    fn from_resp(resp: Resp) -> Command {
        if resp.is_inline() {
            let data = resp.unwrap_data().expect("inline resp data is never empty");
            let dstring = String::from_utf8_lossy(&data);
            let args = dstring.split(' ').map(|x| x.to_string()).collect();
            let mut cmd = new_cmd(args);
            cmd.is_inline = true;
            return cmd;
        }

        let notify = Notify::empty();
        let mut command = Self::inner_from_resp(resp, notify);
        command.mksubs();
        command
    }

    fn get_single_cmd(cmd_resp: Resp) -> Resp {
        if cmd_resp.data.as_ref().expect("cmd must be bulk never nil") == b"MGET" {
            return RESP_OBJ_BULK_GET.clone();
        }
        cmd_resp
    }

    fn cmd_to_upper(resp: &mut Resp) {
        let cmd = resp.get_mut(0).expect("never be empty");
        update_to_upper(cmd.data.as_mut().expect("never null"));
    }

    fn is_resp_complex(resp: &Resp) -> bool {
        let cmd = resp.get(0).expect("never be empty");
        CMD_COMPLEX.contains(&cmd.data.as_ref().expect("never null")[..])
    }

    fn get_resp_cmd_type(resp: &Resp) -> CmdType {
        let cmd = resp.get(0).expect("never be empty");
        if let Some(&ctype) = CMD_TYPE.get(&cmd.data.as_ref().expect("never null")[..]) {
            return ctype;
        }
        CmdType::NotSupport
    }

    fn crc16(resp: &Resp) -> u16 {
        if let Some(cmd) = resp.get(1) {
            let data = &cmd.data.as_ref().expect("never null")[..];
            return crc(data) & MUSK;
        }
        ::std::u16::MAX
    }
}

impl Command {
    pub fn key(&self) -> Vec<u8> {
        let req = self.req.as_ref();
        let is_not_eval = req
            .get(0)
            .map(|y| {
                y.data
                    .as_ref()
                    .map(|x| x != b"EVAL")
                    .expect("command inner is never be empty for Command::key")
            })
            .expect("command is never be empty for Command::key");
        let key_pos = if is_not_eval { 1 } else { 3 };

        if let Some(key_req) = req.get(key_pos) {
            return key_req
                .data
                .as_ref()
                .cloned()
                .expect("key_req's key is never be empty");
        }
        Vec::new()
    }

    pub fn crc(&self) -> u16 {
        self.crc
    }

    pub fn get_cmd_type(&self) -> CmdType {
        self.cmd_type
    }

    fn mksubs(&mut self) {
        if !self.is_complex {
            return;
        }

        if self.req.cmd_bytes() == b"MSET" {
            return self.mk_mset();
        } else if self.req.cmd_bytes() == b"EVAL" {
            return self.mk_eval();
        }
        self.mk_by_keys()
    }

    fn mk_eval(&mut self) {
        let key_resp = self.req.get(3).expect("eval must contains key");
        self.crc = calc_crc16(key_resp.data.as_ref().expect("key must contains value"));
    }

    fn mk_mset(&mut self) {
        let arr_len = self.req.array.as_ref().expect("cmd must be array").len();
        if arr_len < 3 || arr_len % 2 == 0 {
            return self.done_with_error(&RESP_OBJ_ERROR_BAD_CMD);
        }
        // self.notify.done_without_notify();

        let is_complex = self.is_complex;
        let resps = self.req.array.as_ref().expect("cmd must be array");
        let subcmds: Vec<Cmd> = (&resps[1..])
            .chunks(2)
            .map(|x| {
                let key = x[0].clone();
                let val = x[1].clone();
                Resp::new_array(Some(vec![RESP_OBJ_BULK_SET.clone(), key, val]))
            })
            .map(|resp| {
                let cmd = Cmd::from(resp);
                cmd.set_is_complex(is_complex);
                cmd
            })
            .collect();

        self.sub_reqs = Some(subcmds);
    }

    fn mk_by_keys(&mut self) {
        let arr_len = self.req.array.as_ref().expect("cmd must be array").len();
        if arr_len < 2 {
            return self.done_with_error(&RESP_OBJ_ERROR_BAD_CMD);
        }
        // self.notify.done_without_notify();

        let resps = self.req.array.as_ref().expect("cmd must be array").clone();
        let mut iter = resps.into_iter();

        let cmd = iter.next().expect("cmd must be contains");
        let cmd = Self::get_single_cmd(cmd);

        let subcmds: Vec<Cmd> = iter
            .map(|arg| {
                let mut arr = Vec::with_capacity(2);
                arr.push(cmd.clone());
                arr.push(arg);
                Resp::new_array(Some(arr))
            })
            .map(|resp| {
                let mut cmd = Command::inner_from_resp(resp, self.notify.clone());
                cmd.is_complex = self.is_complex;
                // cmd.task = self.task.clone();
                Cmd::new(cmd)
            })
            .collect();
        self.sub_reqs = Some(subcmds);
    }

    pub fn is_done(&self) -> bool {
        if self.is_complex() {
            if let Some(subs) = self.sub_reqs.as_ref() {
                for sub in subs {
                    if !sub.is_done() {
                        return false;
                    }
                }
                return true;
            }
        }
        self.is_done
    }

    pub fn is_complex(&self) -> bool {
        self.is_complex
    }

    pub fn done(&mut self, reply: Resp) {
        self.reply = Some(reply);
        self.is_done = true;
    }

    pub fn done_with_error(&mut self, err: &Resp) {
        self.reply = Some(err.clone());
        self.is_done = true;
    }
}

lazy_static!{
    pub static ref CMD_TYPE: HashMap<&'static [u8], CmdType> = {
        let mut hmap = HashMap::new();

        // special commands
        hmap.insert(&b"DEL"[..], CmdType::Write);
        hmap.insert(&b"DUMP"[..], CmdType::Read);
        hmap.insert(&b"EXISTS"[..], CmdType::Read);
        hmap.insert(&b"EXPIRE"[..], CmdType::Write);
        hmap.insert(&b"EXPIREAT"[..], CmdType::Write);
        hmap.insert(&b"KEYS"[..], CmdType::NotSupport);
        hmap.insert(&b"MIGRATE"[..], CmdType::NotSupport);
        hmap.insert(&b"MOVE"[..], CmdType::NotSupport);
        hmap.insert(&b"OBJECT"[..], CmdType::NotSupport);
        hmap.insert(&b"PERSIST"[..], CmdType::Write);
        hmap.insert(&b"PEXPIRE"[..], CmdType::Write);
        hmap.insert(&b"PEXPIREAT"[..], CmdType::Write);
        hmap.insert(&b"PTTL"[..], CmdType::Read);
        hmap.insert(&b"RANDOMKEY"[..], CmdType::NotSupport);
        hmap.insert(&b"RENAME"[..], CmdType::NotSupport);
        hmap.insert(&b"RENAMENX"[..], CmdType::NotSupport);
        hmap.insert(&b"RESTORE"[..], CmdType::Write);
        hmap.insert(&b"SCAN"[..], CmdType::NotSupport);
        hmap.insert(&b"SORT"[..], CmdType::Write);
        hmap.insert(&b"TTL"[..], CmdType::Read);
        hmap.insert(&b"TYPE"[..], CmdType::Read);
        hmap.insert(&b"WAIT"[..], CmdType::NotSupport);
        // string key
        hmap.insert(&b"APPEND"[..], CmdType::Write);
        hmap.insert(&b"BITCOUNT"[..], CmdType::Read);
        hmap.insert(&b"BITOP"[..], CmdType::NotSupport);
        hmap.insert(&b"BITPOS"[..], CmdType::Read);
        hmap.insert(&b"DECR"[..], CmdType::Write);
        hmap.insert(&b"DECRBY"[..], CmdType::Write);
        hmap.insert(&b"GET"[..], CmdType::Read);
        hmap.insert(&b"GETBIT"[..], CmdType::Read);
        hmap.insert(&b"GETRANGE"[..], CmdType::Read);
        hmap.insert(&b"GETSET"[..], CmdType::Write);
        hmap.insert(&b"INCR"[..], CmdType::Write);
        hmap.insert(&b"INCRBY"[..], CmdType::Write);
        hmap.insert(&b"INCRBYFLOAT"[..], CmdType::Write);
        hmap.insert(&b"MGET"[..], CmdType::Read);
        hmap.insert(&b"MSET"[..], CmdType::Write);
        hmap.insert(&b"MSETNX"[..], CmdType::NotSupport);
        hmap.insert(&b"PSETEX"[..], CmdType::Write);
        hmap.insert(&b"SET"[..], CmdType::Write);
        hmap.insert(&b"SETBIT"[..], CmdType::Write);
        hmap.insert(&b"SETEX"[..], CmdType::Write);
        hmap.insert(&b"SETNX"[..], CmdType::Write);
        hmap.insert(&b"SETRANGE"[..], CmdType::Write);
        hmap.insert(&b"STRLEN"[..], CmdType::Read);
        // hash type
        hmap.insert(&b"HDEL"[..], CmdType::Write);
        hmap.insert(&b"HEXISTS"[..], CmdType::Read);
        hmap.insert(&b"HGET"[..], CmdType::Read);
        hmap.insert(&b"HGETALL"[..], CmdType::Read);
        hmap.insert(&b"HINCRBY"[..], CmdType::Write);
        hmap.insert(&b"HINCRBYFLOAT"[..], CmdType::Write);
        hmap.insert(&b"HKEYS"[..], CmdType::Read);
        hmap.insert(&b"HLEN"[..], CmdType::Read);
        hmap.insert(&b"HMGET"[..], CmdType::Read);
        hmap.insert(&b"HMSET"[..], CmdType::Write);
        hmap.insert(&b"HSET"[..], CmdType::Write);
        hmap.insert(&b"HSETNX"[..], CmdType::Write);
        hmap.insert(&b"HSTRLEN"[..], CmdType::Read);
        hmap.insert(&b"HVALS"[..], CmdType::Read);
        hmap.insert(&b"HSCAN"[..], CmdType::Read);
        // list type
        hmap.insert(&b"BLPOP"[..], CmdType::NotSupport);
        hmap.insert(&b"BRPOP"[..], CmdType::NotSupport);
        hmap.insert(&b"BRPOPLPUSH"[..], CmdType::NotSupport);
        hmap.insert(&b"LINDEX"[..], CmdType::Read);
        hmap.insert(&b"LINSERT"[..], CmdType::Write);
        hmap.insert(&b"LLEN"[..], CmdType::Read);
        hmap.insert(&b"LPOP"[..], CmdType::Write);
        hmap.insert(&b"LPUSH"[..], CmdType::Write);
        hmap.insert(&b"LPUSHX"[..], CmdType::Write);
        hmap.insert(&b"LRANGE"[..], CmdType::Read);
        hmap.insert(&b"LREM"[..], CmdType::Write);
        hmap.insert(&b"LSET"[..], CmdType::Write);
        hmap.insert(&b"LTRIM"[..], CmdType::Write);
        hmap.insert(&b"RPOP"[..], CmdType::Write);
        hmap.insert(&b"RPOPLPUSH"[..], CmdType::Write);
        hmap.insert(&b"RPUSH"[..], CmdType::Write);
        hmap.insert(&b"RPUSHX"[..], CmdType::Write);
        // set type
        hmap.insert(&b"SADD"[..], CmdType::Write);
        hmap.insert(&b"SCARD"[..], CmdType::Read);
        hmap.insert(&b"SDIFF"[..], CmdType::Read);
        hmap.insert(&b"SDIFFSTORE"[..], CmdType::Write);
        hmap.insert(&b"SINTER"[..], CmdType::Read);
        hmap.insert(&b"SINTERSTORE"[..], CmdType::Write);
        hmap.insert(&b"SISMEMBER"[..], CmdType::Read);
        hmap.insert(&b"SMEMBERS"[..], CmdType::Read);
        hmap.insert(&b"SMOVE"[..], CmdType::Write);
        hmap.insert(&b"SPOP"[..], CmdType::Write);
        hmap.insert(&b"SRANDMEMBER"[..], CmdType::Read);
        hmap.insert(&b"SREM"[..], CmdType::Write);
        hmap.insert(&b"SUNION"[..], CmdType::Read);
        hmap.insert(&b"SUNIONSTORE"[..], CmdType::Write);
        hmap.insert(&b"SSCAN"[..], CmdType::Read);
        // zset type
        hmap.insert(&b"ZADD"[..], CmdType::Write);
        hmap.insert(&b"ZCARD"[..], CmdType::Read);
        hmap.insert(&b"ZCOUNT"[..], CmdType::Read);
        hmap.insert(&b"ZINCRBY"[..], CmdType::Write);
        hmap.insert(&b"ZINTERSTORE"[..], CmdType::Write);
        hmap.insert(&b"ZLEXCOUNT"[..], CmdType::Read);
        hmap.insert(&b"ZRANGE"[..], CmdType::Read);
        hmap.insert(&b"ZRANGEBYLEX"[..], CmdType::Read);
        hmap.insert(&b"ZRANGEBYSCORE"[..], CmdType::Read);
        hmap.insert(&b"ZRANK"[..], CmdType::Read);
        hmap.insert(&b"ZREM"[..], CmdType::Write);
        hmap.insert(&b"ZREMRANGEBYLEX"[..], CmdType::Write);
        hmap.insert(&b"ZREMRANGEBYRANK"[..], CmdType::Write);
        hmap.insert(&b"ZREMRANGEBYSCORE"[..], CmdType::Write);
        hmap.insert(&b"ZREVRANGE"[..], CmdType::Read);
        hmap.insert(&b"ZREVRANGEBYLEX"[..], CmdType::Read);
        hmap.insert(&b"ZREVRANGEBYSCORE"[..], CmdType::Read);
        hmap.insert(&b"ZREVRANK"[..], CmdType::Read);
        hmap.insert(&b"ZSCORE"[..], CmdType::Read);
        hmap.insert(&b"ZUNIONSTORE"[..], CmdType::Write);
        hmap.insert(&b"ZSCAN"[..], CmdType::Read);
        // hyper log type
        hmap.insert(&b"PFADD"[..], CmdType::Write);
        hmap.insert(&b"PFCOUNT"[..], CmdType::Read);
        hmap.insert(&b"PFMERGE"[..], CmdType::Write);
        // eval type
        hmap.insert(&b"EVAL"[..], CmdType::Write);
        hmap.insert(&b"EVALSHA"[..], CmdType::NotSupport);
        // ctrl type
        hmap.insert(&b"AUTH"[..], CmdType::NotSupport);
        hmap.insert(&b"ECHO"[..], CmdType::Ctrl);
        hmap.insert(&b"PING"[..], CmdType::Ctrl);
        hmap.insert(&b"INFO"[..], CmdType::Ctrl);
        hmap.insert(&b"PROXY"[..], CmdType::NotSupport);
        hmap.insert(&b"SLOWLOG"[..], CmdType::NotSupport);
        hmap.insert(&b"QUIT"[..], CmdType::NotSupport);
        hmap.insert(&b"SELECT"[..], CmdType::NotSupport);
        hmap.insert(&b"TIME"[..], CmdType::NotSupport);
        hmap.insert(&b"CONFIG"[..], CmdType::NotSupport);

        hmap
    };

    pub static ref RESP_OBJ_ERROR_NOT_SUPPORT: Resp =
    {

        Resp::new_plain(RESP_ERROR, Some((&b"unsupported command"[..]).to_vec())) };

    pub static ref RESP_OBJ_BULK_GET: Resp =
    {

        Resp::new_plain(RESP_BULK, Some((&b"GET"[..]).to_vec())) };
    pub static ref RESP_OBJ_BULK_SET: Resp =
    {

        Resp::new_plain(RESP_BULK, Some((&b"SET"[..]).to_vec())) };
    pub static ref RESP_OBJ_ERROR_BAD_CMD: Resp =
    {
        Resp::new_plain(RESP_ERROR, Some((&b"command format wrong"[..]).to_vec())) };

    pub static ref RESP_OBJ_STRING_PONG: Resp =
    {
        Resp::new_plain(RESP_STRING, Some((&b"PONG"[..]).to_vec())) };

    pub static ref CMD_COMPLEX: BTreeSet<&'static [u8]> = {
        let cmds = vec!["MSET", "MGET", "DEL", "EXISTS", "EVAL"];

        let mut hset = BTreeSet::new();
        for cmd in &cmds[..] {
            hset.insert(cmd.as_bytes());
        }
        hset
    };
}

fn calc_crc16(data: &[u8]) -> u16 {
    crc(data)
}

pub struct CmdCodec {
    rc: RespCodec,
}

impl Decoder for CmdCodec {
    type Item = Resp;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.rc.decode(src)
    }
}

impl Encoder for CmdCodec {
    type Item = Cmd;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.is_complex() {
            if let Some(subreqs) = item.sub_reqs() {
                let cmd_bytes = item.rc_req().cmd_bytes().to_vec();
                if &cmd_bytes[..] == b"MSET" {
                    return self.merge_encode_ok(&subreqs, dst);
                } else if &cmd_bytes[..] == b"DEL" || &cmd_bytes[..] == b"EXISTS" {
                    return self.merge_encode_count(subreqs, dst);
                } else if &cmd_bytes[..] == b"MGET" {
                    return self.merge_encode_join(subreqs, dst);
                } else if &cmd_bytes[..] == b"EVAL" {
                    // return self.merge_encode_join(subreqs, dst);
                } else {
                    unreachable!();
                }
            }
        }

        let reply = item.swap_reply().expect("encode simple reply never empty");
        self.rc.encode(Rc::new(reply), dst)
    }
}

impl Default for CmdCodec {
    fn default() -> Self {
        CmdCodec { rc: RespCodec {} }
    }
}

impl CmdCodec {
    fn merge_encode_count(&mut self, subs: Vec<Cmd>, dst: &mut BytesMut) -> AsResult<()> {
        let mut sum = 0;
        for subcmd in subs {
            let mut reply = &mut subcmd.cmd.borrow_mut().reply;
            let subresp = reply.as_mut().expect("subreply must be some resp but None");
            if subresp.rtype == RESP_ERROR {
                // should swallow the error and convert as 0 count of key.
                continue;
            }
            debug_assert_eq!(subresp.rtype, RESP_INT);
            let count_bs = subresp.data.as_ref().expect("resp_int data must be some");
            let count = btoi::btoi::<i64>(count_bs)?;
            sum += count;
        }

        if !dst.has_remaining_mut() {
            dst.reserve(1);
        }
        dst.put_u8(RESP_INT);
        let buf = format!("{}", sum);
        dst.extend_from_slice(buf.as_bytes());
        dst.extend_from_slice(BYTES_CRLF);
        Ok(())
    }

    fn merge_encode_ok(&mut self, _subs: &[Cmd], dst: &mut BytesMut) -> AsResult<()> {
        dst.extend_from_slice(&b"+OK\r\n"[..]);
        Ok(())
    }

    fn merge_encode_join(&mut self, subs: Vec<Cmd>, dst: &mut BytesMut) -> AsResult<()> {
        if !dst.has_remaining_mut() {
            dst.reserve(1);
        }
        dst.put_u8(RESP_ARRAY);

        let count = subs.len();
        let buf = format!("{}", count);
        dst.extend_from_slice(buf.as_bytes());
        dst.extend_from_slice(BYTES_CRLF);
        for sub in subs {
            self.encode(sub, dst)?;
        }
        Ok(())
    }
}


pub fn new_cmd(args: Vec<String>) -> Command {
    let resps: Vec<_> = args
        .into_iter()
        .map(|x| Resp::new_plain(RESP_BULK, Some(x.as_bytes().to_vec())))
        .collect();
    let req = Resp::new_array(Some(resps));
    Command::from(req)
}

pub fn new_asking_cmd() -> Cmd {
    let req = Resp::new_array(Some(vec![Resp::new_plain(
        RESP_BULK,
        Some(b"ASKING".to_vec()),
    )]));
    let notify = Notify::empty();
    let cmd = Command {
        is_done: false,
        is_ask: false,
        is_inline: false,

        is_complex: false,
        cmd_type: CmdType::IngnoreReply,

        crc: 0u16,
        notify,

        req: Rc::new(req),
        sub_reqs: None,
        reply: None,
    };
    Cmd::new(cmd)
}

pub fn new_cluster_nodes_cmd() -> Cmd {
    let req = Resp::new_array(Some(vec![
        Resp::new_plain(RESP_BULK, Some(b"CLUSTER".to_vec())),
        Resp::new_plain(RESP_BULK, Some(b"NODES".to_vec())),
    ]));
    let notify = Notify::empty();
    let cmd = Command {
        is_done: false,
        is_ask: false,
        is_inline: false,

        is_complex: false,
        cmd_type: CmdType::Ctrl,

        crc: 0u16,
        notify,

        req: Rc::new(req),
        sub_reqs: None,
        reply: None,
    };
    Cmd::new(cmd)
}

pub struct HandleCodec {
    rc: RespCodec,
}

impl HandleCodec {
    fn merge_encode_count(&mut self, subs: Vec<Cmd>, dst: &mut BytesMut) -> AsResult<()> {
        let mut sum = 0;
        for subcmd in subs {
            let mut reply = &mut subcmd.cmd.borrow_mut().reply;
            let subresp = reply.as_mut().expect("subreply must be some resp but None");
            if subresp.rtype == RESP_ERROR {
                // should swallow the error and convert as 0 count of key.
                continue;
            }
            debug_assert_eq!(subresp.rtype, RESP_INT);
            let count_bs = subresp.data.as_ref().expect("resp_int data must be some");
            let count = btoi::btoi::<i64>(count_bs)?;
            sum += count;
        }

        if !dst.has_remaining_mut() {
            dst.reserve(1);
        }
        dst.put_u8(RESP_INT);
        let buf = format!("{}", sum);
        dst.extend_from_slice(buf.as_bytes());
        dst.extend_from_slice(BYTES_CRLF);
        Ok(())
    }

    fn merge_encode_ok(&mut self, _subs: &[Cmd], dst: &mut BytesMut) -> AsResult<()> {
        dst.extend_from_slice(&b"+OK\r\n"[..]);
        Ok(())
    }

    fn merge_encode_join(&mut self, subs: Vec<Cmd>, dst: &mut BytesMut) -> AsResult<()> {
        if !dst.has_remaining_mut() {
            dst.reserve(1);
        }
        dst.put_u8(RESP_ARRAY);

        let count = subs.len();
        let buf = format!("{}", count);
        dst.extend_from_slice(buf.as_bytes());
        dst.extend_from_slice(BYTES_CRLF);
        for sub in subs {
            self.encode(sub, dst)?;
        }
        Ok(())
    }
}

impl Decoder for HandleCodec {
    type Item = Cmd;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.rc.decode(src).map(|x| x.map(|y| y.into()))
    }
}

impl Encoder for HandleCodec {
    type Item = Cmd;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.is_complex() {
            if let Some(subreqs) = item.sub_reqs() {
                let cmd_bytes = item.rc_req().cmd_bytes().to_vec();
                if &cmd_bytes[..] == b"MSET" {
                    return self.merge_encode_ok(&subreqs, dst);
                } else if &cmd_bytes[..] == b"DEL" || &cmd_bytes[..] == b"EXISTS" {
                    return self.merge_encode_count(subreqs, dst);
                } else if &cmd_bytes[..] == b"MGET" {
                    return self.merge_encode_join(subreqs, dst);
                } else if &cmd_bytes[..] == b"EVAL" {
                    // return self.merge_encode_join(subreqs, dst);
                } else {
                    unreachable!();
                }
            }
        }

        let reply = item.swap_reply().expect("encode simple reply never empty");
        self.rc.encode(Rc::new(reply), dst)
    }
}

impl Default for HandleCodec {
    fn default() -> Self {
        HandleCodec { rc: RespCodec {} }
    }
}

pub struct NodeCodec {
    rc: RespCodec,
}

impl Default for NodeCodec {
    fn default() -> Self {
        NodeCodec { rc: RespCodec {} }
    }
}

impl Decoder for NodeCodec {
    type Item = Resp;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.rc.decode(src)
    }
}

impl Encoder for NodeCodec {
    type Item = Cmd;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let req = item.rc_req();
        self.rc.encode(req, dst)
    }
}

fn new_ping_request() -> Cmd {
    let req = Resp::new_array(Some(vec![Resp::new_plain(
        RESP_BULK,
        Some(b"PING".to_vec()),
    )]));
    let notify = Notify::empty();
    let cmd = Command {
        is_done: false,
        is_ask: false,
        is_inline: false,

        is_complex: false,
        cmd_type: CmdType::Read,

        crc: 0u16,
        notify,

        req: Rc::new(req),
        sub_reqs: None,
        reply: None,
    };
    Cmd::new(cmd)
}
