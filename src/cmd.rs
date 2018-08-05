use com::*;
use crc16;
use resp::{Resp, RESP_BULK, RESP_ERROR};

use futures::task::{self, Task};
use tokio::prelude::*;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

#[derive(Clone, Copy, Debug)]
pub enum CmdType {
    Read,
    Write,
    Ctrl,
    NotSupport,
}

pub type Cmd = Rc<RefCell<Command>>;
/// Command is a type for Redis Command.
pub struct Command {
    pub is_done: bool,
    pub is_ask: bool,
    pub is_inline: bool,

    pub is_complex: bool,
    pub cmd_type: CmdType,

    pub crc: u16,
    pub task: Task,

    pub req: Resp,
    pub sub_reqs: Option<Vec<Cmd>>,
    pub reply: Option<Resp>,
}

impl Command {
    fn inner_from_resp(mut resp: Resp) -> Command {
        let local_task = task::current();
        Self::cmd_to_upper(&mut resp);
        let cmd_type = Self::get_cmd_type(&resp);
        let is_complex = Self::is_complex(&resp);
        let crc = Self::crc16(&resp);

        Command {
            is_done: false,
            is_ask: false,
            is_inline: false,

            is_complex: is_complex,
            cmd_type: cmd_type,

            crc: crc,
            task: local_task,
            req: resp,
            sub_reqs: None,
            reply: None,
        }
    }

    pub fn from_resp(resp: Resp) -> Command {
        let mut command = Self::inner_from_resp(resp);
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

    fn is_complex(resp: &Resp) -> bool {
        let cmd = resp.get(0).expect("never be empty");
        CMD_COMPLEX.contains(&cmd.data.as_ref().expect("never null")[..])
    }

    fn get_cmd_type(resp: &Resp) -> CmdType {
        let cmd = resp.get(0).expect("never be empty");
        if let Some(&ctype) = CMD_TYPE.get(&cmd.data.as_ref().expect("never null")[..]) {
            return ctype;
        }
        CmdType::NotSupport
    }

    fn crc16(resp: &Resp) -> u16 {
        let cmd = resp.get(0).expect("never be empty");
        let mut state = crc16::State::<crc16::XMODEM>::new();
        state.update(&cmd.data.as_ref().expect("never null")[..]);
        state.get()
    }
}

impl Command {
    pub fn crc(&self) -> u16 {
        return self.crc
    }

    pub fn is_batch(&self) -> bool {
        self.sub_reqs.is_some()
    }

    pub fn subs(&self) -> &[Cmd] {
        self.sub_reqs
            .as_ref()
            .expect("call subs never fail")
    }

    fn mksubs(&mut self) {
        if !self.is_complex {
            return;
        }

        if self.req.cmd_bytes() == b"MSET" {
            return self.mk_mset();
        } else if self.req.cmd_bytes() == b"EVAL" {
            self.mk_eval();
        }
        return self.mk_by_keys();
    }

    fn mk_eval(&mut self) {
        let key_resp = self.req.get(3).expect("eval must contains key");
        self.crc = calc_crc16(key_resp.data.as_ref().expect("key must contains value"));
    }

    fn mk_mset(&mut self) {
        let arr_len = self.req.array.as_ref().expect("cmd must be array").len();
        if arr_len < 3 || arr_len % 2 == 0 {
            return self.done_with_error(&RESP_OBJ_ERROR_BAD_CMD);
        } else if arr_len == 3 {
            trace!("skip to split MSET");
            return;
        }

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
                let mut cmd = Command::inner_from_resp(resp);
                cmd.is_complex = is_complex;
                Rc::new(RefCell::new(cmd))
            })
            .collect();

        self.sub_reqs = Some(subcmds);
    }

    fn mk_by_keys(&mut self) {
        let arr_len = self.req.array.as_ref().expect("cmd must be array").len();
        if arr_len < 2 {
            return self.done_with_error(&RESP_OBJ_ERROR_BAD_CMD);
        } else if arr_len == 2 {
            trace!("skip to split cmd with only one key");
            return;
        }

        let resps = self.req.array.as_ref().expect("cmd must be array").clone();
        let mut iter = resps.into_iter();

        let cmd = iter.next().expect("cmd must be contains");
        let cmd = Self::get_single_cmd(cmd);

        let subcmds: Vec<Cmd> =
            iter.map(|arg| {
                let mut arr = Vec::with_capacity(2);
                arr.push(cmd.clone());
                arr.push(arg);
                Resp::new_array(Some(arr))
            }).map(|resp| {
                    let mut cmd = Command::inner_from_resp(resp);
                    cmd.is_complex = self.is_complex;
                    cmd.task = self.task.clone();
                    Rc::new(RefCell::new(cmd))
                })
                .collect();
        self.sub_reqs = Some(subcmds);
    }

    pub fn is_done(&self) -> bool {
        self.is_done
    }

    pub fn set_reply(&mut self, reply: Resp) {
        self.reply = Some(reply);
    }

    fn done_with_error(&mut self, err: &Resp) {
        self.reply = Some(err.clone());
        self.is_done = true;
        // ignore if task current.
        self.task.notify();
    }
}

pub struct CommandStream<S: Stream<Item = Resp, Error = Error>> {
    input: S,
}

impl<S> CommandStream<S>
where
    S: Stream<Item = Resp, Error = Error>,
{
    pub fn new(input: S) -> Self {
        Self { input: input }
    }
}

impl<S> Stream for CommandStream<S>
where
    S: Stream<Item = Resp, Error = Error>,
{
    type Item = Command;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if let Some(resp) = try_ready!(self.input.poll()) {
            return Ok(Async::Ready(Some(Command::from_resp(resp))));
        }
        Ok(Async::Ready(None))
    }
}

pub struct RcCmd<S: Stream> {
    input: S,
}

impl<S: Stream> Stream for RcCmd<S> {
    type Item = Rc<S::Item>;
    type Error = S::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if let Some(item) = try_ready!(self.input.poll()) {
            return Ok(Async::Ready(Some(Rc::new(item))));
        }
        Ok(Async::NotReady)
    }
}

lazy_static!{
    pub static ref CMD_TYPE: HashMap<&'static [u8], CmdType> = {
        let mut hmap = HashMap::new();

        // special commands
        hmap.insert("DEL".as_bytes(), CmdType::Write);
        hmap.insert("DUMP".as_bytes(), CmdType::Read);
        hmap.insert("EXISTS".as_bytes(), CmdType::Read);
        hmap.insert("EXPIRE".as_bytes(), CmdType::Write);
        hmap.insert("EXPIREAT".as_bytes(), CmdType::Write);
        hmap.insert("KEYS".as_bytes(), CmdType::NotSupport);
        hmap.insert("MIGRATE".as_bytes(), CmdType::NotSupport);
        hmap.insert("MOVE".as_bytes(), CmdType::NotSupport);
        hmap.insert("OBJECT".as_bytes(), CmdType::NotSupport);
        hmap.insert("PERSIST".as_bytes(), CmdType::Write);
        hmap.insert("PEXPIRE".as_bytes(), CmdType::Write);
        hmap.insert("PEXPIREAT".as_bytes(), CmdType::Write);
        hmap.insert("PTTL".as_bytes(), CmdType::Read);
        hmap.insert("RANDOMKEY".as_bytes(), CmdType::NotSupport);
        hmap.insert("RENAME".as_bytes(), CmdType::NotSupport);
        hmap.insert("RENAMENX".as_bytes(), CmdType::NotSupport);
        hmap.insert("RESTORE".as_bytes(), CmdType::Write);
        hmap.insert("SCAN".as_bytes(), CmdType::NotSupport);
        hmap.insert("SORT".as_bytes(), CmdType::Write);
        hmap.insert("TTL".as_bytes(), CmdType::Read);
        hmap.insert("TYPE".as_bytes(), CmdType::Read);
        hmap.insert("WAIT".as_bytes(), CmdType::NotSupport);
        // string key
        hmap.insert("APPEND".as_bytes(), CmdType::Write);
        hmap.insert("BITCOUNT".as_bytes(), CmdType::Read);
        hmap.insert("BITOP".as_bytes(), CmdType::NotSupport);
        hmap.insert("BITPOS".as_bytes(), CmdType::Read);
        hmap.insert("DECR".as_bytes(), CmdType::Write);
        hmap.insert("DECRBY".as_bytes(), CmdType::Write);
        hmap.insert("GET".as_bytes(), CmdType::Read);
        hmap.insert("GETBIT".as_bytes(), CmdType::Read);
        hmap.insert("GETRANGE".as_bytes(), CmdType::Read);
        hmap.insert("GETSET".as_bytes(), CmdType::Write);
        hmap.insert("INCR".as_bytes(), CmdType::Write);
        hmap.insert("INCRBY".as_bytes(), CmdType::Write);
        hmap.insert("INCRBYFLOAT".as_bytes(), CmdType::Write);
        hmap.insert("MGET".as_bytes(), CmdType::Read);
        hmap.insert("MSET".as_bytes(), CmdType::Write);
        hmap.insert("MSETNX".as_bytes(), CmdType::NotSupport);
        hmap.insert("PSETEX".as_bytes(), CmdType::Write);
        hmap.insert("SET".as_bytes(), CmdType::Write);
        hmap.insert("SETBIT".as_bytes(), CmdType::Write);
        hmap.insert("SETEX".as_bytes(), CmdType::Write);
        hmap.insert("SETNX".as_bytes(), CmdType::Write);
        hmap.insert("SETRANGE".as_bytes(), CmdType::Write);
        hmap.insert("STRLEN".as_bytes(), CmdType::Read);
        // hash type
        hmap.insert("HDEL".as_bytes(), CmdType::Write);
        hmap.insert("HEXISTS".as_bytes(), CmdType::Read);
        hmap.insert("HGET".as_bytes(), CmdType::Read);
        hmap.insert("HGETALL".as_bytes(), CmdType::Read);
        hmap.insert("HINCRBY".as_bytes(), CmdType::Write);
        hmap.insert("HINCRBYFLOAT".as_bytes(), CmdType::Write);
        hmap.insert("HKEYS".as_bytes(), CmdType::Read);
        hmap.insert("HLEN".as_bytes(), CmdType::Read);
        hmap.insert("HMGET".as_bytes(), CmdType::Read);
        hmap.insert("HMSET".as_bytes(), CmdType::Write);
        hmap.insert("HSET".as_bytes(), CmdType::Write);
        hmap.insert("HSETNX".as_bytes(), CmdType::Write);
        hmap.insert("HSTRLEN".as_bytes(), CmdType::Read);
        hmap.insert("HVALS".as_bytes(), CmdType::Read);
        hmap.insert("HSCAN".as_bytes(), CmdType::Read);
        // list type
        hmap.insert("BLPOP".as_bytes(), CmdType::NotSupport);
        hmap.insert("BRPOP".as_bytes(), CmdType::NotSupport);
        hmap.insert("BRPOPLPUSH".as_bytes(), CmdType::NotSupport);
        hmap.insert("LINDEX".as_bytes(), CmdType::Read);
        hmap.insert("LINSERT".as_bytes(), CmdType::Write);
        hmap.insert("LLEN".as_bytes(), CmdType::Read);
        hmap.insert("LPOP".as_bytes(), CmdType::Write);
        hmap.insert("LPUSH".as_bytes(), CmdType::Write);
        hmap.insert("LPUSHX".as_bytes(), CmdType::Write);
        hmap.insert("LRANGE".as_bytes(), CmdType::Read);
        hmap.insert("LREM".as_bytes(), CmdType::Write);
        hmap.insert("LSET".as_bytes(), CmdType::Write);
        hmap.insert("LTRIM".as_bytes(), CmdType::Write);
        hmap.insert("RPOP".as_bytes(), CmdType::Write);
        hmap.insert("RPOPLPUSH".as_bytes(), CmdType::Write);
        hmap.insert("RPUSH".as_bytes(), CmdType::Write);
        hmap.insert("RPUSHX".as_bytes(), CmdType::Write);
        // set type
        hmap.insert("SADD".as_bytes(), CmdType::Write);
        hmap.insert("SCARD".as_bytes(), CmdType::Read);
        hmap.insert("SDIFF".as_bytes(), CmdType::Read);
        hmap.insert("SDIFFSTORE".as_bytes(), CmdType::Write);
        hmap.insert("SINTER".as_bytes(), CmdType::Read);
        hmap.insert("SINTERSTORE".as_bytes(), CmdType::Write);
        hmap.insert("SISMEMBER".as_bytes(), CmdType::Read);
        hmap.insert("SMEMBERS".as_bytes(), CmdType::Read);
        hmap.insert("SMOVE".as_bytes(), CmdType::Write);
        hmap.insert("SPOP".as_bytes(), CmdType::Write);
        hmap.insert("SRANDMEMBER".as_bytes(), CmdType::Read);
        hmap.insert("SREM".as_bytes(), CmdType::Write);
        hmap.insert("SUNION".as_bytes(), CmdType::Read);
        hmap.insert("SUNIONSTORE".as_bytes(), CmdType::Write);
        hmap.insert("SSCAN".as_bytes(), CmdType::Read);
        // zset type
        hmap.insert("ZADD".as_bytes(), CmdType::Write);
        hmap.insert("ZCARD".as_bytes(), CmdType::Read);
        hmap.insert("ZCOUNT".as_bytes(), CmdType::Read);
        hmap.insert("ZINCRBY".as_bytes(), CmdType::Write);
        hmap.insert("ZINTERSTORE".as_bytes(), CmdType::Write);
        hmap.insert("ZLEXCOUNT".as_bytes(), CmdType::Read);
        hmap.insert("ZRANGE".as_bytes(), CmdType::Read);
        hmap.insert("ZRANGEBYLEX".as_bytes(), CmdType::Read);
        hmap.insert("ZRANGEBYSCORE".as_bytes(), CmdType::Read);
        hmap.insert("ZRANK".as_bytes(), CmdType::Read);
        hmap.insert("ZREM".as_bytes(), CmdType::Write);
        hmap.insert("ZREMRANGEBYLEX".as_bytes(), CmdType::Write);
        hmap.insert("ZREMRANGEBYRANK".as_bytes(), CmdType::Write);
        hmap.insert("ZREMRANGEBYSCORE".as_bytes(), CmdType::Write);
        hmap.insert("ZREVRANGE".as_bytes(), CmdType::Read);
        hmap.insert("ZREVRANGEBYLEX".as_bytes(), CmdType::Read);
        hmap.insert("ZREVRANGEBYSCORE".as_bytes(), CmdType::Read);
        hmap.insert("ZREVRANK".as_bytes(), CmdType::Read);
        hmap.insert("ZSCORE".as_bytes(), CmdType::Read);
        hmap.insert("ZUNIONSTORE".as_bytes(), CmdType::Write);
        hmap.insert("ZSCAN".as_bytes(), CmdType::Read);
        // hyper log type
        hmap.insert("PFADD".as_bytes(), CmdType::Write);
        hmap.insert("PFCOUNT".as_bytes(), CmdType::Read);
        hmap.insert("PFMERGE".as_bytes(), CmdType::Write);
        // eval type
        hmap.insert("EVAL".as_bytes(), CmdType::Write);
        hmap.insert("EVALSHA".as_bytes(), CmdType::NotSupport);
        // ctrl type
        hmap.insert("AUTH".as_bytes(), CmdType::NotSupport);
        hmap.insert("ECHO".as_bytes(), CmdType::Ctrl);
        hmap.insert("PING".as_bytes(), CmdType::Ctrl);
        hmap.insert("INFO".as_bytes(), CmdType::Ctrl);
        hmap.insert("PROXY".as_bytes(), CmdType::NotSupport);
        hmap.insert("SLOWLOG".as_bytes(), CmdType::NotSupport);
        hmap.insert("QUIT".as_bytes(), CmdType::NotSupport);
        hmap.insert("SELECT".as_bytes(), CmdType::NotSupport);
        hmap.insert("TIME".as_bytes(), CmdType::NotSupport);
        hmap.insert("CONFIG".as_bytes(), CmdType::NotSupport);

        hmap
    };

    pub static ref RESP_OBJ_BULK_GET: Resp =
    {

        Resp::new_plain(RESP_BULK, Some("GET".as_bytes().to_vec())) };
    pub static ref RESP_OBJ_BULK_SET: Resp =
    {

        Resp::new_plain(RESP_BULK, Some("SET".as_bytes().to_vec())) };
    pub static ref RESP_OBJ_ERROR_BAD_CMD: Resp =
    {
        Resp::new_plain(RESP_ERROR, Some("command format wrong".as_bytes().to_vec())) };

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
    let mut state = crc16::State::<crc16::XMODEM>::new();
    state.update(data);
    state.get()
}
