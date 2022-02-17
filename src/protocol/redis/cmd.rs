use crate::protocol::redis::resp::Message;

use hashbrown::HashMap;

use crate::protocol::CmdType;

lazy_static! {
    pub static ref CMD_TYPE: HashMap<&'static [u8], CmdType> = {
        let mut hmap = HashMap::new();

        // special commands
        hmap.insert(&b"DEL"[..], CmdType::Del);
        hmap.insert(&b"UNLINK"[..], CmdType::Del);
        hmap.insert(&b"DUMP"[..], CmdType::Read);
        hmap.insert(&b"EXISTS"[..], CmdType::Exists);
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
        hmap.insert(&b"MGET"[..], CmdType::MGet);
        hmap.insert(&b"MSET"[..], CmdType::MSet);
        hmap.insert(&b"MSETNX"[..], CmdType::NotSupport);
        hmap.insert(&b"PSETEX"[..], CmdType::Write);
        hmap.insert(&b"SET"[..], CmdType::Write);
        hmap.insert(&b"SETBIT"[..], CmdType::Write);
        hmap.insert(&b"SETEX"[..], CmdType::Write);
        hmap.insert(&b"SETNX"[..], CmdType::Write);
        hmap.insert(&b"SETRANGE"[..], CmdType::Write);
        hmap.insert(&b"BITFIELD"[..], CmdType::Write);
        hmap.insert(&b"STRLEN"[..], CmdType::Read);
        hmap.insert(&b"SUBSTR"[..], CmdType::Read);

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
        // geo
        hmap.insert(&b"GEOADD"[..], CmdType::Write);
        hmap.insert(&b"GEODIST"[..], CmdType::Read);
        hmap.insert(&b"GEOHASH"[..], CmdType::Read);
        hmap.insert(&b"GEOPOS"[..], CmdType::Write);
        hmap.insert(&b"GEORADIUS"[..], CmdType::Write);
        hmap.insert(&b"GEORADIUSBYMEMBER"[..], CmdType::Write);
        // eval type
        hmap.insert(&b"EVAL"[..], CmdType::Eval);
        hmap.insert(&b"EVALSHA"[..], CmdType::NotSupport);
        // ctrl type
        hmap.insert(&b"AUTH"[..], CmdType::Auth);
        hmap.insert(&b"ECHO"[..], CmdType::Ctrl);
        hmap.insert(&b"PING"[..], CmdType::Ctrl);
        hmap.insert(&b"INFO"[..], CmdType::Ctrl);
        hmap.insert(&b"PROXY"[..], CmdType::NotSupport);
        hmap.insert(&b"SLOWLOG"[..], CmdType::NotSupport);
        hmap.insert(&b"QUIT"[..], CmdType::Ctrl);
        hmap.insert(&b"SELECT"[..], CmdType::NotSupport);
        hmap.insert(&b"TIME"[..], CmdType::NotSupport);
        hmap.insert(&b"CONFIG"[..], CmdType::NotSupport);
        hmap.insert(&b"CLUSTER"[..], CmdType::Ctrl);
        hmap.insert(&b"READONLY"[..], CmdType::Ctrl);

        hmap
    };
}

impl CmdType {
    pub fn is_read(self) -> bool {
        CmdType::Read == self || self.is_mget() || self.is_exists()
    }

    pub fn is_write(self) -> bool {
        CmdType::Write == self
    }

    pub fn is_mget(self) -> bool {
        CmdType::MGet == self
    }

    pub fn is_mset(self) -> bool {
        CmdType::MSet == self
    }

    pub fn is_exists(self) -> bool {
        CmdType::Exists == self
    }

    pub fn is_eval(self) -> bool {
        CmdType::Eval == self
    }

    pub fn is_del(self) -> bool {
        CmdType::Del == self
    }

    pub fn is_not_support(self) -> bool {
        CmdType::NotSupport == self
    }

    pub fn is_ctrl(self) -> bool {
        CmdType::Ctrl == self
    }

    pub fn is_auth(self) -> bool {
        CmdType::Auth == self
    }

    pub fn get_cmd_type(msg: &Message) -> CmdType {
        if let Some(data) = msg.nth(0) {
            if let Some(ctype) = CMD_TYPE.get(data) {
                return *ctype;
            }
        }
        CmdType::NotSupport
    }
}
