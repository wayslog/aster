use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};

use crate::com::AsError;
use crate::protocol::CmdFlags;
use crate::protocol::IntoReply;
use crate::utils::simdfind::find_lf_simd;
use crate::utils::Range;

use std::cmp::min;
use std::io::{Cursor, Seek, SeekFrom};

const BIN_HEADER_LEN: usize = 24;

const BYTE_SPACE: u8 = b' ';

const BYTES_CRLF: &[u8] = b"\r\n";
const BYTES_SPACE: &[u8] = b" ";
const BYTES_END: &[u8] = b"END\r\n";
const BYTES_NOREPLY: &[u8] = b"noreply";

const BIN_STATUS_KEY_NOT_FOUND: u16 = 0x0001u16;

const TEXT_CMDS: &[&str] = &[
    "set", "add", "replace", "append", "prepend", "cas", // storage [0, 5]
    "gets", "get",    // retrieval [6, 7]
    "delete", // delete [8, 8]
    "incr", "decr",  // incr/decr [9, 10]
    "touch", // touch [11, 11]
    "gats", "gat", // get and touch [12, 13]
    "version", "quit", // special command [14, 15]
];

const TEXT_PAT_SET: usize = 0;
const TEXT_PAT_ADD: usize = 1;
const TEXT_PAT_REPLACE: usize = 2;
const TEXT_PAT_APPEND: usize = 3;
const TEXT_PAT_PREPEND: usize = 4;
const TEXT_PAT_CAS: usize = 5;

const TEXT_PAT_GET: usize = 7;
const TEXT_PAT_GETS: usize = 6;

const TEXT_PAT_DELETE: usize = 8;

const TEXT_PAT_INCR: usize = 9;
const TEXT_PAT_DECR: usize = 10;

const TEXT_PAT_TOUCH: usize = 11;

const TEXT_PAT_GAT: usize = 13;
const TEXT_PAT_GATS: usize = 12;

const TEXT_PAT_VERSION: usize = 14;
const TEXT_PAT_QUIT: usize = 15;

const TEXT_RESPS: &[&str] = &[
    "VALUE", // response value sets
    "END",
];

const TEXT_RESP_PAT_VALUE: usize = 0;
// const TEXT_RESP_PAT_END: usize = 1;

lazy_static! {
    static ref TEXT_CMD_FINDER: AhoCorasick = {
        AhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostFirst)
            .build(TEXT_CMDS)
    };
    static ref TEXT_RESP_FINDER: AhoCorasick = {
        AhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostFirst)
            .build(TEXT_RESPS)
    };
}

const MSG_TEXT_MAX_CMD_SIZE: usize = 7; // prepend
const MSG_TEXT_MAX_RESP_TYPE_SIZE: usize = 5; // VALUE

const MSG_BIN_REQ: u8 = 0x80;
const MSG_BIN_RESP: u8 = 0x81;

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum TextCmd {
    // storage commands
    Set(Range),
    Add(Range),
    Replace(Range),
    Append(Range),
    Prepend(Range),
    Cas(Range),
    // retrieval commands
    Get(Vec<Range>),
    Gets(Vec<Range>),
    // Deleteion
    Delete(Range),
    // Incr/Decr
    Incr(Range),
    Decr(Range),
    // Touch
    Touch(Range),
    // Get And Touch
    // first range is the expire field range
    // second range is the key range
    Gat(Range, Vec<Range>),
    Gats(Range, Vec<Range>),
    Version,
    Quit,
}

impl TextCmd {
    fn key_range(&self) -> Range {
        use TextCmd::*;

        match self {
            Set(rng) | Add(rng) | Replace(rng) | Append(rng) | Prepend(rng) | Cas(rng)
            | Delete(rng) | Incr(rng) | Decr(rng) | Touch(rng) => *rng,
            Get(rngs) | Gets(rngs) | Gats(_, rngs) | Gat(_, rngs) => {
                if rngs.is_empty() {
                    return Range::new(0, 0);
                }
                if rngs.len() > 1 {
                    return *rngs.first().unwrap();
                }
                *rngs.first().unwrap()
            }
            _ => Range::new(0, 0),
        }
    }

    fn cmd_slice(&self) -> &[u8] {
        use TextCmd::*;
        match &self {
            Set(_) => &b"set"[..],
            Add(_) => &b"add"[..],
            Replace(_) => &b"replace"[..],
            Append(_) => &b"append"[..],
            Prepend(_) => &b"prepend "[..],
            Cas(_) => &b"cas"[..],
            // retrieval commands
            Get(_) => &b"get"[..],
            Gets(_) => &b"gets"[..],
            // Deleteion
            Delete(_) => &b"delete"[..],
            // Incr/Decr
            Incr(_) => &b"incr"[..],
            Decr(_) => &b"decr"[..],
            // Touch
            Touch(_) => &b"touch"[..],
            // Get And Touch
            // first range is the expire field range
            // second range is the key range
            Gat(_, _) => &b"gat"[..],
            Gats(_, _) => &b"gats"[..],
            Version => &b"version"[..],
            Quit => &b"quit"[..],
        }
    }

    fn set_key_range(&mut self, begin: usize, end: usize) {
        match self {
            TextCmd::Set(ref mut rg)
            | TextCmd::Add(ref mut rg)
            | TextCmd::Replace(ref mut rg)
            | TextCmd::Append(ref mut rg)
            | TextCmd::Prepend(ref mut rg)
            | TextCmd::Cas(ref mut rg)
            | TextCmd::Delete(ref mut rg)
            | TextCmd::Incr(ref mut rg)
            | TextCmd::Decr(ref mut rg)
            | TextCmd::Touch(ref mut rg) => {
                rg.set_begin(begin);
                rg.set_end(end);
            }
            TextCmd::Version | TextCmd::Quit => {}
            _ => unreachable!(),
        }
    }

    fn set_multi_key_range(&mut self, ranges: &mut Vec<Range>) {
        match self {
            TextCmd::Get(ref mut rgs)
            | TextCmd::Gets(ref mut rgs)
            | TextCmd::Gat(_, ref mut rgs)
            | TextCmd::Gats(_, ref mut rgs) => {
                std::mem::swap(rgs, ranges);
            }
            _ => unreachable!(),
        }
    }

    fn set_expire_range(&mut self, begin: usize, end: usize) {
        match self {
            TextCmd::Gat(ref mut rg, _) | TextCmd::Gats(ref mut rg, _) => {
                rg.set_begin(begin);
                rg.set_end(end);
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum BinMsgType {
    Get = 0x00,
    Set = 0x01,
    Add = 0x02,
    Replace = 0x03,
    Delete = 0x04,
    Incr = 0x05,
    Decr = 0x06,
    Quit = 0x07,
    GetQ = 0x09,
    Noop = 0x0a,
    Version = 0x0b,
    GetK = 0x0c,
    GetKQ = 0x0d,
    Append = 0x0e,
    Prepend = 0x0f,
    Stat = 0x10,
    SetQ = 0x11,
    AddQ = 0x12,
    ReplaceQ = 0x13,
    DeleteQ = 0x14,
    IncrementQ = 0x15,
    DecrementQ = 0x16,
    QuitQ = 0x17,
    FlushQ = 0x18,
    AppendQ = 0x19,
    PrependQ = 0x1a,
    Verbosity = 0x1b,
    Touch = 0x1c,
    GAT = 0x1d,
    GATQ = 0x1e,
    RGet = 0x30,
    RSet = 0x31,
    RSetQ = 0x32,
    RAppend = 0x33,
    RAppendQ = 0x34,
    RPrepend = 0x35,
    RPrependQ = 0x36,
    RDelete = 0x37,
    RDeleteQ = 0x38,
    RIncr = 0x39,
    RIncrQ = 0x3a,
    RDecr = 0x3b,
    RDecrQ = 0x3c,
}

impl BinMsgType {
    pub(crate) fn is_quiet(self) -> bool {
        use BinMsgType::*;
        match &self {
            GetQ | GetKQ => true,
            _ => false,
        }
    }

    fn from_u8(data: u8) -> Result<BinMsgType, AsError> {
        use BinMsgType::*;

        let bmtype = match data {
            0x00 => Get,
            0x01 => Set,
            0x02 => Add,
            0x03 => Replace,
            0x04 => Delete,
            0x05 => Incr,
            0x06 => Decr,
            0x07 => Quit,
            0x09 => GetQ,
            0x0a => Noop,
            0x0b => Version,
            0x0c => GetK,
            0x0d => GetKQ,
            0x0e => Append,
            0x0f => Prepend,
            0x10 => Stat,
            0x11 => SetQ,
            0x12 => AddQ,
            0x13 => ReplaceQ,
            0x14 => DeleteQ,
            0x15 => IncrementQ,
            0x16 => DecrementQ,
            0x17 => QuitQ,
            0x18 => FlushQ,
            0x19 => AppendQ,
            0x1a => PrependQ,
            0x1b => Verbosity,
            0x1c => Touch,
            0x1d => GAT,
            0x1e => GATQ,
            0x30 => RGet,
            0x31 => RSet,
            0x32 => RSetQ,
            0x33 => RAppend,
            0x34 => RAppendQ,
            0x35 => RPrepend,
            0x36 => RPrependQ,
            0x37 => RDelete,
            0x38 => RDeleteQ,
            0x39 => RIncr,
            0x3a => RIncrQ,
            0x3b => RDecr,
            0x3c => RDecrQ,
            _ => return Err(AsError::BadMessage),
        };
        Ok(bmtype)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum BinType {
    Req = 0x80,
    Resp = 0x81,
}

impl BinType {
    fn from_u8(data: u8) -> Result<BinType, AsError> {
        match data {
            0x80 => Ok(BinType::Req),
            0x81 => Ok(BinType::Resp),
            _ => Err(AsError::BadMessage),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum MsgType {
    TextReq(TextCmd),
    TextInline,    // one line ends with \r\n, maybe one line resp or error Message line
    TextRespValue, // VALUE <key> <flags> <bytes> [<cas unique>]\r\n<data block>\r\n[END\r\n]
    Binary {
        btype: BinType,
        bmtype: BinMsgType,
        key: Range,
    },
}

impl MsgType {
    #[allow(unused)]
    pub(crate) fn is_quiet(&self) -> bool {
        match self {
            MsgType::Binary { bmtype, .. } => bmtype.is_quiet(),
            _ => false,
        }
    }

    #[allow(unused)]
    pub(crate) fn into_noise(self) -> Self {
        if self.is_quiet() {
            return self;
        }

        match self {
            MsgType::Binary { bmtype, btype, key } => match bmtype {
                BinMsgType::GetQ => MsgType::Binary {
                    btype,
                    key,
                    bmtype: BinMsgType::Get,
                },

                BinMsgType::GetKQ => MsgType::Binary {
                    btype,
                    key,
                    bmtype: BinMsgType::GetK,
                },
                other => MsgType::Binary {
                    btype,
                    key,
                    bmtype: other,
                },
            },
            req => req,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    data: Bytes,
    mtype: MsgType,
    flags: CmdFlags,
}

impl Message {
    pub fn parse(data: &mut BytesMut) -> Result<Option<Message>, AsError> {
        if data.is_empty() {
            return Ok(None);
        }
        // detect binary
        if data[0] == MSG_BIN_REQ || data[0] == MSG_BIN_RESP {
            match Self::parse_binary(data) {
                Ok(msg) => return Ok(msg),
                Err(AsError::BadMessage) => {
                    data.advance(BIN_HEADER_LEN);
                    return Err(AsError::BadMessage);
                }
                Err(err) => return Err(err),
            }
        }

        let line_size = if let Some(pos) = find_lf_simd(&data) {
            pos + 1
        } else {
            return Ok(None);
        };

        if line_size <= BYTES_CRLF.len() {
            // for empty line
            data.advance(line_size);
            return Err(AsError::BadMessage);
        }

        if let Some(mat) = TEXT_CMD_FINDER.find(&data[..min(line_size, MSG_TEXT_MAX_CMD_SIZE)]) {
            return Self::parse_text_req(data, line_size, mat.pattern());
        }

        if let Some(mat) =
            TEXT_RESP_FINDER.find(&data[..min(line_size, MSG_TEXT_MAX_RESP_TYPE_SIZE)])
        {
            return Self::parse_text_value(data, line_size, mat.pattern() != TEXT_RESP_PAT_VALUE);
        }

        Self::parse_text_inline(data, line_size)
    }

    fn parse_text_req(
        data: &mut BytesMut,
        line: usize,
        pat: usize,
    ) -> Result<Option<Message>, AsError> {
        match pat {
            TEXT_PAT_SET => {
                let cmd = TextCmd::Set(Range::default());
                Self::parse_text_storage(data, cmd, line, pat)
            }
            TEXT_PAT_ADD => {
                let cmd = TextCmd::Add(Range::default());
                Self::parse_text_storage(data, cmd, line, pat)
            }
            TEXT_PAT_REPLACE => {
                let cmd = TextCmd::Replace(Range::default());
                Self::parse_text_storage(data, cmd, line, pat)
            }
            TEXT_PAT_APPEND => {
                let cmd = TextCmd::Append(Range::default());
                Self::parse_text_storage(data, cmd, line, pat)
            }
            TEXT_PAT_PREPEND => {
                let cmd = TextCmd::Prepend(Range::default());
                Self::parse_text_storage(data, cmd, line, pat)
            }
            TEXT_PAT_CAS => {
                let cmd = TextCmd::Cas(Range::default());
                Self::parse_text_storage(data, cmd, line, pat)
            }
            TEXT_PAT_GET => {
                let cmd = TextCmd::Get(Vec::new());
                Self::parse_text_retrieval(data, cmd, line, pat)
            }
            TEXT_PAT_GETS => {
                let cmd = TextCmd::Gets(Vec::new());
                Self::parse_text_retrieval(data, cmd, line, pat)
            }
            TEXT_PAT_DELETE => {
                let cmd = TextCmd::Delete(Range::default());
                Self::parse_text_one_line(data, cmd, line, pat)
            }
            TEXT_PAT_INCR => {
                let cmd = TextCmd::Incr(Range::default());
                Self::parse_text_one_line(data, cmd, line, pat)
            }
            TEXT_PAT_DECR => {
                let cmd = TextCmd::Decr(Range::default());
                Self::parse_text_one_line(data, cmd, line, pat)
            }
            TEXT_PAT_TOUCH => {
                let cmd = TextCmd::Touch(Range::default());
                Self::parse_text_one_line(data, cmd, line, pat)
            }
            TEXT_PAT_GAT => {
                let cmd = TextCmd::Gat(Range::default(), Vec::new());
                Self::parse_text_get_and_touch(data, cmd, line, pat)
            }
            TEXT_PAT_GATS => {
                let cmd = TextCmd::Gats(Range::default(), Vec::new());
                Self::parse_text_get_and_touch(data, cmd, line, pat)
            }
            TEXT_PAT_VERSION => {
                let cmd = TextCmd::Version;
                Self::parse_text_one_line(data, cmd, line, pat)
            }
            TEXT_PAT_QUIT => {
                let cmd = TextCmd::Quit;
                Self::parse_text_one_line(data, cmd, line, pat)
            }
            _ => unreachable!(),
        }
    }

    fn parse_text_get_and_touch(
        data: &mut BytesMut,
        mut cmd: TextCmd,
        line: usize,
        pat: usize,
    ) -> Result<Option<Message>, AsError> {
        let mut iter = (&data[..line - 2]).split(|x| *x == BYTE_SPACE).skip(1); // skip cmd
        let mut cursor = TEXT_CMDS[pat].len() + 1;
        if let Some(expire) = iter.next() {
            cmd.set_expire_range(cursor, cursor + expire.len());
            cursor += expire.len() + 1;
        } else {
            data.split_to(line);
            return Err(AsError::BadMessage);
        }

        let mut ranges = Vec::new();
        for key in iter {
            ranges.push(Range::new(cursor, cursor + key.len()));
            cursor += key.len() + 1;
        }
        cmd.set_multi_key_range(&mut ranges);
        Ok(Some(Message {
            data: data.split_to(line).freeze(),
            mtype: MsgType::TextReq(cmd),
            flags: CmdFlags::empty(),
        }))
    }

    fn parse_text_one_line(
        data: &mut BytesMut,
        mut cmd: TextCmd,
        line: usize,
        pat: usize,
    ) -> Result<Option<Message>, AsError> {
        let mut iter = (&data[..line - 2]).split(|x| *x == BYTE_SPACE).skip(1);
        {
            let cursor = TEXT_CMDS[pat].len() + 1;
            if let Some(key) = iter.next() {
                cmd.set_key_range(cursor, cursor + key.len());
            }
        }
        let mut flags = CmdFlags::empty();
        if let Some(last) = iter.last() {
            if last == BYTES_NOREPLY {
                flags |= CmdFlags::NOREPLY;
            }
        }
        Ok(Some(Message {
            data: data.split_to(line).freeze(),
            mtype: MsgType::TextReq(cmd),
            flags,
        }))
    }

    fn parse_text_retrieval(
        data: &mut BytesMut,
        mut cmd: TextCmd,
        line: usize,
        pat: usize,
    ) -> Result<Option<Message>, AsError> {
        let iter = (&data[..line - 2]).split(|x| *x == BYTE_SPACE).skip(1); // skip cmd
        let mut cursor = TEXT_CMDS[pat].len() + 1;
        let mut ranges = Vec::new();
        for key in iter {
            ranges.push(Range::new(cursor, cursor + key.len()));
            cursor += key.len() + 1;
        }
        if ranges.is_empty() {
            data.advance(line);
            return Err(AsError::BadMessage);
        }
        cmd.set_multi_key_range(&mut ranges);
        Ok(Some(Message {
            data: data.split_to(line).freeze(),
            mtype: MsgType::TextReq(cmd),
            flags: CmdFlags::empty(),
        }))
    }

    fn parse_text_storage(
        data: &mut BytesMut,
        mut cmd: TextCmd,
        line: usize,
        pat: usize,
    ) -> Result<Option<Message>, AsError> {
        let key_begin = TEXT_CMDS[pat].len() + 1;
        let mut iter = (&data[..line - BYTES_CRLF.len()]).split(|x| *x == BYTE_SPACE);
        let key_end = {
            iter.next();
            if let Some(key) = iter.next() {
                key_begin + key.len()
            } else {
                data.advance(line);
                return Err(AsError::BadMessage);
            }
        };
        cmd.set_key_range(key_begin, key_end);
        let len = {
            // skip <flags> <exptime>
            iter.next();
            iter.next();

            let bs = match iter.next() {
                Some(bs) => bs,
                None => {
                    data.advance(line);
                    return Err(AsError::BadMessage);
                }
            };
            btoi::btoi::<usize>(bs)?
        };
        let mut flags = CmdFlags::empty();
        if let Some(last) = iter.last() {
            if last == BYTES_NOREPLY {
                flags |= CmdFlags::NOREPLY;
            }
        }
        let total_size = line.wrapping_add(len).wrapping_add(BYTES_CRLF.len());
        if data.len() < total_size {
            return Ok(None);
        }

        Ok(Some(Message {
            data: data.split_to(total_size).freeze(),
            mtype: MsgType::TextReq(cmd),
            flags,
        }))
    }

    fn parse_text_inline(data: &mut BytesMut, line: usize) -> Result<Option<Message>, AsError> {
        Ok(Some(Message {
            data: data.split_to(line).freeze(),
            mtype: MsgType::TextInline,
            flags: CmdFlags::empty(),
        }))
    }

    fn parse_text_value(
        data: &mut BytesMut,
        line: usize,
        is_empty: bool,
    ) -> Result<Option<Message>, AsError> {
        if is_empty {
            return Ok(Some(Message {
                data: data.split_to(line).freeze(),
                mtype: MsgType::TextRespValue,
                flags: CmdFlags::empty(),
            }));
        }
        let len = if let Some(len_data) = (&data[..line - 2])
            .as_ref()
            .split(|x| *x == BYTE_SPACE)
            .nth(3)
        {
            match btoi::btoi::<usize>(len_data) {
                Ok(len) => len,
                Err(_err) => {
                    data.advance(line);
                    return Err(AsError::BadMessage);
                }
            }
        } else {
            data.advance(line);
            return Err(AsError::BadMessage);
        };
        let total_size = line + len + BYTES_CRLF.len() + BYTES_END.len();
        if data.len() < total_size {
            return Ok(None);
        }

        Ok(Some(Message {
            data: data.split_to(total_size).freeze(),
            mtype: MsgType::TextRespValue,
            flags: CmdFlags::empty(),
        }))
    }

    pub(crate) fn parse_binary(data: &mut BytesMut) -> Result<Option<Message>, AsError> {
        if data.len() < BIN_HEADER_LEN {
            return Ok(None);
        }

        let btype = BinType::from_u8(data[0])?;
        let bmtype = BinMsgType::from_u8(data[1])?;
        let mut cursor = Cursor::new(&data[..BIN_HEADER_LEN]);
        cursor
            .seek(SeekFrom::Start(2))
            .map_err(|_| AsError::BadMessage)?;
        let key_len = cursor
            .read_u16::<BigEndian>()
            .map_err(|_| AsError::BadMessage)? as usize;

        let extra_len = cursor.read_u8().map_err(|_| AsError::BadMessage)? as usize;
        cursor
            .seek(SeekFrom::Start(8))
            .map_err(|_| AsError::BadMessage)?;

        let body_len = cursor
            .read_u32::<BigEndian>()
            .map_err(|_| AsError::BadMessage)? as usize;

        let tlen = BIN_HEADER_LEN + body_len;
        if data.len() < tlen {
            return Ok(None);
        }
        let flags = if bmtype.is_quiet() {
            CmdFlags::QUIET
        } else {
            CmdFlags::empty()
        };
        Ok(Some(Message {
            data: data.split_to(tlen).freeze(),
            mtype: MsgType::Binary {
                btype,
                bmtype,
                key: Range::new(
                    BIN_HEADER_LEN + extra_len,
                    BIN_HEADER_LEN + extra_len + key_len,
                ),
            },
            flags,
        }))
    }
}

#[cfg(test)]
mod test {
    use self::super::*;
    #[test]
    fn test_parse_twice() {
        let sv = "VALUE a 0 2\r\nab\r\nEND\r\nVALUE a 0 3\r\ncde\r\nEND\r\n";
        let size = sv.len() / 2;
        let mut data = BytesMut::from(sv.as_bytes());

        let msg_opt = Message::parse(&mut data).unwrap();
        let first = Message {
            data: Bytes::from(&sv.as_bytes()[..size]),
            flags: CmdFlags::empty(),
            mtype: MsgType::TextRespValue,
        };

        assert!(msg_opt.is_some());
        assert_eq!(msg_opt.unwrap(), first);

        let second = Message {
            data: Bytes::from(&sv.as_bytes()[size..]),
            flags: CmdFlags::empty(),
            mtype: MsgType::TextRespValue,
        };
        let msg_opt = Message::parse(&mut data).unwrap();
        assert!(msg_opt.is_some());
        assert_eq!(msg_opt.unwrap(), second);
    }

    fn test_mc_parse_ok(msg: Message) {
        let mut data = BytesMut::from(msg.data.clone());
        let msg_opt = Message::parse(&mut data).unwrap();
        assert!(msg_opt.is_some());
        assert_eq!(msg_opt.unwrap(), msg);
    }

    #[test]
    fn test_parse_text_all_ok() {
        let items = vec![
            Message {
                data: Bytes::from("set mykey 0 0 2\r\nab\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Set(Range::new(4, 9))),
            },
            Message {
                data: Bytes::from("replace mykey 0 0 2\r\nab\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Replace(Range::new(8, 13))),
            },
            Message {
                data: Bytes::from("replace mykey 0 0 2 noreply\r\nab\r\n".as_bytes()),
                flags: CmdFlags::NOREPLY,
                mtype: MsgType::TextReq(TextCmd::Replace(Range::new(8, 13))),
            },
            Message {
                data: Bytes::from("cas mykey 0 0 2 47\r\nab\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Cas(Range::new(4, 9))),
            },
            Message {
                data: Bytes::from("cas mykey 0 0 2 47 noreply\r\nab\r\n".as_bytes()),
                flags: CmdFlags::NOREPLY,
                mtype: MsgType::TextReq(TextCmd::Cas(Range::new(4, 9))),
            },
            Message {
                data: Bytes::from("get mykey\r\n".as_bytes()),
                mtype: MsgType::TextReq(TextCmd::Get(vec![Range::new(4, 9)])),
                flags: CmdFlags::empty(),
            },
            Message {
                data: Bytes::from("get mykey yourkey\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Get(vec![Range::new(4, 9), Range::new(10, 17)])),
            },
            Message {
                data: Bytes::from("gets mykey yourkey\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Gets(vec![Range::new(5, 10), Range::new(11, 18)])),
            },
            Message {
                data: Bytes::from("incr mykey 10\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Incr(Range::new(5, 10))),
            },
            Message {
                data: Bytes::from("decr mykey 10\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Decr(Range::new(5, 10))),
            },
            Message {
                data: Bytes::from("gat 10 mykey yourkey\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Gat(
                    Range::new(4, 6),
                    vec![Range::new(7, 12), Range::new(13, 20)],
                )),
            },
            Message {
                data: Bytes::from("quit\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Quit),
            },
            Message {
                data: Bytes::from("version\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Version),
            },
            // next is for binary cases
            Message {
                data: Bytes::from("version\r\n".as_bytes()),
                flags: CmdFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Version),
            },
        ];
        for item in items {
            println!("test for msg {:?}", item);
            test_mc_parse_ok(item);
        }
    }

    #[test]
    fn test_parse_bin() {
        let get_req_bin_data = vec![
            0x80u8, // magic
            0x0c,   // cmd
            0x00, 0x03, // key len
            0x00, // extra len
            0x00, // data type
            0x00, 0x00, // vbucket
            0x00, 0x00, 0x00, 0x03, // body len
            0x00, 0x00, 0x00, 0x00, // opaque
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
            0x41, 0x42, 0x43, // key: ABC
        ];

        let get_resp_bin_data = vec![
            0x81u8, // magic
            0x0c,   // cmd
            0x00, 0x03, // key len
            0x04, // extra len
            0x00, // data type
            0x00, 0x00, // status
            0x00, 0x00, 0x00, 0x0c, // body len
            0x00, 0x00, 0x00, 0x00, // opaque
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // cas
            0x00, 0x00, 0x00, 0x00, // extra: flag
            0x41, 0x42, 0x43, // key: ABC
            0x41, 0x42, 0x43, 0x44, 0x45, // value: ABCDE
        ];
        let items = vec![
            Message {
                data: Bytes::from(&get_req_bin_data[..]),
                flags: CmdFlags::empty(),
                mtype: MsgType::Binary {
                    btype: BinType::Req,
                    bmtype: BinMsgType::GetK,
                    key: Range::new(24, 27),
                },
            },
            // next is for binary cases
            Message {
                data: Bytes::from(&get_resp_bin_data[..]),
                flags: CmdFlags::empty(),
                mtype: MsgType::Binary {
                    btype: BinType::Resp,
                    bmtype: BinMsgType::GetK,
                    key: Range::new(28, 31),
                },
            },
        ];

        for item in items {
            test_mc_parse_ok(item);
        }
    }

    #[test]
    fn test_parser_error() {
        let fuzz_data = vec![
            0x61, 0x64, 0x64, 0x20, 0x20, 0x20, 0x64, 0xa0, 0x20, 0x30, 0x30, 0x30, 0x30, 0x30,
            0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
            0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
            0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x31, 0x38, 0x34, 0x34,
            0x36, 0x37, 0x34, 0x34, 0x30, 0x37, 0x33, 0x37, 0x30, 0x39, 0x35, 0x35, 0x31, 0x36,
            0x31, 0x35, 0x20, 0x10, 0x20, 0x94, 0x20, 0x64, 0x0a,
        ];
        assert!(fuzz_data.len() >= 24);
        let mut data = BytesMut::from(fuzz_data.clone());
        let msg_rslt = Message::parse_binary(&mut data);
        assert!(msg_rslt.is_err());
    }
}

impl Message {
    pub fn mk_subs(&self) -> Vec<Message> {
        let mut subs = Vec::new();
        match &self.mtype {
            MsgType::TextReq(req) => match req {
                TextCmd::Get(ranges) => {
                    for rg in ranges.iter() {
                        subs.push(Message {
                            data: self.data.clone(),
                            mtype: MsgType::TextReq(TextCmd::Get(vec![*rg])),
                            flags: self.flags,
                        })
                    }
                }
                TextCmd::Gets(ranges) => {
                    for rg in ranges.iter() {
                        subs.push(Message {
                            data: self.data.clone(),
                            mtype: MsgType::TextReq(TextCmd::Gets(vec![*rg])),
                            flags: self.flags,
                        })
                    }
                }
                TextCmd::Gats(expire, ranges) => {
                    for rg in ranges.iter() {
                        subs.push(Message {
                            data: self.data.clone(),
                            mtype: MsgType::TextReq(TextCmd::Gats(*expire, vec![*rg])),
                            flags: self.flags,
                        })
                    }
                }
                TextCmd::Gat(expire, ranges) => {
                    for rg in ranges.iter() {
                        subs.push(Message {
                            data: self.data.clone(),
                            mtype: MsgType::TextReq(TextCmd::Gat(*expire, vec![*rg])),
                            flags: self.flags,
                        })
                    }
                }
                _ => return subs,
            },
            _ => return subs,
        }
        subs
    }

    pub(crate) fn version_request() -> Message {
        Message {
            data: Bytes::from(&b"version\r\n"[..]),
            mtype: MsgType::TextReq(TextCmd::Version),
            flags: CmdFlags::empty(),
        }
    }

    pub(crate) fn raw_inline_reply() -> Message {
        Message {
            data: Bytes::new(),
            mtype: MsgType::TextInline,
            flags: CmdFlags::empty(),
        }
    }

    pub(crate) fn is_noreply(&self) -> bool {
        self.flags & CmdFlags::NOREPLY == CmdFlags::NOREPLY
    }

    pub fn try_save_ends(&self, target: &mut BytesMut) {
        match &self.mtype {
            MsgType::TextReq(TextCmd::Get(_))
            | MsgType::TextReq(TextCmd::Gets(_))
            | MsgType::TextReq(TextCmd::Gat(_, _))
            | MsgType::TextReq(TextCmd::Gats(_, _)) => {
                target.extend_from_slice(BYTES_END);
            }
            _ => {}
        }
    }

    pub(crate) fn get_key(&self) -> &[u8] {
        let key = match &self.mtype {
            MsgType::TextReq(cmd) => cmd.key_range(),
            MsgType::Binary { key, .. } => *key,
            MsgType::TextInline => Range::new(0, 0),
            _ => unreachable!(),
        };
        debug_assert!(self.data.len() > key.begin());
        debug_assert!(self.data.len() > key.end());
        &self.data[key.begin()..key.end()]
    }

    pub fn save_reply(&self, reply: Message, target: &mut BytesMut) -> Result<(), AsError> {
        if self.is_noreply() {
            return Ok(());
        }
        match &self.mtype {
            MsgType::TextReq(TextCmd::Get(_))
            | MsgType::TextReq(TextCmd::Gets(_))
            | MsgType::TextReq(TextCmd::Gat(_, _))
            | MsgType::TextReq(TextCmd::Gats(_, _)) => {
                let data = reply.data.as_ref();
                if data.len() >= BYTES_END.len()
                    && &data[data.len() - BYTES_END.len()..] == BYTES_END
                {
                    target.extend_from_slice(&reply.data.as_ref()[..data.len() - BYTES_END.len()]);
                    return Ok(());
                }
            }

            MsgType::Binary { bmtype, .. } => match bmtype {
                BinMsgType::GetKQ | BinMsgType::GetQ => {
                    let mut cursor = Cursor::new(&self.data[6..]);
                    let status = match cursor.read_u16::<BigEndian>() {
                        Ok(status) => status,
                        Err(err) => {
                            warn!("fail to parse status code {}", err);
                            target.extend_from_slice(reply.data.as_ref());
                            return Ok(());
                        }
                    };
                    if status == BIN_STATUS_KEY_NOT_FOUND {
                        return Ok(());
                    }
                }
                _ => {}
            },
            _ => {}
        }

        target.extend_from_slice(reply.data.as_ref());
        Ok(())
    }

    pub fn save_req(&self, target: &mut BytesMut) -> Result<(), AsError> {
        match &self.mtype {
            MsgType::TextReq(ref ttype) => match ttype {
                TextCmd::Get(ref ranges) | TextCmd::Gets(ref ranges) => {
                    target.extend_from_slice(ttype.cmd_slice());
                    for rng in &ranges[..] {
                        target.extend_from_slice(BYTES_SPACE);
                        target.extend_from_slice(&self.data[rng.begin()..rng.end()]);
                    }
                    target.extend_from_slice(BYTES_CRLF);
                    Ok(())
                }
                TextCmd::Gat(ref expire, ref ranges) | TextCmd::Gats(ref expire, ref ranges) => {
                    target.extend_from_slice(ttype.cmd_slice());
                    target.extend_from_slice(BYTES_SPACE);
                    target.extend_from_slice(&self.data[expire.begin()..expire.end()]);
                    for rng in &ranges[..] {
                        target.extend_from_slice(BYTES_SPACE);
                        target.extend_from_slice(&self.data[rng.begin()..rng.end()]);
                    }
                    target.extend_from_slice(BYTES_CRLF);
                    Ok(())
                }
                _ => {
                    target.extend_from_slice(self.data.as_ref());
                    Ok(())
                }
            },
            MsgType::TextInline => {
                target.extend_from_slice(self.data.as_ref());
                Ok(())
            }
            MsgType::Binary { btype, .. } if btype == &BinType::Req => {
                target.extend_from_slice(self.data.as_ref());
                Ok(())
            }
            _ => {
                unreachable!();
            }
        }
    }
}

impl From<AsError> for Message {
    fn from(oe: AsError) -> Message {
        (&oe).into()
    }
}

impl<'a> Into<Message> for &'a AsError {
    fn into(self) -> Message {
        Message {
            data: Bytes::from(format!("ERROR {}\r\n", self).as_bytes()),
            mtype: MsgType::TextInline,
            flags: CmdFlags::empty(),
        }
    }
}

impl<'a> IntoReply<Message> for &'a AsError {
    fn into_reply(self) -> Message {
        self.into()
    }
}

impl IntoReply<Message> for AsError {
    fn into_reply(self) -> Message {
        self.into()
    }
}
