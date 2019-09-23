use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use bitflags::bitflags;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};

use crate::com::AsError;
use crate::utils::simdfind::find_lf_simd;
use crate::utils::Range;

use std::cmp::min;
use std::io::{Cursor, Seek, SeekFrom};

const BIN_HEADER_LEN: usize = 24;

const BYTE_SPACE: u8 = b' ';

const BYTES_CRLF: &[u8] = b"\r\n";
const BYTES_END: &[u8] = b"END\r\n";
const BYTES_NOREPLY: &[u8] = b"noreply";

const TEXT_CMDS: &[&'static str] = &[
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

const TEXT_RESPS: &[&'static str] = &[
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

bitflags! {
    struct MCFlags: u8 {
        const NOREPLY = 0b00000001;
        const QUIET   = 0b00000010;
    }
}

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
    TextInline,    // one line ends with \r\n, maybe one line resp or error MessageMut line
    TextRespValue, // VALUE <key> <flags> <bytes> [<cas unique>]\r\n<data block>\r\n[END\r\n]
    Binary {
        btype: BinType,
        bmtype: BinMsgType,
        key: Range,
    },
}

impl MsgType {
    pub(crate) fn is_quiet(&self) -> bool {
        use BinMsgType::*;
        match self {
            MsgType::Binary { bmtype, .. } => match bmtype {
                GetQ | GetKQ => true,
                _ => false,
            },
            _ => false,
        }
    }

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
pub struct MessageMut {
    data: BytesMut,
    mtype: MsgType,
    flags: MCFlags,
}

impl MessageMut {
    pub fn parse(data: &mut BytesMut) -> Result<Option<MessageMut>, AsError> {
        if data.is_empty() {
            return Ok(None);
        }
        // detect binary
        if data[0] == MSG_BIN_REQ || data[0] == MSG_BIN_RESP {
            return Self::parse_binary(data);
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
    ) -> Result<Option<MessageMut>, AsError> {
        match pat {
            TEXT_PAT_SET => {
                let cmd = TextCmd::Set(Range::default());
                return Self::parse_text_storage(data, cmd, line, pat);
            }
            TEXT_PAT_ADD => {
                let cmd = TextCmd::Set(Range::default());
                return Self::parse_text_storage(data, cmd, line, pat);
            }
            TEXT_PAT_REPLACE => {
                let cmd = TextCmd::Replace(Range::default());
                return Self::parse_text_storage(data, cmd, line, pat);
            }
            TEXT_PAT_APPEND => {
                let cmd = TextCmd::Append(Range::default());
                return Self::parse_text_storage(data, cmd, line, pat);
            }
            TEXT_PAT_PREPEND => {
                let cmd = TextCmd::Prepend(Range::default());
                return Self::parse_text_storage(data, cmd, line, pat);
            }
            TEXT_PAT_CAS => {
                let cmd = TextCmd::Cas(Range::default());
                return Self::parse_text_storage(data, cmd, line, pat);
            }
            TEXT_PAT_GET => {
                let cmd = TextCmd::Get(Vec::new());
                return Self::parse_text_retrieval(data, cmd, line, pat);
            }
            TEXT_PAT_GETS => {
                let cmd = TextCmd::Gets(Vec::new());
                return Self::parse_text_retrieval(data, cmd, line, pat);
            }
            TEXT_PAT_DELETE => {
                let cmd = TextCmd::Delete(Range::default());
                return Self::parse_text_one_line(data, cmd, line, pat);
            }
            TEXT_PAT_INCR => {
                let cmd = TextCmd::Incr(Range::default());
                return Self::parse_text_one_line(data, cmd, line, pat);
            }
            TEXT_PAT_DECR => {
                let cmd = TextCmd::Decr(Range::default());
                return Self::parse_text_one_line(data, cmd, line, pat);
            }
            TEXT_PAT_TOUCH => {
                let cmd = TextCmd::Touch(Range::default());
                return Self::parse_text_one_line(data, cmd, line, pat);
            }
            TEXT_PAT_GAT => {
                let cmd = TextCmd::Gat(Range::default(), Vec::new());
                return Self::parse_text_get_and_touch(data, cmd, line, pat);
            }
            TEXT_PAT_GATS => {
                let cmd = TextCmd::Gats(Range::default(), Vec::new());
                return Self::parse_text_get_and_touch(data, cmd, line, pat);
            }
            TEXT_PAT_VERSION => {
                let cmd = TextCmd::Version;
                return Self::parse_text_one_line(data, cmd, line, pat);
            }
            TEXT_PAT_QUIT => {
                let cmd = TextCmd::Quit;
                return Self::parse_text_one_line(data, cmd, line, pat);
            }
            _ => unreachable!(),
        }
    }

    fn parse_text_get_and_touch(
        data: &mut BytesMut,
        mut cmd: TextCmd,
        line: usize,
        pat: usize,
    ) -> Result<Option<MessageMut>, AsError> {
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
        Ok(Some(MessageMut {
            data: data.split_to(line),
            mtype: MsgType::TextReq(cmd),
            flags: MCFlags::empty(),
        }))
    }

    fn parse_text_one_line(
        data: &mut BytesMut,
        mut cmd: TextCmd,
        line: usize,
        pat: usize,
    ) -> Result<Option<MessageMut>, AsError> {
        let mut iter = (&data[..line - 2]).split(|x| *x == BYTE_SPACE).skip(1);
        {
            let cursor = TEXT_CMDS[pat].len() + 1;
            if let Some(key) = iter.next() {
                cmd.set_key_range(cursor, cursor + key.len());
            }
        }
        let mut flags = MCFlags::empty();
        if let Some(last) = iter.last() {
            if last == BYTES_NOREPLY {
                flags |= MCFlags::NOREPLY;
            }
        }
        Ok(Some(MessageMut {
            data: data.split_to(line),
            mtype: MsgType::TextReq(cmd),
            flags,
        }))
    }

    fn parse_text_retrieval(
        data: &mut BytesMut,
        mut cmd: TextCmd,
        line: usize,
        pat: usize,
    ) -> Result<Option<MessageMut>, AsError> {
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
        Ok(Some(MessageMut {
            data: data.split_to(line),
            mtype: MsgType::TextReq(cmd),
            flags: MCFlags::empty(),
        }))
    }

    fn parse_text_storage(
        data: &mut BytesMut,
        mut cmd: TextCmd,
        line: usize,
        pat: usize,
    ) -> Result<Option<MessageMut>, AsError> {
        let key_begin = TEXT_CMDS[pat].len() + 1;
        let mut iter = (&data[..line - BYTES_CRLF.len()]).split(|x| *x == BYTE_SPACE);
        let key_end = {
            iter.next();
            if let Some(key) = iter.next() {
                key_begin + key.len()
            } else {
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
        let mut flags = MCFlags::empty();
        if let Some(last) = iter.last() {
            if last == BYTES_NOREPLY {
                flags |= MCFlags::NOREPLY;
            }
        }
        let total_size = line + len + BYTES_CRLF.len();
        if data.len() < total_size {
            return Ok(None);
        }

        Ok(Some(MessageMut {
            data: data.split_to(total_size),
            mtype: MsgType::TextReq(cmd),
            flags,
        }))
    }

    fn parse_text_inline(data: &mut BytesMut, line: usize) -> Result<Option<MessageMut>, AsError> {
        Ok(Some(MessageMut {
            data: data.split_to(line),
            mtype: MsgType::TextInline,
            flags: MCFlags::empty(),
        }))
    }

    fn parse_text_value(
        data: &mut BytesMut,
        line: usize,
        is_empty: bool,
    ) -> Result<Option<MessageMut>, AsError> {
        if is_empty {
            return Ok(Some(MessageMut {
                data: data.split_to(line),
                mtype: MsgType::TextRespValue,
                flags: MCFlags::empty(),
            }));
        }
        let len =
            if let Some(len_data) = (&data[..line]).as_ref().split(|x| *x == BYTE_SPACE).nth(2) {
                btoi::btoi::<usize>(len_data)?
            } else {
                data.advance(line);
                return Err(AsError::BadMessage);
            };
        let total_size = line + len + BYTES_CRLF.len() + BYTES_END.len();
        if data.len() < total_size {
            return Ok(None);
        }

        Ok(Some(MessageMut {
            data: data.split_to(total_size),
            mtype: MsgType::TextRespValue,
            flags: MCFlags::empty(),
        }))
    }

    fn parse_binary(data: &mut BytesMut) -> Result<Option<MessageMut>, AsError> {
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
        Ok(Some(MessageMut {
            data: data.split_to(tlen),
            mtype: MsgType::Binary {
                btype,
                bmtype,
                key: Range::new(
                    BIN_HEADER_LEN + extra_len,
                    BIN_HEADER_LEN + extra_len + key_len,
                ),
            },
            flags: MCFlags::empty(),
        }))
    }
}

#[cfg(test)]
mod test {
    use self::super::*;

    fn test_mc_parse_ok(msg: MessageMut) {
        let mut data = BytesMut::from(msg.data.clone());
        let msg_opt = MessageMut::parse(&mut data).unwrap();
        assert!(msg_opt.is_some());
        assert_eq!(msg_opt.unwrap(), msg);
    }

    #[test]
    fn test_parse_text_all_ok() {
        let items = vec![
            MessageMut {
                data: BytesMut::from("set mykey 0 0 2\r\nab\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Set(Range::new(4, 9))),
            },
            MessageMut {
                data: BytesMut::from("replace mykey 0 0 2\r\nab\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Replace(Range::new(8, 13))),
            },
            MessageMut {
                data: BytesMut::from("replace mykey 0 0 2 noreply\r\nab\r\n".as_bytes()),
                flags: MCFlags::NOREPLY,
                mtype: MsgType::TextReq(TextCmd::Replace(Range::new(8, 13))),
            },
            MessageMut {
                data: BytesMut::from("cas mykey 0 0 2 47\r\nab\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Cas(Range::new(4, 9))),
            },
            MessageMut {
                data: BytesMut::from("cas mykey 0 0 2 47 noreply\r\nab\r\n".as_bytes()),
                flags: MCFlags::NOREPLY,
                mtype: MsgType::TextReq(TextCmd::Cas(Range::new(4, 9))),
            },
            MessageMut {
                data: BytesMut::from("get mykey\r\n".as_bytes()),
                mtype: MsgType::TextReq(TextCmd::Get(vec![Range::new(4, 9)])),
                flags: MCFlags::empty(),
            },
            MessageMut {
                data: BytesMut::from("get mykey yourkey\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Get(vec![Range::new(4, 9), Range::new(10, 17)])),
            },
            MessageMut {
                data: BytesMut::from("gets mykey yourkey\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Gets(vec![Range::new(5, 10), Range::new(11, 18)])),
            },
            MessageMut {
                data: BytesMut::from("incr mykey 10\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Incr(Range::new(5, 10))),
            },
            MessageMut {
                data: BytesMut::from("decr mykey 10\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Decr(Range::new(5, 10))),
            },
            MessageMut {
                data: BytesMut::from("gat 10 mykey yourkey\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Gat(
                    Range::new(4, 6),
                    vec![Range::new(7, 12), Range::new(13, 20)],
                )),
            },
            MessageMut {
                data: BytesMut::from("quit\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Quit),
            },
            MessageMut {
                data: BytesMut::from("version\r\n".as_bytes()),
                flags: MCFlags::empty(),
                mtype: MsgType::TextReq(TextCmd::Version),
            },
            // next is for binary cases
            MessageMut {
                data: BytesMut::from("version\r\n".as_bytes()),
                flags: MCFlags::empty(),
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
            MessageMut {
                data: BytesMut::from(&get_req_bin_data[..]),
                flags: MCFlags::empty(),
                mtype: MsgType::Binary {
                    btype: BinType::Req,
                    bmtype: BinMsgType::GetK,
                    key: Range::new(24, 27),
                },
            },
            // next is for binary cases
            MessageMut {
                data: BytesMut::from(&get_resp_bin_data[..]),
                flags: MCFlags::empty(),
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    data: Bytes,
    mtype: MsgType,
    flags: MCFlags,
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
                            mtype: MsgType::TextReq(TextCmd::Get(vec![rg.clone()])),
                            flags: self.flags,
                        })
                    }
                }
                TextCmd::Gets(ranges) => {
                    for rg in ranges.iter() {
                        subs.push(Message {
                            data: self.data.clone(),
                            mtype: MsgType::TextReq(TextCmd::Gets(vec![rg.clone()])),
                            flags: self.flags,
                        })
                    }
                }
                TextCmd::Gats(expire, ranges) => {
                    for rg in ranges.iter() {
                        subs.push(Message {
                            data: self.data.clone(),
                            mtype: MsgType::TextReq(TextCmd::Gats(
                                expire.clone(),
                                vec![rg.clone()],
                            )),
                            flags: self.flags,
                        })
                    }
                }
                TextCmd::Gat(expire, ranges) => {
                    for rg in ranges.iter() {
                        subs.push(Message {
                            data: self.data.clone(),
                            mtype: MsgType::TextReq(TextCmd::Gat(expire.clone(), vec![rg.clone()])),
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
}

impl From<MessageMut> for Message {
    fn from(msg: MessageMut) -> Message {
        let MessageMut { data, mtype, flags } = msg;
        Message {
            data: data.freeze(),
            mtype: mtype.into_noise(),
            flags,
        }
    }
}
