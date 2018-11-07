use com::*;
use notify::Notify;
use proxy::Request;

use btoi;
// use bytes::{BufMut, BytesMut};
use bytes::BytesMut;
use futures::task::Task;
use log::Level;
use tokio_codec::{Decoder, Encoder};

use std::cell::RefCell;
use std::rc::Rc;

const BYTE_LF: u8 = b'\n';
const BYTE_CR: u8 = b'\r';
const BYTE_SPACE: u8 = b' ';

const BYTES_CRLF: &[u8] = b"\r\n";
const BYTES_SPACE: &[u8] = b" ";
const BYTES_END: &[u8] = b"END\r\n";
const BYTES_VALUE: &[u8] = b"VALUE";
const BYTES_NOREPLY: &[u8] = b"noreply";

// storage commands
pub const REQ_SET_BYTES: &[u8] = b"set";
pub const REQ_ADD_BYTES: &[u8] = b"add";
pub const REQ_REPLACE_BYTES: &[u8] = b"replace";
pub const REQ_APPEND_BYTES: &[u8] = b"append";
pub const REQ_PREPEND_BYTES: &[u8] = b"prepend";
pub const REQ_CAS_BYTES: &[u8] = b"cas";

// retrieval commands
pub const REQ_GET_BYTES: &[u8] = b"get";
pub const REQ_GETS_BYTES: &[u8] = b"gets";

// delete commands
pub const REQ_DELETE_BYTES: &[u8] = b"delete";

// Incr/Decr
pub const REQ_INCR_BYTES: &[u8] = b"incr";
pub const REQ_DECR_BYTES: &[u8] = b"decr";

// Touch
pub const REQ_TOUCH_BYTES: &[u8] = b"touch";

// get and touch
pub const REQ_GAT_BYTES: &[u8] = b"gat";
pub const REQ_GATS_BYTES: &[u8] = b"gats";

#[derive(Clone, Copy, Debug)]
pub enum ReqType {
    // storage commands
    Set,
    Add,
    Replace,
    Append,
    Prepend,
    Cas,
    // retrieval commands
    Get,
    Gets,
    // Deleteion
    Delete,
    // Incr/Decr
    Incr,
    Decr,
    // Touch
    Touch,
    // Get And Touch
    Gat,
    Gats,
}

impl ReqType {
    fn len(self) -> usize {
        match self {
            ReqType::Set | ReqType::Get | ReqType::Add | ReqType::Cas | ReqType::Gat => 3,
            ReqType::Gats | ReqType::Gets | ReqType::Incr | ReqType::Decr => 4,
            ReqType::Touch => 5,
            ReqType::Append | ReqType::Delete => 6,
            ReqType::Prepend | ReqType::Replace => 7,
        }
    }

    fn is_complex(self) -> bool {
        match self {
            ReqType::Get | ReqType::Gets | ReqType::Gats | ReqType::Gat => true,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Range {
    pub start: usize,
    pub end: usize,
}

impl Range {
    fn new(start: usize, end: usize) -> Self {
        Range { start, end }
    }
}

impl Request for Req {
    type Reply = BytesMut;
    type HandleCodec = HandleCodec;
    type NodeCodec = NodeCodec;

    fn handle_codec() -> Self::HandleCodec {
        HandleCodec::default()
    }
    fn node_codec() -> Self::NodeCodec {
        NodeCodec::default()
    }

    fn ping_request() -> Self {
        new_ping_request()
    }

    fn reregister(&self, task: Task) {
        // self.req.borrow_mut().notify.reregister();
        self.req.borrow_mut().reregister(&task);
    }

    fn key(&self) -> Vec<u8> {
        // let Range { start, end } = { self.req.borrow().key.clone() };
        // self.req.borrow().data[start..end].to_vec()
        // unsafe { Vec::from_raw_parts(data.as_mut_ptr(), end - start, end - start) }
        self.req.borrow().key()
    }

    fn subs(&self) -> Option<Vec<Self>> {
        self.req.borrow().subs.clone()
    }

    fn is_done(&self) -> bool {
        self.req.borrow().is_done()
    }

    fn valid(&self) -> bool {
        // check is not support and set error fast
        true
    }

    fn done(&self, data: Self::Reply) {
        let mut refreq = self.req.borrow_mut();
        refreq.is_done = true;
        refreq.reply = Some(data);
        refreq.notify.done();
    }
    fn done_with_error(&self, err: Error) {
        let data = format!("{:?}", err);
        let buf = BytesMut::from(data.as_bytes().to_vec());
        let mut refreq = self.req.borrow_mut();
        refreq.reply = Some(buf);
        refreq.notify.done();
    }
}

#[derive(Clone, Debug)]
pub struct Req {
    req: Rc<RefCell<MCReq>>,
}

impl Req {
    pub fn is_complex(&self) -> bool {
        self.req.borrow().rtype.is_complex()
    }
}

#[derive(Clone, Debug)]
pub struct MCReq {
    rtype: ReqType,
    data: BytesMut,
    key: Range,

    is_done: bool,
    notify: Notify,

    subs: Option<Vec<Req>>,
    reply: Option<BytesMut>,
}

impl MCReq {
    fn is_done(&self) -> bool {
        self.subs
            .as_ref()
            .map(|x| x.iter().all(|x| x.is_done()))
            .unwrap_or(self.is_done)
    }

    fn key(&self) -> Vec<u8> {
        let Range { start, end } = self.key;
        self.data[start..end].to_vec()
    }

    fn reregister(&mut self, task: &Task) {
        if self.subs.is_some() {
            self.subs
                .as_mut()
                .map(|x| x.iter_mut().for_each(|y| y.reregister(task.clone())))
                .unwrap();
            return;
        }

        self.notify.reregister(task.clone());
    }
}

pub struct HandleCodec {}

impl HandleCodec {
    fn parse(src: &mut BytesMut) -> AsResult<Req> {
        // get first line position
        let line_pos = src
            .iter()
            .position(|x| *x == BYTE_LF)
            .ok_or(Error::MoreData)?
            + 1;
        let cmd_end_pos = src
            .iter()
            .position(|x| *x == BYTE_CR)
            .ok_or(Error::MoreData)?
            + 1;
        // TODO: validate cmd_end_pos position
        update_to_lower(&mut src[..cmd_end_pos]);
        if src.starts_with(REQ_SET_BYTES) {
            Self::parse_storage(src, ReqType::Set, line_pos)
        } else if src.starts_with(REQ_ADD_BYTES) {
            Self::parse_storage(src, ReqType::Add, line_pos)
        } else if src.starts_with(REQ_REPLACE_BYTES) {
            Self::parse_storage(src, ReqType::Replace, line_pos)
        } else if src.starts_with(REQ_APPEND_BYTES) {
            Self::parse_storage(src, ReqType::Append, line_pos)
        } else if src.starts_with(REQ_PREPEND_BYTES) {
            Self::parse_storage(src, ReqType::Prepend, line_pos)
        } else if src.starts_with(REQ_CAS_BYTES) {
            Self::parse_storage(src, ReqType::Cas, line_pos)
        } else if src.starts_with(REQ_GET_BYTES) {
            Self::parse_retrieval(src, ReqType::Get, line_pos)
        } else if src.starts_with(REQ_GETS_BYTES) {
            Self::parse_retrieval(src, ReqType::Gets, line_pos)
        } else if src.starts_with(REQ_DELETE_BYTES) {
            Self::parse_inline(src, ReqType::Delete, line_pos)
        } else if src.starts_with(REQ_INCR_BYTES) {
            Self::parse_inline(src, ReqType::Incr, line_pos)
        } else if src.starts_with(REQ_DECR_BYTES) {
            Self::parse_inline(src, ReqType::Decr, line_pos)
        } else if src.starts_with(REQ_TOUCH_BYTES) {
            Self::parse_inline(src, ReqType::Touch, line_pos)
        } else if src.starts_with(REQ_GAT_BYTES) {
            Self::parse_touch_retrieval(src, ReqType::Gat, line_pos)
        } else if src.starts_with(REQ_GATS_BYTES) {
            Self::parse_touch_retrieval(src, ReqType::Gats, line_pos)
        } else {
            Err(Error::NotSupport)
        }
    }

    fn parse_touch_retrieval(src: &mut BytesMut, rtype: ReqType, le: usize) -> AsResult<Req> {
        let data = src.split_to(le);
        let fields: Vec<_> = data
            .split(|x| *x == BYTE_SPACE)
            .filter(|v| !v.is_empty())
            .collect();
        let count = fields.len() - 3;
        let cmd_size = fields[0].len();
        let expire_size = fields.len();
        let prefix_size = cmd_size + 1 + expire_size + 1;

        let notify = Notify::empty();

        let subs = (0..count)
            .map(|i| {
                let idx = 2 + i;
                let key_len = fields[idx].len();
                let size = prefix_size + key_len + 2;

                let mut buf = BytesMut::with_capacity(size);
                buf.extend_from_slice(&fields[0]);
                buf.extend_from_slice(BYTES_SPACE);
                buf.extend_from_slice(&fields[1]);
                buf.extend_from_slice(BYTES_SPACE);
                buf.extend_from_slice(&fields[idx]);
                buf.extend_from_slice(BYTES_CRLF);
                Req {
                    req: Rc::new(RefCell::new(MCReq {
                        rtype,
                        data: buf,
                        key: Range::new(cmd_size + 1, cmd_size + 1 + key_len),

                        is_done: false,
                        notify: notify.clone(),

                        subs: None,
                        reply: None,
                    })),
                }
            })
            .collect();
        let mcreq = MCReq {
            rtype,
            data: BytesMut::with_capacity(0),
            key: Range::new(0, 0),
            is_done: false,
            notify: notify.clone(),
            subs: Some(subs),
            reply: None,
        };
        mcreq.notify.done();
        Ok(Req {
            req: Rc::new(RefCell::new(mcreq)),
        })
    }

    fn parse_key_range(src: &BytesMut, skip: usize) -> Range {
        let mut fields = src.split(|x| *x == BYTE_SPACE);
        let cmd = fields
            .next()
            .ok_or(Error::MoreData)
            .expect("parse cmd range never be empty");
        let mut len = cmd.len() + 1;
        for _ in 0..skip {
            let tmp = fields
                .next()
                .ok_or(Error::MoreData)
                .expect("parse skip field range never be empty");
            len += tmp.len() + 1;
        }

        let key = fields
            .next()
            .ok_or(Error::MoreData)
            .expect("parse key range is never be empty");
        Range::new(len, len + key.len())
    }

    fn parse_inline(src: &mut BytesMut, rtype: ReqType, le: usize) -> AsResult<Req> {
        let data = src.split_to(le);
        let range = Self::parse_key_range(&data, 0);

        let notify = Notify::empty();
        notify.add(1);
        Ok(Req {
            req: Rc::new(RefCell::new(MCReq {
                rtype,
                data,
                notify,
                key: range,
                is_done: false,
                subs: None,
                reply: None,
            })),
        })
    }

    fn parse_retrieval(src: &mut BytesMut, rtype: ReqType, le: usize) -> AsResult<Req> {
        let data = src.split_to(le);
        let fields: Vec<_> = data[..le - 2]
            .split(|x| *x == BYTE_SPACE)
            .filter(|v| !v.is_empty())
            .collect();
        let count = fields.len() - 1;
        let cmd_size = rtype.len();

        let notify = Notify::empty();

        let subs = (0..count)
            .map(|i| {
                let idx = 1 + i;
                let key_len = fields[idx].len();
                let size = cmd_size + 1 + key_len + 2;
                let mut buf = BytesMut::with_capacity(size);
                buf.extend_from_slice(&fields[0]);
                buf.extend_from_slice(BYTES_SPACE);
                buf.extend_from_slice(&fields[idx]);
                buf.extend_from_slice(BYTES_CRLF);
                Req {
                    req: Rc::new(RefCell::new(MCReq {
                        rtype,
                        data: buf,
                        key: Range::new(cmd_size + 1, cmd_size + 1 + key_len),

                        is_done: false,
                        notify: notify.clone(),

                        subs: None,
                        reply: None,
                    })),
                }
            })
            .collect();
        let mcreq = MCReq {
            rtype,
            data: BytesMut::with_capacity(0),
            key: Range::new(0, 0),
            is_done: false,
            notify: notify.clone(),
            subs: Some(subs),
            reply: None,
        };
        mcreq.notify.done();
        Ok(Req {
            req: Rc::new(RefCell::new(mcreq)),
        })
    }

    fn parse_storage(src: &mut BytesMut, rtype: ReqType, le: usize) -> AsResult<Req> {
        let body_size = {
            // FIXME: set no reply flag and ignore reply for NodeDown
            let mut fields = (&src[..le - 2])
                .split(|x| *x == BYTE_SPACE)
                .filter(|v| !v.is_empty() && *v != BYTES_NOREPLY);

            let size_bytes = fields.nth(5).ok_or(Error::BadMsg)?;
            btoi::btoi::<usize>(size_bytes)?
        };
        let range = Self::parse_key_range(&src, 0);
        let tsize = body_size + le + 2;
        if tsize > src.len() {
            return Err(Error::MoreData);
        }

        let data = src.split_to(tsize);
        let notify = Notify::empty();
        notify.add(1);
        let req = Req {
            req: Rc::new(RefCell::new(MCReq {
                rtype,
                data,
                notify,
                key: range,
                is_done: false,
                subs: None,
                reply: None,
            })),
        };
        Ok(req)
    }
}

impl Decoder for HandleCodec {
    type Item = Req;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let opt_value = Self::parse(src).map(Some).or_else(|err| match err {
            Error::MoreData => Ok(None),
            ev => Err(ev),
        })?;

        Ok(opt_value)
    }
}

impl Encoder for HandleCodec {
    type Item = Req;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.is_complex() {
            let req = item.req.borrow();
            req.subs
                .as_ref()
                .expect("HandleCodec subs is never be empty")
                .iter()
                .for_each(|x| {
                    let subreq = x.req.borrow();
                    let subreply = subreq
                        .reply
                        .as_ref()
                        .expect("HandleCodec subs reply is never be empty");
                    if subreply.ends_with(BYTES_END) {
                        dst.extend(&subreply[..subreply.len() - BYTES_END.len()]);
                    }
                    if log_enabled!(Level::Trace) {
                        trace!("skip merge complex bytes as {:?}", subreply);
                    }
                });
            dst.extend(BYTES_END);
            return Ok(());
        }

        let req = item.req.borrow();
        let buf = req
            .reply
            .as_ref()
            .expect("HandleCodec encode reply is never be empty");
        dst.extend(&*buf);
        Ok(())
    }
}

impl Default for HandleCodec {
    fn default() -> Self {
        HandleCodec {}
    }
}

pub struct NodeCodec {}

impl Decoder for NodeCodec {
    type Item = BytesMut;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let le_opt = src.iter().position(|x| *x == BYTE_LF);
        let le = if let Some(le) = le_opt {
            le + 1
        } else {
            return Ok(None);
        };

        let buf = if src.starts_with(BYTES_VALUE) {
            // 读取 value body
            let size = {
                let mut iter = src[..le - 2].split(|x| *x == BYTE_SPACE);
                let lbs = iter
                    .nth(4)
                    .expect("NodeCodec decode body length never be empty");
                btoi::btoi::<usize>(lbs)?
            };
            let tsize = le + size + BYTES_CRLF.len() + BYTES_END.len();
            if src.len() < tsize {
                return Ok(None);
            }
            src.split_to(tsize)
        } else {
            src.split_to(le)
        };

        Ok(Some(buf))
    }
}

impl Encoder for NodeCodec {
    type Item = Req;
    type Error = Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let req = item.req.borrow();
        dst.extend(&req.data);
        Ok(())
    }
}

impl Default for NodeCodec {
    fn default() -> Self {
        NodeCodec {}
    }
}

fn new_ping_request() -> Req {
    let mcreq = MCReq {
        rtype: ReqType::Set,
        data: BytesMut::from(b"set ass-we-can-ping-value 0 0 1\r\n1\r\n".to_vec()),
        key: Range::new(4, 4 + 21),
        is_done: false,
        notify: Notify::empty(),
        subs: None,
        reply: None,
    };
    mcreq.notify.add(1);
    Req {
        req: Rc::new(RefCell::new(mcreq)),
    }
}
