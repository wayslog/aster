use crate::com::*;
use crate::notify::Notify;
use crate::proxy::Request;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::BytesMut;
use futures::task::Task;
use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;
use tokio_codec::{Decoder, Encoder};
const HEADER_LEN: usize = 24;
//magic
#[derive(Clone, Debug)]
enum Magic {
    Unknow,
    Req = 0x80,
    Resp = 0x81,
}

// conmmand op
#[derive(Clone, Debug)]
enum ReqType {
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

fn parse(src: &mut BytesMut, req: bool) -> Result<Option<MCBinReq>, Error> {
    if src.len() < 24 {
        return Err(Error::MoreData);
    }
    let mut header = [0; 24];
    header.copy_from_slice(&src[..24]);
    let mut req = match parse_header(&header, req) {
        Ok(req) => req,
        Err(err) => return Err(err),
    };
    match parse_body(&mut req, src) {
        Ok(()) => Ok(Some(req)),
        Err(err) => Err(err),
    }
}

#[derive(Clone, Debug)]
pub struct MCBinReq {
    magic: Magic,
    req_type: ReqType,
    key_len: u16,
    extra_len: u8,
    // dataType: u8,
    // status: [u8; 2],
    body_len: u32,
    // opaque: [u8; 4],
    // cas: [u8; 4],
    key: Range,
    data: BytesMut,
    is_done: bool,
    notify: Notify,
    reply: Option<BytesMut>,
}
#[derive(Debug)]
pub struct Req {
    req: Rc<RefCell<MCBinReq>>,
}
impl Drop for Req {
    fn drop(&mut self) {
        self.req.borrow().notify.notify();
    }
}
impl Clone for Req {
    fn clone(&self) -> Req {
        self.req.borrow().notify.add(1);
        Req {
            req: self.req.clone(),
        }
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
        self.req.borrow_mut().notify.reregister(task.clone());
    }
    fn key(&self) -> Vec<u8> {
        let req = self.req.borrow();
        let key = &req.data[req.key.start..req.key.end];
        key.to_vec()
    }
    fn subs(&self) -> Option<Vec<Self>> {
        None
    }
    fn is_done(&self) -> bool {
        self.req.borrow().is_done
    }
    fn valid(&self) -> bool {
        true
    }
    fn done(&self, data: Self::Reply) {
        let mut req = self.req.borrow_mut();
        req.is_done = true;
        req.reply = Some(data)
    }
    fn done_with_error(&self, err: Error) {
        let data = format!("{:?}", err);
        let buf = BytesMut::from(data.as_bytes().to_vec());
        let mut req = self.req.borrow_mut();
        req.reply = Some(buf);
    }
}
impl Default for MCBinReq {
    fn default() -> Self {
        MCBinReq {
            magic: Magic::Unknow,
            req_type: ReqType::Get,
            key_len: 0,
            extra_len: 0,
            notify: Notify::empty(),
            is_done: false,
            // dataType: 0,
            // status: [0; 2],
            body_len: 0,
            // opaque: [0; 4],
            // cas: [0; 4],
            key: Range { start: 0, end: 0 },
            data: BytesMut::with_capacity(9),
            reply: None,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Range {
    pub start: usize,
    pub end: usize,
}
fn parse_header(src: &[u8], decode: bool) -> Result<MCBinReq, Error> {
    let mut req = MCBinReq::default();
    match src[0] {
        0x80 => req.magic = Magic::Req,
        0x81 => req.magic = Magic::Resp,
        _ => return Err(Error::BadCmd),
    }
    if decode {
        use self::ReqType::*;
        req.req_type = match src[1] {
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
            _ => return Err(Error::BadCmd),
        }
    }
    let mut len = Cursor::new(&src[2..4]);
    req.key_len = len.read_u16::<BigEndian>().unwrap();
    req.extra_len = src[4];
    // req.dataType = src[5];
    // if !decode {
    //     req.status.copy_from_slice(&src[6..8]);
    // }
    let mut bodylen = Cursor::new(&src[8..12]);
    req.body_len = bodylen.read_u32::<BigEndian>().unwrap();
    // req.opaque.copy_from_slice(&src[12..16]);
    // req.cas.copy_from_slice(&src[16..24]);
    Ok(req)
}
fn parse_body(req: &mut MCBinReq, src: &mut BytesMut) -> Result<(), Error> {
    let tsize = HEADER_LEN as usize + req.body_len as usize;
    if src.len() < tsize {
        return Err(Error::MoreData);
    }
    req.key = Range {
        start: HEADER_LEN + req.extra_len as usize,
        end: HEADER_LEN + req.extra_len as usize + req.key_len as usize,
    };
    req.data = src.split_to(tsize);
    Ok(())
}

pub struct HandleCodec {}
impl Default for HandleCodec {
    fn default() -> Self {
        HandleCodec {}
    }
}
impl Decoder for HandleCodec {
    type Item = Req;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match parse(src, true) {
            Ok(Some(req)) => Ok(Some(Req {
                req: Rc::new(RefCell::new(req)),
            })),
            Ok(_) | Err(Error::MoreData) => Ok(None),
            Err(ev) => Err(ev),
        }
    }
}

impl Encoder for HandleCodec {
    type Item = Req;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let req = item.req.borrow();
        let buf = req.reply.as_ref().expect("reply never be null");
        dst.extend(buf);
        Ok(())
    }
}

pub struct NodeCodec {}
impl Default for NodeCodec {
    fn default() -> Self {
        NodeCodec {}
    }
}
impl Decoder for NodeCodec {
    type Item = BytesMut;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match parse(src, false) {
            Ok(Some(resp)) => Ok(Some(resp.data)),
            Err(Error::MoreData) => Ok(None),
            Err(err) => Err(err),
            Ok(None) => Ok(None),
        }
    }
}

impl Encoder for NodeCodec {
    type Item = Req;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend(&item.req.borrow().data);
        Ok(())
    }
}

fn new_ping_request() -> Req {
    let mut req = MCBinReq::default();
    req.req_type = ReqType::Noop;
    Req {
        req: Rc::new(RefCell::new(req)),
    }
}
