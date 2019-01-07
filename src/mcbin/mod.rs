use byteorder::{BigEndian, ReadBytesExt};
use bytes::BytesMut;
use com::*;
use futures::task::Task;
use notify::Notify;
use proxy::Request;
use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;
use tokio_codec::{Decoder, Encoder};
const HEADER_LEN: usize = 24;
//magic
#[derive(Clone, Debug)]
enum Magic {
    MagicUnknow,
    MagicReq = 0x80,
    MagicResp = 0x81,
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
    GetQ = 0x09,
    Noop = 0x0a,
    GetK = 0x0c,
    GetKQ = 0x0d,
    Append = 0x0e,
    Prepend = 0x0f,
}

// enum RespStatus {
//     NoErr = 0x0000,
//     KeyNotFound = 0x0001,
//     KeyExists = 0x0002,
//     ValueTooLarge = 0x0003,
//     InvalidArg = 0x0004,
//     ItemNotStored = 0x0005,
//     NonNumeric = 0x0006,
//     UnknownCmd = 0x0081,
//     OutOfMem = 0x0082,
//     NotSupported = 0x0083,
//     InternalErr = 0x0084,
//     Busy = 0x0085,
//     Temporary = 0x0086,
// }
pub struct McBinCodec {}
impl Decoder for McBinCodec {
    type Item = MCBinReq;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        use self::ReqType::*;
        if src.len() < 24 {
            return Err(Error::MoreData);
        }
        let mut header = [0; 24];
        header.copy_from_slice(&src[..24]);
        let mut req = match parse_header(&header, true) {
            Ok(req) => req,
            Err(err) => return Err(err),
        };
        match req.req_type {
            Get | Set | Add | Replace | Delete | Incr | Decr | Noop | GetK | Append | Prepend => {
                let r = match parse_body(&mut req, src) {
                    Ok(()) => Ok(Some(req)),
                    Err(err) => Err(err),
                };
                return r;
            }
            _ => println!("todo"),
        }

        Ok(Some(req))
    }
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
impl Encoder for McBinCodec {
    type Item = MCBinReq;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let buf = item.reply.as_ref().expect("encode neverbe nul");
        dst.extend(&*buf);
        Ok(())
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
            magic: Magic::MagicUnknow,
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
        0x80 => req.magic = Magic::MagicReq,
        0x81 => req.magic = Magic::MagicResp,
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
            0x09 => GetQ,
            0x0a => Noop,
            0x0c => GetK,
            0x0d => GetKQ,
            0x0e => Append,
            0x0f => Prepend,
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

pub struct HandleCodec {
    mc: McBinCodec,
}
impl Default for HandleCodec {
    fn default() -> Self {
        HandleCodec { mc: McBinCodec {} }
    }
}
impl Decoder for HandleCodec {
    type Item = Req;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.mc.decode(src) {
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
    //todo mock ping req.
    Req {
        req: Rc::new(RefCell::new(MCBinReq::default())),
    }
}
