use crate::redis::resp;

use btoi;
use futures::unsync::mpsc::SendError;
use net2::TcpBuilder;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use std::convert::From;
use std::io;
use std::net;
use std::net::SocketAddr;
use std::num;
use std::result;

#[derive(Debug)]
pub enum Error {
    None,
    MoreData,
    NotSupport,
    BadMsg,
    BadKey,
    BadCmd,
    BadConfig,
    BadSlotsMap,
    ClusterDown,
    BackendNotFound,
    IoError(io::Error),
    Critical,
    StrParseIntError(num::ParseIntError),
    ParseIntError(btoi::ParseIntegerError),
    SendError(SendError<resp::Resp>),
    AddrParseError(net::AddrParseError),
}

impl From<net::AddrParseError> for Error {
    fn from(oe: net::AddrParseError) -> Error {
        Error::AddrParseError(oe)
    }
}

impl From<SendError<resp::Resp>> for Error {
    fn from(oe: SendError<resp::Resp>) -> Error {
        Error::SendError(oe)
    }
}

impl From<io::Error> for Error {
    fn from(oe: io::Error) -> Error {
        Error::IoError(oe)
    }
}

impl From<btoi::ParseIntegerError> for Error {
    fn from(oe: btoi::ParseIntegerError) -> Error {
        Error::ParseIntError(oe)
    }
}

impl From<num::ParseIntError> for Error {
    fn from(oe: num::ParseIntError) -> Error {
        Error::StrParseIntError(oe)
    }
}

pub type AsResult<T> = result::Result<T, Error>;

const LOWER_BEGIN: u8 = b'a';
const LOWER_END: u8 = b'z';
const UPPER_BEGIN: u8 = b'A';
const UPPER_END: u8 = b'Z';
const UPPER_TO_LOWER: u8 = b'a' - b'A';

pub fn update_to_upper(src: &mut [u8]) {
    for b in src {
        if *b < LOWER_BEGIN || *b > LOWER_END {
            continue;
        }
        *b -= UPPER_TO_LOWER;
    }
}

pub fn update_to_lower(src: &mut [u8]) {
    for b in src {
        if *b < UPPER_BEGIN || *b > UPPER_END {
            continue;
        }
        *b += UPPER_TO_LOWER;
    }
}

#[cfg(not(linux))]
#[inline]
pub fn set_read_write_timeout(
    sock: TcpStream,
    _rt: Option<u64>,
    _wt: Option<u64>,
) -> AsResult<TcpStream> {
    Ok(sock)
}

#[cfg(linux)]
#[inline]
pub fn set_read_write_timeout(
    mut sock: TcpStream,
    rt: Option<u64>,
    wt: Option<u64>,
) -> AsResult<TcpStream> {
    use std::os::unix::AsRawFd;
    use std::os::unix::FromRawFd;
    use std::time::Duration;

    let nrt = rt.map(Duration::from_millis);
    let nwt = wt.map(Duration::from_millis);
    let fd = sock.as_raw_fd();
    let mut nsock = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    nsock.set_read_timeout(nrt)?;
    nsock.set_write_timeout(nwt)?;
    let hd = tokio::reactor::Handle::default();
    let stream = TcpStream::from_std(nsock, &hd)?;
    return Ok(stream);
}

#[cfg(windows)]
pub fn create_reuse_port_listener(addr: &SocketAddr) -> Result<TcpListener, std::io::Error> {
    let builder = TcpBuilder::new_v4()?;
    let std_listener = builder
        .reuse_address(true)
        .expect("os not support SO_REUSEADDR")
        .bind(addr)?
        .listen(std::i32::MAX)?;
    let hd = tokio::reactor::Handle::current();
    TcpListener::from_std(std_listener, &hd)
}

#[cfg(not(windows))]
pub fn create_reuse_port_listener(addr: &SocketAddr) -> Result<TcpListener, std::io::Error> {
    use net2::unix::UnixTcpBuilderExt;

    let builder = TcpBuilder::new_v4()?;
    let std_listener = builder
        .reuse_address(true)
        .expect("os not support SO_REUSEADDR")
        .reuse_port(true)
        .expect("os not support SO_REUSEPORT")
        .bind(addr)?
        .listen(std::i32::MAX)?;
    let hd = tokio::reactor::Handle::default();
    TcpListener::from_std(std_listener, &hd)
}
