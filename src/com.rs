use btoi;
use futures::unsync::mpsc::SendError;
use std::convert::From;
use std::io;
use std::result;
use std::net;

#[derive(Debug)]
pub enum Error {
    None,
    MoreData,
    BadMsg,
    BadKey,
    BadCmd,
    BadSlotsMap,
    IoError(io::Error),
    Critical,
    ParseIntError(btoi::ParseIntegerError),
    SendError(SendError<::Resp>),
    AddrParseError(net::AddrParseError),
}


impl From<net::AddrParseError> for Error {
    fn from(oe: net::AddrParseError) -> Error {
        Error::AddrParseError(oe)
    }
}

impl From<SendError<::Resp>> for Error {
    fn from(oe: SendError<::Resp>) -> Error {
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

pub type AsResult<T> = result::Result<T, Error>;

const LOWER_BEGIN: u8 = 'a' as u8;
const LOWER_END: u8 = 'z' as u8;
const UPPER_TO_LOWER: u8 = 'a' as u8 - 'A' as u8;

pub fn update_to_upper(src: &mut [u8]) {
    for b in src {
        if *b < LOWER_BEGIN || *b > LOWER_END {
            continue;
        }
        *b = *b - UPPER_TO_LOWER;
    }
}
