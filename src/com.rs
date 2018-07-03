use std::convert::From;
use std::io;
use std::result;

#[derive(Debug)]
pub enum Error {
    None,
    MoreData,
    BadMsg,
    BadKey,
    BadCmd,
    IoError(io::Error),
    Critical,
}

impl From<io::Error> for Error {
    fn from(oe: io::Error) -> Error {
        Error::IoError(oe)
    }
}

pub type AsResult<T> = result::Result<T, Error>;
