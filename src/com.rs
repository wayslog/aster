use std::convert::From;
use std::io;
use std::result;
use btoi;

#[derive(Debug)]
pub enum Error {
    None,
    MoreData,
    BadMsg,
    BadKey,
    BadCmd,
    IoError(io::Error),
    Critical,
    ParseIntError(btoi::ParseIntegerError),
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
