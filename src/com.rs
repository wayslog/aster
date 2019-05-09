pub use failure::Error;

#[derive(Debug, Fail)]
pub enum RespError {
    #[fail(display = "invalid message")]
    BadMessage,

    #[fail(display = "message is ok but request bad or not allowed")]
    BadReqeust,
}
