use failure::Fail;

pub mod proxy;

pub const ENV_ASTER_DEFAULT_THREADS: &str = "ASTER_DEFAULT_THREAD";
pub const DEFAULT_FETCH_INTERVAL_MS: u64 = 30 * 60 * 1000;

#[derive(Debug, Fail)]
pub enum AsError {
    #[fail(display = "config is bad for fields {}", _0)]
    BadConfig(String),

    #[fail(display = "unexpected io error {}", _0)]
    IoError(tokio::io::Error), // io_error

    #[fail(display = "there is nothing happening")]
    None,
}

impl From<std::io::Error> for AsError {
    fn from(oe: std::io::Error) -> AsError {
        AsError::IoError(oe)
    }
}

impl From<toml::de::Error> for AsError {
    fn from(oe: toml::de::Error) -> AsError {
        AsError::BadConfig(format!("{}", oe))
    }
}
