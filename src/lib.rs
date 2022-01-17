#[macro_use]
extern crate serde_derive;

use com::AsError;

pub mod com;
pub mod config;
pub mod proto;

pub fn run() -> std::result::Result<(), AsError> {
    Ok(())
}

pub fn proxy() -> std::result::Result<(), AsError> {
    Ok(())
}

pub fn cached() -> std::result::Result<(), AsError> {
    Ok(())
}
