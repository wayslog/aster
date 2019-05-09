// #![deny(warnings)]
#[macro_use(try_ready)]
extern crate futures;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate failure;

pub mod com;
pub mod protocol;
pub mod proxy;
pub(crate) mod utils;

use failure::Error;

pub fn run() -> Result<(), Error> {
    unimplemented!();
}
