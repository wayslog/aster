#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate bytes;
extern crate libaster;

use bytes::BytesMut;
use libaster::protocol::mc::{Cmd, Message};

fuzz_target!(|data: &[u8]| {
    let mut src = BytesMut::from(data);
    loop {
        match Message::parse(&mut src).map(|x| x.map(Into::<Cmd>::into)) {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_err) => break,
        }
    }
});
