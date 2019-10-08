#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate bytes;
extern crate libaster;

use bytes::BytesMut;

use libaster::protocol::redis::Command;

fuzz_target!(|data: &[u8]| {
    let mut src = BytesMut::from(data);
    loop {
        let result = Command::parse_cmd(&mut src);
        match result {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_err) => break,
        }
    }
});
