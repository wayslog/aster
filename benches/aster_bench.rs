#![feature(repeat_generic_slice)]
#[macro_use]
extern crate criterion;

use libaster::proxy::fnv::Fnv1a64;
use libaster::proxy::ketama::HashRing;
use libaster::redis::resp::{Resp, RespFSMCodec};

use bytes::BytesMut;
use criterion::Criterion;

fn bench_resp(c: &mut Criterion) {
    // fn bench_parse_plain(b:&mut Bencher) {
    c.bench_function("resp parse plain", |b| {
        let sdata = "+baka for you\r\n".as_bytes();
        b.iter(|| Resp::parse(sdata).unwrap())
    });

    c.bench_function("resp parse bulk", |b| {
        let sdata = "$5\r\nojbK\n\r\n".as_bytes();
        b.iter(|| Resp::parse(sdata).unwrap())
    });

    c.bench_function("resp parse array", |b| {
        let sdata = "*2\r\n$1\r\na\r\n$5\r\nojbK\n\r\n".as_bytes();
        b.iter(|| Resp::parse(sdata).unwrap())
    });

    c.bench_function("resp parse array long tail", |b| {
        let mut sdata = b"*10000\r\n".to_vec();
        let ndata = b"$5\r\nojbK\n\r\n".repeat(5000);
        sdata.extend(ndata.clone());

        let mut buf = BytesMut::new();
        b.iter(|| {
            buf.extend_from_slice(&sdata);
            match Resp::parse(&buf) {
                Ok(_) => {}
                Err(_) => {}
            };
            buf.extend_from_slice(&ndata);
            Resp::parse(&buf).unwrap();
            buf.take();
        });
    });
}

fn bench_ketama(c: &mut Criterion) {
    c.bench_function("ketama get", |b| {
        let ring = HashRing::<Fnv1a64>::new(
            vec![
                "redis-1".to_owned(),
                "redis-2".to_owned(),
                "redis-3".to_owned(),
            ],
            vec![10, 10, 10],
        )
        .expect("create new hash ring success");
        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"val-a".to_vec()];

        b.iter(|| {
            for key in &*keys {
                let _ = ring.get_node(key);
            }
        })
    });

    c.bench_function("ketama add", |b| {
        let mut ring = HashRing::<Fnv1a64>::new(
            vec![
                "redis-1".to_owned(),
                "redis-2".to_owned(),
                "redis-3".to_owned(),
            ],
            vec![10, 10, 10],
        )
        .expect("create new hash ring success");

        b.iter(|| ring.add_node("redis-4".to_owned(), 10))
    });
}

fn bench_fsm_codec(c: &mut Criterion) {
    // fn bench_parse_plain(b:&mut Bencher) {
    c.bench_function("fsm resp parse plain", |b| {
        let sdata = b"+baka for you\r\n";
        let mut codec = RespFSMCodec::default();
        // assert_eq!(RESP_STRING, resp.rtype);
        // assert_eq!(Some(BytesMut::from(&b"baka for you"[..])), resp.data);
        b.iter(|| {
            let _ = codec
                .parse(&mut BytesMut::from(&sdata[..]))
                .unwrap()
                .unwrap();
        });
    });

    c.bench_function("fsm resp parse bulk", |b| {
        let sdata = "$5\r\nojbK\n\r\n".as_bytes();
        let mut codec = RespFSMCodec::default();
        // assert_eq!(RESP_STRING, resp.rtype);
        // assert_eq!(Some(BytesMut::from(&b"baka for you"[..])), resp.data);
        b.iter(|| {
            let _ = codec
                .parse(&mut BytesMut::from(&sdata[..]))
                .unwrap()
                .unwrap();
        });
    });

    c.bench_function("fsm resp parse array", |b| {
        let sdata = "*2\r\n$1\r\na\r\n$5\r\nojbK\n\r\n".as_bytes();
        let mut codec = RespFSMCodec::default();
        // assert_eq!(RESP_STRING, resp.rtype);
        // assert_eq!(Some(BytesMut::from(&b"baka for you"[..])), resp.data);
        b.iter(|| {
            let _ = codec
                .parse(&mut BytesMut::from(&sdata[..]))
                .unwrap()
                .unwrap();
        });
    });

    c.bench_function("fsm resp parse long tail array", |b| {
        let mut sdata = b"*10000\r\n".to_vec();
        let ndata = b"$5\r\nojbK\n\r\n".repeat(5000);
        sdata.extend(ndata.clone());

        let mut buf = BytesMut::new();
        let mut codec = RespFSMCodec::default();

        b.iter(|| {
            buf.extend_from_slice(&sdata);
            let _ = codec.parse(&mut buf).unwrap();
            buf.extend_from_slice(&ndata);
            let _ = codec.parse(&mut buf).unwrap().unwrap();
        })
    });
}

criterion_group!(benches, bench_ketama, bench_resp, bench_fsm_codec);
criterion_main!(benches);
