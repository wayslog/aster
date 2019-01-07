#[macro_use]
extern crate criterion;

use libaster::proxy::fnv::Fnv1a64;
use libaster::proxy::ketama::HashRing;
use libaster::redis::resp::Resp;

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

criterion_group!(benches, bench_ketama, bench_resp);
criterion_main!(benches);
