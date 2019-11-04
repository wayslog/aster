use failure::Error;

use std::time::Instant;
use redis::Client;

use redis::Commands;

use std::env;


fn measure_redis(addr: &str, max: usize) -> Result<(), Error> {
    let cli = Client::open(&*format!("redis://{}", addr))?;
    let mut conn = cli.get_connection()?;
    let start = Instant::now();
    for i in 0..max {
        let _: () = conn.set(i, i)?;
    }
    let mut sum = 0;
    for i in 0..max {
        let ival: i32 = conn.get(i)?;
        sum += ival;
    }
    let dur = start.elapsed();
    println!("measure redis {} with dur {:?} and sum is {}", addr, dur, sum);
    Ok(())
}

fn main() {
    let mut args = env::args();
    args.next().unwrap();
    let addr = args.next().expect("addr must exists");
    let count = args.next().unwrap_or("10".to_string());
    let count = count.parse::<usize>().unwrap_or(10);
    measure_redis(&addr, count).unwrap();
}
