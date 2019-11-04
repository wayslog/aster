use failure::Error;

use std::time::Instant;
use redis::Client;

use std::env;


fn measure_redis(addr: &str, max: usize) -> Result<(), Error> {
    let cli = Client::open(&*format!("redis://{}", addr))?;
    let mut conn = cli.get_connection()?;
    let start = Instant::now();
    let mut pipeline = redis::pipe();
    for i in 0..max {
        pipeline.cmd("SET").arg(i).arg(i).ignore()
            .cmd("GET").arg(i);
    }

    let gets: Vec<i32> = pipeline.query(&mut conn)?;
    let dur = start.elapsed();
    println!("measure redis {} with dur {:?} and result count {}", addr, dur, gets.len());
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