use std::fs::{self, File, OpenOptions};
use std::io::{Write, BufWriter};
use std::time::{Duration, Instant};

use super::thread_incr;

use serde::{Deserialize, Serialize};
use serde_json;
use tokio::timer::Delay;
use futures::{Async, Future, Stream};
use futures::sync::mpsc::Receiver;
use tokio::runtime::current_thread;

const BUFF_SIZE: usize = 512usize;
const FLUSH_INTERVAL: u64 = 2; // seconds

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub cluster: String,
    pub cmd: String,
    pub start: String, 
    pub total_dur: u128,
    pub remote_dur: u128,
    pub subs: Option<Vec<Entry>>,
}

pub struct SlowlogHandle<I>
where
    I: Stream<Item = Entry, Error = ()>,
{
    input: I,

    file_path: String,
    file_size: u64,  
    max_file_size: u64,
    file_bakckup: u8,

    bw: BufWriter<File>,
    buff_size: usize,

    delay: Delay,
}

impl<I> SlowlogHandle<I>
where
    I: Stream<Item = Entry, Error = ()>,
{
    pub fn new(
        file_path: String,
        max_file_size: u64,
        file_bakckup: u8,
        input: I,
    ) -> SlowlogHandle<I> {
        let (file_size, bw) = Self::open(file_path.clone());

        let delay = Delay::new(Instant::now());

        SlowlogHandle {
            file_path,
            file_size,
            max_file_size: max_file_size*1024*1024,
            file_bakckup,
            bw,
            buff_size: 0usize,
            input,
            delay,
        }
    }

    fn open(file_path: String) -> (u64, BufWriter<File>) {
        let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path)
                .expect("fail to open slowlog file");

        let file_size = file
                .metadata()
                .unwrap()
                .len();

        (file_size, BufWriter::new(file))
    }

    fn try_rotate(&mut self) {
        if self.max_file_size <= 0 {
            return
        }

        if self.file_size < self.max_file_size {
            return
        }

        if self.file_bakckup > 0 {
            (0..self.file_bakckup)
                .for_each(|n|{
                    let mut from = format!("{}.{}", self.file_path, self.file_bakckup-n-1);
                    let to = format!("{}.{}", self.file_path, self.file_bakckup-n);
                    if self.file_bakckup-n-1 == 0 {
                        from = self.file_path.clone();
                    }
                    let _ = fs::rename(from, to);
                });
            
             let (file_size, bw) = Self::open(self.file_path.clone());
             self.file_size = file_size;
             self.bw = bw;
        }
    }

    fn try_recv(&mut self) {
        loop {
            match self.input.poll() {
                Ok(Async::Ready(entry)) => {
                    if let Ok(mut entry) = serde_json::to_string(&entry) {
                        entry.push_str("\r\n");
                        if let Ok(size) = self.bw.write(entry.as_bytes()) {
                            self.buff_size += size;
                            self.file_size += size as u64;
                        }
                    }
                },
                Ok(Async::NotReady) => return,
                Err(_) => return,
            };
        }
    }

    fn try_flush(&mut self) {
        if self.buff_size > BUFF_SIZE || self.delay.is_elapsed() {
            match self.bw.flush() {
                Ok(_) => {
                    self.buff_size = 0;
                    self.delay.reset(Instant::now() + Duration::from_secs(FLUSH_INTERVAL));
                    self.try_rotate();
                },
                Err(err) => {
                    error!("fail to save slowlog: {}", err);
                },
            }
            return
        }
    }
}

impl<I> Future for SlowlogHandle<I>
where
    I: Stream<Item = Entry, Error = ()>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let _ = self.delay.poll();
        self.try_recv();
        if self.buff_size > 0 {
            self.try_flush();
        }
        return Ok(Async::NotReady)
    }
}

pub fn run(
    file_path: String,
    max_file_size: u64,
    file_bakckup: u8,
    input: Receiver<Entry>,
) 
{
    thread_incr();
    current_thread::block_on_all(
        SlowlogHandle::new(file_path, max_file_size, file_bakckup, input).map_err(|_| error!("fail to init slowlog handle")),
    ).unwrap();
    
}