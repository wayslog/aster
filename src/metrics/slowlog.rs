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

const BUFF_SIZE: usize = 200usize;
const FLUSH_INTERVAL: u64 = 1; // seconds

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub cluster: String,
    pub cmd: String,
    pub start: String, 
    pub total_dur: u128,
    pub remote_dur: u128,
    pub subs: Option<Vec<String>>,
}

pub struct SlowlogHandle<I>
where
    I: Stream<Item = Entry, Error = ()>,
{
    input: I,

    file_path: String,
    file_size: u32,  // MB
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
        file_size: u32,
        file_bakckup: u8,
        input: I,
    ) -> SlowlogHandle<I> {
        let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path.clone())
                .unwrap();
        let bw = BufWriter::new(file);

        let delay = Delay::new(Instant::now());

        SlowlogHandle {
            file_path,
            file_size,
            file_bakckup,
            bw,
            buff_size: 0usize,
            input,
            delay,
        }
    }

    fn try_rotate(&mut self) {
        let file_size = match fs::metadata(self.file_path.clone()) {
            Ok(meta) => meta.len(),
            Err(_) => return,
        };

        if self.file_size <= 0 {
            return
        }

        if file_size < (self.file_size * 1024 * 1024) as u64 {
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
            
            self.bw = BufWriter::new(
                OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.file_path.clone())
                .unwrap()
            );
        }
    }

    fn try_recv(&mut self) {
        loop {
            match self.input.poll() {
                Ok(Async::Ready(entry)) => {
                    if let Ok(entry) = serde_json::to_string(&entry) {
                        let _ = self.bw.write(entry.as_bytes());
                        let _ = self.bw.write(b"\r\n");
                        self.buff_size += 1;
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
    file_size: u32,
    file_bakckup: u8,
    input: Receiver<Entry>,
) 
{
    thread_incr();
    current_thread::block_on_all(
        SlowlogHandle::new(file_path, file_size, file_bakckup, input).map_err(|_| error!("fail to init slowlog handle")),
    ).unwrap();
    
}