use futures::task::{self, Task};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct Notify {
    task: Task,
    count: Rc<AtomicUsize>,
}

impl Notify {
    pub fn new(task: Task) -> Self {
        Notify {
            task: task,
            count: Rc::new(AtomicUsize::new(0)),
        }
    }

    pub fn done_without_notify(&self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn done(&self) {
        let pv = self.count.fetch_sub(1, Ordering::Relaxed);
        if pv == 1 {
            self.task.notify();
        }
    }

    pub fn add(&self, val: usize) {
        self.count.fetch_add(val, Ordering::Relaxed);
    }

    pub fn reregister(&mut self) {
        self.task = task::current();
    }
}

impl Clone for Notify {
    fn clone(&self) -> Notify {
        self.count.fetch_add(1, Ordering::Relaxed);
        Notify {
            task: self.task.clone(),
            count: self.count.clone(),
        }
    }
}
